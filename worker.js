// ═══════════════════════════════════════════════════════════════════
//  CALL LEDGER — Cloudflare Worker  (simplified, grouped-by-person)
//  GHL Webhook → GHL API (recording) → Whisper → Claude → Google Sheets
// ═══════════════════════════════════════════════════════════════════
//
//  Required secrets (Cloudflare > Worker > Settings > Variables & Secrets):
//    GHL_API_KEY, GHL_LOCATION_ID, ANTHROPIC_API_KEY, OPENAI_API_KEY,
//    GOOGLE_SERVICE_ACCOUNT_EMAIL, GOOGLE_PRIVATE_KEY, GOOGLE_SHEET_ID
//
//  Non-secret vars: LEDGER_SHEET_NAME ("Call Ledger"), FIRST_DATA_ROW ("5")
//
//  Sheet columns (Call Ledger tab), data starts at FIRST_DATA_ROW:
//    A: (your auto-number formula — untouched)
//    B: Date            C: Seller Name     D: Phone
//    E: Property Addr   F: City / State    G: Property Type
//    H: Beds            I: Baths           J: Sqft
//    K: Asking Price    L: Owed / Liens    M: Repairs Needed
//    N: Occupancy/Access O: Motivation     P: Key Dates
//    Q: Call Summary    R: Acq. Coaching   S: Next Steps
//    T: Callback Date   U: Recording URL (auto — de-dupe fingerprint)
//
//  ORGANIZED BY PERSON:
//    Every call is logged as its own row. A new call is inserted directly
//    below that seller's most recent existing row (matched by phone, then
//    name), so all of one person's calls stay grouped together. Brand-new
//    sellers are appended at the bottom.
//
//  CAPTURING PRIOR CALLS:
//    The worker collects EVERY recording on the contact (newest-first) and
//    picks the newest one NOT already in column U. Re-trigger the webhook to
//    back-fill an earlier call.
//
//  A concise summary of each call is also pushed back to the GHL contact's
//  Notes section.
// ═══════════════════════════════════════════════════════════════════

// ── Column layout — matches the EXISTING sheet headers ──
//  A: Call # (formula)   B: Date        C: Seller Name   D: Phone
//  E: Property Address   F: City/State  G: Asking Price  H: Property Type
//  I: Motivation Level   J: Call Summary (+ deal facts + coaching packed in)
//  K: Next Steps         L: Callback Date
//  M: Outcome (manual — untouched)   N: Recording URL (auto, de-dupe fingerprint)
const COL = {
  DATE: "B", NAME: "C", PHONE: "D", ADDRESS: "E", CITY_STATE: "F",
  PRICE: "G", PROP_TYPE: "H", MOTIVATION: "I", SUMMARY: "J",
  NEXT_STEPS: "K", CALLBACK: "L", REC_URL: "N",
};
const WRITE_FIRST = "B";   // first column of the main row write
const WRITE_LAST = "L";    // last column of the main row write (N written separately)

export default {
  async fetch(request, env, ctx) {
    if (request.method === "OPTIONS") {
      return new Response(null, { status: 204, headers: corsHeaders() });
    }
    if (request.method === "GET") {
      return json({ status: "ok", message: "Call Ledger worker is active", build: BUILD_VERSION });
    }
    if (request.method === "POST") {
      try {
        const payload = await request.json();
        console.log("Webhook received — queuing job:", JSON.stringify(payload).substring(0, 800));
        await env.CALL_QUEUE.send(payload);
        return json({ status: "queued", message: "Call processing queued" });
      } catch (err) {
        console.error("Worker error:", err.message, err.stack);
        return json({ status: "error", message: err.message }, 500);
      }
    }
    return json({ status: "error", message: "Method not allowed" }, 405);
  },

  async queue(batch, env) {
    for (const message of batch.messages) {
      try {
        const payload = message.body || {};
        const attempt = payload.__attempt || 1;
        const result = await processCall(payload, env, attempt);
        if (result && result.retry) {
          // Recording not ready yet — re-queue with a delay so GHL has time to
          // attach it. Self-managed (doesn't depend on queue max_retries config).
          await env.CALL_QUEUE.send({ ...payload, __attempt: attempt + 1 }, { delaySeconds: 60 });
          message.ack();
          console.log(`Recording not ready — re-queued for attempt ${attempt + 1} in 60s`);
        } else {
          message.ack();
          console.log("Queue message processed and acknowledged");
        }
      } catch (err) {
        console.error("Queue processing error:", err.message, err.stack);
        message.retry();
      }
    }
  },
};


// ═══════════════════════════════════════════════════════════════════
//  MAIN PIPELINE
// ═══════════════════════════════════════════════════════════════════

const BUILD_VERSION = "v7-existing-headers";
const MAX_ATTEMPTS = 5; // GHL attaches call recordings up to ~1 min after the call ends

async function processCall(payload, env, attempts = 1) {
  console.log(`>>> BUILD ${BUILD_VERSION} <<< (attempt ${attempts})`);
  const contactInfo = extractContactInfo(payload);
  console.log("Contact info:", JSON.stringify(contactInfo));

  if (!contactInfo.contactId) {
    console.log("No contact ID — writing basic info only");
    await writeToSheet(env, contactInfo, "", {});
    return { status: "partial", reason: "No contact ID for API lookup", seller: contactInfo.sellerName };
  }

  const ghlData = await fetchGHLCallData(contactInfo.contactId, env);
  const callData = mergeCallData(contactInfo, ghlData);
  console.log(`Found ${callData.allRecordingUrls.length} total recording(s) for this contact`);

  // Which recordings haven't been logged yet?
  let candidates = [];
  if (callData.allRecordingUrls.length > 0) {
    const processed = await getProcessedRecordingUrls(env);
    candidates = callData.allRecordingUrls.filter(
      u => !processed.has(normalizeRecordingUrl(u))
    );
    console.log(`${candidates.length} recording(s) not yet in the ledger`);
    if (candidates.length === 0) {
      console.log("All recordings already in the ledger — nothing to do");
      return { status: "skipped", reason: "All recordings already processed", seller: callData.sellerName };
    }
  }

  // Transcribe — try each candidate until one works.
  let transcript = "";
  let recDiag = "";
  let notReady = false;
  if (candidates.length > 0) {
    for (const cand of candidates) {
      console.log("Transcribing recording...");
      const r = await transcribeRecording(cand, env);
      callData.recordingUrl = cand;
      if (r.text) { transcript = r.text; recDiag = ""; notReady = false; break; }
      recDiag = r.diag;
      notReady = r.notReady;
      console.log("Transcript length: 0 | diag:", recDiag);
    }
    if (transcript) console.log("Transcript length:", transcript.length);
  } else {
    recDiag = "No call recording found yet in GHL for this contact.";
    notReady = true;
    console.log(recDiag);
  }

  // GHL attaches recordings up to ~1 min after a call ends. If it's not ready
  // yet, defer and let the queue re-deliver after a delay (don't write a blank row).
  if (!transcript && notReady && attempts < MAX_ATTEMPTS) {
    console.log(`Recording not ready (attempt ${attempts}/${MAX_ATTEMPTS}) — deferring for retry`);
    return { retry: true, reason: "recording-not-ready", seller: callData.sellerName };
  }

  // Extract with Claude — or, if we couldn't transcribe, surface WHY in the sheet.
  let aiExtracted = {};
  if (transcript) {
    console.log("Sending to Claude for extraction...");
    aiExtracted = await extractCallDetails(transcript, callData, env);
    console.log("AI extracted:", JSON.stringify(aiExtracted));
  } else if (recDiag) {
    aiExtracted = { call_summary: "⚠️ " + recDiag };
  }

  // Write to the ledger (grouped by person) + stamp recording URL
  await writeToSheet(env, callData, transcript, aiExtracted);

  // Push a concise summary note back to the GHL contact (only for real transcripts)
  if (transcript && aiExtracted.call_summary && callData.contactId) {
    await updateGHLContactNotes(callData.contactId, callData, aiExtracted, env);
  }

  return { status: "success", seller: callData.sellerName, hasTranscript: !!transcript, hasSummary: !!aiExtracted.call_summary };
}


// ═══════════════════════════════════════════════════════════════════
//  EXTRACT CONTACT INFO FROM WEBHOOK
// ═══════════════════════════════════════════════════════════════════

function extractContactInfo(payload) {
  const contact = payload.contact || payload.Contact || payload;

  const contactId = contact.id || contact.contactId || contact.contact_id ||
                    payload.contactId || payload.contact_id || "";

  const firstName = contact.firstName || contact.first_name || contact.name || "";
  const lastName = contact.lastName || contact.last_name || "";
  const sellerName = (firstName + " " + lastName).trim() || "Unknown Seller";
  const phone = formatPhone(contact.phone || contact.phoneNumber || contact.phone_number || "");

  const address1 = contact.address1 || contact.streetAddress || contact.street_address || "";
  const city = contact.city || "";
  const state = contact.state || "";
  const postalCode = contact.postal_code || contact.postalCode || "";
  const cityState = [city, state].filter(Boolean).join(", ") + (postalCode ? " " + postalCode : "");

  const callDate = payload.dateAdded || payload.date_added || payload.timestamp ||
                   contact.dateAdded || new Date().toISOString();

  return {
    contactId,
    sellerName,
    phone,
    callDate: new Date(callDate).toISOString(),
    propertyAddress: address1,
    cityState: cityState.trim(),
    askingPrice: "",
    propertyType: "",
    recordingUrl: "",
    allRecordingUrls: [],
  };
}


// ═══════════════════════════════════════════════════════════════════
//  GHL API — Fetch contact + ALL recordings (newest-first)
// ═══════════════════════════════════════════════════════════════════

const GHL_API_BASE = "https://services.leadconnectorhq.com";

// Matches audio/video recording URLs (mp4 included — GHL A2P calls are mp4).
function isRecordingUrl(url) {
  return /\.(mp3|mp4|m4a|wav|ogg|oga|webm|aac|mpe?g|mpga)(\?|$)/i.test(String(url || ""));
}

async function fetchGHLCallData(contactId, env) {
  const result = {
    sellerName: "", phone: "", propertyAddress: "", cityState: "",
    askingPrice: "", propertyType: "", recordingUrl: "", allRecordingUrls: [],
  };

  const headers = {
    "Authorization": "Bearer " + env.GHL_API_KEY,
    "Version": "2021-07-28",
  };

  // 1. Contact details
  try {
    const resp = await fetch(`${GHL_API_BASE}/contacts/${contactId}`, { headers });
    if (resp.ok) {
      const data = await resp.json();
      const c = data.contact || data;
      result.sellerName = ((c.firstName || "") + " " + (c.lastName || "")).trim();
      result.phone = formatPhone(c.phone || "");
      const cf = c.customFields || c.customField || [];
      result.propertyAddress = getCustomField(cf, ["property_address", "address", "Property Address"]);
      result.cityState = getCustomField(cf, ["city_state", "city", "City / State", "City State"]);
      result.askingPrice = getCustomField(cf, ["asking_price", "price", "Asking Price"]);
      result.propertyType = getCustomField(cf, ["property_type", "Property Type"]);
      console.log("GHL contact fetched:", result.sellerName);
    } else {
      console.error("GHL contact error:", resp.status, (await resp.text()).substring(0, 300));
    }
  } catch (err) {
    console.error("GHL contact fetch failed:", err.message);
  }

  // 2. Conversations → ALL recording URLs
  try {
    const convResp = await fetch(
      `${GHL_API_BASE}/conversations/search?contactId=${contactId}&locationId=${env.GHL_LOCATION_ID}`,
      { headers }
    );

    if (convResp.ok) {
      const convData = await convResp.json();
      const conversations = convData.conversations || [];
      const typesSeen = new Set(); // diagnostic: what message types GHL returned

      for (const conv of conversations) {
        let lastMessageId = null;
        const MAX_PAGES = 5;

        for (let page = 0; page < MAX_PAGES; page++) {
          let msgUrl = `${GHL_API_BASE}/conversations/${conv.id}/messages?limit=20`;
          if (lastMessageId) msgUrl += `&lastMessageId=${lastMessageId}`;

          const msgResp = await fetch(msgUrl, { headers });
          if (!msgResp.ok) {
            console.error("GHL messages error:", msgResp.status, (await msgResp.text()).substring(0, 300));
            break;
          }

          const msgData = await msgResp.json();
          if (page === 0) console.log("GHL messages raw (first 600):", JSON.stringify(msgData).substring(0, 600));

          const msgContainer = msgData.messages || msgData;
          let messages = [];
          if (msgContainer && Array.isArray(msgContainer.messages)) messages = msgContainer.messages;
          else if (Array.isArray(msgData.messages)) messages = msgData.messages;
          else if (Array.isArray(msgData.data)) messages = msgData.data;
          else if (Array.isArray(msgData)) messages = msgData;

          console.log(`Page ${page + 1}: ${messages.length} messages`);
          if (messages.length === 0) break;

          for (const msg of messages) {
            if (!msg || typeof msg !== "object") continue;

            // Is this a call message? GHL uses types like TYPE_CALL / "call".
            const msgType = (msg.type || msg.messageType || msg.type_ || "").toString().toUpperCase();
            typesSeen.add(msgType || "(none)");
            const isCall = msgType.includes("CALL") ||
                           msg.callDuration != null || msg.callStatus != null ||
                           !!(msg.meta && msg.meta.call);

            let foundUrl = "";

            // 1) Public attachment URL (accept any recording extension, or ANY
            //    attachment on a call message — GHL signed URLs may lack an ext).
            const attachments = msg.attachments || [];
            for (const att of attachments) {
              const url = typeof att === "string" ? att : (att.url || att.href || att.link || att.fileUrl || "");
              if (!url) continue;
              if (isRecordingUrl(url) || isCall) { foundUrl = url; break; }
            }

            // 2) Dedicated recording fields on the message.
            if (!foundUrl) {
              foundUrl = msg.recordingUrl || msg.recording_url ||
                         msg.meta?.recordingUrl || msg.meta?.recording_url ||
                         msg.meta?.call?.recordingUrl ||
                         (msg.call && msg.call.recordingUrl) ||
                         msg.mediaUrl || msg.media_url || "";
            }

            // 3) GHL A2P calls: the recording is NOT in the message body — it's
            //    behind an authenticated endpoint keyed by the call message ID.
            //    Build that URL; transcribeRecording() downloads it with the API key.
            if (!foundUrl && isCall && msg.id) {
              foundUrl = `${GHL_API_BASE}/conversations/messages/${msg.id}/locations/${env.GHL_LOCATION_ID}/recording`;
              console.log(`Call message ${msg.id} — using recording endpoint`);
            }

            if (foundUrl) {
              const norm = normalizeRecordingUrl(foundUrl);
              const have = result.allRecordingUrls.some(u => normalizeRecordingUrl(u) === norm);
              if (!have) {
                result.allRecordingUrls.push(foundUrl);
                console.log(`Found recording #${result.allRecordingUrls.length} (page ${page + 1}):`, foundUrl.substring(0, 100));
              }
            }
          }

          const hasNextPage = msgContainer.nextPage === true;
          lastMessageId = msgContainer.lastMessageId || messages[messages.length - 1]?.id;
          if (!hasNextPage || !lastMessageId) break;
        }
      }
      console.log("Message types seen:", [...typesSeen].join(", ") || "(none)");
    } else {
      console.error("GHL conversation search error:", convResp.status);
    }

    result.recordingUrl = result.allRecordingUrls[0] || "";
    if (!result.recordingUrl) console.log("No recording URL found in GHL conversations");
  } catch (err) {
    console.error("GHL conversation search failed:", err.message);
  }

  return result;
}

function getCustomField(customFields, possibleKeys) {
  if (Array.isArray(customFields)) {
    for (const field of customFields) {
      const fKey = (field.id || field.key || field.name || field.fieldKey || "").toLowerCase();
      const fName = (field.name || field.fieldName || "").toLowerCase();
      for (const key of possibleKeys) {
        if (fKey === key.toLowerCase() || fName === key.toLowerCase()) {
          return field.value || field.fieldValue || "";
        }
      }
    }
  } else if (typeof customFields === "object" && customFields !== null) {
    for (const key of possibleKeys) {
      for (const k of Object.keys(customFields)) {
        if (k.toLowerCase() === key.toLowerCase()) return customFields[k];
      }
    }
  }
  return "";
}

function mergeCallData(webhookData, apiData) {
  return {
    contactId: webhookData.contactId,
    sellerName: apiData.sellerName || webhookData.sellerName,
    phone: apiData.phone || webhookData.phone,
    callDate: webhookData.callDate,
    propertyAddress: apiData.propertyAddress || webhookData.propertyAddress,
    cityState: apiData.cityState || webhookData.cityState,
    askingPrice: apiData.askingPrice || webhookData.askingPrice,
    propertyType: apiData.propertyType || webhookData.propertyType,
    recordingUrl: apiData.recordingUrl || webhookData.recordingUrl,
    allRecordingUrls: apiData.allRecordingUrls || [],
  };
}


// ═══════════════════════════════════════════════════════════════════
//  RECORDING URL HELPERS
// ═══════════════════════════════════════════════════════════════════

function normalizeRecordingUrl(url) {
  if (!url) return "";
  return String(url).split("?")[0].trim();
}

async function getProcessedRecordingUrls(env) {
  try {
    const accessToken = await getGoogleAccessToken(env);
    if (!accessToken) return new Set();

    const sheetId = env.GOOGLE_SHEET_ID;
    const sheetName = env.LEDGER_SHEET_NAME || "Call Ledger";
    const firstDataRow = parseInt(env.FIRST_DATA_ROW || "5");

    const resp = await fetch(
      `https://sheets.googleapis.com/v4/spreadsheets/${sheetId}/values/${encodeURIComponent(sheetName)}!${COL.REC_URL}${firstDataRow}:${COL.REC_URL}2000`,
      { headers: { "Authorization": "Bearer " + accessToken } }
    );
    if (!resp.ok) {
      console.error("Failed to read processed URLs:", resp.status);
      return new Set();
    }
    const data = await resp.json();
    const urls = (data.values || [])
      .map(row => (row && row[0]) || "")
      .filter(Boolean)
      .map(normalizeRecordingUrl);
    console.log(`Loaded ${urls.length} previously-processed recording URL(s)`);
    return new Set(urls);
  } catch (err) {
    console.error("getProcessedRecordingUrls failed:", err.message);
    return new Set();
  }
}


// ═══════════════════════════════════════════════════════════════════
//  TRANSCRIPTION — OpenAI Whisper
// ═══════════════════════════════════════════════════════════════════

// Returns { text, diag }. `diag` is a short human-readable failure reason that
// gets surfaced in the sheet, so you never have to dig through logs to see why
// a call didn't transcribe.
async function transcribeRecording(recordingUrl, env) {
  try {
    // GHL's recording endpoint requires the API key; public signed URLs don't.
    const isGhlApi = recordingUrl.startsWith(GHL_API_BASE);
    const dlOpts = isGhlApi
      ? { headers: { "Authorization": "Bearer " + env.GHL_API_KEY, "Version": "2021-07-28" } }
      : undefined;

    let audioResp = await fetch(recordingUrl, dlOpts);

    // Fallback: some accounts expose the recording without the /locations/{id} segment.
    if (isGhlApi && audioResp.status === 404) {
      const alt = recordingUrl.replace(/\/locations\/[^/]+\/recording$/, "/recording");
      if (alt !== recordingUrl) {
        console.log("Recording 404 — trying fallback path:", alt);
        audioResp = await fetch(alt, dlOpts);
      }
    }

    if (!audioResp.ok) {
      const body = (await audioResp.text().catch(() => "")).substring(0, 160);
      const diag = `Recording download failed: HTTP ${audioResp.status}${body ? " — " + body : ""}`;
      console.error(diag);
      const s = audioResp.status;
      // 422 = "message does not have recording yet"; 404/408/429/5xx = transient.
      const notReady = s === 422 || s === 404 || s === 408 || s === 425 || s === 429 || s >= 500;
      return { text: "", diag, notReady };
    }

    // The endpoint may return the audio directly, OR a JSON wrapper with a link.
    let contentType = (audioResp.headers.get("content-type") || "").toLowerCase();
    let audioBuffer;

    if (contentType.includes("application/json")) {
      const j = await audioResp.json().catch(() => null);
      const link = j && (j.url || j.recordingUrl || j.signedUrl || j.link ||
                         (j.data && (j.data.url || j.data.recordingUrl)));
      if (!link) {
        const diag = `Recording endpoint returned JSON without a URL: ${JSON.stringify(j).substring(0, 160)}`;
        console.error(diag);
        return { text: "", diag, notReady: false };
      }
      const linkResp = await fetch(link);
      if (!linkResp.ok) {
        const diag = `Recording link fetch failed: HTTP ${linkResp.status}`;
        console.error(diag);
        return { text: "", diag, notReady: true };
      }
      contentType = (linkResp.headers.get("content-type") || "audio/mpeg").toLowerCase();
      audioBuffer = await linkResp.arrayBuffer();
    } else {
      audioBuffer = await audioResp.arrayBuffer();
    }

    const sizeMB = audioBuffer.byteLength / (1024 * 1024);
    console.log(`Recording size: ${sizeMB.toFixed(2)}MB, type: ${contentType || "unknown"}`);

    if (audioBuffer.byteLength < 1024) {
      const diag = `Recording was empty (${audioBuffer.byteLength} bytes) — not ready yet.`;
      console.error(diag);
      return { text: "", diag, notReady: true };
    }
    if (sizeMB > 25) {
      const diag = `Recording too large for Whisper (${sizeMB.toFixed(1)}MB > 25MB limit).`;
      console.error(diag);
      return { text: "", diag, notReady: false };
    }

    let ext = "mp3";
    if (contentType.includes("mp4")) ext = "mp4";
    else if (contentType.includes("m4a") || contentType.includes("x-m4a")) ext = "m4a";
    else if (contentType.includes("wav")) ext = "wav";
    else if (contentType.includes("ogg")) ext = "ogg";
    else if (contentType.includes("webm")) ext = "webm";
    else if (/\.(mp4|m4a|wav|ogg|webm)/i.test(recordingUrl)) {
      ext = recordingUrl.match(/\.(mp4|m4a|wav|ogg|webm)/i)[1].toLowerCase();
    }

    const formData = new FormData();
    formData.append("file", new Blob([audioBuffer], { type: contentType || "audio/mpeg" }), `recording.${ext}`);
    formData.append("model", "whisper-1");
    formData.append("response_format", "text");

    const resp = await fetch("https://api.openai.com/v1/audio/transcriptions", {
      method: "POST",
      headers: { "Authorization": "Bearer " + env.OPENAI_API_KEY },
      body: formData,
    });

    if (resp.ok) {
      const transcript = await resp.text();
      console.log("Whisper done:", transcript.substring(0, 150) + "...");
      return { text: transcript, diag: "", notReady: false };
    }
    const err = (await resp.text()).substring(0, 200);
    const diag = `Whisper transcription error: ${err}`;
    console.error(diag);
    return { text: "", diag, notReady: false };
  } catch (err) {
    const diag = `Transcription exception: ${err.message}`;
    console.error(diag);
    return { text: "", diag, notReady: true };
  }
}


// ═══════════════════════════════════════════════════════════════════
//  AI EXTRACTION — Claude (lean: deal data + concise coaching)
// ═══════════════════════════════════════════════════════════════════

async function extractCallDetails(transcript, callData, env) {
  try {
    const prompt = `You are an acquisitions analyst for Summit Group Acquisitions, a real estate wholesaling company, reviewing a phone call transcript with a property seller.

Pull out the concrete deal facts the acquisitions AND dispositions teams need, plus a short, practical coaching note. Be specific and factual. Quote the seller where useful. If something was NOT mentioned in the call, use an empty string "" — never guess.

For context, Summit's initial (Stage 1) call should cover: motivation for selling, plans after selling, house details (beds/baths/sqft/condition), timeline, any liens/mortgage owed, asking price and whether it's negotiable, and asking for photos.

Return ONLY a valid JSON object with EXACTLY these keys:
{
  "property_address": "street address if stated, else empty string",
  "city_state": "city and state/zip if stated, else empty string",
  "property_type": "SFR | Multi-Family | Condo/Townhouse | Mobile Home | Land | Commercial | Other, else empty string",
  "beds": "number of bedrooms as a number, else empty string",
  "baths": "number of bathrooms as a number, else empty string",
  "sqft": "square footage, numbers only, else empty string",
  "asking_price": "NUMBERS ONLY, no $ or commas (e.g. 250000). Convert words: 'a million'=1000000. Else empty string",
  "amount_owed": "What is owed / any liens or payoff. Include loan balance, back taxes, HOA, second mortgages, or 'Free and clear' if stated. Else empty string",
  "repairs_needed": "Condition and repairs mentioned: roof, HVAC, foundation, cosmetic, age of systems, known issues. Else empty string",
  "occupancy_access": "Is it occupied, vacant, or tenant-occupied? How would we access it to show buyers (lockbox, owner present, tenant coordination, key location)? Else empty string",
  "motivation": "Why they're selling + short read on urgency. Start with a 1-10 score then a one-line reason, e.g. '8/10 — behind on payments, relocating in 30 days'. Else empty string",
  "key_dates": "Any dates that matter: desired close date, move-out, foreclosure/auction date, when they bought, callback time. Else empty string",
  "call_summary": "2-3 sentence plain summary: what was discussed, what the seller wants, key takeaway.",
  "acquisitions_coaching": "3-4 sentences MAX, practical and specific to THIS seller: (1) quick read on the seller, (2) recommended acquisition strategy — Cash / Subject-To / Seller Finance / Novation — with a one-line why, (3) the single most important thing to do or ask on the next call. No generic advice.",
  "next_steps": "Concrete follow-up action items from this call.",
  "callback_date": "MM/DD/YYYY if a specific callback was set, else empty string"
}

Known seller info:
- Name: ${callData.sellerName}
- Phone: ${callData.phone}
- Known property address: ${callData.propertyAddress || "Unknown"}

TRANSCRIPT:
${transcript}`;

    const resp = await fetch("https://api.anthropic.com/v1/messages", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-api-key": env.ANTHROPIC_API_KEY,
        "anthropic-version": "2023-06-01",
      },
      body: JSON.stringify({
        model: env.ANTHROPIC_MODEL || "claude-sonnet-5",
        max_tokens: 2048,
        messages: [{ role: "user", content: prompt }],
      }),
    });

    if (resp.ok) {
      const result = await resp.json();
      const blocks = Array.isArray(result.content) ? result.content : [];
      const text = blocks.map(b => (b && typeof b.text === "string" ? b.text : "")).join("").trim();
      if (!text) {
        console.error("Claude returned no text block. Raw:", JSON.stringify(result).substring(0, 300));
        return {};
      }
      const jsonMatch = text.match(/\{[\s\S]*\}/);
      if (jsonMatch) return JSON.parse(jsonMatch[0]);
      console.error("Claude response had no JSON object:", text.substring(0, 200));
    } else {
      console.error("Claude error:", (await resp.text()).substring(0, 300));
    }
    return {};
  } catch (err) {
    console.error("AI extraction failed:", err.message);
    return {};
  }
}


// ═══════════════════════════════════════════════════════════════════
//  GOOGLE SHEETS — Write, grouped by person
// ═══════════════════════════════════════════════════════════════════

async function writeToSheet(env, callData, transcript, aiExtracted) {
  const sheetName = env.LEDGER_SHEET_NAME || "Call Ledger";
  const firstDataRow = parseInt(env.FIRST_DATA_ROW || "5");

  const accessToken = await getGoogleAccessToken(env);
  if (!accessToken) {
    console.error("Failed to get Google access token");
    return;
  }

  const sheetId = env.GOOGLE_SHEET_ID;
  const baseUrl = `https://sheets.googleapis.com/v4/spreadsheets/${sheetId}`;

  // Read existing Name + Phone columns to figure out where this call belongs.
  const scanResp = await fetch(
    `${baseUrl}/values/${encodeURIComponent(sheetName)}!${COL.NAME}${firstDataRow}:${COL.PHONE}2000`,
    { headers: { "Authorization": "Bearer " + accessToken } }
  );
  const scanRows = scanResp.ok ? ((await scanResp.json()).values || []) : [];

  // Number of existing data rows (stop at first fully-empty row).
  let existingCount = 0;
  for (let i = 0; i < scanRows.length; i++) {
    const name = (scanRows[i] && scanRows[i][0]) || "";
    const phone = (scanRows[i] && scanRows[i][1]) || "";
    if (!name && !phone) break;
    existingCount = i + 1;
  }
  const appendRow = firstDataRow + existingCount;

  // Find this seller's rows — match by phone digits first, then name.
  const targetDigits = digitsOnly(callData.phone);
  const targetName = (callData.sellerName || "").trim().toLowerCase();
  let lastMatchRow = -1;
  for (let i = 0; i < existingCount; i++) {
    const rowName = ((scanRows[i] && scanRows[i][0]) || "").trim().toLowerCase();
    const rowDigits = digitsOnly((scanRows[i] && scanRows[i][1]) || "");
    const phoneMatch = targetDigits && rowDigits && targetDigits === rowDigits;
    const nameMatch = targetName && targetName !== "unknown seller" && rowName === targetName;
    if (phoneMatch || nameMatch) lastMatchRow = firstDataRow + i;
  }

  // Insert directly below the seller's last existing row; else append.
  let targetRow;
  if (lastMatchRow !== -1 && lastMatchRow + 1 < appendRow) {
    targetRow = lastMatchRow + 1;
    const numericSheetId = await getSheetIdByName(baseUrl, accessToken, sheetName);
    if (numericSheetId !== null) {
      const inserted = await insertBlankRow(baseUrl, accessToken, numericSheetId, targetRow);
      if (!inserted) targetRow = appendRow; // fall back to append if insert failed
    } else {
      targetRow = appendRow;
    }
  } else {
    targetRow = appendRow;
  }

  // Build the row values (B..L) to match the existing sheet headers.
  const address = aiExtracted.property_address || callData.propertyAddress || "";
  const cityState = aiExtracted.city_state || callData.cityState || "";
  const propType = aiExtracted.property_type || callData.propertyType || "";
  const price = parseAskingPrice(aiExtracted.asking_price || callData.askingPrice || "");

  const callDate = new Date(callData.callDate);
  const dateStr = `${pad(callDate.getMonth() + 1)}/${pad(callDate.getDate())}/${callDate.getFullYear()}`;

  // Pack the summary + deal facts + coaching into the single Call Summary cell (J).
  const parts = [];
  if (aiExtracted.call_summary) parts.push(aiExtracted.call_summary);
  const facts = [];
  if (aiExtracted.beds || aiExtracted.baths || aiExtracted.sqft) {
    facts.push(`Beds/Baths/Sqft: ${aiExtracted.beds || "?"}/${aiExtracted.baths || "?"}/${aiExtracted.sqft || "?"}`);
  }
  if (aiExtracted.amount_owed) facts.push(`Owed/Liens: ${aiExtracted.amount_owed}`);
  if (aiExtracted.repairs_needed) facts.push(`Repairs: ${aiExtracted.repairs_needed}`);
  if (aiExtracted.occupancy_access) facts.push(`Occupancy/Access: ${aiExtracted.occupancy_access}`);
  if (aiExtracted.key_dates) facts.push(`Key Dates: ${aiExtracted.key_dates}`);
  if (facts.length) parts.push(facts.join("\n"));
  if (aiExtracted.acquisitions_coaching) parts.push(`Coaching: ${aiExtracted.acquisitions_coaching}`);
  const summaryCell = parts.join("\n\n");

  const rowValues = [
    dateStr,                          // B Date
    callData.sellerName,              // C Seller Name
    callData.phone,                   // D Phone Number
    address,                          // E Property Address
    cityState,                        // F City / State
    price,                            // G Asking Price
    propType,                         // H Property Type
    aiExtracted.motivation || "",     // I Motivation Level
    summaryCell,                      // J Call Summary (+ deal facts + coaching)
    aiExtracted.next_steps || "",     // K Next Steps / Action Items
    aiExtracted.callback_date || "",  // L Callback Date
  ];

  const writeRange = `${sheetName}!${WRITE_FIRST}${targetRow}:${WRITE_LAST}${targetRow}`;
  const writeResp = await fetch(
    `${baseUrl}/values/${encodeURIComponent(writeRange)}?valueInputOption=USER_ENTERED`,
    {
      method: "PUT",
      headers: { "Authorization": "Bearer " + accessToken, "Content-Type": "application/json" },
      body: JSON.stringify({ values: [rowValues] }),
    }
  );
  if (writeResp.ok) console.log(`Row ${targetRow} written for: ${callData.sellerName}`);
  else console.error("Sheets write error:", await writeResp.text());

  // Stamp the recording URL fingerprint into column U — only once we actually
  // transcribed it, so a not-yet-ready recording can be re-captured on retry.
  if (callData.recordingUrl && transcript) {
    const urlResp = await fetch(
      `${baseUrl}/values/${encodeURIComponent(`${sheetName}!${COL.REC_URL}${targetRow}`)}?valueInputOption=USER_ENTERED`,
      {
        method: "PUT",
        headers: { "Authorization": "Bearer " + accessToken, "Content-Type": "application/json" },
        body: JSON.stringify({ values: [[normalizeRecordingUrl(callData.recordingUrl)]] }),
      }
    );
    if (!urlResp.ok) console.error("Recording URL write error:", await urlResp.text());
  }
}

// Look up the numeric sheetId (gid) for a tab by title.
async function getSheetIdByName(baseUrl, accessToken, title) {
  try {
    const resp = await fetch(baseUrl, { headers: { "Authorization": "Bearer " + accessToken } });
    if (!resp.ok) return null;
    const spreadsheet = await resp.json();
    const sheet = (spreadsheet.sheets || []).find(s => s.properties.title === title);
    return sheet ? sheet.properties.sheetId : null;
  } catch (err) {
    console.error("getSheetIdByName failed:", err.message);
    return null;
  }
}

// Insert one blank row above `rowNumber` (1-based), inheriting formatting/formula
// from the row above (keeps your column-A auto-number formula intact).
async function insertBlankRow(baseUrl, accessToken, numericSheetId, rowNumber) {
  try {
    const resp = await fetch(`${baseUrl}:batchUpdate`, {
      method: "POST",
      headers: { "Authorization": "Bearer " + accessToken, "Content-Type": "application/json" },
      body: JSON.stringify({
        requests: [{
          insertDimension: {
            range: {
              sheetId: numericSheetId,
              dimension: "ROWS",
              startIndex: rowNumber - 1, // zero-based; inserts above this row
              endIndex: rowNumber,
            },
            inheritFromBefore: true,
          },
        }],
      }),
    });
    if (resp.ok) return true;
    console.error("insertBlankRow error:", (await resp.text()).substring(0, 300));
    return false;
  } catch (err) {
    console.error("insertBlankRow failed:", err.message);
    return false;
  }
}


// ═══════════════════════════════════════════════════════════════════
//  GOOGLE AUTH — JWT → Access Token
// ═══════════════════════════════════════════════════════════════════

async function getGoogleAccessToken(env) {
  try {
    const now = Math.floor(Date.now() / 1000);
    const header = { alg: "RS256", typ: "JWT" };
    const claimSet = {
      iss: env.GOOGLE_SERVICE_ACCOUNT_EMAIL,
      scope: "https://www.googleapis.com/auth/spreadsheets",
      aud: "https://oauth2.googleapis.com/token",
      iat: now,
      exp: now + 3600,
    };
    const signInput = `${base64url(JSON.stringify(header))}.${base64url(JSON.stringify(claimSet))}`;
    const privateKey = await importPrivateKey(env.GOOGLE_PRIVATE_KEY);
    const signature = await crypto.subtle.sign(
      { name: "RSASSA-PKCS1-v1_5" }, privateKey, new TextEncoder().encode(signInput)
    );
    const jwt = `${signInput}.${base64url(signature)}`;

    const tokenResp = await fetch("https://oauth2.googleapis.com/token", {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body: `grant_type=urn:ietf:params:oauth:grant-type:jwt-bearer&assertion=${jwt}`,
    });
    if (tokenResp.ok) return (await tokenResp.json()).access_token;
    console.error("Google token error:", await tokenResp.text());
    return null;
  } catch (err) {
    console.error("Google auth failed:", err.message);
    return null;
  }
}

async function importPrivateKey(pemKey) {
  const pem = pemKey.replace(/\\n/g, "\n");
  const pemContents = pem
    .replace("-----BEGIN PRIVATE KEY-----", "")
    .replace("-----END PRIVATE KEY-----", "")
    .replace(/\s/g, "");
  const binaryKey = Uint8Array.from(atob(pemContents), c => c.charCodeAt(0));
  return crypto.subtle.importKey(
    "pkcs8", binaryKey,
    { name: "RSASSA-PKCS1-v1_5", hash: "SHA-256" },
    false, ["sign"]
  );
}

function base64url(input) {
  const str = typeof input === "string"
    ? btoa(input)
    : btoa(String.fromCharCode(...new Uint8Array(input)));
  return str.replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}


// ═══════════════════════════════════════════════════════════════════
//  GHL — Push a concise summary note back to the contact's Notes
// ═══════════════════════════════════════════════════════════════════

async function updateGHLContactNotes(contactId, callData, aiExtracted, env) {
  try {
    const headers = {
      "Authorization": "Bearer " + env.GHL_API_KEY,
      "Version": "2021-07-28",
      "Content-Type": "application/json",
    };
    const callDate = new Date(callData.callDate);
    const dateStr = callDate.toLocaleDateString("en-US", { month: "2-digit", day: "2-digit", year: "numeric" });

    const noteBody = [
      `CALL — ${dateStr}`,
      ``,
      `Summary: ${aiExtracted.call_summary || "N/A"}`,
      aiExtracted.motivation ? `Motivation: ${aiExtracted.motivation}` : "",
      aiExtracted.asking_price ? `Asking: ${aiExtracted.asking_price}` : "",
      aiExtracted.amount_owed ? `Owed/Liens: ${aiExtracted.amount_owed}` : "",
      aiExtracted.repairs_needed ? `Repairs: ${aiExtracted.repairs_needed}` : "",
      aiExtracted.occupancy_access ? `Occupancy/Access: ${aiExtracted.occupancy_access}` : "",
      aiExtracted.key_dates ? `Key Dates: ${aiExtracted.key_dates}` : "",
      ``,
      aiExtracted.acquisitions_coaching ? `Coaching: ${aiExtracted.acquisitions_coaching}` : "",
      ``,
      `Next Steps: ${aiExtracted.next_steps || "N/A"}`,
      aiExtracted.callback_date ? `Callback: ${aiExtracted.callback_date}` : "",
    ].filter(line => line !== "").join("\n");

    const noteResp = await fetch(`${GHL_API_BASE}/contacts/${contactId}/notes`, {
      method: "POST",
      headers,
      body: JSON.stringify({ body: noteBody }),
    });
    if (noteResp.ok) console.log("GHL note created for:", callData.sellerName);
    else console.error("GHL note error:", noteResp.status, (await noteResp.text()).substring(0, 300));
  } catch (err) {
    console.error("GHL note creation failed:", err.message);
  }
}


// ═══════════════════════════════════════════════════════════════════
//  HELPERS
// ═══════════════════════════════════════════════════════════════════

function digitsOnly(s) {
  const d = String(s || "").replace(/\D/g, "");
  return d.length === 11 && d.startsWith("1") ? d.substring(1) : d;
}

function pad(n) { return n.toString().padStart(2, "0"); }

function formatPhone(raw) {
  if (!raw) return "";
  let digits = raw.replace(/\D/g, "");
  if (digits.length === 11 && digits.startsWith("1")) digits = digits.substring(1);
  if (digits.length === 10) {
    return `(${digits.substring(0, 3)}) ${digits.substring(3, 6)}-${digits.substring(6)}`;
  }
  return raw;
}

function parseAskingPrice(raw) {
  if (!raw) return "";
  const str = String(raw).trim();
  const direct = parseFloat(str.replace(/[$,\s]/g, ""));
  if (!isNaN(direct) && direct > 0) return direct;

  const lower = str.toLowerCase();
  const multipliers = {
    "million": 1000000, "mil": 1000000, "m": 1000000,
    "thousand": 1000, "k": 1000, "hundred thousand": 100000,
  };
  for (const [word, mult] of Object.entries(multipliers)) {
    const match = lower.match(new RegExp(`([\\d.]+)?\\s*${word}`, "i"));
    if (match) return (match[1] ? parseFloat(match[1]) : 1) * mult;
  }
  if (lower.includes("million")) return 1000000;
  if (lower.includes("hundred thousand")) return 100000;
  return str;
}

function json(data, status = 200) {
  return new Response(JSON.stringify(data, null, 2), {
    status,
    headers: { "Content-Type": "application/json", ...corsHeaders() },
  });
}

function corsHeaders() {
  return {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type",
  };
}
