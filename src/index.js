/* =========================================================
   UTILITIES & HELPERS
   ========================================================= */

function json(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "Content-Type": "application/json" }
  });
}

function clamp(n) {
  if (typeof n !== "number" || isNaN(n)) return 0;
  return Math.max(0, Math.min(1, n));
}

function buildKey({ scope, operator_id, mission_id, fleet_id, doc_type, file }) {
  const id =
    scope === "operator"
      ? operator_id
      : scope === "mission"
      ? mission_id
      : scope === "fleet"
      ? fleet_id
      : scope === "autofleet"
      ? "autofleet"
      : null;

  if (!id) throw new Error("MISSING_SCOPE_IDENTIFIER");
  return `${scope}/${id}/${doc_type}/${crypto.randomUUID()}-${file.name}`;
}

async function putToR2(scope, key, file, env) {
  if (scope === "mission") return env.MISSION_DOCS.put(key, file);
  if (scope === "operator") return env.OPERATOR_DOCS.put(key, file);
  if (scope === "fleet") return env.FLEET_DOCS.put(key, file);
  if (scope === "autofleet") return env.AUTOFLEET_DOCS.put(key, file);
  throw new Error("INVALID_SCOPE");
}

function computeRisk({ validation_score, expiry_date, fraud_signal, completeness_score }) {
  const now = Date.now();
  let expiryRisk = 0;
  if (expiry_date && expiry_date < now) expiryRisk = 1;
  else if (expiry_date && expiry_date - now < 1000 * 60 * 60 * 24 * 7) expiryRisk = 0.5;

  return (
    (1 - validation_score) * 0.4 +
    expiryRisk * 0.3 +
    (1 - completeness_score) * 0.2 +
    fraud_signal * 0.1
  );
}

/* =========================================================
   EVENT EMITTER & TELEMETRY
   ========================================================= */

async function emitEvent(type, payload, env) {
  try {
    await env["icc-mission-engine"].fetch("https://mission-engine/event", {
      method: "POST",
      body: JSON.stringify({
        event: type,
        version: "telemetry_v1",
        payload
      })
    });
  } catch (e) {
    console.error(`Telemetry error: ${type}`, e);
  }
}

async function runSecurityTelemetry(context) {
  const {
    mission_id,
    operator_id,
    doc_type,
    detected_doc_type,
    validation_score,
    expiry_date,
    fraud_signal,
    ai_confidence,
    risk,
    fileHash,
    now,
    env
  } = context;

  // 1. DUPLICATE DOCUMENT DETECTION
  const duplicate = await env.schema.prepare(`
    SELECT mission_id, doc_type FROM mission_documents WHERE file_hash = ? LIMIT 1
  `).bind(fileHash).first();

  if (duplicate) {
    await emitEvent("DOCUMENT_REUSE_DETECTED", { mission_id, original_mission: duplicate.mission_id, doc_type }, env);
  }

  // 2. CROSS-SCOPE DOCUMENT REUSE
  const operatorDuplicate = await env.schema.prepare(`
    SELECT operator_id FROM operator_documents WHERE file_hash = ? LIMIT 1
  `).bind(fileHash).first();

  if (operatorDuplicate) {
    await emitEvent("CROSS_SCOPE_DOCUMENT_REUSE", { mission_id, operator_id: operatorDuplicate.operator_id, doc_type }, env);
  }

  // 3. AI LOW CONFIDENCE
  if (ai_confidence && ai_confidence < 0.4) {
    await emitEvent("AI_LOW_CONFIDENCE", { mission_id, doc_type, ai_confidence }, env);
  }

  // 4. DOCUMENT TYPE MISMATCH
  if (detected_doc_type && detected_doc_type !== doc_type) {
    await emitEvent("DOCUMENT_TYPE_MISMATCH", { mission_id, declared: doc_type, detected: detected_doc_type }, env);
  }

  // 5. MISSION UPLOAD SPIKE
  if (mission_id) {
    const uploads = await env.schema.prepare(`
      SELECT COUNT(*) as c FROM mission_documents WHERE mission_id = ? AND created_at > ?
    `).bind(mission_id, now - 60000).first();
    if (uploads && uploads.c > 5) await emitEvent("UPLOAD_SPIKE_DETECTED", { mission_id, count: uploads.c }, env);
  }

  // 6. GLOBAL UPLOAD SPIKE
  const globalUploads = await env.schema.prepare(`
    SELECT COUNT(*) as c FROM mission_documents WHERE created_at > ?
  `).bind(now - 60000).first();
  if (globalUploads && globalUploads.c > 50) await emitEvent("GLOBAL_UPLOAD_SPIKE", { count: globalUploads.c }, env);

  // 7. EXPIRED UPLOAD
  if (expiry_date && expiry_date < now) {
    await emitEvent("EXPIRED_DOCUMENT_UPLOADED", { mission_id, doc_type, expiry_date }, env);
  }

  // 8. FRAUD SIGNAL
  if (fraud_signal && fraud_signal > 0.6) {
    await emitEvent("DOCUMENT_FRAUD_SIGNAL", { mission_id, doc_type, fraud_signal }, env);
  }

  // 9. HIGH RISK
  if (risk && risk > 0.7) {
    await emitEvent("HIGH_DOCUMENT_RISK", { mission_id, doc_type, risk }, env);
  }

  // 10. COMPLIANCE DRIFT
  if (mission_id) {
    await emitEvent("COMPLIANCE_DRIFT_ANALYSIS_TRIGGERED", { mission_id }, env);
  }
}

/* =========================================================
   GEMINI ANALYSIS
   ========================================================= */

async function analyzeWithGemini(file, declared_doc_type, context, env) {
  const buffer = await file.arrayBuffer();
  const bytes = new Uint8Array(buffer);
  let binary = "";
  for (let i = 0; i < bytes.length; i += 0x8000) {
    binary += String.fromCharCode(...bytes.subarray(i, i + 0x8000));
  }
  const base64 = btoa(binary);

  const response = await fetch(
    `https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash-lite:generateContent?key=${env.AI_API_KEY}`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        contents: [{
          parts: [
            {
              text: `
You are an ICAO-aligned aviation compliance AI for aviation operators globally with 49 years of experience across diverse regions and jurisdictions

Mission Context:
- Scope: ${context.scope}
- Mission ID: ${context.mission_id || "N/A"}
- Operator ID: ${context.operator_id || "N/A"}
- Fleet ID: ${context.fleet_id || "N/A"}

Declared document type: "${declared_doc_type}"

System sensitivity: STRICT.
False positives are dangerous.
False negatives are catastrophic.

Tasks:
1. Perform OCR extraction.
2. Detect true document type.
3. Validate regulatory identifiers and authority seals.
4. Extract expiry date (ISO8601).
5. Detect tampering indicators.
6. Score:
   - validation_score (0-1)
   - fraud_signal (0-1)
   - completeness_score (0-1)

Return STRICT JSON:
{
  "detected_doc_type": "string",
  "expiry_date": "ISO8601 or null",
  "validation_score": number,
  "fraud_signal": number,
  "completeness_score": number
}
`
            },
            { inlineData: { mimeType: file.type, data: base64 } }
          ]
        }],
        generationConfig: { temperature: 0, response_mime_type: "application/json" }
      })
    }
  );

  if (!response.ok) return fallbackAnalysis();

  try {
    const data = await response.json();
    const text = data.candidates?.[0]?.content?.parts?.[0]?.text;
    const parsed = JSON.parse(text);

    const detected_doc_type = parsed.detected_doc_type || "unknown";
    const expiry_date = parsed.expiry_date ? new Date(parsed.expiry_date).getTime() : null;

    let vScore = clamp(parsed.validation_score);
    let fSignal = clamp(parsed.fraud_signal);
    let cScore = clamp(parsed.completeness_score);

    if (detected_doc_type !== declared_doc_type) {
      vScore *= 0.5;
      fSignal = Math.min(1, fSignal + 0.3);
    }
    if (!expiry_date) vScore *= 0.7;

    return { detected_doc_type, expiry_date, validation_score: vScore, fraud_signal: fSignal, completeness_score: cScore };
  } catch {
    return fallbackAnalysis();
  }
}

function fallbackAnalysis() {
  return { detected_doc_type: "unknown", expiry_date: null, validation_score: 0.4, fraud_signal: 0.2, completeness_score: 0.4 };
}

/* =========================================================
   DURABLE OBJECT CLASSES (Mandatory Exports)
   ========================================================= */

export class MissionState {
  constructor(state) { this.state = state; }
  async fetch(request) {
    const url = new URL(request.url);
    if (request.method === "POST" && url.pathname === "/mission/update") {
      const data = await request.json();
      const current = await this.state.storage.get("mission_state") || {};
      await this.state.storage.put("mission_state", { ...current, ...data });
      return new Response("OK");
    }
    if (url.pathname === "/mission/state") {
      const state = await this.state.storage.get("mission_state");
      return new Response(JSON.stringify(state || {}), { headers: { "Content-Type": "application/json" } });
    }
    return new Response("Not Found", { status: 404 });
  }
}

export class MissionClocks {
  constructor(state) { this.state = state; }
  async fetch(request) {
    const url = new URL(request.url);
    if (request.method === "POST" && url.pathname === "/clock/update") {
      const data = await request.json();
      const current = await this.state.storage.get("clock_state") || {};
      await this.state.storage.put("clock_state", { ...current, ...data });
      return new Response("OK");
    }
    return new Response("Not Found", { status: 404 });
  }
}

/* =========================================================
   ENGINE ACTIONS
   ========================================================= */

async function updateMissionState({ mission_id, doc_type, expiry_date, risk, env }) {
  const stub = env.MISSION_STATE.get(env.MISSION_STATE.idFromName(mission_id));
  const update = { document_last_verified_at: Date.now(), document_risk_score: risk };
  if (doc_type === "permit") update.permit_expiry = expiry_date;
  if (doc_type === "crew_cert") update.crew_cert_expiry = expiry_date;
  if (doc_type === "maintenance") update.maintenance_clearance_expiry = expiry_date;
  return stub.fetch("https://mission/update", { method: "POST", body: JSON.stringify(update) });
}

async function updateMissionClocks({ mission_id, doc_type, expiry_date, risk, env }) {
  const stub = env.MISSION_CLOCKS.get(env.MISSION_CLOCKS.idFromName(mission_id));
  return stub.fetch("https://clock/update", {
    method: "POST",
    body: JSON.stringify({ doc_type, expiry_date, risk, updated_at: Date.now() })
  });
}

async function updateOperatorCompliance(operator_id, validation_score, expiry_date, risk, env) {
  let status = "FIT";
  if (expiry_date && expiry_date < Date.now()) status = "UNFIT";
  else if (validation_score < 0.7 || risk > 0.6) status = "AT_RISK";
  const score = Math.max(0, 100 - risk * 100);

  return env["icc-orm-engine"].fetch("https://orm/compliance-update", {
    method: "POST",
    body: JSON.stringify({ operator_id, compliance_status: status, compliance_score: score })
  });
}

/* =========================================================
   MAIN UPLOAD HANDLER
   ========================================================= */

async function handleUpload(request, env, ctx) {
  const form = await request.formData();
  const file = form.get("file");
  const mission_id = form.get("mission_id");
  const operator_id = form.get("operator_id");
  const fleet_id = form.get("fleet_id");
  const doc_type = form.get("doc_type");
  const scope = form.get("scope");

  if (!file || !doc_type || !scope) return json({ error: "INVALID_INPUT" }, 400);

  const key = buildKey({ scope, operator_id, mission_id, fleet_id, doc_type, file });
  await putToR2(scope, key, file, env);

  const analysis = await analyzeWithGemini(file, doc_type, { mission_id, operator_id, fleet_id, scope }, env);
  const { validation_score, expiry_date, fraud_signal, completeness_score, detected_doc_type } = analysis;
  const risk = computeRisk({ validation_score, expiry_date, fraud_signal, completeness_score });
  const now = Date.now();

  // Run Telemetry in background
  ctx.waitUntil(runSecurityTelemetry({ 
    mission_id, operator_id, doc_type, detected_doc_type, validation_score, 
    expiry_date, fraud_signal, ai_confidence: validation_score, risk, fileHash: key, now, env 
  }));

  const isValid = validation_score > 0.7 && risk < 0.6 ? 1 : 0;

  if (scope === "mission" && mission_id) {
    await env.schema.prepare(`INSERT INTO mission_documents (mission_id, doc_type, r2_key, file_hash, validation_score, expiry_date, uploaded, valid, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`)
      .bind(mission_id, detected_doc_type, key, key, validation_score, expiry_date, 1, isValid, now).run();
    
    await updateMissionState({ mission_id, doc_type, expiry_date, risk, env });
    await updateMissionClocks({ mission_id, doc_type, expiry_date, risk, env });
    ctx.waitUntil(env["icc-pivot-engine"].fetch("https://pivot/evaluate", { method: "POST", body: JSON.stringify({ mission_id }) }));
  }

  if (scope === "operator" && operator_id) {
    await env.schema.prepare(`INSERT INTO operator_documents (operator_id, doc_type, r2_key, file_hash, validation_score, expiry_date, status, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`)
      .bind(operator_id, detected_doc_type, key, key, validation_score, expiry_date, isValid ? "valid" : "rejected", now).run();
    ctx.waitUntil(updateOperatorCompliance(operator_id, validation_score, expiry_date, risk, env));
  }

  // Notifications
  ctx.waitUntil(env["mission-comms"].fetch("https://internal/notify", {
    method: "POST",
    body: JSON.stringify({ to: "15dwingsltd@gmail.com", channel: "EMAIL_AND_PUSH", type: "DOCUMENT_VERIFICATION", mission_id, operator_id, doc_type: detected_doc_type, validation_score, risk, expiry_date, storage_key: key, created_at: now })
  }));

  return json({ status: "VERIFIED", detected_doc_type, validation_score, expiry_date, document_risk_score: risk, storage_key: key });
}

/* =========================================================
   FETCH EVENT LISTENER
   ========================================================= */

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    if (url.pathname === "/") return new Response("Aviation Document Engine Alive", { status: 200 });
    if (url.pathname === "/upload" && request.method === "POST") return handleUpload(request, env, ctx);
    
    if (url.pathname.startsWith("/mission/")) {
      const mission_id = url.pathname.split("/")[2];
      if (!mission_id) return new Response("Missing ID", { status: 400 });
      const stub = env.MISSION_STATE.get(env.MISSION_STATE.idFromName(mission_id));
      return stub.fetch(request);
    }
    if (url.pathname.startsWith("/clock/")) {
      const mission_id = url.pathname.split("/")[2];
      if (!mission_id) return new Response("Missing ID", { status: 400 });
      const stub = env.MISSION_CLOCKS.get(env.MISSION_CLOCKS.idFromName(mission_id));
      return stub.fetch(request);
    }

    return new Response("Not Found", { status: 404 });
  }
};