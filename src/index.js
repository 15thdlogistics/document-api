export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);

    if (request.method === "POST" && url.pathname === "/upload") {
      return handleUpload(request, env, ctx);
    }

    if (request.method === "POST" && url.pathname === "/mission/compliance") {
      return missionCompliance(request, env);
    }

    return new Response("Not Found", { status: 404 });
  }
};

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

  if (!file || !doc_type || !scope) {
    return json({ error: "INVALID_INPUT" }, 400);
  }

  const key = buildKey({
    scope,
    operator_id,
    mission_id,
    fleet_id,
    doc_type,
    file
  });

  await putToR2(scope, key, file, env);

  const analysis = await analyzeWithGemini(
    file,
    doc_type,
    { mission_id, operator_id, fleet_id, scope },
    env
  );

  const {
    validation_score,
    expiry_date,
    fraud_signal,
    completeness_score,
    detected_doc_type
  } = analysis;

  const risk = computeRisk({
    validation_score,
    expiry_date,
    fraud_signal,
    completeness_score
  });

  /* =========================================================
     D1 METADATA PERSISTENCE (env.schema)
     ========================================================= */

  const now = Date.now();
  const isValid = validation_score > 0.7 && risk < 0.6 ? 1 : 0;

  if (scope === "mission" && mission_id) {
    await env.schema.prepare(`
      INSERT INTO mission_documents (
        mission_id,
        doc_type,
        r2_key,
        validation_score,
        expiry_date,
        uploaded,
        valid,
        created_at
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    `)
    .bind(
      mission_id,
      detected_doc_type,
      key,
      validation_score,
      expiry_date,
      1,
      isValid,
      now
    )
    .run();
  }

  if (scope === "operator" && operator_id) {
    await env.schema.prepare(`
      INSERT INTO operator_documents (
        operator_id,
        doc_type,
        r2_key,
        validation_score,
        expiry_date,
        status,
        created_at
      )
      VALUES (?, ?, ?, ?, ?, ?, ?)
    `)
    .bind(
      operator_id,
      detected_doc_type,
      key,
      validation_score,
      expiry_date,
      isValid ? "valid" : "rejected",
      now
    )
    .run();
  }

  /* =========================================================
     DURABLE OBJECT UPDATES
     ========================================================= */

  if (scope === "mission" && mission_id) {
    await updateMissionState({
      mission_id,
      doc_type,
      expiry_date,
      validation_score,
      risk,
      env
    });

    await updateMissionClocks({
      mission_id,
      doc_type,
      expiry_date,
      risk,
      env
    });

    ctx.waitUntil(triggerPivot(mission_id, env));
  }

  if (scope === "operator" && operator_id) {
    ctx.waitUntil(
      updateOperatorCompliance(
        operator_id,
        validation_score,
        expiry_date,
        risk,
        env
      )
    );
  }

  /* =========================================================
     INTERNAL NOTIFICATIONS (mission-comms binding)
     ========================================================= */

  ctx.waitUntil(
    env["mission-comms"].fetch("https://internal/notify", {
      method: "POST",
      body: JSON.stringify({
        to: "15dwingsltd@gmail.com",
        channel: "EMAIL_AND_PUSH",
        type: "DOCUMENT_VERIFICATION",
        mission_id,
        operator_id,
        fleet_id,
        doc_type: detected_doc_type,
        validation_score,
        risk,
        expiry_date,
        storage_key: key,
        created_at: now
      })
    })
  );

  ctx.waitUntil(
    emitEvent(
      "VERIFICATION_UPDATED",
      {
        mission_id,
        operator_id,
        fleet_id,
        doc_type: detected_doc_type,
        validation_score,
        expiry_date,
        risk,
        storage_key: key
      },
      env
    )
  );

  return json({
    status: "VERIFIED",
    declared_doc_type: doc_type,
    detected_doc_type,
    validation_score,
    expiry_date,
    document_risk_score: risk,
    storage_key: key
  });
}

/* =========================================================
   KEY BUILDER
   ========================================================= */

function buildKey({
  scope,
  operator_id,
  mission_id,
  fleet_id,
  doc_type,
  file
}) {
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

/* =========================================================
   R2 STORAGE
   ========================================================= */

async function putToR2(scope, key, file, env) {
  if (scope === "mission") return env.MISSION_DOCS.put(key, file);
  if (scope === "operator") return env.OPERATOR_DOCS.put(key, file);
  if (scope === "fleet") return env.FLEET_DOCS.put(key, file);
  if (scope === "autofleet") return env.AUTOFLEET_DOCS.put(key, file);
  throw new Error("INVALID_SCOPE");
}

/* =========================================================
   GEMINI ANALYSIS — CONTEXT-AWARE
   ========================================================= */

async function analyzeWithGemini(file, declared_doc_type, context, env) {
  const buffer = await file.arrayBuffer();
  const base64 = btoa(String.fromCharCode(...new Uint8Array(buffer)));

  const response = await fetch(
    "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash-lite:generateContent?key=" +
      env.AI_API_KEY,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        contents: [
          {
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

Rules:
- Type mismatch → reduce validation_score significantly.
- Expiry within 30 days → reduce validation_score moderately.
- Forgery signals → increase fraud_signal.

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
              {
                inlineData: {
                  mimeType: file.type,
                  data: base64
                }
              }
            ]
          }
        ],
        generationConfig: {
          temperature: 0,
          response_mime_type: "application/json"
        }
      })
    }
  );

  if (!response.ok) return fallbackAnalysis();

  try {
    const data = await response.json();
    const text = data.candidates?.[0]?.content?.parts?.[0]?.text;
    const parsed = JSON.parse(text);

    const detected_doc_type = parsed.detected_doc_type || "unknown";
    const expiry_date = parsed.expiry_date
      ? new Date(parsed.expiry_date).getTime()
      : null;

    let validation_score = clamp(parsed.validation_score);
    let fraud_signal = clamp(parsed.fraud_signal);
    let completeness_score = clamp(parsed.completeness_score);

    if (detected_doc_type !== declared_doc_type) {
      validation_score *= 0.5;
      fraud_signal = Math.min(1, fraud_signal + 0.3);
    }

    if (!expiry_date) validation_score *= 0.7;

    return {
      detected_doc_type,
      expiry_date,
      validation_score,
      fraud_signal,
      completeness_score
    };
  } catch {
    return fallbackAnalysis();
  }
}

function fallbackAnalysis() {
  return {
    detected_doc_type: "unknown",
    expiry_date: null,
    validation_score: 0.4,
    fraud_signal: 0.2,
    completeness_score: 0.4
  };
}

function clamp(n) {
  if (typeof n !== "number" || isNaN(n)) return 0;
  return Math.max(0, Math.min(1, n));
}

/* =========================================================
   RISK ENGINE
   ========================================================= */

function computeRisk({
  validation_score,
  expiry_date,
  fraud_signal,
  completeness_score
}) {
  const now = Date.now();

  let expiryRisk = 0;

  if (expiry_date && expiry_date < now) expiryRisk = 1;
  else if (expiry_date && expiry_date - now < 1000 * 60 * 60 * 24 * 7)
    expiryRisk = 0.5;

  return (
    (1 - validation_score) * 0.4 +
    expiryRisk * 0.3 +
    (1 - completeness_score) * 0.2 +
    fraud_signal * 0.1
  );
}

/* =========================================================
   DURABLE OBJECT + ENGINE FUNCTIONS
   ========================================================= */

async function updateMissionState({ mission_id, doc_type, expiry_date, risk, env }) {
  const id = env.MISSION_STATE.idFromName(mission_id);
  const stub = env.MISSION_STATE.get(id);

  const update = {
    document_last_verified_at: Date.now(),
    document_risk_score: risk
  };

  if (doc_type === "permit") update.permit_expiry = expiry_date;
  if (doc_type === "crew_cert") update.crew_cert_expiry = expiry_date;
  if (doc_type === "maintenance") update.maintenance_clearance_expiry = expiry_date;

  await stub.fetch("https://mission/update", {
    method: "POST",
    body: JSON.stringify(update)
  });
}

async function updateMissionClocks({ mission_id, doc_type, expiry_date, risk, env }) {
  const id = env.MISSION_CLOCKS.idFromName(mission_id);
  const stub = env.MISSION_CLOCKS.get(id);

  await stub.fetch("https://clock/update", {
    method: "POST",
    body: JSON.stringify({
      doc_type,
      expiry_date,
      risk,
      updated_at: Date.now()
    })
  });
}

async function updateOperatorCompliance(operator_id, validation_score, expiry_date, risk, env) {
  let status = "FIT";
  const now = Date.now();

  if (expiry_date && expiry_date < now) status = "UNFIT";
  else if (validation_score < 0.7 || risk > 0.6) status = "AT_RISK";

  const score = Math.max(0, 100 - risk * 100);

  await env["icc-orm-engine"].fetch("https://orm/compliance-update", {
    method: "POST",
    body: JSON.stringify({
      operator_id,
      compliance_status: status,
      compliance_score: score
    })
  });
}

async function triggerPivot(mission_id, env) {
  await env["icc-pivot-engine"].fetch("https://pivot/evaluate", {
    method: "POST",
    body: JSON.stringify({ mission_id })
  });
}

async function missionCompliance(request, env) {
  const { mission_id } = await request.json();
  const id = env.MISSION_STATE.idFromName(mission_id);
  const stub = env.MISSION_STATE.get(id);
  return stub.fetch("https://mission/state");
}

async function emitEvent(type, payload, env) {
  await env["icc-mission-engine"].fetch("https://mission-engine/event", {
    method: "POST",
    body: JSON.stringify({ event: type, payload })
  });
}

function json(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "Content-Type": "application/json" }
  });
}