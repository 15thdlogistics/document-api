/* =========================================================
   UPLOAD HANDLER (NON-BLOCKING VERIFICATION)
   ========================================================= */

export async function handleUpload(request, env, ctx) {

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

  /* =========================================================
     STORE FILE IN R2
     ========================================================= */

  await putToR2(scope, key, file, env);

  const now = Date.now();

  /* =========================================================
     INITIAL METADATA WRITE (PENDING VERIFICATION)
     ========================================================= */

  if (scope === "mission" && mission_id) {

    await env.schema.prepare(`
      INSERT INTO mission_documents (
        mission_id,
        doc_type,
        r2_key,
        uploaded,
        created_at
      )
      VALUES (?, ?, ?, ?, ?)
    `)
    .bind(
      mission_id,
      doc_type,
      key,
      1,
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
        status,
        created_at
      )
      VALUES (?, ?, ?, ?, ?)
    `)
    .bind(
      operator_id,
      doc_type,
      key,
      "pending",
      now
    )
    .run();
  }

  /* =========================================================
     BACKGROUND AI VERIFICATION
     ========================================================= */

  ctx.waitUntil(
    runVerification({
      file,
      doc_type,
      mission_id,
      operator_id,
      fleet_id,
      scope,
      key
    }, env)
  );

  /* =========================================================
     IMMEDIATE RESPONSE
     ========================================================= */

  return json({
    status: "UPLOAD_RECEIVED",
    scope,
    doc_type,
    storage_key: key,
    verification: "processing"
  });

}

/* =========================================================
   BACKGROUND VERIFICATION ENGINE
   ========================================================= */

async function runVerification(input, env) {

  const {
    file,
    doc_type,
    mission_id,
    operator_id,
    fleet_id,
    scope,
    key
  } = input;

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

  const now = Date.now();
  const isValid = validation_score > 0.7 && risk < 0.6 ? 1 : 0;

  /* =========================================================
     UPDATE DATABASE AFTER AI VERIFICATION
     ========================================================= */

  if (scope === "mission" && mission_id) {

    await env.schema.prepare(`
      UPDATE mission_documents
      SET
        validation_score = ?,
        expiry_date = ?,
        valid = ?
      WHERE r2_key = ?
    `)
    .bind(
      validation_score,
      expiry_date,
      isValid,
      key
    )
    .run();

  }

  if (scope === "operator" && operator_id) {

    await env.schema.prepare(`
      UPDATE operator_documents
      SET
        validation_score = ?,
        expiry_date = ?,
        status = ?
      WHERE r2_key = ?
    `)
    .bind(
      validation_score,
      expiry_date,
      isValid ? "valid" : "rejected",
      key
    )
    .run();

  }

}

/* =========================================================
   GEMINI DOCUMENT ANALYSIS
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
You are an ICAO aviation compliance AI.

Context:
Scope: ${context.scope}
Mission ID: ${context.mission_id || "N/A"}
Operator ID: ${context.operator_id || "N/A"}
Fleet ID: ${context.fleet_id || "N/A"}

Declared document type: ${declared_doc_type}

Return strict JSON:

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

    return {
      detected_doc_type: parsed.detected_doc_type || "unknown",
      expiry_date: parsed.expiry_date
        ? new Date(parsed.expiry_date).getTime()
        : null,
      validation_score: clamp(parsed.validation_score),
      fraud_signal: clamp(parsed.fraud_signal),
      completeness_score: clamp(parsed.completeness_score)
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
  else if (expiry_date && expiry_date - now < 604800000) expiryRisk = 0.5;

  return (
    (1 - validation_score) * 0.4 +
    expiryRisk * 0.3 +
    (1 - completeness_score) * 0.2 +
    fraud_signal * 0.1
  );

}

/* =========================================================
   STORAGE KEY BUILDER
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
   HELPERS
   ========================================================= */

function clamp(n) {
  if (typeof n !== "number" || isNaN(n)) return 0;
  return Math.max(0, Math.min(1, n));
}

function json(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "Content-Type": "application/json" }
  });
}