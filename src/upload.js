export default {
  async fetch(request, env, ctx) {

    const url = new URL(request.url);

    if (url.pathname === "/upload" && request.method === "POST") {
      return handleUpload(request, env, ctx);
    }

    if (url.pathname === "/mission/compliance" && request.method === "POST") {
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
  const mission_tier = form.get("mission_tier");

  if (!file || !doc_type || !scope) {
    return json({ error: "INVALID_INPUT" }, 400);
  }

  const uploader_ip = request.headers.get("CF-Connecting-IP");
  const uploader_agent = request.headers.get("User-Agent");

  const key = buildKey({
    scope,
    operator_id,
    mission_id,
    fleet_id,
    doc_type,
    file
  });

  const buffer = await file.arrayBuffer();
  const fileHash = await sha256(buffer);

  await putToR2(scope, key, file, env);

  const analysis = await analyzeWithGemini(file, doc_type, env);

  const {
    validation_score,
    expiry_date,
    fraud_signal,
    completeness_score,
    detected_doc_type,
    ai_confidence
  } = analysis;

  const risk = computeRisk({
    validation_score,
    expiry_date,
    fraud_signal,
    completeness_score
  });

  const now = Date.now();

  /* =========================================================
  DUPLICATE DOCUMENT DETECTION
  ========================================================= */

  const duplicate = await env.schema.prepare(`
    SELECT mission_id, doc_type
    FROM mission_documents
    WHERE file_hash = ?
    LIMIT 1
  `).bind(fileHash).first();

  if (duplicate) {
    await emitEvent("DOCUMENT_REUSE_DETECTED", {
      mission_id,
      original_mission: duplicate.mission_id,
      doc_type
    }, env);
  }

  /* =========================================================
  AI CONFIDENCE SIGNAL
  ========================================================= */

  if (ai_confidence < 0.4) {
    await emitEvent("AI_LOW_CONFIDENCE", {
      mission_id,
      doc_type,
      ai_confidence
    }, env);
  }

  /* =========================================================
  RAPID UPLOAD PATTERN DETECTION
  ========================================================= */

  if (mission_id) {
    const uploads = await env.schema.prepare(`
      SELECT COUNT(*) as c
      FROM mission_documents
      WHERE mission_id = ?
      AND created_at > ?
    `).bind(mission_id, now - 60000).first();

    if (uploads && uploads.c > 5) {
      await emitEvent("UPLOAD_SPIKE_DETECTED", { mission_id }, env);
    }
  }

  /* =========================================================
  STORE MISSION DOCUMENT
  ========================================================= */

  if (scope === "mission" && mission_id) {

    await env.schema.prepare(`
      UPDATE mission_documents
      SET superseded = 1
      WHERE mission_id = ?
      AND doc_type = ?
      AND superseded IS NULL
    `).bind(mission_id, detected_doc_type).run();

    await env.schema.prepare(`
      INSERT INTO mission_documents (
        mission_id,
        doc_type,
        r2_key,
        file_hash,
        validation_score,
        expiry_date,
        fraud_signal,
        completeness_score,
        ai_confidence,
        risk_score,
        uploader_ip,
        uploader_agent,
        superseded,
        created_at
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?)
    `).bind(
      mission_id,
      detected_doc_type,
      key,
      fileHash,
      validation_score,
      expiry_date,
      fraud_signal,
      completeness_score,
      ai_confidence,
      risk,
      uploader_ip,
      uploader_agent,
      now
    ).run();

    await computeCompositeMissionTelemetry(mission_id, env);
    await computeComplianceDrift(mission_id, env);
    await detectCrossScopeConflict(mission_id, operator_id, env);
  }

  /* =========================================================
  STORE OPERATOR DOCUMENT
  ========================================================= */

  if (scope === "operator" && operator_id) {

    await env.schema.prepare(`
      INSERT INTO operator_documents (
        operator_id,
        doc_type,
        r2_key,
        file_hash,
        validation_score,
        expiry_date,
        risk_score,
        created_at
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    `).bind(
      operator_id,
      detected_doc_type,
      key,
      fileHash,
      validation_score,
      expiry_date,
      risk,
      now
    ).run();
  }

  /* =========================================================
  EVENT TAXONOMY
  ========================================================= */

  const events = deriveEvents({
    validation_score,
    expiry_date,
    fraud_signal,
    detected_doc_type,
    declared_doc_type: doc_type
  });

  for (const type of events) {
    ctx.waitUntil(
      emitEvent(type, {
        mission_id,
        operator_id,
        fleet_id,
        mission_tier,
        doc_type: detected_doc_type,
        validation_score,
        risk,
        expiry_date,
        ai_confidence,
        storage_key: key
      }, env)
    );
  }

  return json({
    status: "TELEMETRY_RECORDED",
    detected_doc_type,
    validation_score,
    expiry_date,
    risk,
    ai_confidence
  });
}

/* =========================================================
MISSION READINESS ENDPOINT
========================================================= */

async function missionCompliance(request, env) {

  const { mission_id } = await request.json();

  const docs = await env.schema.prepare(`
    SELECT doc_type, validation_score, risk_score, expiry_date
    FROM mission_documents
    WHERE mission_id = ?
    AND superseded IS NULL
  `).bind(mission_id).all();

  const results = docs.results;

  const compositeRisk =
    results.reduce((sum, d) => sum + d.risk_score, 0) /
    (results.length || 1);

  const nextExpiry = results
    .map(d => d.expiry_date)
    .filter(Boolean)
    .sort()[0] || null;

  const required = ["permit","crew_cert","maintenance"];

  const present = results.map(r => r.doc_type);

  const missing = required.filter(r => !present.includes(r));

  const readiness =
    compositeRisk < 0.5 && missing.length === 0
      ? "READY"
      : "AT_RISK";

  return json({
    mission_id,
    readiness_status: readiness,
    composite_risk_score: compositeRisk,
    document_count: results.length,
    missing_documents: missing,
    next_expiry_timestamp: nextExpiry,
    documents: results
  });
}

/* =========================================================
UTILITIES
========================================================= */

async function sha256(buffer) {
  const hashBuffer = await crypto.subtle.digest("SHA-256", buffer);

  return Array.from(new Uint8Array(hashBuffer))
    .map(b=>b.toString(16).padStart(2,"0"))
    .join("");
}

function clamp(n) {
  if (typeof n !== "number" || isNaN(n)) return 0;
  return Math.max(0, Math.min(1, n));
}

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

function buildKey({
  scope,
  operator_id,
  mission_id,
  fleet_id,
  doc_type,
  file
}) {

  const id =
    scope === "operator" ? operator_id :
    scope === "mission" ? mission_id :
    scope === "fleet" ? fleet_id :
    scope === "autofleet" ? "autofleet" :
    null;

  if (!id) throw new Error("MISSING_SCOPE_IDENTIFIER");

  return `${scope}/${id}/${doc_type}/${crypto.randomUUID()}-${file.name}`;
}

async function putToR2(scope, key, file, env) {

  if (scope === "mission") return env.MISSION_DOCS.put(key,file);
  if (scope === "operator") return env.OPERATOR_DOCS.put(key,file);
  if (scope === "fleet") return env.FLEET_DOCS.put(key,file);
  if (scope === "autofleet") return env.AUTOFLEET_DOCS.put(key,file);

  throw new Error("INVALID_SCOPE");
}

async function emitEvent(type, payload, env) {

  await env["icc-mission-engine"].fetch(
    "https://mission-engine/event",
    {
      method:"POST",
      body:JSON.stringify({
        event:type,
        version:"telemetry_v1",
        payload
      })
    }
  );
}

function json(data,status=200) {
  return new Response(JSON.stringify(data),{
    status,
    headers:{ "Content-Type":"application/json" }
  });
}