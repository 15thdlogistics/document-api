export async function handleUpload(request, env, ctx) {
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
COMPOSITE MISSION TELEMETRY
========================================================= */

async function computeCompositeMissionTelemetry(mission_id, env) {

  const docs = await env.schema.prepare(`
    SELECT doc_type, validation_score, risk_score, expiry_date
    FROM mission_documents
    WHERE mission_id = ?
    AND superseded IS NULL
  `).bind(mission_id).all();

  if (!docs.results.length) return;

  let totalRisk = 0;
  let nextExpiry = null;

  for (const d of docs.results) {

    totalRisk += d.risk_score;

    if (d.expiry_date) {
      if (!nextExpiry || d.expiry_date < nextExpiry) {
        nextExpiry = d.expiry_date;
      }
    }
  }

  const compositeRisk = totalRisk / docs.results.length;

  await emitEvent("MISSION_COMPOSITE_UPDATED", {
    mission_id,
    composite_risk: compositeRisk,
    next_expiry: nextExpiry
  }, env);
}

/* =========================================================
COMPLIANCE DRIFT
========================================================= */

async function computeComplianceDrift(mission_id, env) {

  const rows = await env.schema.prepare(`
    SELECT validation_score, risk_score, created_at
    FROM mission_documents
    WHERE mission_id = ?
    ORDER BY created_at DESC
    LIMIT 10
  `).bind(mission_id).all();

  if (rows.results.length < 2) return;

  const latest = rows.results[0];
  const previous = rows.results[1];

  const validationDrop =
    previous.validation_score - latest.validation_score;

  const riskIncrease =
    latest.risk_score - previous.risk_score;

  if (validationDrop > 0.3) {
    await emitEvent("VALIDATION_DROP_DETECTED", {
      mission_id,
      drop: validationDrop
    }, env);
  }

  if (riskIncrease > 0.3) {
    await emitEvent("RISK_SPIKE_DETECTED", {
      mission_id,
      increase: riskIncrease
    }, env);
  }
}

/* =========================================================
CROSS SCOPE CORRELATION
========================================================= */

async function detectCrossScopeConflict(mission_id, operator_id, env) {

  if (!mission_id || !operator_id) return;

  const missionRisk = await env.schema.prepare(`
    SELECT AVG(risk_score) as r
    FROM mission_documents
    WHERE mission_id = ?
    AND superseded IS NULL
  `).bind(mission_id).first();

  const operatorRisk = await env.schema.prepare(`
    SELECT AVG(risk_score) as r
    FROM operator_documents
    WHERE operator_id = ?
  `).bind(operator_id).first();

  if (missionRisk?.r > 0.7 && operatorRisk?.r < 0.2) {

    await emitEvent("CROSS_SCOPE_CONTRADICTION", {
      mission_id,
      operator_id
    }, env);
  }
}

/* =========================================================
EVENT DERIVATION
========================================================= */

function deriveEvents({
  validation_score,
  expiry_date,
  fraud_signal,
  detected_doc_type,
  declared_doc_type
}) {

  const events = [];
  const now = Date.now();

  if (validation_score > 0.8)
    events.push("DOCUMENT_VALIDATED");
  else
    events.push("DOCUMENT_REJECTED");

  if (fraud_signal > 0.6)
    events.push("FRAUD_SIGNAL_DETECTED");

  if (detected_doc_type !== declared_doc_type)
    events.push("TYPE_MISMATCH_DETECTED");

  if (expiry_date && expiry_date < now)
    events.push("DOCUMENT_EXPIRED");

  else if (expiry_date && expiry_date - now < 604800000)
    events.push("PRE_EXPIRY_WARNING");

  if (expiry_date && expiry_date - now < 172800000)
    events.push("CRITICAL_EXPIRY_WARNING");

  return events;
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
AI ANALYSIS
========================================================= */

async function analyzeWithGemini(file, declared_doc_type, env) {

  const buffer = await file.arrayBuffer();

  const base64 =
    btoa(String.fromCharCode(...new Uint8Array(buffer)));

  const response = await fetch(
    "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash-lite:generateContent?key=" + env.AI_API_KEY,
    {
      method:"POST",
      headers:{ "Content-Type":"application/json" },
      body:JSON.stringify({
        contents:[{
          parts:[
            {text:"Return strict JSON with detected_doc_type, expiry_date, validation_score, fraud_signal, completeness_score, ai_confidence"},
            {inlineData:{mimeType:file.type,data:base64}}
          ]
        }],
        generationConfig:{
          temperature:0,
          response_mime_type:"application/json"
        }
      })
    }
  );

  if (!response.ok) return fallbackAnalysis();

  try {

    const data = await response.json();

    const text =
      data.candidates?.[0]?.content?.parts?.[0]?.text;

    const parsed = JSON.parse(text);

    return {
      detected_doc_type:
        parsed.detected_doc_type || declared_doc_type,
      expiry_date:
        parsed.expiry_date
          ? new Date(parsed.expiry_date).getTime()
          : null,
      validation_score: clamp(parsed.validation_score),
      fraud_signal: clamp(parsed.fraud_signal),
      completeness_score: clamp(parsed.completeness_score),
      ai_confidence: clamp(parsed.ai_confidence ?? 0.5)
    };

  } catch {
    return fallbackAnalysis();
  }
}

function fallbackAnalysis() {
  return {
    detected_doc_type:"unknown",
    expiry_date:null,
    validation_score:0.4,
    fraud_signal:0.2,
    completeness_score:0.4,
    ai_confidence:0.3
  };
}

/* =========================================================
UTILITIES
========================================================= */

async function sha256(buffer) {
  const hashBuffer = await crypto.subtle.digest(
    "SHA-256",
    buffer
  );

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

  if (expiry_date && expiry_date < now)
    expiryRisk = 1;

  else if (expiry_date && expiry_date - now < 604800000)
    expiryRisk = 0.5;

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

  if (scope === "mission")
    return env.MISSION_DOCS.put(key,file);

  if (scope === "operator")
    return env.OPERATOR_DOCS.put(key,file);

  if (scope === "fleet")
    return env.FLEET_DOCS.put(key,file);

  if (scope === "autofleet")
    return env.AUTOFLEET_DOCS.put(key,file);

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