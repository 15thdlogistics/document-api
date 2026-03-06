
/* =========================================================
SECURITY TELEMETRY ENGINE
========================================================= */

export async function runSecurityTelemetry(context) {

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

  /* =========================================================
  1. DUPLICATE DOCUMENT DETECTION (MISSION SCOPE)
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
  2. CROSS-SCOPE DOCUMENT REUSE (OPERATOR VS MISSION)
  ========================================================= */

  const operatorDuplicate = await env.schema.prepare(`
    SELECT operator_id
    FROM operator_documents
    WHERE file_hash = ?
    LIMIT 1
  `).bind(fileHash).first();

  if (operatorDuplicate) {
    await emitEvent("CROSS_SCOPE_DOCUMENT_REUSE", {
      mission_id,
      operator_id: operatorDuplicate.operator_id,
      doc_type
    }, env);
  }

  /* =========================================================
  3. AI LOW CONFIDENCE
  ========================================================= */

  if (ai_confidence && ai_confidence < 0.4) {
    await emitEvent("AI_LOW_CONFIDENCE", {
      mission_id,
      doc_type,
      ai_confidence
    }, env);
  }

  /* =========================================================
  4. DOCUMENT TYPE MISMATCH
  ========================================================= */

  if (detected_doc_type && detected_doc_type !== doc_type) {
    await emitEvent("DOCUMENT_TYPE_MISMATCH", {
      mission_id,
      declared: doc_type,
      detected: detected_doc_type
    }, env);
  }

  /* =========================================================
  5. MISSION UPLOAD SPIKE DETECTION
  ========================================================= */

  if (mission_id) {

    const uploads = await env.schema.prepare(`
      SELECT COUNT(*) as c
      FROM mission_documents
      WHERE mission_id = ?
      AND created_at > ?
    `).bind(mission_id, now - 60000).first();

    if (uploads && uploads.c > 5) {
      await emitEvent("UPLOAD_SPIKE_DETECTED", {
        mission_id,
        count: uploads.c
      }, env);
    }
  }

  /* =========================================================
  6. GLOBAL UPLOAD SPIKE DETECTION (BOT ATTACK SIGNAL)
  ========================================================= */

  const globalUploads = await env.schema.prepare(`
    SELECT COUNT(*) as c
    FROM mission_documents
    WHERE created_at > ?
  `).bind(now - 60000).first();

  if (globalUploads && globalUploads.c > 50) {
    await emitEvent("GLOBAL_UPLOAD_SPIKE", {
      count: globalUploads.c
    }, env);
  }

  /* =========================================================
  7. EXPIRED DOCUMENT UPLOAD
  ========================================================= */

  if (expiry_date && expiry_date < now) {
    await emitEvent("EXPIRED_DOCUMENT_UPLOADED", {
      mission_id,
      doc_type,
      expiry_date
    }, env);
  }

  /* =========================================================
  8. FRAUD SIGNAL DETECTION
  ========================================================= */

  if (fraud_signal && fraud_signal > 0.6) {
    await emitEvent("DOCUMENT_FRAUD_SIGNAL", {
      mission_id,
      doc_type,
      fraud_signal
    }, env);
  }

  /* =========================================================
  9. HIGH DOCUMENT RISK
  ========================================================= */

  if (risk && risk > 0.7) {
    await emitEvent("HIGH_DOCUMENT_RISK", {
      mission_id,
      doc_type,
      risk
    }, env);
  }

  /* =========================================================
  10. COMPLIANCE DRIFT TRIGGER
  ========================================================= */

  if (mission_id) {
    await emitEvent("COMPLIANCE_DRIFT_ANALYSIS_TRIGGERED", {
      mission_id
    }, env);
  }
}

/* =========================================================
EVENT EMITTER
========================================================= */

async function emitEvent(type, payload, env) {

  await env["icc-mission-engine"].fetch(
    "https://mission-engine/event",
    {
      method: "POST",
      body: JSON.stringify({
        event: type,
        version: "telemetry_v1",
        payload
      })
    }
  );

}