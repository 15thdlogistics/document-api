/**
 * CLOUDFLARE WORKER ENTRYPOINT
 * Re-exporting Durable Objects here is REQUIRED for deployment success.
 */
export { MissionState, MissionClocks } from "./document-engine.js";

import { handleUpload } from "./document-engine.js";

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);

    // 1. Health Check
    if (url.pathname === "/") {
      return new Response("Document API Alive", { status: 200 });
    }

    // 2. Main Upload Entrypoint
    if (url.pathname === "/upload" && request.method === "POST") {
      return handleUpload(request, env, ctx);
    }

    // 3. Mission State Proxy (Routing to Durable Object)
    if (url.pathname.startsWith("/mission/")) {
      const parts = url.pathname.split("/");
      const mission_id = parts[2];

      if (!mission_id) {
        return new Response("Missing mission_id", { status: 400 });
      }

      const id = env.MISSION_STATE.idFromName(mission_id);
      const stub = env.MISSION_STATE.get(id);

      // Reconstruct the path so the DO receives exactly what it expects:
      // e.g. "/mission/state" or "/mission/update"
      const subPath = parts.slice(3).join("/"); 
      const targetPath = subPath ? `/mission/${subPath}` : "/mission/state";

      const newRequest = new Request(
        new URL(targetPath, "https://mission-internal"),
        request
      );

      return stub.fetch(newRequest);
    }

    // 4. Mission Clocks Proxy (Routing to Durable Object)
    if (url.pathname.startsWith("/clock/")) {
      const parts = url.pathname.split("/");
      const mission_id = parts[2];

      if (!mission_id) {
        return new Response("Missing mission_id", { status: 400 });
      }

      const id = env.MISSION_CLOCKS.idFromName(mission_id);
      const stub = env.MISSION_CLOCKS.get(id);

      const subPath = parts.slice(3).join("/");
      const targetPath = subPath ? `/clock/${subPath}` : "/clock/state";

      const newRequest = new Request(
        new URL(targetPath, "https://clock-internal"),
        request
      );

      return stub.fetch(newRequest);
    }

    return new Response("Not Found", { status: 404 });
  }
};
