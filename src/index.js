import { handleUpload } from "./document-engine.js";
import { MissionState, MissionClocks } from "./mission-state.js";

/* REQUIRED EXPORTS FOR DURABLE OBJECTS */
export { MissionState, MissionClocks };

export default {
  async fetch(request, env, ctx) {

    const url = new URL(request.url);

    if (url.pathname === "/") {
      return new Response("Document API Alive");
    }

    if (url.pathname === "/upload" && request.method === "POST") {
      return handleUpload(request, env, ctx);
    }

    if (url.pathname.startsWith("/mission/")) {

      const parts = url.pathname.split("/");
      const mission_id = parts[2];

      if (!mission_id) {
        return new Response("Missing mission_id", { status: 400 });
      }

      const id = env.MISSION_STATE.idFromName(mission_id);
      const stub = env.MISSION_STATE.get(id);

      const newPath = "/" + parts.slice(3).join("/");

      const newRequest = new Request(
        new URL(newPath || "/mission/state", request.url),
        request
      );

      return stub.fetch(newRequest);
    }

    return new Response("Not Found", { status: 404 });
  }
};