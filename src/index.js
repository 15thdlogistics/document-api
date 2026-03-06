import { handleUpload } from "./document-engine.js";

/* =====================================================
   DURABLE OBJECTS
===================================================== */

export class MissionState {

  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.sessions = new Set();
  }

  async fetch(request) {

    const url = new URL(request.url);

    if (url.pathname === "/mission/update" && request.method === "POST") {

      const data = await request.json();

      const current =
        await this.state.storage.get("mission_state") || {};

      const updated = {
        ...current,
        ...data,
        updated_at: Date.now()
      };

      await this.state.storage.put("mission_state", updated);

      return new Response("OK");
    }

    const state = await this.state.storage.get("mission_state");

    return new Response(JSON.stringify(state || {}), {
      headers: { "Content-Type": "application/json" }
    });
  }
}


export class MissionClocks {

  constructor(state, env) {
    this.state = state;
    this.env = env;
  }

  async fetch(request) {

    const url = new URL(request.url);

    if (url.pathname === "/clock/update" && request.method === "POST") {

      const data = await request.json();

      const current =
        await this.state.storage.get("clock_state") || {};

      await this.state.storage.put("clock_state", {
        ...current,
        ...data
      });

      return new Response("OK");
    }

    return new Response("Not Found", { status: 404 });
  }

}

/* =====================================================
   MAIN WORKER
===================================================== */

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