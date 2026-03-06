import {
  handleUpload,
  MissionState,
  MissionClocks
} from "./document-engine.js";

export { MissionState, MissionClocks };

export default {

  async fetch(request, env, ctx) {

    const url = new URL(request.url);

    /* HEALTH CHECK */

    if (url.pathname === "/") {
      return new Response("Document API Alive");
    }

    /* DOCUMENT UPLOAD */

    if (url.pathname === "/upload" && request.method === "POST") {
      return handleUpload(request, env, ctx);
    }

    /* ROUTE TO MISSION STATE DURABLE OBJECT */

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

    /* ROUTE TO MISSION CLOCKS */

    if (url.pathname.startsWith("/clock/")) {

      const parts = url.pathname.split("/");
      const mission_id = parts[2];

      if (!mission_id) {
        return new Response("Missing mission_id", { status: 400 });
      }

      const id = env.MISSION_CLOCKS.idFromName(mission_id);
      const stub = env.MISSION_CLOCKS.get(id);

      const newPath = "/" + parts.slice(3).join("/");

      const newRequest = new Request(
        new URL(newPath || "/clock/state", request.url),
        request
      );

      return stub.fetch(newRequest);
    }

    return new Response("Not Found", { status: 404 });

  }

};

export class MissionState {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.sessions = new Set();
  }

  async fetch(request) {
    const url = new URL(request.url);

    if (url.pathname === "/connect") {

      let session;

      const stream = new ReadableStream({
        start: (controller) => {

          session = { controller };
          this.sessions.add(session);

          controller.enqueue(
            `data: ${JSON.stringify({
              event: "CONNECTED",
              time: Date.now()
            })}\n\n`
          );

        },

        cancel: () => {
          if (session) this.sessions.delete(session);
        }
      });

      return new Response(stream, {
        headers: {
          "content-type": "text/event-stream",
          "cache-control": "no-cache",
          "connection": "keep-alive"
        }
      });
    }

    if (request.method === "POST" && url.pathname === "/mission/update") {

      const data = await request.json();

      const current =
        await this.state.storage.get("mission_state") || {};

      const updated = {
        ...current,
        ...data,
        updated_at: Date.now()
      };

      await this.state.storage.put("mission_state", updated);

      this.broadcast({
        event: "MISSION_UPDATED",
        payload: updated
      });

      return new Response("OK");
    }

    if (request.method === "GET" || url.pathname === "/mission/state") {

      const state = await this.state.storage.get("mission_state");

      return new Response(JSON.stringify(state || {}), {
        headers: { "Content-Type": "application/json" }
      });
    }

    return new Response("Not Found", { status: 404 });
  }

  broadcast(message) {

    const data = `data: ${JSON.stringify(message)}\n\n`;

    for (const session of this.sessions) {
      try {
        session.controller.enqueue(data);
      } catch {
        this.sessions.delete(session);
      }
    }
  }
}


export class MissionClocks {

  constructor(state, env) {
    this.state = state;
    this.env = env;
  }

  async fetch(request) {

    const url = new URL(request.url);

    if (request.method === "POST" && url.pathname === "/clock/update") {

      const data = await request.json();

      const current =
        await this.state.storage.get("clock_state") || {};

      const updated = {
        ...current,
        ...data,
        updated_at: Date.now()
      };

      await this.state.storage.put("clock_state", updated);

      return new Response("OK");
    }

    if (request.method === "GET" || url.pathname === "/clock/state") {

      const state = await this.state.storage.get("clock_state");

      return new Response(JSON.stringify(state || {}), {
        headers: { "Content-Type": "application/json" }
      });
    }

    return new Response("Not Found", { status: 404 });
  }
}