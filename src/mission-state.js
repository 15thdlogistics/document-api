export class MissionState {

  constructor(state, env) {
    this.state = state;
    this.env = env;

    this.sessions = new Set();
  }

  async fetch(request) {

    const url = new URL(request.url);

    /* =====================================
       LIVE CONNECTION (Event Stream)
    ===================================== */

    if (url.pathname === "/connect") {

      const stream = new ReadableStream({
        start: (controller) => {

          const session = { controller };

          this.sessions.add(session);

          controller.enqueue(
            `data: ${JSON.stringify({
              event: "CONNECTED",
              time: Date.now()
            })}\n\n`
          );

        },

        cancel: () => {
          this.sessions.delete(session);
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

    /* =====================================
       MISSION STATE UPDATE
    ===================================== */

    if (url.pathname === "/mission/update") {

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

    return new Response("Not Found", { status: 404 });

  }

  broadcast(message) {

    const data = `data: ${JSON.stringify(message)}\n\n`;

    for (const session of this.sessions) {
      session.controller.enqueue(data);
    }

  }

}