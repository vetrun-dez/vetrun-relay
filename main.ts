// Vet Run online co-op relay for Deno Deploy (port of relay.js, same wire protocol; see protocol.md).
// Zero game logic: room codes + message forwarding. Host = dog, guest = cat (decided by the client).
// Run locally:  deno run --allow-net --allow-env main.ts        (PORT env var, default 9080)
// Health check: any plain HTTP request (e.g. GET /) -> "vetrun relay ok, rooms=N"
//
// Deno Deploy may run several isolates, so a host and a guest can land on different ones.
// Every isolate keeps only its own sockets. The isolate where the host connected OWNS the room
// (code, membership, room_full checks). Isolates talk over one shared BroadcastChannel:
//   join_req/join_res  guest isolate asks the owner to admit a guest
//   fwd                game data for peers that live on other isolates
//   leave / closed     guest left / host left (room closed)
//   opened             a code was taken (avoids handing out the same code twice)
//   beat               every 15 s: which rooms/guests an isolate still has, so a vanished
//                      isolate's peers get peer_left / room_closed instead of hanging forever.

const VERSION = 1;
const MAX_PEERS = 2;
const MAX_BYTES = 65536;
const LETTERS = "ABCDEFGHJKMNOPQRSTUVWXYZ";
const DIGITS = "23456789";
const ROOM_IDLE_MS = 2 * 60 * 60 * 1000; // drop rooms idle for 2 h
const BEAT_MS = 15000;
const STALE_MS = 50000;
const JOIN_TIMEOUT_MS = 2500;

type Json = Record<string, unknown>;
interface Sock { id: number; ws: WebSocket; room: string; host: number; owned: boolean }
interface Member { sock: Sock | null; iso: string; seen: number }
interface Room { host: number; peers: Map<number, Member>; last: number }

export function startRelay(opts: { port?: number; channel?: string; quiet?: boolean } = {}) {
  const ISO = crypto.randomUUID();
  const log = (...a: unknown[]) => { if (!opts.quiet) console.log(`[${ISO.slice(0, 4)}]`, ...a); };
  const bus = new BroadcastChannel(opts.channel ?? "vetrun-relay-v1");
  const rooms = new Map<string, Room>();          // rooms OWNED by this isolate
  const local = new Map<string, Set<Sock>>();     // room code -> this isolate's sockets in it
  const remoteRooms = new Map<string, number>();  // code -> last beat, rooms owned elsewhere
  const takenElsewhere = new Map<string, number>(); // code -> ms, codes other isolates hold
  const pending = new Map<string, (r: Json) => void>();

  const newId = () => 1 + Math.floor(Math.random() * 2147483646); // unique across isolates (practically)
  const validCode = (c: string) => /^[A-HJKM-Z]{3}[2-9]{2}$/.test(c);
  const send = (s: Sock | null | undefined, obj: Json) => {
    if (s && s.ws.readyState === WebSocket.OPEN) { try { s.ws.send(JSON.stringify(obj)); } catch { /* closing */ } }
  };
  const post = (obj: Json) => { try { bus.postMessage(obj); } catch { /* channel closed */ } };
  const addLocal = (s: Sock) => { let set = local.get(s.room); if (!set) local.set(s.room, set = new Set()); set.add(s); };
  const dropLocal = (s: Sock) => { const set = local.get(s.room); if (set) { set.delete(s); if (!set.size) local.delete(s.room); } };
  const leaveRoom = (s: Sock) => { dropLocal(s); s.room = ""; s.host = 0; s.owned = false; };

  function newCode(): string {
    for (;;) {
      let c = "";
      for (let i = 0; i < 3; i++) c += LETTERS[Math.floor(Math.random() * LETTERS.length)];
      for (let i = 0; i < 2; i++) c += DIGITS[Math.floor(Math.random() * DIGITS.length)];
      if (!rooms.has(c) && !remoteRooms.has(c) && !takenElsewhere.has(c)) return c;
    }
  }

  // Deliver a forwarded message to this isolate's sockets in the room (same filter as relay.js).
  function deliver(code: string, from: number, to: unknown, data: unknown) {
    const set = local.get(code);
    if (!set) return;
    const own = rooms.get(code);
    if (own) { if (!own.peers.has(from)) return; own.last = Date.now(); }
    const out = { t: "msg", from, data };
    for (const p of set) {
      if (p.id === from) continue;
      if (to === "all" || to === undefined || (to === "host" && p.id === p.host) || to === p.id) send(p, out);
    }
  }

  function closeGuestsLocally(code: string) {
    const set = local.get(code);
    if (!set) return;
    for (const p of [...set]) {
      send(p, { t: "room_closed" });
      leaveRoom(p);
      setTimeout(() => { try { p.ws.close(1000, "room closed"); } catch { /* gone */ } }, 1500);
    }
  }

  // Owner side: a guest (local or remote) wants in.
  function admit(code: string, peer: number, iso: string, sock: Sock | null): Json {
    const r = rooms.get(code);
    if (!r) return { ok: false, reason: "no_such_room" };
    if (r.peers.size >= MAX_PEERS) return { ok: false, reason: "room_full" };
    r.peers.set(peer, { sock, iso, seen: Date.now() });
    r.last = Date.now();
    log(`peer ${peer} joined ${code}${sock ? "" : " (other isolate)"}`);
    send(r.peers.get(r.host)?.sock, { t: "peer_joined", peer });
    return { ok: true, host: r.host };
  }

  // Owner side: a member is gone.
  function memberGone(code: string, peer: number) {
    const r = rooms.get(code);
    if (!r || !r.peers.has(peer)) return;
    r.peers.delete(peer);
    if (r.host === peer) {
      rooms.delete(code);
      post({ op: "closed", code });
      closeGuestsLocally(code);
      log(`host left, room ${code} closed`);
    } else {
      send(r.peers.get(r.host)?.sock, { t: "peer_left", peer });
      log(`peer ${peer} left ${code}`);
    }
  }

  bus.onmessage = (ev: MessageEvent) => {
    const m = ev.data as Json;
    if (!m || typeof m !== "object") return;
    const code = String(m.code ?? "");
    switch (m.op) {
      case "join_req": {
        if (!rooms.has(code)) return; // not ours; the owner (if any) answers
        const res = admit(code, Number(m.peer), String(m.iso), null);
        post({ op: "join_res", req: m.req, ...res });
        return;
      }
      case "join_res": { const f = pending.get(String(m.req)); if (f) { pending.delete(String(m.req)); f(m); } return; }
      case "fwd": deliver(code, Number(m.from), m.to, m.data); return;
      case "leave": memberGone(code, Number(m.peer)); return;
      case "closed": remoteRooms.delete(code); takenElsewhere.delete(code); closeGuestsLocally(code); return;
      case "opened": takenElsewhere.set(code, Date.now()); return;
      case "beat": {
        const now = Date.now();
        for (const c of (m.owned as string[]) ?? []) { takenElsewhere.set(c, now); if (remoteRooms.has(c)) remoteRooms.set(c, now); }
        for (const [c, p] of (m.guests as [string, number][]) ?? []) { const mem = rooms.get(c)?.peers.get(p); if (mem) mem.seen = now; }
        return;
      }
    }
  };

  async function onMessage(s: Sock, raw: string) {
    let m: Json;
    try { m = JSON.parse(raw); } catch { return send(s, { t: "error", reason: "bad_message" }); }
    if (!m || typeof m !== "object") return send(s, { t: "error", reason: "bad_message" });
    const own = s.room ? rooms.get(s.room) : undefined;
    if (own) own.last = Date.now();
    switch (m.t) {
      case "ping": return send(s, { t: "pong" });
      case "host": {
        if (m.v !== VERSION) return send(s, { t: "error", reason: "version_mismatch" });
        if (s.room) return send(s, { t: "error", reason: "already_in_room" });
        const code = newCode();
        rooms.set(code, { host: s.id, peers: new Map([[s.id, { sock: s, iso: ISO, seen: Date.now() }]]), last: Date.now() });
        s.room = code; s.host = s.id; s.owned = true; addLocal(s);
        post({ op: "opened", code });
        log(`room ${code} created by ${s.id}`);
        return send(s, { t: "hosted", code, peer: s.id });
      }
      case "join": {
        if (m.v !== VERSION) return send(s, { t: "error", reason: "version_mismatch" });
        if (s.room) return send(s, { t: "error", reason: "already_in_room" });
        const code = String(m.code ?? "").trim().toUpperCase();
        if (!validCode(code)) return send(s, { t: "error", reason: "bad_code" });
        s.room = "joining"; // blocks a second host/join while we wait
        let res: Json;
        if (rooms.has(code)) {
          res = admit(code, s.id, ISO, s);
        } else {
          const req = crypto.randomUUID();
          res = await new Promise<Json>((resolve) => {
            pending.set(req, resolve);
            post({ op: "join_req", req, code, peer: s.id, iso: ISO });
            setTimeout(() => { if (pending.delete(req)) resolve({ ok: false, reason: "no_such_room" }); }, JOIN_TIMEOUT_MS);
          });
          if (res.ok) remoteRooms.set(code, Date.now());
        }
        s.room = "";
        if (!res.ok) return send(s, { t: "error", reason: String(res.reason) });
        if (s.ws.readyState !== WebSocket.OPEN) { // guest vanished while waiting
          if (rooms.has(code)) memberGone(code, s.id); else post({ op: "leave", code, peer: s.id });
          return;
        }
        s.room = code; s.host = Number(res.host); s.owned = rooms.has(code); addLocal(s);
        return send(s, { t: "joined", code, peer: s.id, host: s.host });
      }
      case "send": {
        if (!s.room || s.room === "joining") return send(s, { t: "error", reason: "not_in_room" });
        const data = m.data || {};
        deliver(s.room, s.id, m.to, data);
        const r = rooms.get(s.room);
        const remote = !r || [...r.peers.values()].some((p) => p.iso !== ISO);
        if (remote) post({ op: "fwd", code: s.room, from: s.id, to: m.to, data });
        return;
      }
      default: return send(s, { t: "error", reason: "unknown_type" });
    }
  }

  function onClose(s: Sock) {
    const code = s.room;
    if (!code || code === "joining") return;
    leaveRoom(s);
    if (rooms.has(code)) memberGone(code, s.id);
    else post({ op: "leave", code, peer: s.id });
  }

  const timer = setInterval(() => {
    const now = Date.now();
    const guests: [string, number][] = [];
    for (const [code, set] of local) for (const p of set) if (!p.owned) guests.push([code, p.id]);
    post({ op: "beat", iso: ISO, owned: [...rooms.keys()], guests });
    for (const [code, r] of rooms) {
      if (now - r.last > ROOM_IDLE_MS) {
        for (const p of r.peers.values()) { try { p.sock?.ws.close(1000, "idle"); } catch { /* gone */ } }
        rooms.delete(code); post({ op: "closed", code }); continue;
      }
      for (const [id, p] of r.peers) if (p.iso !== ISO && now - p.seen > STALE_MS) memberGone(code, id);
    }
    for (const [code, t] of remoteRooms) if (now - t > STALE_MS) { remoteRooms.delete(code); closeGuestsLocally(code); }
    for (const [code, t] of takenElsewhere) if (now - t > STALE_MS) takenElsewhere.delete(code);
  }, BEAT_MS);

  const handler = (req: Request): Response => {
    if ((req.headers.get("upgrade") ?? "").toLowerCase() !== "websocket") {
      return new Response("vetrun relay ok, rooms=" + rooms.size + "\n", {
        headers: { "content-type": "text/plain", "access-control-allow-origin": "*" },
      });
    }
    const { socket, response } = Deno.upgradeWebSocket(req, { idleTimeout: 30 }); // server pings keep proxies awake
    const s: Sock = { id: newId(), ws: socket, room: "", host: 0, owned: false };
    socket.onmessage = (ev) => {
      const raw = typeof ev.data === "string" ? ev.data : new TextDecoder().decode(ev.data as ArrayBuffer);
      if (raw.length > MAX_BYTES) { try { socket.close(1009, "too big"); } catch { /* gone */ } return; }
      onMessage(s, raw).catch((e) => log("message error", e));
    };
    socket.onclose = () => onClose(s);
    socket.onerror = () => { /* close follows */ };
    return response;
  };

  const server = opts.port === undefined ? Deno.serve(handler) : Deno.serve({
    port: opts.port,
    onListen: ({ port }) => log(`vetrun relay listening on :${port}`),
  }, handler);
  return { server, stop: async () => { clearInterval(timer); bus.close(); await server.shutdown(); } };
}

if (import.meta.main) {
  // On Deno Deploy no port is needed (the platform routes to Deno.serve); locally PORT or 9080 like relay.js.
  let port: number | undefined = 9080;
  try {
    const p = Deno.env.get("PORT");
    if (p) port = parseInt(p, 10);
    else if (Deno.env.get("DENO_DEPLOYMENT_ID")) port = undefined;
  } catch { /* no env permission: use 9080 */ }
  startRelay({ port });
}
