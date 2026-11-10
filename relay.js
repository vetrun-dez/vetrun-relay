// Vet Run online co-op relay (v0.21.25). Zero game logic: room codes + message forwarding.
// Run:  npm install && node relay.js        (PORT env var, default 9080)
// Health check: GET /  -> "vetrun relay ok"
'use strict';
const http = require('http');
const { WebSocketServer } = require('ws');

const PORT = parseInt(process.env.PORT || '9080', 10);
const VERSION = 1;
const MAX_PEERS = 2;
const MAX_BYTES = 65536;
const LETTERS = 'ABCDEFGHJKMNOPQRSTUVWXYZ';
const DIGITS = '23456789';
const ROOM_IDLE_MS = 2 * 60 * 60 * 1000; // drop rooms idle for 2 h

const rooms = new Map(); // code -> {host:id, peers:Map(id->ws), last:ms}
let nextId = 1;

function newCode() {
  for (;;) {
    let c = '';
    for (let i = 0; i < 3; i++) c += LETTERS[Math.floor(Math.random() * LETTERS.length)];
    for (let i = 0; i < 2; i++) c += DIGITS[Math.floor(Math.random() * DIGITS.length)];
    if (!rooms.has(c)) return c;
  }
}
function validCode(c) { return /^[A-HJKM-Z]{3}[2-9]{2}$/.test(c); }
function send(ws, obj) { if (ws && ws.readyState === 1) ws.send(JSON.stringify(obj)); }

const server = http.createServer((req, res) => {
  res.writeHead(200, { 'content-type': 'text/plain', 'access-control-allow-origin': '*' });
  res.end('vetrun relay ok, rooms=' + rooms.size + '\n');
});
const wss = new WebSocketServer({ server, maxPayload: MAX_BYTES });

wss.on('connection', (ws) => {
  ws.id = nextId++;
  ws.room = '';
  ws.alive = true;
  ws.on('pong', () => { ws.alive = true; });
  ws.on('message', (buf) => {
    ws.alive = true;
    let m;
    try { m = JSON.parse(buf.toString()); } catch (e) { return send(ws, { t: 'error', reason: 'bad_message' }); }
    if (!m || typeof m !== 'object') return send(ws, { t: 'error', reason: 'bad_message' });
    const room = ws.room ? rooms.get(ws.room) : null;
    if (room) room.last = Date.now();
    switch (m.t) {
      case 'ping': return send(ws, { t: 'pong' });
      case 'host': {
        if (m.v !== VERSION) return send(ws, { t: 'error', reason: 'version_mismatch' });
        if (ws.room) return send(ws, { t: 'error', reason: 'already_in_room' });
        const code = newCode();
        rooms.set(code, { host: ws.id, peers: new Map([[ws.id, ws]]), last: Date.now() });
        ws.room = code;
        console.log(`room ${code} created by ${ws.id}`);
        return send(ws, { t: 'hosted', code, peer: ws.id });
      }
      case 'join': {
        if (m.v !== VERSION) return send(ws, { t: 'error', reason: 'version_mismatch' });
        if (ws.room) return send(ws, { t: 'error', reason: 'already_in_room' });
        const code = String(m.code || '').trim().toUpperCase();
        if (!validCode(code)) return send(ws, { t: 'error', reason: 'bad_code' });
        const r = rooms.get(code);
        if (!r) return send(ws, { t: 'error', reason: 'no_such_room' });
        if (r.peers.size >= MAX_PEERS) return send(ws, { t: 'error', reason: 'room_full' });
        r.peers.set(ws.id, ws);
        ws.room = code;
        console.log(`peer ${ws.id} joined ${code}`);
        send(ws, { t: 'joined', code, peer: ws.id, host: r.host });
        return send(r.peers.get(r.host), { t: 'peer_joined', peer: ws.id });
      }
      case 'send': {
        if (!room) return send(ws, { t: 'error', reason: 'not_in_room' });
        const out = { t: 'msg', from: ws.id, data: m.data || {} };
        for (const [id, p] of room.peers) {
          if (id === ws.id) continue;
          if (m.to === 'all' || m.to === undefined || (m.to === 'host' && id === room.host) || m.to === id) send(p, out);
        }
        return;
      }
      default: return send(ws, { t: 'error', reason: 'unknown_type' });
    }
  });
  ws.on('close', () => {
    const r = ws.room ? rooms.get(ws.room) : null;
    if (!r) return;
    r.peers.delete(ws.id);
    if (r.host === ws.id) {
      rooms.delete(ws.room);
      for (const p of r.peers.values()) { send(p, { t: 'room_closed' }); p.room = ''; setTimeout(() => p.close(1000, 'room closed'), 1500); }
      console.log(`host left, room ${ws.room} closed`);
    } else {
      send(r.peers.get(r.host), { t: 'peer_left', peer: ws.id });
      console.log(`peer ${ws.id} left ${ws.room}`);
    }
  });
});

// keep-alive pings (proxies such as Cloudflare/Render drop idle sockets) + idle room cleanup
setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.alive) { ws.terminate(); continue; }
    ws.alive = false;
    try { ws.ping(); } catch (e) {}
  }
  const now = Date.now();
  for (const [code, r] of rooms) if (now - r.last > ROOM_IDLE_MS) { for (const p of r.peers.values()) p.close(1000, 'idle'); rooms.delete(code); }
}, 25000);

server.listen(PORT, () => console.log(`vetrun relay listening on :${PORT}`));
