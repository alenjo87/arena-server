// Arena Shooter - room relay server (Node + ws)
// Env vars: ADMIN_KEY (your secret), MAINTENANCE=true (optional initial state), PORT
// The game stays client-side; this server relays each player's state to everyone in the same room,
// and holds the maintenance switch + admin controls.
const http = require('http');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const ADMIN_KEY = process.env.ADMIN_KEY || '';
let maintenance = process.env.MAINTENANCE === 'true';

const MAX_ROOM = 12, MAX_TOTAL = 100, SEND_HZ = 15;
const rooms = new Map();             // code -> Map(id -> client)
let nextId = 1;

const send = (ws, o) => { if (ws.readyState === 1) ws.send(JSON.stringify(o)); };
const num = (v, lo, hi, d = 0) => { v = Number(v); return Number.isFinite(v) ? Math.min(hi, Math.max(lo, v)) : d; };
const txt = (v, n) => (typeof v === 'string' ? v.replace(/[^\w \-]/g, '').slice(0, n) : '');
const PEER = /^p\d+$/;

// Only known fields, clamped, so a modified client can't send junk to other players.
function clean(p) {
  if (!p || typeof p !== 'object') return null;
  const dd = [];
  if (Array.isArray(p.dd)) {
    for (let i = 0; i + 1 < Math.min(p.dd.length, 40); i += 2) {
      if (typeof p.dd[i] === 'string' && PEER.test(p.dd[i])) dd.push(p.dd[i], num(p.dd[i + 1], 0, 1e6));
    }
  }
  return {
    n: txt(p.n, 14) || 'Player',
    x: num(p.x, -40, 40), z: num(p.z, -40, 40), py: num(p.py, -5, 10), yw: num(p.yw, -1e5, 1e5),
    a: p.a === 0 ? 0 : 1, hp: num(p.hp, 0, 200), k: num(p.k, 0, 1e4), d: num(p.d, 0, 1e4),
    kb: typeof p.kb === 'string' && PEER.test(p.kb) ? p.kb : '', dd, w: p.w === 1 ? 1 : 0,
  };
}

function totalClients() { let n = 0; for (const r of rooms.values()) n += r.size; return n; }

setInterval(() => {
  for (const room of rooms.values()) {
    if (room.size < 2) continue;
    const list = [];
    for (const c of room.values()) if (c.presence) list.push({ peer: c.peer, p: c.presence });
    const str = JSON.stringify({ type: 'peers', list });
    for (const c of room.values()) if (c.ws.readyState === 1) c.ws.send(str);
  }
}, 1000 / SEND_HZ);

// ---- admin ----
function okKey(k) {
  if (!ADMIN_KEY || typeof k !== 'string') return false;
  const a = crypto.createHash('sha256').update(k).digest();
  const b = crypto.createHash('sha256').update(ADMIN_KEY).digest();
  return crypto.timingSafeEqual(a, b);
}
function allClients() { const a = []; for (const r of rooms.values()) for (const c of r.values()) a.push(c); return a; }
function playerList() {
  return allClients().map(c => ({ id: c.id, name: `${c.name} [${c.roomCode}]`, alive: false }));
}
function handleAdmin(ws, m) {
  if (!okKey(m.key)) { setTimeout(() => send(ws, { type: 'admin', ok: false, msg: 'Wrong key' }), 500); return; }
  let msg = 'ok';
  switch (m.cmd) {
    case 'maintenance':
      maintenance = !!m.value;
      if (maintenance) for (const c of allClients()) { send(c.ws, { type: 'maintenance' }); c.ws.close(); }
      msg = 'Maintenance is now ' + (maintenance ? 'ON (players kicked, game locked)' : 'OFF (game is open)');
      break;
    case 'kick': {
      const c = allClients().find(x => x.id === Number(m.id));
      if (c) { send(c.ws, { type: 'kicked' }); c.ws.close(); msg = 'Kicked ' + c.name; } else msg = 'Player not found';
      break;
    }
    case 'list': msg = 'Players online: ' + totalClients(); break;
    case 'status': msg = `Maintenance: ${maintenance ? 'ON' : 'OFF'}, players: ${totalClients()}, rooms: ${rooms.size}`; break;
    default: msg = 'Not used in this game';
  }
  send(ws, { type: 'admin', ok: true, msg, players: playerList(), maintenance });
}

// ---- http + websocket ----
const server = http.createServer((req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.url === '/status') {
    res.setHeader('Content-Type', 'application/json');
    res.setHeader('Cache-Control', 'no-store');
    res.end(JSON.stringify({ maintenance, players: totalClients() }));
  } else res.end('Arena server running');
});
const wss = new WebSocketServer({ server, maxPayload: 4096 });

wss.on('connection', ws => {
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });
  let me = null, count = 0;
  const rl = setInterval(() => { count = 0; }, 1000);

  ws.on('message', raw => {
    if (++count > 60) return;
    let m; try { m = JSON.parse(raw); } catch { return; }
    if (!m || typeof m !== 'object') return;

    if (m.type === 'admin') return handleAdmin(ws, m);

    if (m.type === 'join' && !me) {
      if (maintenance) { send(ws, { type: 'maintenance' }); ws.close(); return; }
      const code = String(m.room || 'lobby').toLowerCase().replace(/[^a-z0-9_.-]/g, '').slice(0, 40) || 'lobby';
      let room = rooms.get(code);
      if ((room && room.size >= MAX_ROOM) || totalClients() >= MAX_TOTAL) { send(ws, { type: 'full' }); ws.close(); return; }
      if (!room) { room = new Map(); rooms.set(code, room); }
      const id = nextId++;
      me = { id, peer: 'p' + id, ws, name: txt(m.name, 14) || 'Player', roomCode: code, presence: null };
      room.set(id, me);
      send(ws, { type: 'joined', id: me.peer });
      return;
    }
    if (m.type === 'pres' && me) {
      const p = clean(m.p);
      if (p) me.presence = p;
    }
  });
  ws.on('close', () => {
    clearInterval(rl);
    if (me) {
      const room = rooms.get(me.roomCode);
      if (room) { room.delete(me.id); if (!room.size) rooms.delete(me.roomCode); }
    }
  });
  ws.on('error', () => {});
});
setInterval(() => {
  for (const ws of wss.clients) { if (!ws.isAlive) { ws.terminate(); continue; } ws.isAlive = false; ws.ping(); }
}, 30000);

server.listen(PORT, () => console.log('Server on port ' + PORT + (ADMIN_KEY ? '' : ' (WARNING: ADMIN_KEY not set, admin disabled)')));
