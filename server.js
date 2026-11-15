// Servidor de sinalização: salas, aprovação de entrada pelo dono, chat, reações.
// O vídeo/áudio vai direto entre os navegadores (WebRTC).
// TURN opcional: variáveis TURN_URL, TURN_USER, TURN_PASS.
const http = require('http'), fs = require('fs'), path = require('path');
const { WebSocketServer } = require('ws');
const J = JSON.stringify;
const ice = () => ({ iceServers: [{ urls: 'stun:stun.l.google.com:19302' },
  ...(process.env.TURN_URL ? [{ urls: process.env.TURN_URL, username: process.env.TURN_USER, credential: process.env.TURN_PASS }] : [])] });
const pub = path.join(__dirname, 'public');
const types = { '.gif': 'image/gif', '.png': 'image/png', '.jpg': 'image/jpeg', '.jpeg': 'image/jpeg',
  '.webp': 'image/webp', '.svg': 'image/svg+xml', '.ico': 'image/x-icon' };

const srv = http.createServer((q, r) => {
  let u = '/'; try { u = decodeURIComponent(q.url.split('?')[0]); } catch {}
  if (u === '/ice') { r.writeHead(200, { 'Content-Type': 'application/json' }); return r.end(J(ice())); }
  const f = path.join(pub, path.normalize(u));
  if (f.startsWith(pub + path.sep) && types[path.extname(f)] && fs.existsSync(f) && fs.statSync(f).isFile()) {
    r.writeHead(200, { 'Content-Type': types[path.extname(f)], 'Cache-Control': 'public, max-age=3600' });
    return fs.createReadStream(f).pipe(r);
  }
  r.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
  r.end(fs.readFileSync(path.join(pub, 'index.html')));
});

const wss = new WebSocketServer({ server: srv });
const rooms = new Map();
let n = 0;
// só aceita imagens PNG/JPG/GIF/WebP em base64 e de tamanho limitado (evita SVG/scripts)
const okAv = u => /^data:image\/(png|jpe?g|gif|webp);base64,[A-Za-z0-9+/=]+$/.test(u) && u.length <= 650000 ? u : '';
const all = (set, o) => set.forEach(w => w.send(J(o)));

function enter(ws, set) {
  if (!set.size) set.owner = ws.id;
  set.ok.add(ws.cid); ws.muted = set.muted.has(ws.cid);
  ws.send(J({ t: 'welcome', id: ws.id, owner: set.owner, restrict: set.restrict, muted: ws.muted,
    peers: [...set.values()].map(w => ({ id: w.id, name: w.name, av: w.av, muted: w.muted })) }));
  all(set, { t: 'join', id: ws.id, name: ws.name, av: ws.av, muted: ws.muted });
  set.set(ws.id, ws);
}

wss.on('connection', (ws, req) => {
  ws.ip = String(req.headers['x-forwarded-for'] || req.socket.remoteAddress || '').split(',')[0].trim();
  ws.on('message', raw => {
    let m; try { m = JSON.parse(raw); } catch { return; }
    if (m.t === 'join' && !ws.room) {
      const room = String(m.room || '').toUpperCase().slice(0, 40);
      ws.name = String(m.name || 'Anônimo').slice(0, 24); ws.av = okAv(String(m.av || ''));
      ws.id = String(++n); ws.cid = String(m.cid || 'x' + n).slice(0, 60);
      let set = rooms.get(room);
      if (!set) { set = Object.assign(new Map(), { owner: null, restrict: false, ok: new Set(), pend: new Map(), ban: new Set(), banip: new Set(), muted: new Set() }); rooms.set(room, set); }
      if (set.ban.has(ws.cid) || (ws.ip && set.banip.has(ws.ip))) { ws.send(J({ t: 'banned' })); return ws.close(); }
      ws.room = room;
      if (!set.size || set.ok.has(ws.cid)) return enter(ws, set);   // 1º a entrar vira dono; já aprovados voltam direto
      ws.pending = true; set.pend.set(ws.id, ws);
      ws.send(J({ t: 'wait' }));
      const o = set.get(set.owner); o && o.send(J({ t: 'req', id: ws.id, name: ws.name, av: ws.av }));
      return;
    }
    const set = rooms.get(ws.room); if (!set || ws.pending) return;
    const isOwner = ws.id === set.owner;
    if (m.t === 'sig') { const w = set.get(m.to); w && w.send(J({ t: 'sig', from: ws.id, d: m.d })); }
    else if (m.t === 'chat') all(set, { t: 'chat', id: ws.id, name: ws.name, text: String(m.text).slice(0, 500), ts: Date.now() });
    else if (m.t === 'react') all(set, { t: 'react', e: String(m.e).slice(0, 4) });
    else if (!isOwner) return;
    else if (m.t === 'allow') {
      const w = set.pend.get(m.id); if (!w) return;
      set.pend.delete(m.id); w.pending = false;
      if (m.ok) enter(w, set); else { w.send(J({ t: 'denied' })); w.close(); }
    }
    else if (m.t === 'restrict') { set.restrict = !!m.v; all(set, { t: 'cfg', restrict: set.restrict }); }
    else if (m.t === 'mod') {
      const w = set.get(m.id); if (!w || w === ws) return;
      if (m.a === 'owner') { set.owner = m.id; all(set, { t: 'owner', id: m.id }); }
      else if (m.a === 'stop') w.send(J({ t: 'mod', a: 'stop' }));
      else if (m.a === 'mute' || m.a === 'unmute') {   // silenciar "estilo Discord": só o dono libera
        const v = m.a === 'mute'; w.muted = v; v ? set.muted.add(w.cid) : set.muted.delete(w.cid);
        all(set, { t: 'muted', id: w.id, v });
      }
    }
    else if (m.t === 'kick') { const w = set.get(m.id); if (w && w !== ws) { set.ok.delete(w.cid); w.send(J({ t: 'kicked' })); w.close(); } }
    else if (m.t === 'ban') {   // banimento vale enquanto a sala existir (por dispositivo e por IP)
      const w = set.get(m.id); if (!w || w === ws) return;
      set.ban.add(w.cid); w.ip && set.banip.add(w.ip); set.ok.delete(w.cid);
      w.send(J({ t: 'banned' })); w.close();
    }
    else if (m.t === 'end') {
      rooms.delete(ws.room);
      const all2 = [...set.values(), ...set.pend.values()];
      all2.forEach(w => { w.send(J({ t: 'ended' })); w.close(); });
    }
  });
  ws.on('close', () => {
    const set = rooms.get(ws.room); if (!set) return;
    if (ws.pending) { set.pend.delete(ws.id); const o = set.get(set.owner); o && o.send(J({ t: 'reqx', id: ws.id })); return; }
    set.delete(ws.id);
    all(set, { t: 'leave', id: ws.id });
    if (!set.size) { set.pend.forEach(w => { w.send(J({ t: 'denied', closed: 1 })); w.close(); }); return rooms.delete(ws.room); }
    if (set.owner === ws.id) {
      set.owner = set.keys().next().value; all(set, { t: 'owner', id: set.owner });
      const o = set.get(set.owner); set.pend.forEach(w => o.send(J({ t: 'req', id: w.id, name: w.name, av: w.av })));
    }
  });
});
const port = process.env.PORT || 3000;
srv.listen(port, () => console.log('Bahamas RP Transmissões em http://localhost:' + port));
