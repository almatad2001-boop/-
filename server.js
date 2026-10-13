const http = require('node:http');
const fs = require('node:fs');
const path = require('node:path');
const { WebSocketServer, WebSocket } = require('ws');

const root = __dirname;
const port = Number(process.env.PORT) || 3000;
const rooms = new Map();
const contentTypes = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript; charset=utf-8', '.css': 'text/css; charset=utf-8', '.json': 'application/json; charset=utf-8', '.svg': 'image/svg+xml', '.png': 'image/png', '.jpg': 'image/jpeg', '.webp': 'image/webp' };
const server = http.createServer((req, res) => {
  const pathname = decodeURIComponent(new URL(req.url, 'http://localhost').pathname);
  const requested = pathname === '/' ? 'index.html' : pathname.replace(/^\/+/, '');
  const file = path.resolve(root, requested);
  if (!file.startsWith(root + path.sep) && file !== path.join(root, 'index.html')) { res.writeHead(403).end(); return; }
  fs.readFile(file, (error, data) => {
    if (error) { res.writeHead(404).end('Not found'); return; }
    res.writeHead(200, { 'Content-Type': contentTypes[path.extname(file)] || 'application/octet-stream', 'Cache-Control': 'no-cache' });
    res.end(data);
  });
});
const wss = new WebSocketServer({ server });
const send = (socket, payload) => { if (socket?.readyState === WebSocket.OPEN) socket.send(JSON.stringify(payload)); };
const broadcast = (room, payload) => room.members.forEach(member => send(member.socket, payload));
const roomCode = () => Math.random().toString(36).slice(2, 7).toUpperCase();
function lobby(room) {
  return { code: room.code, hostId: room.hostId, started: !!room.snapshot, revision: room.revision, players: room.members.map(({ id, name, online }) => ({ id, name, online })) };
}
function announce(room) { broadcast(room, { type: 'lobby', ...lobby(room) }); }

wss.on('connection', socket => {
  socket.on('message', raw => {
    let message;
    try { message = JSON.parse(raw.toString()); } catch { return; }
    if (message.type === 'create') {
      let code; do { code = roomCode(); } while (rooms.has(code));
      const room = { code, hostId: null, members: [], snapshot: null, revision: 0 };
      const member = { id: 0, name: String(message.name || 'اللاعب 1').slice(0, 18), token: message.token, socket, online: true };
      room.hostId = member.token; room.members.push(member); rooms.set(code, room); socket.roomCode = code; socket.token = member.token;
      send(socket, { ...lobby(room), type: 'joined', playerId: member.id, host: true }); announce(room); return;
    }
    const code = String(message.code || socket.roomCode || '').toUpperCase();
    const room = rooms.get(code);
    if (!room) { send(socket, { type: 'error', message: 'لم يتم العثور على الغرفة.' }); return; }
    if (message.type === 'join') {
      let member = room.members.find(item => item.token === message.token);
      if (!member) {
        if (room.snapshot) { send(socket, { type: 'error', message: 'بدأت المباراة ولا يمكن الانضمام إليها الآن.' }); return; }
        if (room.members.length >= 6) { send(socket, { type: 'error', message: 'الغرفة ممتلئة (6 لاعبين كحد أقصى).' }); return; }
        member = { id: room.members.length, name: String(message.name || `لاعب ${room.members.length + 1}`).slice(0, 18), token: message.token, socket, online: true };
        room.members.push(member);
      } else { member.name = String(message.name || member.name).slice(0, 18); member.socket = socket; member.online = true; }
      socket.roomCode = code; socket.token = member.token;
      send(socket, { ...lobby(room), type: 'joined', playerId: member.id, host: member.token === room.hostId, snapshot: room.snapshot }); announce(room); return;
    }
    const member = room.members.find(item => item.token === socket.token);
    if (!member) return;
    if (message.type === 'start') {
      if (member.token !== room.hostId || room.snapshot) return;
      const onlineMembers = room.members.filter(item => item.online);
      if (onlineMembers.length < 2) { send(socket, { type: 'error', message: 'يلزم لاعبان على الأقل لبدء مباراة أونلاين.' }); return; }
      if (onlineMembers.length !== room.members.length) { send(socket, { type: 'error', message: 'أعد اتصال جميع اللاعبين قبل بدء المباراة، أو أنشئ غرفة جديدة.' }); return; }
      room.snapshot = message.snapshot; room.revision = 1; broadcast(room, { type: 'snapshot', revision: room.revision, snapshot: room.snapshot }); send(socket, { type: 'ack', revision: room.revision, started: true }); announce(room); return;
    }
    if (message.type === 'state') {
      if (!room.snapshot || Number(message.revision) !== room.revision) { send(socket, { type: 'stale', revision: room.revision, snapshot: room.snapshot }); return; }
      const active = Number(room.snapshot.turn);
      if (member.id !== active) { send(socket, { type: 'error', message: 'هذا ليس دورك.' }); return; }
      room.snapshot = message.snapshot; room.revision++;
      room.members.forEach(item => { if (item !== member) send(item.socket, { type: 'snapshot', revision: room.revision, snapshot: room.snapshot, from: member.id }); });
      send(socket, { type: 'ack', revision: room.revision }); return;
    }
    if (message.type === 'decision') {
      if (!room.snapshot || Number(room.snapshot.turn) !== member.id) { send(socket, { type: 'error', message: 'لا يمكنك إرسال هذا الطلب الآن.' }); return; }
      const target = room.members.find(item => item.id === Number(message.to));
      if (!target?.online) { send(socket, { type: 'decisionResult', id: message.id, value: null, error: 'اللاعب غير متصل.' }); return; }
      send(target.socket, { type: 'decisionRequest', id: message.id, from: member.id, fromName: member.name, decision: message.decision }); return;
    }
    if (message.type === 'decisionResult') {
      const target = room.members.find(item => item.id === Number(message.to));
      send(target?.socket, { type: 'decisionResult', id: message.id, value: message.value });
    }
  });
  socket.on('close', () => {
    const room = rooms.get(socket.roomCode); if (!room) return;
    const member = room.members.find(item => item.token === socket.token); if (member && member.socket === socket) member.online = false;
    announce(room);
    if (room.members.every(item => !item.online)) setTimeout(() => { if (room.members.every(item => !item.online)) rooms.delete(room.code); }, 30 * 60 * 1000);
  });
});
server.listen(port, '0.0.0.0', () => console.log(`Raya Risk online server listening on ${port}`));
