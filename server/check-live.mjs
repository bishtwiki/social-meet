import assert from 'node:assert/strict';
import { io } from 'socket.io-client';

const url = process.argv[2];
if (!url) throw new Error('Usage: node check-live.mjs https://your-api.onrender.com');
const sockets = [];
function event(socket, name, timeoutMs = 20000) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`${name} timed out`)), timeoutMs);
    socket.once(name, value => { clearTimeout(timer); resolve(value); });
  });
}
async function connect() {
  const socket = io(url, { autoConnect: false, transports: ['websocket'], reconnection: false });
  sockets.push(socket);
  const ready = event(socket, 'session:ready');
  socket.connect();
  await ready;
  return socket;
}
const emit = (socket, name, payload) => socket.timeout(10000).emitWithAck(name, payload);
try {
  const health = await (await fetch(`${url}/health`)).json();
  assert.equal(health.ok, true);
  for (const mode of ['text', 'video']) {
    const a = await connect(), b = await connect();
    const first = event(a, 'match:found'), second = event(b, 'match:found');
    // Unique interests keep this diagnostic pair separate from real searches.
    const interests = [`diagnostic-${crypto.randomUUID()}`];
    for (const socket of [a, b]) {
      assert.equal((await emit(socket, 'queue:join', {
        mode, interests, interestMode: 'strict', adultConfirmed: true,
        requestId: crypto.randomUUID(),
      })).ok, true);
    }
    const [ma, mb] = await Promise.all([first, second]);
    assert.equal(ma.roomId, mb.roomId);
    const message = event(b, 'chat:message');
    assert.equal((await emit(a, 'chat:message', {
      roomId: ma.roomId, text: 'Deployment connection check', clientMessageId: crypto.randomUUID(),
    })).ok, true);
    assert.equal((await message).text, 'Deployment connection check');
    if (mode === 'video') {
      const signal = event(b, 'signal');
      assert.equal((await emit(a, 'signal', {
        roomId: ma.roomId, description: { type: 'offer', sdp: 'diagnostic-only' },
      })).ok, true);
      assert.equal((await signal).description.type, 'offer');
    }
    const ended = event(b, 'match:ended');
    await emit(a, 'match:end', { roomId: ma.roomId });
    await ended;
    if (mode === 'text') {
      const rematchA = event(a, 'match:found', 45000);
      const rematchB = event(b, 'match:found', 45000);
      for (const socket of [a, b]) {
        assert.equal((await emit(socket, 'queue:join', {
          mode, interests, interestMode: 'strict', adultConfirmed: true,
          requestId: crypto.randomUUID(),
        })).ok, true);
      }
      const [ra, rb] = await Promise.all([rematchA, rematchB]);
      assert.equal(ra.roomId, rb.roomId);
      assert.notEqual(ra.roomId, ma.roomId);
      await emit(a, 'match:end', { roomId: ra.roomId });
      console.log('Previous pair rematched automatically after the cooldown');
    }
    a.disconnect(); b.disconnect();
    console.log(`${mode}: WebSocket, matching, message delivery, and cleanup passed`);
  }
  const ice = await (await fetch(`${url}/ice-servers`)).json();
  console.log('TURN configured:', ice.iceServers.some(s => [].concat(s.urls).some(u => /^turns?:/.test(u))));
  console.log('Camera/audio media transport requires a separate two-device test.');
} finally {
  sockets.forEach(socket => socket.disconnect());
}
