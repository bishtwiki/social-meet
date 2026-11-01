import { test } from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { io } from "socket.io-client";
import { createSocialMeetServer } from "../app.js";
async function service(t, options = {}) {
  const service = createSocialMeetServer(options);
  await new Promise((resolve) =>
    service.server.listen(0, "127.0.0.1", resolve),
  );
  const url = `http://127.0.0.1:${service.server.address().port}`;
  const sockets = [];
  t.after(async () => {
    sockets.forEach((s) => s.disconnect());
    await service.close();
  });
  const connect = (token) =>
    new Promise((resolve, reject) => {
      const socket = io(url, {
        transports: ["websocket"],
        forceNew: true,
        autoConnect: false,
        auth: { sessionToken: token },
      });
      sockets.push(socket);
      socket.matches = [];
      socket.messages = [];
      socket.ends = [];
      socket.waits = [];
      socket.on("match:found", (p) => socket.matches.push(p));
      socket.on("chat:message", (p) => socket.messages.push(p));
      socket.on("match:ended", (p) => socket.ends.push(p));
      socket.on("queue:waiting", (p) => socket.waits.push(p));
      socket.once("connect_error", reject);
      socket.once("session:ready", ({ token }) => {
        socket.token = token;
        resolve(socket);
      });
      socket.connect();
    });
  return {
    ...service,
    url,
    connect,
    health: async () => (await fetch(`${url}/health`)).json(),
  };
}
const emit = (socket, event, payload = {}) =>
  new Promise((resolve, reject) =>
    socket
      .timeout(2000)
      .emit(event, payload, (error, result) =>
        error ? reject(error) : resolve(result),
      ),
  );
const joinQueue = (socket, options = {}) =>
  emit(socket, "queue:join", {
    mode: "text",
    interests: [],
    adultConfirmed: true,
    ...options,
  });
const nextEvent = (socket, event) =>
  new Promise((resolve, reject) => {
    const timer = setTimeout(
      () => reject(new Error(`${event} timed out`)),
      2000,
    );
    socket.once(event, (payload) => {
      clearTimeout(timer);
      resolve(payload);
    });
  });
async function pair(a, b) {
  const first = nextEvent(a, "match:found"),
    second = nextEvent(b, "match:found");
  await joinQueue(a);
  await joinQueue(b);
  await Promise.all([first, second]);
}

test("first stranger waits; incompatible interests do not stall other pairs; modes stay separate", async (t) => {
  const s = await service(t),
    a = await s.connect(),
    b = await s.connect(),
    c = await s.connect(),
    d = await s.connect();
  assert.equal((await joinQueue(a, { interests: ["art"] })).ok, true);
  assert.equal((await s.health()).waiting, 1);
  await joinQueue(b, { mode: "video" });
  await joinQueue(c, { interests: ["music"] });
  const cFound = nextEvent(c, "match:found");
  await joinQueue(d, { interests: [" MUSIC ", "music"] });
  await cFound;
  assert.equal(a.matches.length, 0);
  assert.equal(b.matches.length, 0);
  assert.equal(c.matches.length, 1);
  assert.equal(c.matches[0].roomId, d.matches[0].roomId);
  assert.deepEqual(c.matches[0].sharedInterests, ["music"]);
  assert.equal((await s.health()).waiting, 2);
});

test("messages are acknowledged once, typing is relayed, and other rooms cannot inject traffic", async (t) => {
  const s = await service(t),
    a = await s.connect(),
    b = await s.connect(),
    stranger = await s.connect();
  await pair(a, b);
  const roomId = a.matches[0].roomId;
  const message = {
    roomId,
    text: " Hello world ",
    clientMessageId: "message-1",
  };
  const delivered = nextEvent(b, "chat:message");
  assert.equal((await emit(a, "chat:message", message)).ok, true);
  await delivered;
  assert.equal(b.messages[0].text, "Hello world");
  await emit(a, "chat:message", message);
  assert.equal(b.messages.length, 1);
  assert.equal((await emit(stranger, "chat:message", message)).ok, false);
  assert.equal(
    (
      await emit(stranger, "signal", {
        roomId,
        description: { type: "offer", sdp: "x" },
      })
    ).ok,
    false,
  );
  assert.equal(
    (await emit(a, "chat:message", { ...message, text: "x".repeat(1001) })).ok,
    false,
  );
  const typing = new Promise((resolve) => b.once("chat:typing", resolve));
  a.emit("chat:typing", { roomId, typing: true });
  assert.equal((await typing).typing, true);
});

test("next avoids the last stranger and connects to a new eligible person", async (t) => {
  const s = await service(t),
    a = await s.connect(),
    b = await s.connect(),
    c = await s.connect();
  await pair(a, b);
  const peerEnded = nextEvent(b, "match:ended");
  assert.equal(
    (await emit(a, "match:next", { mode: "text", adultConfirmed: true })).ok,
    true,
  );
  await peerEnded;
  assert.equal(b.ends.at(-1).reason, "next");
  await joinQueue(b);
  assert.equal(a.matches.length, 1);
  const found = nextEvent(a, "match:found");
  await joinQueue(c);
  await found;
  assert.equal(a.matches.length, 2);
  assert.equal(a.matches[1].roomId, c.matches[0].roomId);
  assert.equal(b.matches.length, 1);
});

test("queue cancellation and disconnect clean up without restarting the peer camera", async (t) => {
  const s = await service(t),
    a = await s.connect(),
    b = await s.connect();
  await joinQueue(a);
  await emit(a, "match:end");
  assert.equal((await s.health()).waiting, 0);
  await pair(a, b);
  const ended = nextEvent(b, "match:ended");
  a.disconnect();
  await ended;
  assert.equal(b.ends.at(-1).reason, "disconnected");
  assert.equal((await s.health()).waiting, 0);
  assert.equal((await s.health()).conversations, 0);
});

test("report and block persist across server restart using signed anonymous identity", async (t) => {
  const directory = mkdtempSync(join(tmpdir(), "social-meet-test-"));
  t.after(() => rmSync(directory, { recursive: true, force: true }));
  const s = await service(t, { dataDirectory: directory }),
    a = await s.connect(),
    b = await s.connect();
  await pair(a, b);
  const peerReported = nextEvent(b, "match:ended");
  const report = await emit(a, "moderation:report", {
    roomId: a.matches[0].roomId,
    reason: "Spam or scam",
    details: "Test-only report",
  });
  assert.equal(report.ok, true);
  await peerReported;
  assert.equal(b.ends.at(-1).reason, "reported");
  const persisted = JSON.parse(
    readFileSync(join(directory, "moderation.json"), "utf8"),
  );
  assert.equal(persisted.reports.length, 1);
  assert.equal(persisted.reports[0].id, report.reportId);
  const firstToken = a.token,
    secondToken = b.token;
  a.disconnect();
  b.disconnect();
  const second = await service(t, {
    dataDirectory: directory,
    interestFallbackMs: 30,
  });
  const aa = await second.connect(firstToken),
    bb = await second.connect(secondToken);
  assert.equal(aa.token, firstToken);
  assert.equal(bb.token, secondToken);
  await joinQueue(aa, { interests: ["art"] });
  await joinQueue(bb, { interests: ["music"] });
  await new Promise((resolve) => setTimeout(resolve, 90));
  assert.equal(aa.matches.length, 0);
  assert.equal(bb.matches.length, 0);
  assert.equal((await second.health()).waiting, 2);
});

test("malformed input, missing age confirmation, and forged identity cannot corrupt matching", async (t) => {
  const s = await service(t),
    a = await s.connect();
  assert.equal((await emit(a, "queue:join", null)).ok, false);
  assert.equal((await emit(a, "queue:join", { mode: "text" })).ok, false);
  const forged = await s.connect(
    a.token.slice(0, -1) + (a.token.endsWith("a") ? "b" : "a"),
  );
  assert.notEqual(forged.token, a.token);
  await pair(a, forged);
  assert.equal(a.matches.length, 1);
  const sameIdentity = await s.connect(a.token);
  await emit(a, "match:end");
  await joinQueue(a);
  await joinQueue(sameIdentity);
  assert.equal(sameIdentity.matches.length, 0);
});

test("shared interests take priority over an earlier random stranger", async (t) => {
  const s = await service(t, { interestFallbackMs: 500 }),
    random = await s.connect(),
    artist = await s.connect(),
    anotherArtist = await s.connect();
  await joinQueue(random);
  await joinQueue(artist, { interests: ["art"] });
  assert.equal(random.matches.length, 0);
  assert.equal(artist.waits[0].searchStage, "interests");
  assert.equal(artist.waits[0].interestMode, "prefer");
  assert.equal(
    artist.waits[0].fallbackAt - artist.waits[0].queuedAt,
    500,
  );
  const matched = nextEvent(artist, "match:found");
  await joinQueue(anotherArtist, { interests: [" Art "] });
  await matched;
  assert.equal(random.matches.length, 0);
  assert.equal(artist.matches[0].roomId, anotherArtist.matches[0].roomId);
  assert.equal(artist.matches[0].matchKind, "interests");
  assert.deepEqual(artist.matches[0].sharedInterests, ["art"]);
  assert.equal((await s.health()).waiting, 1);
});

test("preferred interests broaden to random chat after the announced deadline", async (t) => {
  const s = await service(t, { interestFallbackMs: 80 }),
    artist = await s.connect(),
    musician = await s.connect();
  const first = nextEvent(artist, "match:found"),
    second = nextEvent(musician, "match:found");
  await joinQueue(artist, { interests: ["art"] });
  await joinQueue(musician, { interests: ["music"] });
  assert.equal((await s.health()).waiting, 2);
  await Promise.all([first, second]);
  assert.equal(artist.matches[0].roomId, musician.matches[0].roomId);
  assert.equal(artist.matches[0].matchKind, "random");
  assert.deepEqual(artist.matches[0].sharedInterests, []);
  assert.equal(artist.waits.at(-1).searchStage, "random");
  assert.ok(artist.matches[0].createdAt >= artist.waits[0].fallbackAt);
  assert.ok(musician.matches[0].createdAt >= musician.waits[0].fallbackAt);
});

test("strict interests never broaden and can still match a shared interest", async (t) => {
  const s = await service(t, { interestFallbackMs: 30 }),
    artist = await s.connect(),
    random = await s.connect(),
    anotherArtist = await s.connect();
  await joinQueue(artist, { interests: ["art"], interestMode: "strict" });
  await joinQueue(random);
  await new Promise((resolve) => setTimeout(resolve, 90));
  assert.equal(artist.matches.length, 0);
  assert.equal(random.matches.length, 0);
  assert.equal(artist.waits.length, 1);
  assert.equal(artist.waits[0].fallbackAt, null);
  const matched = nextEvent(artist, "match:found");
  await joinQueue(anotherArtist, { interests: ["art"] });
  await matched;
  assert.equal(artist.matches[0].roomId, anotherArtist.matches[0].roomId);
  assert.equal(random.matches.length, 0);
});

test("cancelling an interest search clears its fallback and never requeues it", async (t) => {
  const s = await service(t, { interestFallbackMs: 60 }),
    artist = await s.connect(),
    random = await s.connect();
  await joinQueue(artist, { interests: ["art"] });
  await emit(artist, "match:end");
  await joinQueue(random);
  await new Promise((resolve) => setTimeout(resolve, 110));
  assert.equal(artist.matches.length, 0);
  assert.equal(artist.waits.length, 1);
  assert.equal(random.matches.length, 0);
  assert.equal((await s.health()).waiting, 1);
});

test("random fallback still excludes the same anonymous identity and the last stranger", async (t) => {
  const s = await service(t, { interestFallbackMs: 30 }),
    a = await s.connect(),
    b = await s.connect();
  await pair(a, b);
  await emit(a, "match:end");
  const sameIdentity = await s.connect(a.token);
  await joinQueue(a, { interests: ["art"] });
  await joinQueue(b, { interests: ["music"] });
  await joinQueue(sameIdentity, { interests: ["art"], interestMode: "strict" });
  await new Promise((resolve) => setTimeout(resolve, 90));
  assert.equal(a.matches.length, 1);
  assert.equal(b.matches.length, 1);
  assert.equal(sameIdentity.matches.length, 0);
  assert.equal((await s.health()).waiting, 3);
});

test("search IDs follow each participant and stale Stop requests cannot end newer searches or rooms", async (t) => {
  const s = await service(t),
    a = await s.connect(),
    b = await s.connect(),
    c = await s.connect();
  const firstMatch = nextEvent(a, "match:found");
  await joinQueue(a, { requestId: "a-first" });
  await joinQueue(b, { requestId: "b-first" });
  await firstMatch;
  const oldRoom = a.matches[0].roomId;
  assert.equal(a.waits[0].requestId, "a-first");
  assert.equal(a.matches[0].requestId, "a-first");
  assert.equal(b.matches[0].requestId, "b-first");
  const oldEnded = nextEvent(b, "match:ended");
  await emit(a, "match:end", { requestId: "a-first", roomId: oldRoom });
  assert.equal((await oldEnded).requestId, "b-first");

  await joinQueue(a, { requestId: "a-second" });
  await emit(a, "match:end", { requestId: "a-first" });
  await emit(a, "queue:leave", { requestId: "a-first" });
  assert.equal((await s.health()).waiting, 1);
  const nextMatch = nextEvent(a, "match:found");
  await joinQueue(c, { requestId: "c-first" });
  await nextMatch;
  const currentRoom = a.matches[1].roomId;
  assert.equal(a.matches[1].requestId, "a-second");
  assert.equal(c.matches[0].roomId, currentRoom);
  await emit(a, "match:end", { requestId: "a-first", roomId: currentRoom });
  await emit(a, "match:end", { requestId: "a-second", roomId: oldRoom });
  assert.equal(c.ends.length, 0);
  assert.equal((await s.health()).conversations, 1);
  const currentEnded = nextEvent(c, "match:ended");
  await emit(a, "match:end", { requestId: "a-second", roomId: currentRoom });
  assert.equal((await currentEnded).requestId, "c-first");
  assert.equal((await s.health()).conversations, 0);
});

test("search IDs are bounded and legacy queue cancellation still acknowledges", async (t) => {
  const s = await service(t),
    a = await s.connect();
  for (const requestId of ["", "x".repeat(101), 123, {}])
    assert.equal((await joinQueue(a, { requestId })).ok, false);
  assert.equal((await s.health()).waiting, 0);
  await joinQueue(a);
  const cancelled = await new Promise((resolve) => a.emit("queue:leave", resolve));
  assert.equal(cancelled.ok, true);
  assert.equal((await s.health()).waiting, 0);
});
