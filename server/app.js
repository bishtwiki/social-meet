import express from "express";
import http from "node:http";
import { createHmac, randomUUID, timingSafeEqual } from "node:crypto";
import {
  mkdirSync,
  readFileSync,
  writeFileSync,
  renameSync,
  existsSync,
} from "node:fs";
import { join } from "node:path";
import cors from "cors";
import helmet from "helmet";
import { Server } from "socket.io";

// One-process local service. State adapters can be replaced when scaling out.
export function createSocialMeetServer({
  dataDirectory,
  sessionSecret,
  iceServers = [{ urls: "stun:stun.l.google.com:19302" }],
  interestFallbackMs = 12000,
  rematchCooldownMs = 30000,
} = {}) {
  if (dataDirectory) mkdirSync(dataDirectory, { recursive: true });
  const secretPath = dataDirectory && join(dataDirectory, "session-secret");
  const secret =
    sessionSecret ||
    (secretPath && existsSync(secretPath)
      ? readFileSync(secretPath, "utf8")
      : randomUUID() + randomUUID());
  if (secretPath && !existsSync(secretPath))
    writeFileSync(secretPath, secret, { mode: 0o600 });
  const statePath = dataDirectory && join(dataDirectory, "moderation.json");
  const state =
    statePath && existsSync(statePath)
      ? JSON.parse(readFileSync(statePath, "utf8"))
      : { reports: [], blocks: {} };
  function persist() {
    if (!statePath) return;
    writeFileSync(`${statePath}.tmp`, JSON.stringify(state), { mode: 0o600 });
    renameSync(`${statePath}.tmp`, statePath);
  }
  const sign = (id) => createHmac("sha256", secret).update(id).digest("hex");
  function identity(token) {
    if (typeof token === "string" && token.length < 200) {
      const [id, signature] = token.split(".");
      if (
        /^[a-f0-9-]{36}$/.test(id || "") &&
        /^[a-f0-9]{64}$/.test(signature || "") &&
        timingSafeEqual(Buffer.from(sign(id)), Buffer.from(signature))
      )
        return id;
    }
    return randomUUID();
  }
  const app = express();
  app.use(helmet());
  app.use(cors());
  app.use(express.json({ limit: "8kb" }));
  const server = http.createServer(app);
  const io = new Server(server, {
    cors: { origin: true },
    maxHttpBufferSize: 64 * 1024,
  });
  const clients = new Map();
  const queue = new Set();
  const rooms = new Map();
  const fallbackTimers = new Map();
  const rematchTimers = new Set();
  const rematchDelay = Number.isFinite(rematchCooldownMs)
    ? Math.max(0, Math.ceil(rematchCooldownMs))
    : 30000;
  const fallbackDelay = Number.isFinite(interestFallbackMs)
    ? Math.max(0, Math.ceil(interestFallbackMs))
    : 12000;
  const stats = () => ({
    online: clients.size,
    waiting: queue.size,
    conversations: rooms.size,
  });
  const broadcastStats = () => io.emit("community:stats", stats());
  app.get("/health", (_req, res) => res.json({ ok: true, ...stats() }));
  app.get("/plans", (_req, res) =>
    res.json({
      currency: "INR",
      basic: { price: 0, worldwide: true },
      plus: { price: 20, minutes: 20, paymentsEnabled: false },
    }),
  );
  app.get("/ice-servers", (_req, res) => res.json({ iceServers }));
  const reply = (ack, body) => {
    if (typeof ack === "function") ack(body);
  };
  const fail = (ack, error) => reply(ack, { ok: false, error });
  const clean = (value) =>
    value && typeof value === "object" && !Array.isArray(value) ? value : {};
  const interests = (value) =>
    Array.isArray(value)
      ? [
          ...new Set(
            value
              .filter((v) => typeof v === "string")
              .map((v) => v.trim().toLowerCase().slice(0, 32))
              .filter(Boolean),
          ),
        ].slice(0, 10)
      : [];
  function compatible(a, b) {
    if (!a || !b) return false;
    if (a.userId === b.userId || a.mode !== b.mode) return false;
    if (
      state.blocks[a.userId]?.includes(b.userId) ||
      state.blocks[b.userId]?.includes(a.userId)
    )
      return false;
    const now = Date.now();
    if (
      (a.lastPeer === b.userId && now < a.lastPeerUntil) ||
      (b.lastPeer === a.userId && now < b.lastPeerUntil)
    ) return false;
    return true;
  }
  const sharedInterests = (a, b) =>
    a.interests.filter((interest) => b.interests.includes(interest));
  const canMatchRandom = (client) =>
    !client.interests.length ||
    (client.interestMode === "prefer" && Date.now() >= client.fallbackAt);
  function removeFromQueue(id) {
    queue.delete(id);
    clearTimeout(fallbackTimers.get(id));
    fallbackTimers.delete(id);
  }
  function waitingPayload(client) {
    return {
      queuedAt: client.queuedAt,
      searchStage: canMatchRandom(client) ? "random" : "interests",
      fallbackAt: client.fallbackAt,
      interestMode: client.interestMode,
      requestId: client.requestId,
    };
  }
  function matchPass(commonInterestsOnly) {
    for (const firstId of [...queue]) {
      if (!queue.has(firstId)) continue;
      const first = clients.get(firstId);
      let secondIdToMatch = null;
      let mostShared = 0;
      for (const secondId of queue) {
        if (firstId === secondId) continue;
        const second = clients.get(secondId);
        if (!compatible(first, second)) continue;
        const count = sharedInterests(first, second).length;
        if (commonInterestsOnly) {
          if (count <= mostShared) continue;
          mostShared = count;
          secondIdToMatch = secondId;
        } else if (canMatchRandom(first) && canMatchRandom(second)) {
          secondIdToMatch = secondId;
          break;
        }
      }
      if (secondIdToMatch) {
        const secondId = secondIdToMatch;
        const second = clients.get(secondId);
        const commonInterests = sharedInterests(first, second);
        removeFromQueue(firstId);
        removeFromQueue(secondId);
        const roomId = randomUUID();
        const createdAt = Date.now();
        rooms.set(roomId, {
          a: firstId,
          b: secondId,
          createdAt,
          seenMessages: new Map(),
        });
        first.roomId = second.roomId = roomId;
        for (const [id, initiator] of [
          [firstId, true],
          [secondId, false],
        ]) {
          io.to(id).emit("match:found", {
            roomId,
            initiator,
            mode: first.mode,
            createdAt,
            sharedInterests: commonInterests,
            matchKind: commonInterests.length ? "interests" : "random",
            requestId: clients.get(id).requestId,
          });
        }
      }
    }
  }
  function match() {
    // Reserve shared-interest pairs before considering anyone's random fallback.
    matchPass(true);
    matchPass(false);
    broadcastStats();
  }
  function enqueue(id, payload) {
    const client = clients.get(id);
    removeFromQueue(id);
    client.mode = payload.mode;
    client.requestId = payload.requestId ?? null;
    client.interests = interests(payload.interests);
    client.interestMode = payload.interestMode === "strict" ? "strict" : "prefer";
    client.queuedAt = Date.now();
    client.fallbackAt =
      client.interests.length && client.interestMode === "prefer"
        ? client.queuedAt + fallbackDelay
        : null;
    client.roomId = null;
    queue.add(id);
    io.to(id).emit("queue:waiting", waitingPayload(client));
    if (client.fallbackAt && fallbackDelay > 0) {
      const queuedAt = client.queuedAt;
      const timer = setTimeout(() => {
        fallbackTimers.delete(id);
        if (!queue.has(id) || client.queuedAt !== queuedAt) return;
        io.to(id).emit("queue:waiting", waitingPayload(client));
        match();
      }, fallbackDelay);
      timer.unref();
      fallbackTimers.set(id, timer);
    }
    match();
  }
  function peerInRoom(socket, payload, ack) {
    const client = clients.get(socket.id);
    const room = rooms.get(payload.roomId);
    if (!room || client.roomId !== payload.roomId) {
      fail(ack, "This conversation has ended.");
      return null;
    }
    return { client, room, peerId: room.a === socket.id ? room.b : room.a };
  }
  function end(id, reason) {
    removeFromQueue(id);
    const client = clients.get(id);
    const room = client && rooms.get(client.roomId);
    if (!room) {
      broadcastStats();
      return;
    }
    rooms.delete(client.roomId);
    const rematchAt = Date.now() + rematchDelay;
    for (const memberId of [room.a, room.b]) {
      const member = clients.get(memberId);
      if (!member) continue;
      member.lastPeer = clients.get(
        memberId === room.a ? room.b : room.a,
      )?.userId;
      member.lastPeerUntil = rematchAt;
      const roomId = member.roomId;
      member.roomId = null;
      if (memberId !== id)
        io.to(memberId).emit("match:ended", {
          roomId,
          reason,
          requestId: member.requestId,
        });
    }
    // Reconsider existing searches after the cooldown; never requeue stopped users.
    const timer = setTimeout(() => {
      rematchTimers.delete(timer);
      match();
    }, rematchDelay + 1);
    timer.unref();
    rematchTimers.add(timer);
    broadcastStats();
  }
  io.on("connection", (socket) => {
    const userId = identity(socket.handshake.auth?.sessionToken);
    const client = {
      userId,
      roomId: null,
      mode: "video",
      interests: [],
      interestMode: "prefer",
      queuedAt: null,
      fallbackAt: null,
      requestId: null,
      lastPeer: null,
      lastPeerUntil: 0,
    };
    clients.set(socket.id, client);
    socket.emit("session:ready", { token: `${userId}.${sign(userId)}` });
    broadcastStats();
    const limits = new Map();
    function allowed(event, maximum, ack) {
      const now = Date.now();
      let limit = limits.get(event);
      if (!limit || now - limit.start > 10000) {
        limit = { start: now, count: 0 };
        limits.set(event, limit);
      }
      if (++limit.count > maximum) {
        fail(ack, "Slow down for a moment and try again.");
        return false;
      }
      return true;
    }
    function join(raw, ack, next = false) {
      const payload = clean(raw);
      if (!allowed("queue", 15, ack)) return;
      if (!["text", "video"].includes(payload.mode))
        return fail(ack, "Choose video or text chat.");
      if (payload.adultConfirmed !== true)
        return fail(ack, "Confirm you are 18 or older before joining.");
      if (
        payload.interestMode !== undefined &&
        !["prefer", "strict"].includes(payload.interestMode)
      )
        return fail(ack, "Choose preferred or strict interest matching.");
      if (
        payload.requestId !== undefined &&
        (typeof payload.requestId !== "string" ||
          !payload.requestId.length ||
          payload.requestId.length > 100)
      )
        return fail(ack, "Invalid search request.");
      if (client.roomId && !next)
        return fail(ack, "You are already in a conversation.");
      if (next) end(socket.id, "next");
      enqueue(socket.id, payload);
      reply(ack, { ok: true });
    }
    socket.on("queue:join", (payload, ack) => join(payload, ack));
    socket.on("match:next", (payload, ack) => join(payload, ack, true));
    const staleRequest = (payload) =>
      (payload.requestId !== undefined && payload.requestId !== client.requestId) ||
      (payload.roomId !== undefined && payload.roomId !== client.roomId);
    socket.on("queue:leave", (raw, ack) => {
      // Older clients send only an acknowledgment callback for queue:leave.
      if (typeof raw === "function") ack = raw;
      const payload = clean(raw);
      if (!staleRequest(payload)) removeFromQueue(socket.id);
      broadcastStats();
      reply(ack, { ok: true });
    });
    socket.on("match:end", (raw, ack) => {
      if (!staleRequest(clean(raw))) end(socket.id, "ended");
      reply(ack, { ok: true });
    });
    socket.on("signal", (raw, ack) => {
      if (!allowed("signal", 200, ack)) return;
      const payload = clean(raw);
      const match = peerInRoom(socket, payload, ack);
      if (!match || client.mode !== "video") return;
      if (
        payload.description &&
        !["offer", "answer"].includes(payload.description.type)
      )
        return fail(ack, "Invalid signal.");
      io.to(match.peerId).emit("signal", {
        roomId: payload.roomId,
        description: payload.description,
        candidate: payload.candidate,
      });
      reply(ack, { ok: true });
    });
    socket.on("chat:typing", (raw) => {
      if (!allowed("typing", 30)) return;
      const payload = clean(raw),
        room = rooms.get(payload.roomId);
      if (!room || client.roomId !== payload.roomId) return;
      io.to(room.a === socket.id ? room.b : room.a).emit("chat:typing", {
        roomId: payload.roomId,
        typing: payload.typing === true,
      });
    });
    socket.on("chat:message", (raw, ack) => {
      if (!allowed("message", 30, ack)) return;
      const payload = clean(raw),
        match = peerInRoom(socket, payload, ack);
      if (!match) return;
      const text = typeof payload.text === "string" ? payload.text.trim() : "";
      if (
        !text ||
        text.length > 1000 ||
        typeof payload.clientMessageId !== "string" ||
        payload.clientMessageId.length > 100
      )
        return fail(ack, "Messages must contain 1–1,000 characters.");
      const key = `${socket.id}:${payload.clientMessageId}`;
      if (match.room.seenMessages.has(key))
        return reply(ack, {
          ok: true,
          message: match.room.seenMessages.get(key),
        });
      const message = {
        id: payload.clientMessageId,
        roomId: payload.roomId,
        text,
        sentAt: Date.now(),
      };
      match.room.seenMessages.set(key, message);
      if (match.room.seenMessages.size > 1000)
        match.room.seenMessages.delete(
          match.room.seenMessages.keys().next().value,
        );
      io.to(match.peerId).emit("chat:message", message);
      reply(ack, { ok: true, message });
    });
    socket.on("moderation:report", (raw, ack) => {
      if (!allowed("report", 5, ack)) return;
      const payload = clean(raw),
        match = peerInRoom(socket, payload, ack);
      if (!match) return;
      const reasons = [
        "Sexual content",
        "Harassment or hate",
        "Underage user",
        "Spam or scam",
        "Other",
      ];
      if (!reasons.includes(payload.reason))
        return fail(ack, "Choose a report reason.");
      const peer = clients.get(match.peerId);
      const report = {
        id: randomUUID(),
        roomId: payload.roomId,
        reporterUserId: userId,
        reportedUserId: peer.userId,
        reason: payload.reason,
        details: String(payload.details || "").slice(0, 1000),
        createdAt: Date.now(),
      };
      const previousBlocks = state.blocks[userId] || [];
      state.reports.push(report);
      // Reporting also prevents this anonymous identity from matching again.
      state.blocks[userId] = [
        ...new Set([...(state.blocks[userId] || []), peer.userId]),
      ];
      try {
        persist();
      } catch {
        state.reports.pop();
        state.blocks[userId] = previousBlocks;
        return fail(ack, "Could not save the report. Please try again.");
      }
      end(socket.id, "reported");
      reply(ack, { ok: true, reportId: report.id });
    });
    socket.on("moderation:block", (raw, ack) => {
      const payload = clean(raw),
        match = peerInRoom(socket, payload, ack);
      if (!match) return;
      const peer = clients.get(match.peerId);
      const previous = state.blocks[userId] || [];
      state.blocks[userId] = [...new Set([...previous, peer.userId])];
      try {
        persist();
      } catch {
        state.blocks[userId] = previous;
        return fail(ack, "Could not save the block. Please try again.");
      }
      end(socket.id, "blocked");
      reply(ack, { ok: true });
    });
    socket.on("disconnect", () => {
      end(socket.id, "disconnected");
      clients.delete(socket.id);
      broadcastStats();
    });
  });
  return {
    app,
    server,
    io,
    close: () => {
      for (const timer of fallbackTimers.values()) clearTimeout(timer);
      fallbackTimers.clear();
      for (const timer of rematchTimers) clearTimeout(timer);
      rematchTimers.clear();
      return new Promise((resolve) => io.close(resolve));
    },
  };
}
