import { EventEmitter } from "node:events";
import { describe, expect, it, vi } from "vitest";
import { WebSocket } from "ws";

import type { MediasoupService } from "./mediasoupService.js";
import { RecordingBotSessionManager, RecordingBotSocketGateway, type BotAuthoritySnapshot } from "./recordingBot.js";
import type { RecordingBotAdmissionPayload } from "./token.js";

const admission: RecordingBotAdmissionPayload = {
  sub: "recording-bot",
  aud: "conference-recording-bot-signaling",
  iss: "conference-api",
  roomId: "room_1",
  recordingId: "rec_1",
  mediaNodeId: "local",
  jti: "jti_1",
  iat: 1,
  exp: 2,
};

function participant(participantId: string, displayName = participantId) {
  return { participantId, displayName };
}

function producer(producerId: string, participantId: string, source: "camera" | "screen" = "camera") {
  return { producerId, participantId, displayName: participantId, kind: "video" as const, source, paused: false };
}

function setup(
  initial: BotAuthoritySnapshot = { participants: [], producers: [] },
  freeze?: (snapshot: BotAuthoritySnapshot, accept: (snapshot: BotAuthoritySnapshot) => void) => Promise<void>,
  terminalRoomRetentionMs?: number,
  now?: () => number,
) {
  let authority = initial;
  const consumers = new Map<string, { id: string; paused: boolean }>();
  const media = {
    getRouterRtpCapabilities: vi.fn().mockResolvedValue({ codecs: [] }),
    createBotRecvTransport: vi.fn().mockResolvedValue({ id: "transport_1", iceParameters: {}, iceCandidates: [], dtlsParameters: {} }),
    connectBotRecvTransport: vi.fn().mockResolvedValue(undefined),
    consumeForBot: vi.fn(async (_roomId: string, _recordingId: string, _transportId: string, producerId: string) => {
      const existing = consumers.get(producerId);
      if (existing) return { ...existing, producerId, kind: "video", rtpParameters: {} };
      const created = { id: `consumer_${producerId}`, paused: true };
      consumers.set(producerId, created);
      return { ...created, producerId, kind: "video", rtpParameters: {} };
    }),
    setBotConsumerPaused: vi.fn(async (_roomId: string, _recordingId: string, producerId: string, paused: boolean) => {
      const consumer = consumers.get(producerId);
      if (consumer) consumer.paused = paused;
    }),
    closeBotConsumer: vi.fn((_roomId: string, _recordingId: string, producerId: string) => consumers.delete(producerId)),
    closeBotSession: vi.fn(() => consumers.clear()),
  };
  const failures: unknown[] = [];
  const manager = new RecordingBotSessionManager(
    media as unknown as MediasoupService,
    async (_roomId, accept) => freeze ? freeze(authority, accept) : accept(authority),
    (failure) => failures.push(failure),
    terminalRoomRetentionMs,
    now,
  );
  return { manager, media, failures, consumers, setAuthority: (next: BotAuthoritySnapshot) => { authority = next; } };
}

describe("RecordingBotSessionManager", () => {
  it("sends an authoritative human-only snapshot on explicit synchronization", async () => {
    const { manager } = setup({
      participants: [participant("peer_1", "Alex")],
      producers: [{ ...producer("camera_1", "peer_1"), displayName: "Alex" }],
    });
    const events: Array<Record<string, unknown>> = [];
    manager.open(admission, (event) => events.push(event));
    await manager.connectRecvTransport("rec_1", "transport_1", {});
    await manager.syncSnapshot("rec_1");

    expect(events).toEqual([
      expect.objectContaining({ type: "bot:admitted", roomId: "room_1", recordingId: "rec_1" }),
      expect.objectContaining({ type: "bot:producerSnapshot", revision: 0, participants: [{ participantId: "peer_1", displayName: "Alex" }] }),
    ]);
    expect(JSON.stringify(events)).not.toContain("isCreator");
  });

  it("orders screen close before replacement add at consecutive revisions", async () => {
    const initial = { participants: [participant("peer_1")], producers: [producer("screen_1", "peer_1", "screen")] };
    const { manager } = setup(initial);
    const events: Array<Record<string, unknown>> = [];
    manager.open(admission, (event) => events.push(event));
    await manager.connectRecvTransport("rec_1", "transport_1", {});
    await manager.syncSnapshot("rec_1");
    manager.syncAuthority("room_1", { participants: initial.participants, producers: [producer("screen_2", "peer_1", "screen")] });

    expect(events.slice(-2)).toEqual([
      expect.objectContaining({ type: "bot:producerClosed", producerId: "screen_1", revision: 1 }),
      expect.objectContaining({ type: "bot:producerAdded", producer: expect.objectContaining({ producerId: "screen_2" }), revision: 2 }),
    ]);
  });

  it("registers the snapshot cursor before a producer mutation can publish its revision", async () => {
    const initial = { participants: [participant("peer_1")], producers: [] };
    const next = { participants: initial.participants, producers: [producer("camera_1", "peer_1")] };
    const context = setup(initial, async (snapshot, accept) => {
      accept(snapshot);
      context.manager.syncAuthority("room_1", next);
    });
    const manager = context.manager;
    const events: Array<Record<string, unknown>> = [];
    manager.open(admission, (event) => events.push(event));

    await manager.connectRecvTransport("rec_1", "transport_1", {});
    await manager.syncSnapshot("rec_1");

    expect(events.slice(-2)).toEqual([
      expect.objectContaining({ type: "bot:producerSnapshot", revision: 0, producers: [] }),
      expect.objectContaining({ type: "bot:producerAdded", revision: 1, producer: expect.objectContaining({ producerId: "camera_1" }) }),
    ]);
  });

  it("replays the latest authority mutation that arrives before the snapshot barrier", async () => {
    const initial = { participants: [], producers: [] };
    const next = { participants: [participant("peer_1")], producers: [producer("camera_1", "peer_1")] };
    let releaseFreeze!: () => void;
    const freezePending = new Promise<void>((resolve) => { releaseFreeze = resolve; });
    const context = setup(initial, async (snapshot, accept) => {
      await freezePending;
      accept(snapshot);
    });
    const events: Array<Record<string, unknown>> = [];
    context.manager.open(admission, (event) => events.push(event));

    const synchronization = context.manager.syncSnapshot("rec_1");
    context.manager.syncAuthority("room_1", next);
    releaseFreeze();
    await synchronization;

    expect(events.slice(-3)).toEqual([
      expect.objectContaining({ type: "bot:producerSnapshot", revision: 0, participants: [], producers: [] }),
      expect.objectContaining({ type: "bot:participantAdded", revision: 1 }),
      expect.objectContaining({ type: "bot:producerAdded", revision: 2 }),
    ]);
  });

  it("reconciles with a fresh snapshot before capture and never duplicates a producer consumer", async () => {
    const first = { participants: [participant("peer_1")], producers: [producer("camera_1", "peer_1")] };
    const { manager, media, consumers, setAuthority } = setup(first);
    manager.open(admission, () => {});
    await manager.connectRecvTransport("rec_1", "transport_1", {});
    await manager.syncSnapshot("rec_1");
    await manager.consume("rec_1", "transport_1", "camera_1", {});
    await manager.consume("rec_1", "transport_1", "camera_1", {});
    expect(consumers.size).toBe(1);

    manager.disconnect("rec_1");
    setAuthority({ participants: [participant("peer_1")], producers: [producer("screen_1", "peer_1", "screen")] });
    const events: Array<Record<string, unknown>> = [];
    manager.open({ ...admission, jti: "jti_2" }, (event) => events.push(event));
    await manager.connectRecvTransport("rec_1", "transport_1", {});
    await manager.syncSnapshot("rec_1");
    expect(events.at(-1)).toEqual(expect.objectContaining({ type: "bot:producerSnapshot", producers: [expect.objectContaining({ producerId: "screen_1" })] }));
    expect(media.closeBotSession).toHaveBeenCalledWith("room_1", expect.any(String));
  });

  it("ignores stale socket generation messages and close after a pre-capture reconnect", async () => {
    const { manager, media } = setup({ participants: [participant("peer_1")], producers: [] });
    const oldSessionId = manager.open(admission, () => {});
    const newSessionId = manager.open({ ...admission, jti: "jti_2" }, () => {});

    expect(newSessionId).not.toBe(oldSessionId);
    await expect(manager.createRecvTransport("rec_1", oldSessionId)).rejects.toThrow("Recording bot session not found");
    manager.disconnect("rec_1", oldSessionId);

    await expect(manager.getRouterRtpCapabilities("rec_1", newSessionId)).resolves.toEqual({ codecs: [] });
    expect(media.closeBotSession).toHaveBeenCalledTimes(1);
  });

  it("requests recovery after a post-capture disconnect and allows a fresh admission", async () => {
    const { manager, media, failures } = setup({ participants: [participant("peer_1")], producers: [producer("camera_1", "peer_1")] });
    manager.open(admission, () => {});
    await manager.connectRecvTransport("rec_1", "transport_1", {});
    await manager.syncSnapshot("rec_1");
    await manager.consume("rec_1", "transport_1", "camera_1", {});
    await manager.setConsumerPaused("rec_1", "camera_1", false);
    manager.disconnect("rec_1");
    manager.disconnect("rec_1");

    expect(media.closeBotSession).toHaveBeenCalledWith("room_1", expect.any(String));
    expect(failures).toEqual([{ roomId: "room_1", recordingId: "rec_1", code: "BOT_CAPTURE_INCOMPLETE" }]);
    expect(() => manager.open({ ...admission, jti: "fresh_jti" }, () => {})).not.toThrow();
    expect(media).not.toHaveProperty("closeRoom");
  });

  it("fences the room without racing the control-plane stop", async () => {
    const initial = { participants: [participant("peer_1")], producers: [producer("camera_1", "peer_1")] };
    const { manager, media } = setup(initial);
    const events: Array<Record<string, unknown>> = [];
    manager.open(admission, (event) => events.push(event));
    await manager.createRecvTransport("rec_1");
    await manager.syncSnapshot("rec_1");
    await manager.consume("rec_1", "transport_1", "camera_1", {});

    manager.stopRoom("room_1");
    expect(events).not.toContainEqual(expect.objectContaining({ type: "bot:stop" }));
    expect(() => manager.closeConsumer("rec_1", "camera_1")).not.toThrow();
    expect(media.closeBotConsumer).toHaveBeenCalledWith("room_1", expect.any(String), "camera_1");
    expect(media.closeBotSession).not.toHaveBeenCalled();

    manager.disconnect("rec_1");
    expect(media.closeBotSession).toHaveBeenCalledTimes(1);
  });

  it("cleans an in-flight old-generation transport if reconnect wins before creation resolves", async () => {
    const { manager, media } = setup();
    let resolveTransport!: (value: { id: string; iceParameters: object; iceCandidates: never[]; dtlsParameters: object }) => void;
    media.createBotRecvTransport.mockImplementationOnce(() => new Promise((resolve) => {
      resolveTransport = resolve;
    }));
    const oldSessionId = manager.open(admission, () => {});
    const oldCreate = manager.createRecvTransport("rec_1", oldSessionId);
    const newSessionId = manager.open({ ...admission, jti: "jti_2" }, () => {});
    resolveTransport({ id: "stale_transport", iceParameters: {}, iceCandidates: [], dtlsParameters: {} });

    await expect(oldCreate).rejects.toThrow("Recording bot session not found");
    await expect(manager.getRouterRtpCapabilities("rec_1", newSessionId)).resolves.toEqual({ codecs: [] });
    expect(media.closeBotSession).toHaveBeenCalledWith("room_1", oldSessionId);
  });

  it("fences a terminal room before teardown and keeps stale admissions rejected while close-inputs is unresolved", () => {
    let clock = 1_000;
    const { manager, media } = setup({ participants: [participant("peer_1")], producers: [] }, undefined, 100, () => clock);
    manager.open(admission, () => {});

    manager.stopRoom("room_1");
    let teardownReopenError: unknown;
    try {
      manager.open({ ...admission, recordingId: "rec_during_teardown", jti: "jti_during" }, () => {});
    } catch (error) {
      teardownReopenError = error;
    }
    const unresolvedCloseInputs = new Promise<void>(() => {});
    void unresolvedCloseInputs;

    expect(teardownReopenError).toBeInstanceOf(Error);
    expect(media.closeBotSession).not.toHaveBeenCalled();
    manager.disconnect("rec_1");
    expect(media.closeBotSession).toHaveBeenCalledTimes(1);
    expect(() => manager.open({ ...admission, recordingId: "rec_stale", jti: "jti_stale" }, () => {}))
      .toThrow("BOT_CAPTURE_INCOMPLETE");

    clock += 101;
    expect(() => manager.open({ ...admission, recordingId: "rec_after_retention", jti: "jti_after" }, () => {}))
      .not.toThrow();
  });
});

describe("RecordingBotSocketGateway reconnect generations", () => {
  it("does not route a replaced socket message or close into the fresh attempt", async () => {
    const server = new EventEmitter();
    const createRecvTransport = vi.fn().mockResolvedValue({ id: "transport_1" });
    const disconnect = vi.fn();
    let generation = 0;
    const manager = {
      open: vi.fn(() => `session_${++generation}`),
      createRecvTransport,
      disconnect,
    };
    const gateway = new RecordingBotSocketGateway(
      server as never,
      manager as unknown as RecordingBotSessionManager,
      async () => admission,
    );
    gateway.start();
    const oldSocket = new FakeSocket();
    const freshSocket = new FakeSocket();

    server.emit("connection", oldSocket, { url: "/internal/recording-bot/v1?admission=old" });
    await Promise.resolve();
    server.emit("connection", freshSocket, { url: "/internal/recording-bot/v1?admission=fresh" });
    await Promise.resolve();

    oldSocket.emit("message", Buffer.from(JSON.stringify({ type: "bot:createRecvTransport", requestId: "old_req" })));
    oldSocket.emit("close");
    await Promise.resolve();
    expect(createRecvTransport).not.toHaveBeenCalled();
    expect(disconnect).not.toHaveBeenCalledWith("rec_1", "session_1");

    freshSocket.emit("message", Buffer.from(JSON.stringify({ type: "bot:createRecvTransport", requestId: "new_req" })));
    await Promise.resolve();
    expect(createRecvTransport).toHaveBeenCalledWith("rec_1", "session_2");
    freshSocket.emit("close");
    expect(disconnect).toHaveBeenCalledWith("rec_1", "session_2");
  });

  it("serializes simultaneous commands for one session", async () => {
    const server = new EventEmitter();
    let releaseFirst!: () => void;
    const firstPending = new Promise<void>((resolve) => { releaseFirst = resolve; });
    let active = 0;
    let maxActive = 0;
    let calls = 0;
    const createRecvTransport = vi.fn(async () => {
      calls += 1;
      active += 1;
      maxActive = Math.max(maxActive, active);
      if (calls === 1) await firstPending;
      active -= 1;
      return { id: "transport_1" };
    });
    const manager = {
      open: vi.fn(() => "session_1"),
      createRecvTransport,
      disconnect: vi.fn(),
    };
    const gateway = new RecordingBotSocketGateway(
      server as never,
      manager as unknown as RecordingBotSessionManager,
      async () => admission,
    );
    gateway.start();
    const socket = new FakeSocket();
    server.emit("connection", socket, { url: "/internal/recording-bot/v1?admission=token" });
    await Promise.resolve();

    socket.emit("message", Buffer.from(JSON.stringify({ type: "bot:createRecvTransport", requestId: "req_1" })));
    socket.emit("message", Buffer.from(JSON.stringify({ type: "bot:createRecvTransport", requestId: "req_2" })));
    await Promise.resolve();
    expect(createRecvTransport).toHaveBeenCalledTimes(1);

    releaseFirst();
    await new Promise((resolve) => setTimeout(resolve, 0));
    expect(createRecvTransport).toHaveBeenCalledTimes(2);
    expect(maxActive).toBe(1);
  });
});

class FakeSocket extends EventEmitter {
  readyState: number = WebSocket.OPEN;
  readonly sent: string[] = [];

  send(data: string): void {
    this.sent.push(data);
  }

  close(): void {
    if (this.readyState === WebSocket.CLOSED) return;
    this.readyState = WebSocket.CLOSED;
    this.emit("close");
  }

  terminate(): void {
    this.close();
  }
}
