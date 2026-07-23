import { describe, expect, it, vi } from "vitest";

import type { Participant, RoomMetadata } from "#contracts";

import { HttpRecordingBot, RecordingBridge } from "./recordingBridge.js";

const room: RoomMetadata = {
  roomId: "room_1",
  slug: "recording-room",
  mediaNodeId: "local",
  signalingUrl: "ws://localhost:4000/ws",
  createdAt: "2026-07-21T12:00:00.000Z",
  expiresAt: null,
  status: "active",
  closedReason: null,
  accessPolicy: "approval",
};

const participant: Participant = {
  participantId: "peer_1",
  displayName: "Ada Lovelace",
  isCreator: false,
  joinedAt: "2026-07-21T12:00:00.000Z",
  connectionState: "online",
  connectionQuality: "good",
  media: { mic: "on", camera: "on", screen: "off" },
};

const config = {
  controlUrl: "http://api:3000/internal/v1",
  controlJwt: { issuer: "issuer", audience: "audience", secret: "x".repeat(32), maxTtlSeconds: 300 },
  bot: {
    workers: [
      { workerId: "worker-a", controlUrl: "http://worker-a:8090", capacity: 1 },
      { workerId: "worker-b", controlUrl: "http://worker-b:8090", capacity: 1 },
    ],
    workerLeaseTtlMs: 15_000,
    workerHeartbeatMs: 5_000,
    signalingUrl: "ws://127.0.0.1:4000/internal/recording-bot/v1",
    controlJwt: { issuer: "issuer", audience: "audience", secret: "y".repeat(32), maxTtlSeconds: 300 },
  },
};

function harness() {
  const control = {
    lookup: vi.fn(async () => ({ integrationId: "integration_1", recordingId: "rec_1", requestedAt: "2026-07-21T11:59:00.000Z", status: "requested" as const })),
    activate: vi.fn(async () => ({ integrationId: "integration_1", recordingId: "rec_1", requestedAt: "2026-07-21T11:59:00.000Z", status: "capturing" as const })),
    closeInputs: vi.fn(async () => undefined),
    issueBotAdmission: vi.fn(async () => ({ protocolVersion: "recording-bot.v1" as const, admission: "bot-admission", expiresAt: "2026-07-21T12:01:00.000Z" })),
  };
  const bot = {
    create: vi.fn(async () => undefined),
    stop: vi.fn(async () => undefined),
    finalize: vi.fn(async () => undefined),
  };
  return { bridge: new RecordingBridge(control, bot, config), control, bot };
}

describe("RecordingBridge", () => {
  it("starts one bot attempt and finalizes it once when the room stops", async () => {
    const { bridge, control, bot } = harness();
    await bridge.roomBecameLive(room, [participant], []);
    await bridge.producerAdded(room, participant, { producerId: "screen_1", participantId: participant.participantId, kind: "video", source: "screen" });
    await bridge.stopRoom(room.roomId);
    await bridge.stopRoom(room.roomId);

    expect(control.activate).toHaveBeenCalledTimes(1);
    expect(control.issueBotAdmission).toHaveBeenCalledWith("rec_1");
    expect(bot.create).toHaveBeenCalledWith(
      expect.objectContaining({ workerId: "worker-a", recordingId: "rec_1" }),
      expect.objectContaining({ roomId: "room_1", recordingId: "rec_1", admission: "bot-admission" }),
    );
    expect(bot.stop).toHaveBeenCalledTimes(1);
    expect(control.closeInputs).toHaveBeenCalledTimes(1);
  });

  it("does nothing when recording was not requested", async () => {
    const { bridge, control, bot } = harness();
    control.lookup.mockResolvedValueOnce(undefined as never);
    await bridge.roomBecameLive(room, [participant], []);
    expect(bot.create).not.toHaveBeenCalled();
  });

  it("releases capacity and permits a retry after bot startup failure", async () => {
    const { bridge, control, bot } = harness();
    bot.create.mockRejectedValueOnce(new Error("bot unavailable"));
    await expect(bridge.roomBecameLive(room, [participant], [])).rejects.toThrow("bot unavailable");
    await bridge.producerAdded(room, participant, { producerId: "camera_1", participantId: participant.participantId, kind: "video", source: "camera" });

    expect(bot.stop).not.toHaveBeenCalled();
    expect(control.closeInputs).not.toHaveBeenCalled();
    expect(control.activate).toHaveBeenCalledTimes(2);
    expect(bot.create).toHaveBeenCalledTimes(2);
  });

  it("keeps rejecting capacity exhaustion until a slot becomes available", async () => {
    const { bridge, bot } = harness();
    await bridge.workerPool.reserve("rec_other");
    await bridge.workerPool.reserve("rec_other_2");
    await expect(bridge.roomBecameLive(room, [participant], [])).rejects.toThrow("capacity");
    await expect(bridge.roomBecameLive(room, [participant], [])).rejects.toThrow("capacity");
    expect(bot.create).not.toHaveBeenCalled();

    await bridge.workerPool.release("rec_other");
    await expect(bridge.roomBecameLive(room, [participant], [])).resolves.toBeUndefined();
    expect(bot.create).toHaveBeenCalledTimes(1);
  });

  it("uses a fresh admission and generation binding when an active worker lease is lost", async () => {
    const { bridge, control, bot } = harness();
    await bridge.roomBecameLive(room, [participant], []);
    const calls = bot.create.mock.calls as unknown as Array<[
      { workerId: string; recordingId: string },
      Record<string, unknown>,
    ]>;
    const firstBinding = calls[0]?.[0];
    expect(firstBinding?.workerId).toBe("worker-a");

    await bridge.workerPool.register({
      workerId: "worker-a",
      generationId: "worker-a-restarted",
      controlUrl: "http://worker-a:8090",
      capacity: 1,
    });
    await bridge.workerPool.reserve("occupy-restarted-a");
    const lost = await bridge.workerPool.reapExpiredBindings();
    expect(lost).toEqual(["rec_1"]);
    await bridge.recoverRecording("rec_1");

    expect(control.issueBotAdmission).toHaveBeenCalledTimes(2);
    expect(control.closeInputs).not.toHaveBeenCalled();
    expect(bot.create).toHaveBeenCalledTimes(2);
    expect(calls[1]?.[0]).toMatchObject({
      workerId: "worker-b",
      generationId: expect.any(String),
      controlUrl: "http://worker-b:8090",
      recordingId: "rec_1",
    });
  });

  it("requeues recovery after three failed starts and succeeds on the next recovery cycle", async () => {
    const { bridge, control, bot } = harness();
    await bridge.roomBecameLive(room, [participant], []);
    bot.create
      .mockRejectedValueOnce(new Error("recovery start 1 failed"))
      .mockRejectedValueOnce(new Error("recovery start 2 failed"))
      .mockRejectedValueOnce(new Error("recovery start 3 failed"));

    await bridge.recoverRecording("rec_1");

    expect(bot.create).toHaveBeenCalledTimes(4);
    expect(control.issueBotAdmission).toHaveBeenCalledTimes(4);
    expect(control.closeInputs).not.toHaveBeenCalled();
    expect(await bridge.workerPool.reapExpiredBindings()).toEqual(["rec_1"]);
    expect(bridge.getWorkerPoolSnapshot().occupiedSlots).toBe(0);

    await bridge.recoverRecording("rec_1");

    expect(bot.create).toHaveBeenCalledTimes(5);
    expect(control.issueBotAdmission).toHaveBeenCalledTimes(5);
    expect(control.closeInputs).not.toHaveBeenCalled();
    expect(bridge.getWorkerPoolSnapshot().occupiedSlots).toBe(1);
  });

  it("blocks an unrecorded retry after a prepared first-human join is aborted", async () => {
    const { bridge, control } = harness();
    await bridge.roomBecameLive(room, [participant], []);

    await bridge.abortPrepared(room.roomId);

    expect(control.closeInputs).toHaveBeenCalledTimes(1);
    await expect(bridge.roomBecameLive(room, [participant], []))
      .rejects.toThrow("aborted before the first participant");
  });

  it("rebinds a crashed DELETE to a healthy worker finalization command before close-inputs", async () => {
    const { bridge, control, bot } = harness();
    await bridge.roomBecameLive(room, [participant], []);
    bot.stop.mockRejectedValueOnce(new Error("worker crashed during DELETE"));

    await bridge.stopRoom(room.roomId);

    expect(bot.finalize).toHaveBeenCalledWith(
      expect.objectContaining({
        workerId: "worker-b",
        controlUrl: "http://worker-b:8090",
        recordingId: "rec_1",
      }),
      expect.objectContaining({ roomId: "room_1", recordingId: "rec_1" }),
    );
    expect(control.closeInputs).toHaveBeenCalledTimes(1);
    expect(bridge.getWorkerPoolSnapshot()).toMatchObject({ healthyWorkers: 1, occupiedSlots: 0 });
  });

  it("retains and requeues finalization until a worker recovery command succeeds", async () => {
    const { bridge, control, bot } = harness();
    await bridge.roomBecameLive(room, [participant], []);
    bot.stop.mockRejectedValueOnce(new Error("assigned worker stopped"));
    bot.finalize
      .mockRejectedValueOnce(new Error("finalize 1 failed"))
      .mockRejectedValueOnce(new Error("finalize 2 failed"))
      .mockRejectedValueOnce(new Error("finalize 3 failed"));

    await bridge.stopRoom(room.roomId);

    expect(bot.finalize).toHaveBeenCalledTimes(3);
    expect(control.closeInputs).not.toHaveBeenCalled();
    expect(bridge.getWorkerPoolSnapshot().occupiedSlots).toBe(0);

    await bridge.retryFinalization("rec_1");

    expect(bot.finalize).toHaveBeenCalledTimes(4);
    expect(control.closeInputs).toHaveBeenCalledTimes(1);
    expect(bridge.getWorkerPoolSnapshot().occupiedSlots).toBe(0);
  });
});

describe("HttpRecordingBot generation fencing", () => {
  it("sends worker identity and generation on create, stop and finalization recovery commands", async () => {
    const request = vi.fn(async () => new Response(undefined, { status: 200 }));
    vi.stubGlobal("fetch", request);
    try {
      const worker = new HttpRecordingBot(config.bot.controlJwt);
      const binding = {
        workerId: "worker-a",
        generationId: "generation-a",
        controlUrl: "http://worker-a:8090",
        recordingId: "rec_1",
      };
      const common = {
        attemptId: "attempt_1",
        roomId: "room_1",
        recordingId: "rec_1",
        integrationId: "integration_1",
        requestedAt: "2026-07-21T11:59:00.000Z",
      };
      await worker.create(binding, {
        ...common,
        signalingUrl: "ws://signaling:4000/internal/recording-bot/v1",
        admission: "admission",
      });
      await worker.stop(binding, "rec_1");
      await worker.finalize(binding, common);

      expect(request).toHaveBeenCalledTimes(3);
      const calls = request.mock.calls as unknown as Array<[string, RequestInit]>;
      expect(calls[0]?.[0]).toBe("http://worker-a:8090/internal/v1/attempts/rec_1");
      expect(calls[0]?.[1].method).toBe("PUT");
      expect(calls[1]?.[0]).toBe("http://worker-a:8090/internal/v1/attempts/rec_1");
      expect(calls[1]?.[1].method).toBe("DELETE");
      expect(calls[2]?.[0]).toBe("http://worker-a:8090/internal/v1/finalizations/rec_1");
      expect(calls[2]?.[1].method).toBe("PUT");
      for (const [, init] of calls) {
        expect(init.headers).toMatchObject({
          "X-Recording-Worker-Id": "worker-a",
          "X-Recording-Worker-Generation-Id": "generation-a",
        });
        expect(init.headers).not.toHaveProperty("X-Recording-Worker-Generation");
      }
      expect(JSON.parse(String(calls[0]?.[1].body)))
        .toMatchObject({ workerId: "worker-a", workerGenerationId: "generation-a" });
      expect(calls[1]?.[1].body).toBeUndefined();
      expect(JSON.parse(String(calls[2]?.[1].body)))
        .toMatchObject({ workerId: "worker-a", workerGenerationId: "generation-a" });
      expect(JSON.parse(String(calls[0]?.[1].body)))
        .not.toHaveProperty("generationId");
      expect(JSON.parse(String(calls[2]?.[1].body)))
        .not.toHaveProperty("generationId");
    } finally {
      vi.unstubAllGlobals();
    }
  });
});
