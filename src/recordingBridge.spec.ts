import { describe, expect, it, vi } from "vitest";

import type { Participant, RoomMetadata } from "#contracts";

import { HttpRecordingBot, HttpRecordingControl, RecordingBridge } from "./recordingBridge.js";
import { RecordingWorkerPool } from "./recordingWorkerPool.js";
import type {
  RecordingRecoveryContext,
  RecordingV2Activation,
  RecordingV2FinalizationFence,
} from "./redisRoomRepository.js";

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
  recordingRequested: true,
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
    activate: vi.fn(async () => ({
      integrationId: "integration_1",
      recordingId: "rec_1",
      requestedAt: "2026-07-21T11:59:00.000Z",
      status: "capturing" as const,
      protocolVersion: "recording-capture.v2" as const,
      captureGeneration: 1,
      captureStartedAt: "2026-07-21T12:00:00.000Z",
    })),
    closeInputs: vi.fn(async () => undefined),
    bindFinalization: vi.fn(async () => ({
      recordingId: "rec_1",
      protocolVersion: "recording-capture.v2" as const,
      captureGeneration: 2,
      captureStartedAt: "2026-07-21T12:00:00.000Z",
      bindingMode: "seal-only" as const,
      terminalTimelineEndMs: 8_000,
    })),
    issueBotAdmission: vi.fn(async () => ({ protocolVersion: "recording-bot.v1" as const, admission: "bot-admission", expiresAt: "2026-07-21T12:01:00.000Z" })),
    recoveryContext: vi.fn<() => ReturnType<HttpRecordingControl["recoveryContext"]>>(async () => undefined),
  };
  const bot = {
    create: vi.fn(async () => undefined),
    stop: vi.fn(async () => undefined),
    finalize: vi.fn(async () => undefined),
  };
  const finalizationStore = {
    getRecordingRecoveryContext: vi.fn<() => Promise<RecordingRecoveryContext | undefined>>(async () => undefined),
    getV2RecordingActivation: vi.fn<() => Promise<RecordingV2Activation | undefined>>(async () => undefined),
    getV2RecordingFinalization: vi.fn<() => Promise<RecordingV2FinalizationFence | undefined>>(async () => undefined),
    beginV2RecordingFinalization: vi.fn(async () => ({
      protocolVersion: "recording-capture.v2" as const,
      captureGeneration: 1,
      activatedAt: "2026-07-21T12:00:00.000Z",
      captureStartedAt: "2026-07-21T12:00:00.000Z",
      terminalObservedAt: "2026-07-21T12:00:08.000Z",
      terminalTimelineEndMs: 8_000,
    })),
  };
  return {
    bridge: new RecordingBridge(
      control,
      bot,
      finalizationStore,
      config,
      undefined,
      new RecordingWorkerPool(config.bot.workers, config.bot.workerLeaseTtlMs, Date.now, true),
    ),
    control,
    bot,
    finalizationStore,
  };
}

describe("RecordingBridge", () => {
  it("starts one bot attempt and finalizes it once when the room stops", async () => {
    const { bridge, control, bot } = harness();
    await bridge.roomBecameLive(room, [participant], []);
    await bridge.producerAdded(room, participant, { producerId: "screen_1", participantId: participant.participantId, kind: "video", source: "screen" });
    await bridge.stopRoom(room.roomId);
    await bridge.stopRoom(room.roomId);

    expect(control.activate).toHaveBeenCalledTimes(1);
    expect(control.activate).toHaveBeenCalledWith(
      "room_1",
      expect.objectContaining({ workerId: "worker-a" }),
    );
    expect(control.issueBotAdmission).toHaveBeenCalledWith("rec_1");
    expect(bot.create).toHaveBeenCalledWith(
      expect.objectContaining({ workerId: "worker-a", recordingId: "rec_1" }),
      expect.objectContaining({ roomId: "room_1", recordingId: "rec_1", admission: "bot-admission" }),
    );
    expect(bot.stop).toHaveBeenCalledTimes(1);
    expect(control.closeInputs).toHaveBeenCalledWith("rec_1", {
      captureGeneration: 1,
      terminalTimelineEndMs: 8_000,
    });
    expect(control.closeInputs.mock.invocationCallOrder[0])
      .toBeLessThan(bot.stop.mock.invocationCallOrder[0]!);
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

    expect(bot.stop).toHaveBeenCalledTimes(1);
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

  it("ignores a stale worker recovery report while the current binding is healthy", async () => {
    const { bridge, control, bot } = harness();
    await bridge.workerPool.register({
      workerId: "worker-a",
      generationId: "generation-a",
      controlUrl: "http://worker-a:8090",
      capacity: 1,
    });
    await bridge.workerPool.register({
      workerId: "worker-b",
      generationId: "generation-b",
      controlUrl: "http://worker-b:8090",
      capacity: 1,
    });
    await bridge.roomBecameLive(room, [participant], []);

    const workerReportedRecovery = bridge as unknown as {
      recoverWorkerReportedRecording(recordingId: string, signal: AbortSignal): Promise<void>;
    };
    await workerReportedRecovery.recoverWorkerReportedRecording("rec_1", new AbortController().signal);

    expect(control.issueBotAdmission).toHaveBeenCalledTimes(1);
    expect(bot.create).toHaveBeenCalledTimes(1);
    expect(bridge.getWorkerPoolSnapshot()).toMatchObject({
      occupiedSlots: 1,
      availableSlots: 1,
    });
  });

  it("upgrades an in-flight advisory recovery when the bot disconnects", async () => {
    const { bridge, control, bot } = harness();
    await bridge.workerPool.register({
      workerId: "worker-a",
      generationId: "generation-a",
      controlUrl: "http://worker-a:8090",
      capacity: 1,
    });
    await bridge.workerPool.register({
      workerId: "worker-b",
      generationId: "generation-b",
      controlUrl: "http://worker-b:8090",
      capacity: 1,
    });
    await bridge.roomBecameLive(room, [participant], []);

    let releaseAdvisory!: () => void;
    const advisoryBlocked = new Promise<void>((resolve) => {
      releaseAdvisory = resolve;
    });
    const invalidationStarted = vi.fn();
    vi.spyOn(bridge.workerPool, "invalidateUnavailable").mockImplementation(async () => {
      invalidationStarted();
      await advisoryBlocked;
      return false;
    });
    const workerReportedRecovery = bridge as unknown as {
      recoverWorkerReportedRecording(recordingId: string, signal: AbortSignal): Promise<void>;
    };
    const advisory = workerReportedRecovery.recoverWorkerReportedRecording(
      "rec_1",
      new AbortController().signal,
    );
    await vi.waitFor(() => expect(invalidationStarted).toHaveBeenCalledOnce());

    const forced = bridge.recoverRecording("rec_1");
    releaseAdvisory();
    await Promise.all([advisory, forced]);

    expect(control.issueBotAdmission).toHaveBeenCalledTimes(2);
    expect(bot.create).toHaveBeenCalledTimes(2);
  });

  it("coalesces a bot disconnect when advisory recovery already replaced the worker", async () => {
    const { bridge, control, bot } = harness();
    await bridge.workerPool.register({
      workerId: "worker-a",
      generationId: "generation-a",
      controlUrl: "http://worker-a:8090",
      capacity: 1,
    });
    await bridge.workerPool.register({
      workerId: "worker-b",
      generationId: "generation-b",
      controlUrl: "http://worker-b:8090",
      capacity: 1,
    });
    await bridge.roomBecameLive(room, [participant], []);
    await bridge.workerPool.markWorkerUnavailable("worker-a", "generation-a");

    let releaseAdvisory!: () => void;
    const advisoryBlocked = new Promise<void>((resolve) => {
      releaseAdvisory = resolve;
    });
    const invalidateUnavailable = bridge.workerPool.invalidateUnavailable.bind(bridge.workerPool);
    const invalidationStarted = vi.fn();
    vi.spyOn(bridge.workerPool, "invalidateUnavailable").mockImplementation(async (recordingId) => {
      invalidationStarted();
      await advisoryBlocked;
      return invalidateUnavailable(recordingId);
    });
    const workerReportedRecovery = bridge as unknown as {
      recoverWorkerReportedRecording(recordingId: string, signal: AbortSignal): Promise<void>;
    };
    const advisory = workerReportedRecovery.recoverWorkerReportedRecording(
      "rec_1",
      new AbortController().signal,
    );
    await vi.waitFor(() => expect(invalidationStarted).toHaveBeenCalledOnce());

    const forced = bridge.recoverRecording("rec_1");
    releaseAdvisory();
    await Promise.all([advisory, forced]);

    expect(control.issueBotAdmission).toHaveBeenCalledTimes(2);
    expect(bot.create).toHaveBeenCalledTimes(2);
    expect(bridge.workerPool.getBinding("rec_1")).toMatchObject({ workerId: "worker-b" });
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

  it("does not start a bot when preparation is aborted while lookup is pending", async () => {
    const { bridge, control, bot } = harness();
    let resolveLookup: ((value: Awaited<ReturnType<typeof control.lookup>>) => void) | undefined;
    control.lookup.mockImplementationOnce(() => new Promise((resolve) => { resolveLookup = resolve; }));
    const preparation = bridge.roomBecameLive(room, [participant], []);
    await vi.waitFor(() => expect(control.lookup).toHaveBeenCalledOnce());

    await bridge.abortPrepared(room.roomId);
    resolveLookup?.({
      recordingTray: undefined,
    } as never);
    await preparation;

    expect(bot.create).not.toHaveBeenCalled();
    await expect(bridge.roomBecameLive(room, [participant], []))
      .rejects.toThrow("aborted before the first participant");
  });

  it("rejects a new first human after the recording has reached terminal state", async () => {
    const { bridge, bot } = harness();
    await bridge.roomBecameLive(room, [participant], []);
    await bridge.stopRoom(room.roomId);

    await expect(bridge.roomBecameLive(room, [participant], []))
      .rejects.toThrow("terminal state");
    expect(bot.create).toHaveBeenCalledTimes(1);
  });

  it("forces a worker-reported capture into seal-only finalization after a full signaling restart", async () => {
    const { bridge, control, bot, finalizationStore } = harness();
    finalizationStore.getRecordingRecoveryContext.mockResolvedValueOnce({
      roomId: room.roomId,
      integrationId: "integration_1",
      requestedAt: "2026-07-21T11:59:00.000Z",
    });
    finalizationStore.getV2RecordingActivation.mockResolvedValueOnce({
      captureGeneration: 1,
      activatedAt: "2026-07-21T12:00:00.000Z",
    });
    await bridge.workerPool.register({
      workerId: "worker-a",
      generationId: "generation-a",
      controlUrl: "http://worker-a:8090",
      capacity: 1,
    });

    await bridge.recoverRecording("rec_1");

    expect(bot.create).not.toHaveBeenCalled();
    expect(control.closeInputs).toHaveBeenCalledTimes(1);
    expect(control.bindFinalization).toHaveBeenCalledTimes(1);
    expect(bot.finalize).toHaveBeenCalledTimes(1);
  });

  it("resumes a worker-reported pending finalization after a full signaling restart", async () => {
    const { bridge, control, bot, finalizationStore } = harness();
    finalizationStore.getRecordingRecoveryContext.mockResolvedValueOnce({
      roomId: room.roomId,
      integrationId: "integration_1",
      requestedAt: "2026-07-21T11:59:00.000Z",
    });
    finalizationStore.getV2RecordingFinalization.mockResolvedValueOnce({
      protocolVersion: "recording-capture.v2",
      captureGeneration: 1,
      activatedAt: "2026-07-21T12:00:00.000Z",
      captureStartedAt: "2026-07-21T12:00:00.000Z",
      terminalObservedAt: "2026-07-21T12:00:08.000Z",
      terminalTimelineEndMs: 8_000,
    });
    await bridge.workerPool.register({
      workerId: "worker-a",
      generationId: "generation-a",
      controlUrl: "http://worker-a:8090",
      capacity: 1,
    });

    await bridge.retryFinalization("rec_1");

    expect(bot.create).not.toHaveBeenCalled();
    expect(control.closeInputs).toHaveBeenCalledTimes(1);
    expect(finalizationStore.beginV2RecordingFinalization).not.toHaveBeenCalled();
    expect(control.bindFinalization).toHaveBeenCalledTimes(1);
    expect(bot.finalize).toHaveBeenCalledTimes(1);
  });

  it("recovers terminal finalization from durable context after Redis TTL expiry", async () => {
    const { bridge, control, bot, finalizationStore } = harness();
    control.recoveryContext.mockResolvedValueOnce({
      roomId: room.roomId,
      integrationId: "integration_1",
      requestedAt: "2026-07-21T11:59:00.000Z",
      protocolVersion: "recording-capture.v2",
      captureGeneration: 1,
      activatedAt: "2026-07-21T12:00:00.000Z",
      captureStartedAt: "2026-07-21T12:00:00.000Z",
      terminalTimelineEndMs: 8_000,
    });
    await bridge.workerPool.register({
      workerId: "worker-a",
      generationId: "generation-a",
      controlUrl: "http://worker-a:8090",
      capacity: 1,
    });

    await bridge.retryFinalization("rec_1");

    expect(finalizationStore.beginV2RecordingFinalization).not.toHaveBeenCalled();
    expect(control.lookup).not.toHaveBeenCalled();
    expect(control.closeInputs).toHaveBeenCalledTimes(1);
    expect(control.bindFinalization).toHaveBeenCalledTimes(1);
    expect(bot.finalize).toHaveBeenCalledTimes(1);
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
    expect(control.bindFinalization).toHaveBeenCalledWith(
      "rec_1",
      expect.objectContaining({ workerId: "worker-b" }),
    );
    expect(control.closeInputs).toHaveBeenCalledTimes(1);
    expect(bridge.getWorkerPoolSnapshot()).toMatchObject({ healthyWorkers: 1, occupiedSlots: 0 });
  });

  it("retains and requeues finalization until a worker recovery command succeeds", async () => {
    const { bridge, control, bot } = harness();
    await bridge.workerPool.register({
      workerId: "worker-a",
      generationId: "generation-a",
      controlUrl: "http://worker-a:8090",
      capacity: 1,
    });
    await bridge.workerPool.register({
      workerId: "worker-b",
      generationId: "generation-b",
      controlUrl: "http://worker-b:8090",
      capacity: 1,
    });
    await bridge.roomBecameLive(room, [participant], []);
    bot.stop.mockRejectedValueOnce(new Error("assigned worker stopped"));
    bot.finalize
      .mockRejectedValueOnce(new Error("finalize 1 failed"))
      .mockRejectedValueOnce(new Error("finalize 2 failed"))
      .mockRejectedValueOnce(new Error("finalize 3 failed"));

    await bridge.stopRoom(room.roomId);

    expect(bot.finalize).toHaveBeenCalledTimes(3);
    expect(control.closeInputs).toHaveBeenCalledTimes(1);
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
        captureGeneration: 1,
        captureStartedAt: "2026-07-21T12:00:00.000Z",
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
        .toMatchObject({
          protocolVersion: "recording-capture.v2",
          workerId: "worker-a",
          workerGenerationId: "generation-a",
          captureGeneration: 1,
          captureStartedAt: "2026-07-21T12:00:00.000Z",
        });
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

describe("HttpRecordingControl v2 lifecycle", () => {
  it("sends the worker binding on activation and the terminal fence on close", async () => {
    const request = vi.fn()
      .mockResolvedValueOnce(new Response(JSON.stringify({
        integrationId: "integration_1",
        recordingId: "rec_1",
        requestedAt: "2026-07-21T11:59:00.000Z",
        status: "capturing",
        protocolVersion: "recording-capture.v2",
        captureGeneration: 4,
        captureStartedAt: "2026-07-21T12:00:00.000Z",
      }), { status: 201, headers: { "Content-Type": "application/json" } }))
      .mockResolvedValueOnce(new Response(JSON.stringify({
        recordingId: "rec_1",
        status: "finalizing",
        protocolVersion: "recording-capture.v2",
        captureGeneration: 4,
        terminalTimelineEndMs: 8_000,
      }), { status: 201, headers: { "Content-Type": "application/json" } }))
      .mockResolvedValueOnce(new Response(JSON.stringify({
        recordingId: "rec_1",
        protocolVersion: "recording-capture.v2",
        captureGeneration: 5,
        captureStartedAt: "2026-07-21T12:00:00.000Z",
        bindingMode: "seal-only",
        terminalTimelineEndMs: 8_000,
      }), { status: 201, headers: { "Content-Type": "application/json" } }));
    vi.stubGlobal("fetch", request);
    try {
      const control = new HttpRecordingControl(config.controlUrl, config.controlJwt);
      const binding = {
        workerId: "worker-a",
        generationId: "generation-a",
        controlUrl: "http://worker-a:8090",
        recordingId: "rec_1",
      };
      await control.activate("room_1", binding);
      await control.closeInputs("rec_1", { captureGeneration: 4, terminalTimelineEndMs: 8_000 });
      await control.bindFinalization("rec_1", binding);

      const calls = request.mock.calls as unknown as Array<[string, RequestInit]>;
      expect(JSON.parse(String(calls[0]?.[1].body))).toEqual({
        protocolVersion: "recording-capture.v2",
        workerId: "worker-a",
        workerGenerationId: "generation-a",
      });
      expect(JSON.parse(String(calls[1]?.[1].body))).toEqual({
        protocolVersion: "recording-capture.v2",
        captureGeneration: 4,
        terminalTimelineEndMs: 8_000,
      });
      expect(calls[2]?.[0]).toContain("/capture-v2/finalization-bind");
      expect(JSON.parse(String(calls[2]?.[1].body))).toEqual({
        protocolVersion: "recording-capture.v2",
        workerId: "worker-a",
        workerGenerationId: "generation-a",
      });
    } finally {
      vi.unstubAllGlobals();
    }
  });

  it("treats only a structured stale-generation close as an idempotent replay", async () => {
    const request = vi.fn(async () => new Response(JSON.stringify({
      code: "STALE_CAPTURE_GENERATION",
      message: "Capture generation is stale",
    }), { status: 409, headers: { "Content-Type": "application/json" } }));
    vi.stubGlobal("fetch", request);
    try {
      const control = new HttpRecordingControl(config.controlUrl, config.controlJwt);

      await expect(control.closeInputs("rec_1", {
        captureGeneration: 4,
        terminalTimelineEndMs: 8_000,
      })).resolves.toBeUndefined();
    } finally {
      vi.unstubAllGlobals();
    }
  });
});
