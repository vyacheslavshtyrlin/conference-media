import { EventEmitter } from "node:events";
import { beforeEach, describe, expect, it, vi } from "vitest";

import { SignalingError } from "./errors.js";
import { MediasoupService } from "./mediasoupService.js";

const { createWorker } = vi.hoisted(() => ({
  createWorker: vi.fn(),
}));

vi.mock("mediasoup", () => ({
  createWorker,
}));

type FakeProducer = {
  id: string;
  kind: "audio" | "video";
  type: "simple" | "simulcast" | "svc";
  observer: EventEmitter;
  paused: boolean;
  close: () => void;
};

type FakeConsumer = {
  id: string;
  kind: "audio" | "video";
  rtpParameters: Record<string, unknown>;
  paused: boolean;
  observer: EventEmitter;
  close: () => void;
  pause: () => Promise<void>;
  resume: () => Promise<void>;
  setPreferredLayers: (layers: { spatialLayer: number; temporalLayer?: number }) => Promise<void>;
};

type FakeAudioLevelObserver = EventEmitter & {
  addProducer: () => Promise<void>;
  removeProducer: () => Promise<void>;
  close: () => void;
};

let transportCounter = 0;
let producerCounter = 0;
let consumerCounter = 0;
let fakeAudioLevelObservers: FakeAudioLevelObserver[] = [];
let fakeAudioLevelObserverOptions: Array<Record<string, unknown>> = [];
let fakeTransportConnections: Array<{ ip?: string; port?: number }> = [];
let fakeRouters: unknown[] = [];
let fakePreferredLayers: Array<{ spatialLayer: number; temporalLayer?: number }> = [];
let fakeConsumerPaused: boolean[] = [];
let fakeRouterMediaCodecs: Array<Array<Record<string, unknown>>> = [];
let fakeProducedRtpParameters: Array<Record<string, unknown>> = [];
let fakeConsumedRtpCapabilities: Array<Record<string, unknown>> = [];
let fakeCanConsume: (rtpCapabilities: Record<string, unknown>) => boolean = () => true;
let nextFakeProducerType: FakeProducer["type"] = "simple";

function createFakeWorker() {
  const worker = new EventEmitter() as EventEmitter & {
    observer: EventEmitter;
    close: () => void;
    createRouter: (options: { mediaCodecs: Array<Record<string, unknown>> }) => Promise<ReturnType<typeof createFakeRouter>>;
  };
  worker.observer = new EventEmitter();
  worker.close = () => {
    worker.observer.emit("close");
  };
  worker.createRouter = async ({ mediaCodecs }) => {
    fakeRouterMediaCodecs.push(mediaCodecs);
    return createFakeRouter(mediaCodecs);
  };
  return worker;
}

function createFakeRouter(mediaCodecs: Array<Record<string, unknown>>) {
  const router = {
    rtpCapabilities: { codecs: mediaCodecs },
    canConsume: ({ rtpCapabilities }: { rtpCapabilities: Record<string, unknown> }) => fakeCanConsume(rtpCapabilities),
    createAudioLevelObserver: async (options: Record<string, unknown>) => {
      fakeAudioLevelObserverOptions.push(options);
      return createFakeAudioLevelObserver();
    },
    createWebRtcTransport: async () => createFakeTransport(),
  };
  fakeRouters.push(router);
  return router;
}

function createFakeAudioLevelObserver(): FakeAudioLevelObserver {
  const observer = new EventEmitter() as FakeAudioLevelObserver;
  observer.addProducer = async () => {};
  observer.removeProducer = async () => {};
  observer.close = () => {
    observer.emit("close");
  };
  fakeAudioLevelObservers.push(observer);
  return observer;
}

function createFakeTransport() {
  const id = `transport_${++transportCounter}`;
  return {
    id,
    iceParameters: {},
    iceCandidates: [],
    dtlsParameters: {},
    observer: new EventEmitter(),
    closed: false,
    connect: async (parameters: { ip?: string; port?: number }) => {
      fakeTransportConnections.push(parameters);
    },
    produce: async ({ kind, rtpParameters }: { kind: "audio" | "video"; rtpParameters: Record<string, unknown> }) => {
      fakeProducedRtpParameters.push(rtpParameters);
      return createFakeProducer(kind);
    },
    consume: async ({ producerId, paused, rtpCapabilities }: { producerId: string; paused?: boolean; rtpCapabilities: Record<string, unknown> }) => {
      fakeConsumerPaused.push(paused ?? false);
      fakeConsumedRtpCapabilities.push(rtpCapabilities);
      return createFakeConsumer(producerId, paused ?? false);
    },
    close() {
      this.closed = true;
      this.observer.emit("close");
    },
  };
}

function createFakeProducer(kind: "audio" | "video"): FakeProducer {
  const producer: FakeProducer = {
    id: `producer_${++producerCounter}`,
    kind,
    type: nextFakeProducerType,
    observer: new EventEmitter(),
    paused: false,
    close: vi.fn(() => {
      producer.observer.emit("close");
    }),
  };
  return producer;
}

function createFakeConsumer(_producerId: string, paused: boolean): FakeConsumer {
  const consumer: FakeConsumer = {
    id: `consumer_${++consumerCounter}`,
    kind: "video",
    rtpParameters: {},
    paused,
    observer: new EventEmitter(),
    close: vi.fn(() => {
      consumer.observer.emit("close");
    }),
    pause: async () => {
      consumer.paused = true;
    },
    resume: async () => {
      consumer.paused = false;
    },
    setPreferredLayers: async (layers) => {
      fakePreferredLayers.push(layers);
    },
  };
  return consumer;
}

async function createService(overrides: Partial<ConstructorParameters<typeof MediasoupService>[0]> = {}) {
  const service = new MediasoupService(
    {
      numWorkers: 1,
      listenIp: "127.0.0.1",
      rtcMinPort: 40000,
      rtcMaxPort: 49999,
      maxTransportsPerPeer: 4,
      maxProducersPerPeer: 3,
      maxConsumersPerPeer: 32,
      iceServers: [],
      ...overrides,
    },
    undefined,
    undefined,
  );
  await service.init();
  return service;
}

describe("MediasoupService", () => {
  beforeEach(() => {
    transportCounter = 0;
    producerCounter = 0;
    consumerCounter = 0;
    fakeAudioLevelObservers = [];
    fakeAudioLevelObserverOptions = [];
    fakeTransportConnections = [];
    fakeRouters = [];
    fakePreferredLayers = [];
    fakeConsumerPaused = [];
    fakeRouterMediaCodecs = [];
    fakeProducedRtpParameters = [];
    fakeConsumedRtpCapabilities = [];
    fakeCanConsume = () => true;
    nextFakeProducerType = "simple";
    createWorker.mockImplementation(async () => createFakeWorker());
  });

  it("does not create a recording worker when recording is disabled", async () => {
    const service = new MediasoupService({
      numWorkers: 1,
      listenIp: "127.0.0.1",
      rtcMinPort: 40000,
      rtcMaxPort: 49999,
      maxTransportsPerPeer: 4,
      maxProducersPerPeer: 3,
      maxConsumersPerPeer: 32,
      iceServers: [],
    });

    await service.init();

    expect(createWorker).toHaveBeenCalledTimes(1);
  });

  it("configures active speaker detection to ignore quiet background noise", async () => {
    const service = await createService();

    await service.ensureRoom("room_1");

    expect(fakeAudioLevelObserverOptions[0]).toMatchObject({
      maxEntries: 1,
      threshold: -55,
      interval: 800,
    });
  });

  it("advertises VP8 before the constrained-baseline H.264 tuple", async () => {
    const service = await createService();

    const capabilities = await service.getRouterRtpCapabilities("room_1");

    expect(fakeRouterMediaCodecs[0]).toEqual([
      expect.objectContaining({ kind: "audio", mimeType: "audio/opus", clockRate: 48000 }),
      expect.objectContaining({ kind: "video", mimeType: "video/VP8", clockRate: 90000 }),
      {
        kind: "video",
        mimeType: "video/H264",
        clockRate: 90000,
        parameters: {
          "packetization-mode": 1,
          "level-asymmetry-allowed": 1,
          "profile-level-id": "42e01f",
        },
      },
    ]);
    expect(capabilities.codecs?.map((codec) => codec.mimeType)).toEqual([
      "audio/opus",
      "video/VP8",
      "video/H264",
    ]);
  });

  it("produces and consumes the supported constrained-baseline H.264 tuple for a bot", async () => {
    const service = await createService();
    const sendTransport = await service.createWebRtcTransport("room_1", "peer_1", "send");
    const h264RtpParameters = {
      codecs: [{
        mimeType: "video/H264",
        payloadType: 102,
        clockRate: 90000,
        parameters: {
          "packetization-mode": 1,
          "level-asymmetry-allowed": 1,
          "profile-level-id": "42e01f",
        },
      }],
    };
    const h264Capabilities = {
      codecs: [{
        kind: "video",
        mimeType: "video/H264",
        preferredPayloadType: 102,
        clockRate: 90000,
        parameters: {
          "packetization-mode": 1,
          "level-asymmetry-allowed": 1,
          "profile-level-id": "42e01f",
        },
      }],
    };
    fakeCanConsume = (capabilities) => capabilities === h264Capabilities;
    const producer = await service.produce(
      "room_1",
      "peer_1",
      sendTransport.id,
      "video",
      h264RtpParameters,
      "camera",
      () => {},
    );
    const botTransport = await service.createBotRecvTransport("room_1", "rec_1");

    await expect(
      service.consumeForBot("room_1", "rec_1", botTransport.id, producer.id, h264Capabilities),
    ).resolves.toMatchObject({ producerId: producer.id, paused: true });
    expect(fakeProducedRtpParameters).toEqual([h264RtpParameters]);
    expect(fakeConsumedRtpCapabilities).toEqual([h264Capabilities]);
  });

  it("keeps VP8 consumption available and explicitly rejects an unsupported H.264 tuple", async () => {
    const service = await createService();
    const sendTransport = await service.createWebRtcTransport("room_1", "peer_1", "send");
    const recvTransport = await service.createWebRtcTransport("room_1", "peer_2", "recv");
    const vp8RtpParameters = {
      codecs: [{ mimeType: "video/VP8", payloadType: 96, clockRate: 90000 }],
    };
    const vp8Capabilities = {
      codecs: [{ kind: "video", mimeType: "video/VP8", preferredPayloadType: 96, clockRate: 90000 }],
    };
    const unsupportedH264Capabilities = {
      codecs: [{
        kind: "video",
        mimeType: "video/H264",
        preferredPayloadType: 102,
        clockRate: 90000,
        parameters: { "packetization-mode": 0, "profile-level-id": "64001f" },
      }],
    };
    fakeCanConsume = (capabilities) => capabilities === vp8Capabilities;
    const producer = await service.produce(
      "room_1",
      "peer_1",
      sendTransport.id,
      "video",
      vp8RtpParameters,
      "camera",
      () => {},
    );

    await expect(
      service.consume("room_1", "peer_2", recvTransport.id, producer.id, vp8Capabilities),
    ).resolves.toMatchObject({ producerId: producer.id });
    expect(fakeProducedRtpParameters).toEqual([vp8RtpParameters]);

    const botTransport = await service.createBotRecvTransport("room_1", "rec_1");
    await expect(
      service.consumeForBot("room_1", "rec_1", botTransport.id, producer.id, unsupportedH264Capabilities),
    ).rejects.toMatchObject(new SignalingError("MEDIASOUP_ERROR", "Bot cannot consume producer"));
  });

  it("closes consumers for a producer when the producer owner leaves", async () => {
    const service = await createService();
    const sendTransport = await service.createWebRtcTransport("room_1", "peer_1", "send");
    const recvTransport = await service.createWebRtcTransport("room_1", "peer_2", "recv");
    const producer = await service.produce("room_1", "peer_1", sendTransport.id, "video", {}, "camera", () => {});

    await service.consume("room_1", "peer_2", recvTransport.id, producer.id, {});

    expect(service.getRuntimeStats()).toMatchObject({ producers: 1, consumers: 1 });

    service.closePeer("room_1", "peer_1");

    expect(service.getRuntimeStats()).toMatchObject({ producers: 0, consumers: 0 });
  });

  it("keeps one idempotent bot receive transport/consumer and tears it down without touching the live room", async () => {
    const service = await createService();
    const sendTransport = await service.createWebRtcTransport("room_1", "peer_1", "send");
    const camera = await service.produce("room_1", "peer_1", sendTransport.id, "video", {}, "camera", () => {});
    const [firstTransport, secondTransport] = await Promise.all([
      service.createBotRecvTransport("room_1", "rec_1"),
      service.createBotRecvTransport("room_1", "rec_1"),
    ]);

    const [firstConsumer, secondConsumer] = await Promise.all([
      service.consumeForBot("room_1", "rec_1", firstTransport.id, camera.id, {}),
      service.consumeForBot("room_1", "rec_1", firstTransport.id, camera.id, {}),
    ]);

    expect(secondTransport.id).toBe(firstTransport.id);
    expect(transportCounter).toBe(2);
    expect(secondConsumer.id).toBe(firstConsumer.id);
    expect(firstConsumer.paused).toBe(true);
    expect(secondConsumer.paused).toBe(true);
    expect(fakeConsumerPaused).toEqual([true]);

    service.closeBotSession("room_1", "rec_1");
    expect(service.getRuntimeStats()).toMatchObject({ rooms: 1, producers: 1, transports: 1 });
    expect(service.listProducers("room_1")).toContainEqual(expect.objectContaining({ producerId: camera.id }));
    await expect(service.closeProducer("room_1", "peer_1", camera.id)).resolves.toMatchObject({ producerId: camera.id });
  });

  it("enforces per-peer producer limits", async () => {
    const service = await createService({ maxProducersPerPeer: 1 });
    const sendTransport = await service.createWebRtcTransport("room_1", "peer_1", "send");

    await service.produce("room_1", "peer_1", sendTransport.id, "audio", {}, "mic", () => {});

    await expect(
      service.produce("room_1", "peer_1", sendTransport.id, "video", {}, "camera", () => {}),
    ).rejects.toMatchObject(new SignalingError("MEDIASOUP_ERROR", "Peer producer limit reached"));
  });

  it("allows only one active screen share producer per room", async () => {
    const service = await createService();
    const firstTransport = await service.createWebRtcTransport("room_1", "peer_1", "send");
    const secondTransport = await service.createWebRtcTransport("room_1", "peer_2", "send");

    await service.produce("room_1", "peer_1", firstTransport.id, "video", {}, "screen", () => {});

    await expect(
      service.produce("room_1", "peer_2", secondTransport.id, "video", {}, "screen", () => {}),
    ).rejects.toMatchObject(new SignalingError("MEDIASOUP_ERROR", "Room already has an active screen share"));

    service.closePeer("room_1", "peer_1");

    await expect(
      service.produce("room_1", "peer_2", secondTransport.id, "video", {}, "screen", () => {}),
    ).resolves.toMatchObject({ id: expect.stringMatching(/^producer_/) });
  });

  it("prevents one peer from publishing camera and screen at the same time", async () => {
    const service = await createService();
    const cameraFirstTransport = await service.createWebRtcTransport("room_1", "peer_1", "send");
    const screenFirstTransport = await service.createWebRtcTransport("room_2", "peer_2", "send");

    await service.produce("room_1", "peer_1", cameraFirstTransport.id, "video", {}, "camera", () => {});

    await expect(
      service.produce("room_1", "peer_1", cameraFirstTransport.id, "video", {}, "screen", () => {}),
    ).rejects.toMatchObject(
      new SignalingError("MEDIASOUP_ERROR", "Peer cannot publish camera and screen at the same time"),
    );

    await service.produce("room_2", "peer_2", screenFirstTransport.id, "video", {}, "screen", () => {});

    await expect(
      service.produce("room_2", "peer_2", screenFirstTransport.id, "video", {}, "camera", () => {}),
    ).rejects.toMatchObject(
      new SignalingError("MEDIASOUP_ERROR", "Peer cannot publish camera and screen at the same time"),
    );
  });

  it("releases the screen share slot when the active producer is closed", async () => {
    const service = await createService();
    const firstTransport = await service.createWebRtcTransport("room_1", "peer_1", "send");
    const secondTransport = await service.createWebRtcTransport("room_1", "peer_2", "send");
    const firstProducer = await service.produce("room_1", "peer_1", firstTransport.id, "video", {}, "screen", () => {});

    await service.closeProducer("room_1", "peer_1", firstProducer.id);

    await expect(
      service.produce("room_1", "peer_2", secondTransport.id, "video", {}, "screen", () => {}),
    ).resolves.toMatchObject({ id: expect.stringMatching(/^producer_/) });
  });

  it("emits active speaker changes from mic audio levels and silence", async () => {
    const service = await createService();
    const changes: Array<string | null> = [];
    service.setActiveSpeakerChangedCallback((_roomId, participantId) => {
      changes.push(participantId);
    });
    const sendTransport = await service.createWebRtcTransport("room_1", "peer_1", "send");
    const producer = await service.produce("room_1", "peer_1", sendTransport.id, "audio", {}, "mic", () => {});

    fakeAudioLevelObservers[0]?.emit("volumes", [{ producer: { id: producer.id }, volume: -32 }]);
    fakeAudioLevelObservers[0]?.emit("silence");

    expect(changes).toEqual(["peer_1", null]);
  });

  it("removes a dead worker from readiness, closes its rooms and replaces it", async () => {
    const firstWorker = createFakeWorker();
    const replacementWorker = createFakeWorker();
    createWorker.mockReset();
    createWorker.mockResolvedValueOnce(firstWorker).mockResolvedValueOnce(replacementWorker);
    const service = await createService();
    const closedRooms: string[] = [];
    service.setRoomDeadCallback((roomId) => closedRooms.push(roomId));
    await service.ensureRoom("room_1");

    firstWorker.emit("died");
    await new Promise((resolve) => setTimeout(resolve, 0));

    expect(closedRooms).toEqual(["room_1"]);
    expect(createWorker).toHaveBeenCalledTimes(2);
    expect(service.getRuntimeStats().workers).toBe(1);
    expect(service.isReady()).toBe(true);
  });

  it("becomes unready when a dead worker cannot be replaced", async () => {
    const firstWorker = createFakeWorker();
    createWorker.mockReset();
    createWorker.mockResolvedValueOnce(firstWorker).mockRejectedValueOnce(new Error("replacement failed"));
    const service = await createService();

    firstWorker.emit("died");
    await new Promise((resolve) => setTimeout(resolve, 0));

    expect(service.getRuntimeStats().workers).toBe(0);
    expect(service.isReady()).toBe(false);
  });

});
