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
  observer: EventEmitter;
  close: () => void;
};

type FakeConsumer = {
  id: string;
  kind: "audio" | "video";
  rtpParameters: Record<string, unknown>;
  observer: EventEmitter;
  close: () => void;
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

function createFakeWorker() {
  const worker = new EventEmitter() as EventEmitter & {
    observer: EventEmitter;
    close: () => void;
    createRouter: () => Promise<ReturnType<typeof createFakeRouter>>;
  };
  worker.observer = new EventEmitter();
  worker.close = () => {
    worker.observer.emit("close");
  };
  worker.createRouter = async () => createFakeRouter();
  return worker;
}

function createFakeRouter() {
  return {
    rtpCapabilities: {},
    canConsume: () => true,
    createAudioLevelObserver: async (options: Record<string, unknown>) => {
      fakeAudioLevelObserverOptions.push(options);
      return createFakeAudioLevelObserver();
    },
    createWebRtcTransport: async () => createFakeTransport(),
  };
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
    connect: async () => {},
    produce: async ({ kind }: { kind: "audio" | "video" }) => createFakeProducer(kind),
    consume: async ({ producerId }: { producerId: string }) => createFakeConsumer(producerId),
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
    observer: new EventEmitter(),
    close: () => {
      producer.observer.emit("close");
    },
  };
  return producer;
}

function createFakeConsumer(_producerId: string): FakeConsumer {
  const consumer: FakeConsumer = {
    id: `consumer_${++consumerCounter}`,
    kind: "video",
    rtpParameters: {},
    observer: new EventEmitter(),
    close: () => {
      consumer.observer.emit("close");
    },
  };
  return consumer;
}

async function createService(overrides: Partial<ConstructorParameters<typeof MediasoupService>[0]> = {}) {
  const service = new MediasoupService({
    numWorkers: 1,
    listenIp: "127.0.0.1",
    rtcMinPort: 40000,
    rtcMaxPort: 49999,
    maxTransportsPerPeer: 4,
    maxProducersPerPeer: 3,
    maxConsumersPerPeer: 32,
    iceServers: [],
    ...overrides,
  });
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
    createWorker.mockResolvedValue(createFakeWorker());
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
