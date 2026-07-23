import * as mediasoup from "mediasoup";
import type { CreateWebRtcTransportResponseData, ParticipantMedia, RoomClosedReason } from "#contracts";
import type {
  Consumer,
  DtlsParameters,
  MediaKind,
  AudioLevelObserver,
  Producer,
  RtpCapabilities,
  RtpParameters,
  Router,
  Transport,
  WebRtcTransport,
  Worker,
} from "mediasoup/types";

import type { SignalingConfig } from "./config.js";
import { SignalingError } from "./errors.js";
import type { Logger } from "./logger.js";
import { getMediaMetrics, type MediaMetrics } from "./metrics.js";

type TransportRecord = {
  transport: WebRtcTransport;
  participantId: string;
  direction: "send" | "recv";
};

type ProducerRecord = {
  producer: Producer;
  participantId: string;
  source: "mic" | "camera" | "screen";
};

type ConsumerRecord = {
  consumer: Consumer;
  participantId: string;
  producerId: string;
};

type BotSessionRecord = {
  transport?: WebRtcTransport;
  transportPromise?: Promise<WebRtcTransport>;
  consumers: Map<string, Consumer>;
  consumerPromises: Map<string, Promise<Consumer>>;
};

type RuntimeRoom = {
  workerIndex: number;
  router: Router;
  audioLevelObserver: AudioLevelObserver;
  activeSpeakerParticipantId: string | null;
  transports: Map<string, TransportRecord>;
  producers: Map<string, ProducerRecord>;
  consumers: Map<string, ConsumerRecord>;
  botSessions: Map<string, BotSessionRecord>;
};

const mediaCodecs = [
  {
    kind: "audio" as const,
    mimeType: "audio/opus",
    clockRate: 48000,
    channels: 2,
  },
  {
    kind: "video" as const,
    mimeType: "video/VP8",
    clockRate: 90000,
    parameters: {
      "x-google-start-bitrate": 1000,
    },
  },
];

const ACTIVE_SPEAKER_THRESHOLD_DBOV = -55;
const ACTIVE_SPEAKER_INTERVAL_MS = 800;

export class MediasoupService {
  private readonly rooms = new Map<string, RuntimeRoom>();
  private workers: Array<Worker | undefined> = [];
  private nextWorkerIndex = 0;
  private closing = false;
  private onRoomDeadCallback?: (roomId: string, reason: RoomClosedReason) => void;
  private onActiveSpeakerChangedCallback?: (roomId: string, participantId: string | null) => void;

  constructor(
    private readonly config: SignalingConfig["mediasoup"],
    private readonly logger?: Logger,
    private readonly metrics: MediaMetrics = getMediaMetrics(),
  ) {}

  setRoomDeadCallback(callback: (roomId: string, reason: RoomClosedReason) => void): void {
    this.onRoomDeadCallback = callback;
  }

  setActiveSpeakerChangedCallback(callback: (roomId: string, participantId: string | null) => void): void {
    this.onActiveSpeakerChangedCallback = callback;
  }

  async init(): Promise<void> {
    const count = this.config.numWorkers;
    this.closing = false;
    this.workers = Array.from({ length: count });
    await Promise.all(Array.from({ length: count }, (_, index) => this.createWorkerAt(index)));
    this.logger?.info("mediasoup worker pool ready", {
      operation: "mediasoup.workerPool.init",
      count: this.getUsableWorkerCount(),
    });
  }

  closeAll(): void {
    this.closing = true;
    // Notify RoomManager (and through it SocketGateway) for every live room so
    // Redis participant records are cleaned up even on graceful shutdown.
    for (const roomId of [...this.rooms.keys()]) {
      this.closeDeadRoom(roomId, "server_shutdown");
    }
    for (const worker of this.workers) {
      worker?.close();
    }
    this.workers = [];
  }

  async ensureRoom(roomId: string): Promise<RuntimeRoom> {
    const existing = this.rooms.get(roomId);
    if (existing) {
      return existing;
    }

    const workerIndex = this.findNextWorkerIndex();
    const worker = workerIndex === undefined ? undefined : this.workers[workerIndex];

    if (workerIndex === undefined || !worker) {
      throw new SignalingError("MEDIASOUP_ERROR", "Mediasoup worker pool is not initialized");
    }

    const router = await worker.createRouter({ mediaCodecs });
    const audioLevelObserver = await router.createAudioLevelObserver({
      maxEntries: 1,
      threshold: ACTIVE_SPEAKER_THRESHOLD_DBOV,
      interval: ACTIVE_SPEAKER_INTERVAL_MS,
    });
    const room: RuntimeRoom = {
      workerIndex,
      router,
      audioLevelObserver,
      activeSpeakerParticipantId: null,
      transports: new Map(),
      producers: new Map(),
      consumers: new Map(),
      botSessions: new Map(),
    };

    audioLevelObserver.on("volumes", ([volume]) => {
      const producerId = volume?.producer.id;
      if (!producerId) return;

      const record = room.producers.get(producerId);
      if (!record || record.source !== "mic") return;

      this.setActiveSpeaker(roomId, room, record.participantId);
    });
    audioLevelObserver.on("silence", () => {
      this.setActiveSpeaker(roomId, room, null);
    });

    this.rooms.set(roomId, room);
    this.logger?.info("mediasoup room created", {
      operation: "mediasoup.room.create",
      roomId,
      workerIndex,
      outcome: "ok",
    });
    return room;
  }

  async getRouterRtpCapabilities(
    roomId: string,
  ): Promise<Router["rtpCapabilities"]> {
    return (await this.ensureRoom(roomId)).router.rtpCapabilities;
  }

  async createWebRtcTransport(
    roomId: string,
    participantId: string,
    direction: "send" | "recv",
  ): Promise<CreateWebRtcTransportResponseData> {
    const room = await this.ensureRoom(roomId);
    this.assertPeerLimit(
      room.transports,
      participantId,
      this.config.maxTransportsPerPeer,
      "Peer transport limit reached",
    );

    const transport = await room.router.createWebRtcTransport({
      listenIps: [
        {
          ip: this.config.listenIp,
          announcedIp: this.config.announcedIp,
        },
      ],
      enableUdp: true,
      enableTcp: true,
      preferTcp: true,
      initialAvailableOutgoingBitrate: 1_000_000,
    });

    room.transports.set(transport.id, { transport, participantId, direction });
    transport.observer.once("close", () => {
      room.transports.delete(transport.id);
      this.logger?.info("mediasoup transport closed", {
        operation: "mediasoup.transport.close",
        roomId,
        participantId,
        transportId: transport.id,
        direction,
        outcome: "ok",
      });
    });
    this.logger?.info("mediasoup transport created", {
      operation: "mediasoup.transport.create",
      roomId,
      participantId,
      transportId: transport.id,
      direction,
      outcome: "ok",
    });

    return {
      id: transport.id,
      iceParameters: transport.iceParameters as CreateWebRtcTransportResponseData["iceParameters"],
      iceCandidates: transport.iceCandidates as CreateWebRtcTransportResponseData["iceCandidates"],
      dtlsParameters: transport.dtlsParameters as CreateWebRtcTransportResponseData["dtlsParameters"],
      iceServers: this.config.iceServers.length > 0 ? this.config.iceServers : undefined,
    };
  }

  async connectTransport(
    roomId: string,
    participantId: string,
    transportId: string,
    dtlsParameters: unknown,
  ) {
    const record = await this.getTransport(roomId, participantId, transportId);
    await record.transport.connect({
      dtlsParameters: dtlsParameters as DtlsParameters,
    });
  }

  async produce(
    roomId: string,
    participantId: string,
    transportId: string,
    kind: MediaKind,
    rtpParameters: unknown,
    source: "mic" | "camera" | "screen",
    onClose: (producerId: string) => void,
  ): Promise<{ id: string }> {
    const record = await this.getTransport(roomId, participantId, transportId);
    if (record.direction !== "send") {
      throw new SignalingError(
        "TRANSPORT_NOT_FOUND",
        "Send transport not found",
      );
    }
    const room = await this.ensureRoom(roomId);
    this.assertPeerLimit(
      room.producers,
      participantId,
      this.config.maxProducersPerPeer,
      "Peer producer limit reached",
    );
    if (this.hasProducerForSource(room, participantId, source)) {
      throw new SignalingError(
        "MEDIASOUP_ERROR",
        "Peer already has an active producer for this source",
      );
    }
    const exclusiveVideoSource = source === "camera" ? "screen" : source === "screen" ? "camera" : null;
    if (exclusiveVideoSource && this.hasProducerForSource(room, participantId, exclusiveVideoSource)) {
      throw new SignalingError(
        "MEDIASOUP_ERROR",
        "Peer cannot publish camera and screen at the same time",
      );
    }
    if (source === "screen" && this.hasActiveScreenProducer(room)) {
      throw new SignalingError(
        "MEDIASOUP_ERROR",
        "Room already has an active screen share",
      );
    }

    const producer = await record.transport.produce({
      kind,
      rtpParameters: rtpParameters as RtpParameters,
      appData: { participantId, source },
    });
    room.producers.set(producer.id, { producer, participantId, source });

    producer.observer.once("close", () => {
      room.producers.delete(producer.id);
      if (source === "mic" && room.activeSpeakerParticipantId === participantId) {
        this.setActiveSpeaker(roomId, room, null);
      }
      this.closeConsumersForProducer(room, producer.id);
      onClose(producer.id);
      this.logger?.info("mediasoup producer closed", {
        operation: "mediasoup.producer.close",
        roomId,
        participantId,
        producerId: producer.id,
        source,
        kind,
        outcome: "ok",
      });
    });

    if (source === "mic") {
      try {
        await room.audioLevelObserver.addProducer({ producerId: producer.id });
      } catch {
        producer.close();
        throw new SignalingError("MEDIASOUP_ERROR", "Audio level observer failed");
      }
    }

    this.logger?.info("mediasoup producer created", {
      operation: "mediasoup.producer.create",
      roomId,
      participantId,
      producerId: producer.id,
      source,
      kind,
      outcome: "ok",
    });
    return { id: producer.id };
  }

  async consume(
    roomId: string,
    participantId: string,
    transportId: string,
    producerId: string,
    rtpCapabilities: unknown,
  ): Promise<{
    id: string;
    producerId: string;
    kind: MediaKind;
    rtpParameters: RtpParameters;
    participantId: string;
    source: "mic" | "camera" | "screen";
  }> {
    const room = await this.ensureRoom(roomId);
    const transportRecord = await this.getTransport(
      roomId,
      participantId,
      transportId,
    );
    if (transportRecord.direction !== "recv") {
      throw new SignalingError(
        "TRANSPORT_NOT_FOUND",
        "Receive transport not found",
      );
    }
    this.assertPeerLimit(
      room.consumers,
      participantId,
      this.config.maxConsumersPerPeer,
      "Peer consumer limit reached",
    );

    const producerRecord = room.producers.get(producerId);
    if (!producerRecord) {
      throw new SignalingError("PRODUCER_NOT_FOUND", "Producer not found");
    }

    if (
      !room.router.canConsume({
        producerId,
        rtpCapabilities: rtpCapabilities as RtpCapabilities,
      })
    ) {
      throw new SignalingError(
        "MEDIASOUP_ERROR",
        "Client cannot consume producer",
      );
    }

    const consumer = await transportRecord.transport.consume({
      producerId,
      rtpCapabilities: rtpCapabilities as RtpCapabilities,
      paused: false,
    });
    room.consumers.set(consumer.id, { consumer, participantId, producerId });
    consumer.observer.once("close", () => {
      room.consumers.delete(consumer.id);
      this.logger?.info("mediasoup consumer closed", {
        operation: "mediasoup.consumer.close",
        roomId,
        participantId,
        consumerId: consumer.id,
        producerId,
        outcome: "ok",
      });
    });
    this.logger?.info("mediasoup consumer created", {
      operation: "mediasoup.consumer.create",
      roomId,
      participantId,
      consumerId: consumer.id,
      producerId,
      outcome: "ok",
    });

    return {
      id: consumer.id,
      producerId,
      kind: consumer.kind,
      rtpParameters: consumer.rtpParameters,
      participantId: producerRecord.participantId,
      source: producerRecord.source,
    };
  }

  async createBotRecvTransport(roomId: string, recordingId: string): Promise<CreateWebRtcTransportResponseData> {
    const room = await this.ensureRoom(roomId);
    const session = room.botSessions.get(recordingId) ?? {
      consumers: new Map<string, Consumer>(),
      consumerPromises: new Map<string, Promise<Consumer>>(),
    };
    room.botSessions.set(recordingId, session);
    if (session.transport && !session.transport.closed) {
      return this.describeWebRtcTransport(session.transport);
    }
    if (!session.transportPromise) {
      session.transportPromise = room.router.createWebRtcTransport({
        listenIps: [{ ip: this.config.listenIp, announcedIp: this.config.announcedIp }],
        enableUdp: true,
        enableTcp: true,
        preferTcp: true,
        initialAvailableOutgoingBitrate: 1_000_000,
      });
    }
    const pending = session.transportPromise;
    let transport: WebRtcTransport;
    try {
      transport = await pending;
    } finally {
      if (session.transportPromise === pending) session.transportPromise = undefined;
    }
    if (room.botSessions.get(recordingId) !== session) {
      transport.close();
      throw new SignalingError("TRANSPORT_NOT_FOUND", "Bot session was closed");
    }
    if (session.transport === transport) return this.describeWebRtcTransport(transport);
    session.transport = transport;
    transport.observer.once("close", () => {
      if (session.transport === transport) session.transport = undefined;
      for (const consumer of session.consumers.values()) consumer.close();
      session.consumers.clear();
    });
    return this.describeWebRtcTransport(transport);
  }

  async connectBotRecvTransport(roomId: string, recordingId: string, transportId: string, dtlsParameters: unknown): Promise<void> {
    const transport = this.requireBotTransport(roomId, recordingId, transportId);
    await transport.connect({ dtlsParameters: dtlsParameters as DtlsParameters });
  }

  async consumeForBot(
    roomId: string,
    recordingId: string,
    transportId: string,
    producerId: string,
    rtpCapabilities: unknown,
  ): Promise<{ id: string; producerId: string; kind: MediaKind; rtpParameters: RtpParameters; paused: boolean }> {
    const room = await this.ensureRoom(roomId);
    const session = room.botSessions.get(recordingId);
    const transport = this.requireBotTransport(roomId, recordingId, transportId);
    const existing = session?.consumers.get(producerId);
    if (existing && !existing.closed) {
      return { id: existing.id, producerId, kind: existing.kind, rtpParameters: existing.rtpParameters, paused: existing.paused };
    }
    const producer = room.producers.get(producerId)?.producer;
    if (!producer) throw new SignalingError("PRODUCER_NOT_FOUND", "Producer not found");
    if (!room.router.canConsume({ producerId, rtpCapabilities: rtpCapabilities as RtpCapabilities })) {
      throw new SignalingError("MEDIASOUP_ERROR", "Bot cannot consume producer");
    }
    if (!session) throw new SignalingError("TRANSPORT_NOT_FOUND", "Bot receive transport not found");
    const existingPromise = session.consumerPromises.get(producerId);
    const pending = existingPromise ?? transport.consume({
      producerId,
      rtpCapabilities: rtpCapabilities as RtpCapabilities,
      paused: true,
    });
    if (!existingPromise) session.consumerPromises.set(producerId, pending);
    let consumer: Consumer;
    try {
      consumer = await pending;
    } finally {
      if (session.consumerPromises.get(producerId) === pending) session.consumerPromises.delete(producerId);
    }
    if (room.botSessions.get(recordingId) !== session) {
      consumer.close();
      throw new SignalingError("TRANSPORT_NOT_FOUND", "Bot session was closed");
    }
    const installed = session.consumers.get(producerId);
    if (installed === consumer) {
      return { id: consumer.id, producerId, kind: consumer.kind, rtpParameters: consumer.rtpParameters, paused: consumer.paused };
    }
    session.consumers.set(producerId, consumer);
    consumer.observer.once("close", () => {
      if (session.consumers.get(producerId) === consumer) session.consumers.delete(producerId);
    });
    return { id: consumer.id, producerId, kind: consumer.kind, rtpParameters: consumer.rtpParameters, paused: consumer.paused };
  }

  async setBotConsumerPaused(roomId: string, recordingId: string, producerId: string, paused: boolean): Promise<void> {
    const consumer = this.rooms.get(roomId)?.botSessions.get(recordingId)?.consumers.get(producerId);
    if (!consumer || consumer.closed || consumer.paused === paused) return;
    if (paused) await consumer.pause();
    else await consumer.resume();
  }

  closeBotConsumer(roomId: string, recordingId: string, producerId: string): void {
    const session = this.rooms.get(roomId)?.botSessions.get(recordingId);
    const consumer = session?.consumers.get(producerId);
    if (!consumer) return;
    consumer.close();
    session?.consumers.delete(producerId);
  }

  closeBotSession(roomId: string, recordingId: string): void {
    const room = this.rooms.get(roomId);
    const session = room?.botSessions.get(recordingId);
    if (!room || !session) return;
    for (const consumer of session.consumers.values()) consumer.close();
    session.consumers.clear();
    void session.transportPromise?.then((transport) => transport.close()).catch(() => {});
    for (const pending of session.consumerPromises.values()) {
      void pending.then((consumer) => consumer.close()).catch(() => {});
    }
    session.consumerPromises.clear();
    session.transport?.close();
    room.botSessions.delete(recordingId);
  }

  async closeProducer(
    roomId: string,
    participantId: string,
    producerId: string,
  ): Promise<{ producerId: string; source: "mic" | "camera" | "screen" }> {
    const room = await this.ensureRoom(roomId);
    const record = room.producers.get(producerId);
    if (!record || record.participantId !== participantId) {
      throw new SignalingError("PRODUCER_NOT_FOUND", "Producer not found");
    }

    const { source } = record;
    record.producer.close();
    this.logger?.info("mediasoup producer close requested", {
      operation: "mediasoup.producer.closeRequested",
      roomId,
      participantId,
      producerId,
      source,
      outcome: "ok",
    });
    return { producerId, source };
  }

  async setProducerPaused(
    roomId: string,
    participantId: string,
    producerId: string,
    paused: boolean,
  ): Promise<{ producerId: string; source: "mic" | "camera" | "screen" }> {
    const room = await this.ensureRoom(roomId);
    const record = room.producers.get(producerId);
    if (!record || record.participantId !== participantId) {
      throw new SignalingError("PRODUCER_NOT_FOUND", "Producer not found");
    }

    if (paused) {
      await record.producer.pause();
    } else {
      await record.producer.resume();
    }
    return { producerId, source: record.source };
  }

  async closeProducersForSource(
    roomId: string,
    participantId: string,
    source: "mic" | "camera" | "screen",
  ): Promise<string[]> {
    const room = await this.ensureRoom(roomId);
    const closedProducerIds: string[] = [];

    for (const [producerId, record] of Array.from(room.producers.entries())) {
      if (record.participantId !== participantId || record.source !== source) {
        continue;
      }

      closedProducerIds.push(producerId);
      record.producer.close();
    }

    if (closedProducerIds.length > 0) {
      this.logger?.info("mediasoup source producers closed", {
        operation: "mediasoup.producer.closeSource",
        roomId,
        participantId,
        source,
        closedProducerCount: closedProducerIds.length,
        outcome: "ok",
      });
    }

    return closedProducerIds;
  }

  listProducers(roomId: string): Array<{
    producerId: string;
    participantId: string;
    kind: MediaKind;
    source: "mic" | "camera" | "screen";
    paused: boolean;
  }> {
    const room = this.rooms.get(roomId);
    if (!room) {
      return [];
    }

    return Array.from(room.producers.values()).map((record) => ({
      producerId: record.producer.id,
      participantId: record.participantId,
      kind: record.producer.kind,
      source: record.source,
      paused: record.producer.paused,
    }));
  }

  getParticipantMedia(roomId: string, participantId: string): ParticipantMedia {
    const room = this.rooms.get(roomId);
    const media: ParticipantMedia = { mic: "off", camera: "off", screen: "off" };
    if (!room) {
      return media;
    }

    for (const record of room.producers.values()) {
      if (record.participantId !== participantId) continue;
      if (!record.producer.paused) {
        media[record.source] = "on";
      } else if (record.source === "mic") {
        media.mic = "muted";
      }
    }

    return media;
  }

  getActiveSpeakerParticipantId(roomId: string): string | null {
    return this.rooms.get(roomId)?.activeSpeakerParticipantId ?? null;
  }

  closePeer(roomId: string, participantId: string): string[] {
    const room = this.rooms.get(roomId);
    if (!room) {
      return [];
    }

    const closedProducerIds: string[] = [];
    for (const record of Array.from(room.consumers.values())) {
      if (record.participantId === participantId) {
        record.consumer.close();
      }
    }

    for (const record of Array.from(room.producers.values())) {
      if (record.participantId === participantId) {
        closedProducerIds.push(record.producer.id);
        record.producer.close();
      }
    }

    for (const record of Array.from(room.transports.values())) {
      if (record.participantId === participantId) {
        record.transport.close();
      }
    }

    this.logger?.info("mediasoup peer media closed", {
      operation: "mediasoup.peer.close",
      roomId,
      participantId,
      closedProducerCount: closedProducerIds.length,
      outcome: "ok",
    });

    return closedProducerIds;
  }

  closeRoom(roomId: string): void {
    const room = this.rooms.get(roomId);
    if (!room) {
      return;
    }

    room.router.close();
    this.rooms.delete(roomId);
    this.logger?.info("mediasoup room closed", {
      operation: "mediasoup.room.close",
      roomId,
      outcome: "ok",
    });
  }

  getRuntimeStats(): {
    rooms: number;
    workers: number;
    transports: number;
    producers: number;
    consumers: number;
  } {
    let transports = 0;
    let producers = 0;
    let consumers = 0;

    for (const room of this.rooms.values()) {
      transports += room.transports.size;
      producers += room.producers.size;
      consumers += room.consumers.size;
    }

    return {
      rooms: this.rooms.size,
      workers: this.getUsableWorkerCount(),
      transports,
      producers,
      consumers,
    };
  }

  isReady(): boolean {
    return this.getUsableWorkerCount() > 0;
  }

  private requireBotTransport(roomId: string, recordingId: string, transportId: string): WebRtcTransport {
    const transport = this.rooms.get(roomId)?.botSessions.get(recordingId)?.transport;
    if (!transport || transport.closed || transport.id !== transportId) {
      throw new SignalingError("TRANSPORT_NOT_FOUND", "Bot receive transport not found");
    }
    return transport;
  }

  private describeWebRtcTransport(transport: WebRtcTransport): CreateWebRtcTransportResponseData {
    return {
      id: transport.id,
      iceParameters: transport.iceParameters as CreateWebRtcTransportResponseData["iceParameters"],
      iceCandidates: transport.iceCandidates as CreateWebRtcTransportResponseData["iceCandidates"],
      dtlsParameters: transport.dtlsParameters as CreateWebRtcTransportResponseData["dtlsParameters"],
      iceServers: this.config.iceServers.length > 0 ? this.config.iceServers : undefined,
    };
  }

  private async getTransport(
    roomId: string,
    participantId: string,
    transportId: string,
  ): Promise<TransportRecord> {
    const room = await this.ensureRoom(roomId);
    const record = room.transports.get(transportId);
    if (
      !record ||
      record.participantId !== participantId ||
      (record.transport as Transport).closed
    ) {
      throw new SignalingError("TRANSPORT_NOT_FOUND", "Transport not found");
    }

    return record;
  }

  private assertPeerLimit<T extends { participantId: string }>(
    records: Map<string, T>,
    participantId: string,
    max: number,
    message: string,
  ): void {
    let count = 0;
    for (const record of records.values()) {
      if (record.participantId === participantId) {
        count += 1;
      }
    }

    if (count >= max) {
      throw new SignalingError("MEDIASOUP_ERROR", message);
    }
  }

  private hasProducerForSource(
    room: RuntimeRoom,
    participantId: string,
    source: "mic" | "camera" | "screen",
  ): boolean {
    for (const record of room.producers.values()) {
      if (record.participantId === participantId && record.source === source) {
        return true;
      }
    }

    return false;
  }

  private hasActiveScreenProducer(room: RuntimeRoom): boolean {
    for (const record of room.producers.values()) {
      if (record.source === "screen") {
        return true;
      }
    }

    return false;
  }

  private closeConsumersForProducer(
    room: RuntimeRoom,
    producerId: string,
  ): void {
    for (const record of Array.from(room.consumers.values())) {
      if (record.producerId === producerId) {
        record.consumer.close();
      }
    }
  }

  private setActiveSpeaker(roomId: string, room: RuntimeRoom, participantId: string | null): void {
    if (room.activeSpeakerParticipantId === participantId) {
      return;
    }

    room.activeSpeakerParticipantId = participantId;
    this.onActiveSpeakerChangedCallback?.(roomId, participantId);
  }

  private closeDeadRoom(roomId: string, reason: RoomClosedReason): void {
    const room = this.rooms.get(roomId);
    if (!room) {
      return;
    }

    room.transports.clear();
    room.producers.clear();
    room.consumers.clear();
    this.rooms.delete(roomId);
    this.logger?.error("mediasoup runtime room closed after failure", {
      operation: "mediasoup.room.dead",
      errorCode: reason === "media_server_error" ? "MEDIA_SERVER_ERROR" : "ROOM_CLOSED_BY_SERVER",
      roomId,
      reason,
      outcome: "ok",
    });
    this.onRoomDeadCallback?.(roomId, reason);
  }

  private async createWorkerAt(index: number): Promise<void> {
    try {
      const worker = await mediasoup.createWorker({
        rtcMinPort: this.config.rtcMinPort,
        rtcMaxPort: this.config.rtcMaxPort,
      });
      worker.on("died", (error) => {
        void this.handleWorkerDeath(index, worker, error);
      });
      this.workers[index] = worker;
    } catch (error) {
      this.workers[index] = undefined;
      this.logger?.error("mediasoup worker replacement failed", {
        operation: "mediasoup.worker.replace",
        errorCode: "MEDIASOUP_WORKER_REPLACEMENT_FAILED",
        workerIndex: index,
        error: String(error),
      });
      throw error;
    }
  }

  private async handleWorkerDeath(index: number, worker: Worker, error: Error): Promise<void> {
    if (this.closing || this.workers[index] !== worker) {
      return;
    }

    this.logger?.error("mediasoup worker died", {
      operation: "mediasoup.worker.died",
      errorCode: "MEDIASOUP_WORKER_DIED",
      workerIndex: index,
      error: String(error),
    });
    this.workers[index] = undefined;
    for (const [roomId, room] of [...this.rooms.entries()]) {
      if (room.workerIndex === index) {
        this.closeDeadRoom(roomId, "media_server_error");
      }
    }
    try {
      await this.createWorkerAt(index);
      this.logger?.info("mediasoup worker replaced", {
        operation: "mediasoup.worker.replace",
        workerIndex: index,
        usableWorkers: this.getUsableWorkerCount(),
      });
    } catch {
      // createWorkerAt emits the structured failure and readiness stays false if no worker remains.
    }
  }

  private findNextWorkerIndex(): number | undefined {
    if (this.workers.length === 0) {
      return undefined;
    }

    for (let offset = 0; offset < this.workers.length; offset += 1) {
      const index = (this.nextWorkerIndex + offset) % this.workers.length;
      if (this.workers[index]) {
        this.nextWorkerIndex = (index + 1) % this.workers.length;
        return index;
      }
    }

    return undefined;
  }

  private getUsableWorkerCount(): number {
    return this.workers.filter((worker) => worker !== undefined).length;
  }
}
