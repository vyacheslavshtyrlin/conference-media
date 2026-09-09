import { z } from "zod";
import type { IncomingMessage } from "node:http";
import { randomUUID } from "node:crypto";
import type { RtpCapabilities } from "mediasoup/types";
import { WebSocket, WebSocketServer } from "ws";

import { EMPTY_ROOM_TTL_SECONDS } from "#contracts";
import type { MediaKind, MediaSource, Participant } from "#contracts";
import type { Logger } from "./logger.js";
import type { MediasoupService } from "./mediasoupService.js";
import type { RecordingBotAdmissionPayload } from "./token.js";

export const RECORDING_BOT_PROTOCOL_VERSION = "recording-bot.v1" as const;

const requestIdSchema = z.string().min(1).max(128);
const botRequestSchema = z.discriminatedUnion("type", [
  z.object({ type: z.literal("bot:getRouterRtpCapabilities"), requestId: requestIdSchema }).strict(),
  z.object({ type: z.literal("bot:createRecvTransport"), requestId: requestIdSchema }).strict(),
  z.object({ type: z.literal("bot:syncSnapshot"), requestId: requestIdSchema }).strict(),
  z.object({ type: z.literal("bot:connectRecvTransport"), requestId: requestIdSchema, transportId: z.string().min(1), dtlsParameters: z.custom<unknown>((value) => value !== undefined) }).strict(),
  z.object({ type: z.literal("bot:consume"), requestId: requestIdSchema, transportId: z.string().min(1), producerId: z.string().min(1), rtpCapabilities: z.custom<unknown>((value) => value !== undefined) }).strict(),
  z.object({ type: z.literal("bot:pauseConsumer"), requestId: requestIdSchema, producerId: z.string().min(1) }).strict(),
  z.object({ type: z.literal("bot:resumeConsumer"), requestId: requestIdSchema, producerId: z.string().min(1) }).strict(),
  z.object({ type: z.literal("bot:closeConsumer"), requestId: requestIdSchema, producerId: z.string().min(1) }).strict(),
  z.object({ type: z.literal("bot:teardown"), requestId: requestIdSchema }).strict(),
]);

type BotRequest = z.infer<typeof botRequestSchema>;

export type BotParticipantDescriptor = Pick<Participant, "participantId" | "displayName">;
export type BotProducerDescriptor = {
  producerId: string;
  participantId: string;
  displayName: string;
  kind: MediaKind;
  source: MediaSource;
  paused: boolean;
};
export type BotAuthoritySnapshot = {
  participants: BotParticipantDescriptor[];
  producers: BotProducerDescriptor[];
};

type BotEvent = Record<string, unknown> & { type: string };
type BotSender = (event: BotEvent) => void;

type Attempt = {
  sessionId: string;
  roomId: string;
  recordingId: string;
  revision: number;
  participants: Map<string, BotParticipantDescriptor>;
  producers: Map<string, BotProducerDescriptor>;
  send: BotSender;
  snapshotSent: boolean;
  pendingAuthority?: BotAuthoritySnapshot;
  captureStarted: boolean;
  stopping: boolean;
  stopped: boolean;
};

export type RecordingBotFailureCode = "BOT_CAPTURE_INCOMPLETE" | "BOT_SIGNALING_GAP";

export class RecordingBotSessionManager {
  private readonly attempts = new Map<string, Attempt>();
  private readonly terminalRooms = new Map<string, { expiresAt: number; timer: NodeJS.Timeout }>();

  constructor(
    private readonly media: MediasoupService,
    private readonly freezeAuthority: (
      roomId: string,
      accept: (snapshot: BotAuthoritySnapshot) => void,
    ) => Promise<void>,
    private readonly onRecoveryRequired: (input: { roomId: string; recordingId: string; code: RecordingBotFailureCode }) => void = () => {},
    private readonly terminalRoomRetentionMs = EMPTY_ROOM_TTL_SECONDS * 1_000,
    private readonly now: () => number = Date.now,
  ) {}

  open(payload: RecordingBotAdmissionPayload, send: BotSender): string {
    if (this.isRoomTerminal(payload.roomId)) {
      throw new Error("BOT_CAPTURE_INCOMPLETE");
    }
    const existing = this.attempts.get(payload.recordingId);
    if (existing?.captureStarted && !existing.stopped) {
      this.failAttempt(existing);
      throw new Error("BOT_CAPTURE_INCOMPLETE");
    }
    if (existing) this.media.closeBotSession(existing.roomId, existing.sessionId);
    const sessionId = randomUUID();
    this.attempts.set(payload.recordingId, {
      sessionId,
      roomId: payload.roomId,
      recordingId: payload.recordingId,
      revision: existing?.revision ?? 0,
      participants: new Map(),
      producers: new Map(),
      send,
      snapshotSent: false,
      captureStarted: false,
      stopping: false,
      stopped: false,
    });
    send({ type: "bot:admitted", protocolVersion: RECORDING_BOT_PROTOCOL_VERSION, roomId: payload.roomId, recordingId: payload.recordingId });
    return sessionId;
  }

  getRouterRtpCapabilities(recordingId: string, sessionId?: string): Promise<RtpCapabilities> {
    const attempt = this.require(recordingId, sessionId);
    return this.media.getRouterRtpCapabilities(attempt.roomId);
  }

  async createRecvTransport(recordingId: string, sessionId?: string) {
    const attempt = this.require(recordingId, sessionId);
    const transport = await this.media.createBotRecvTransport(attempt.roomId, attempt.sessionId);
    this.assertCurrent(attempt);
    return transport;
  }

  async connectRecvTransport(recordingId: string, transportId: string, dtlsParameters: unknown, sessionId?: string): Promise<void> {
    const attempt = this.require(recordingId, sessionId);
    await this.media.connectBotRecvTransport(attempt.roomId, attempt.sessionId, transportId, dtlsParameters);
    this.assertCurrent(attempt);
  }

  async syncSnapshot(recordingId: string, sessionId?: string): Promise<void> {
    const attempt = this.require(recordingId, sessionId);
    if (attempt.snapshotSent) return;
    await this.freezeAuthority(attempt.roomId, (frozen) => {
      if (this.attempts.get(recordingId) !== attempt || attempt.stopping || attempt.snapshotSent) return;
      attempt.participants = mapParticipants(frozen.participants);
      attempt.producers = mapProducers(frozen.producers);
      attempt.snapshotSent = true;
      attempt.send({
        type: "bot:producerSnapshot",
        protocolVersion: RECORDING_BOT_PROTOCOL_VERSION,
        recordingId,
        revision: attempt.revision,
        participants: [...attempt.participants.values()],
        producers: [...attempt.producers.values()],
      });
      const pending = attempt.pendingAuthority;
      attempt.pendingAuthority = undefined;
      if (pending) this.reconcileAuthority(attempt, pending);
    });
  }

  async consume(recordingId: string, transportId: string, producerId: string, rtpCapabilities: unknown, sessionId?: string) {
    const attempt = this.require(recordingId, sessionId);
    if (!attempt.snapshotSent) throw new Error("Snapshot is required before consume");
    const consumer = await this.media.consumeForBot(attempt.roomId, attempt.sessionId, transportId, producerId, rtpCapabilities);
    this.assertCurrent(attempt);
    return consumer;
  }

  async setConsumerPaused(recordingId: string, producerId: string, paused: boolean, sessionId?: string): Promise<void> {
    const attempt = this.require(recordingId, sessionId);
    await this.media.setBotConsumerPaused(attempt.roomId, attempt.sessionId, producerId, paused);
    this.assertCurrent(attempt);
    if (!paused) attempt.captureStarted = true;
  }

  closeConsumer(recordingId: string, producerId: string, sessionId?: string): void {
    const attempt = this.require(recordingId, sessionId);
    this.media.closeBotConsumer(attempt.roomId, attempt.sessionId, producerId);
  }

  syncAuthority(roomId: string, snapshot: BotAuthoritySnapshot): void {
    for (const attempt of this.attempts.values()) {
      if (attempt.roomId !== roomId || attempt.stopping || attempt.stopped) continue;
      if (!attempt.snapshotSent) {
        attempt.pendingAuthority = cloneAuthority(snapshot);
        continue;
      }
      this.reconcileAuthority(attempt, snapshot);
    }
  }

  disconnect(recordingId: string, sessionId?: string): void {
    const attempt = this.attempts.get(recordingId);
    if (!attempt || attempt.stopped || (sessionId !== undefined && attempt.sessionId !== sessionId)) return;
    if (attempt.stopping) {
      this.closeAttempt(attempt);
      return;
    }
    if (attempt.captureStarted) {
      this.failAttempt(attempt);
      return;
    }
    this.closeAttempt(attempt);
  }

  stopRoom(roomId: string): void {
    this.fenceRoom(roomId);
    for (const attempt of [...this.attempts.values()]) {
      if (attempt.roomId !== roomId || attempt.stopping) continue;
      attempt.stopping = true;
    }
  }

  private reconcileAuthority(attempt: Attempt, snapshot: BotAuthoritySnapshot): void {
    const nextParticipants = mapParticipants(snapshot.participants);
    const nextProducers = mapProducers(snapshot.producers);

    for (const [producerId, producer] of attempt.producers) {
      if (!nextProducers.has(producerId)) this.emit(attempt, "bot:producerClosed", { producerId, participantId: producer.participantId });
    }
    for (const [participantId] of attempt.participants) {
      if (!nextParticipants.has(participantId)) this.emit(attempt, "bot:participantLeft", { participantId });
    }
    for (const [participantId, participant] of nextParticipants) {
      const previous = attempt.participants.get(participantId);
      if (!previous) this.emit(attempt, "bot:participantAdded", { participant });
      else if (previous.displayName !== participant.displayName) this.emit(attempt, "bot:participantUpdated", { participant });
    }
    for (const [producerId, producer] of nextProducers) {
      const previous = attempt.producers.get(producerId);
      if (!previous) this.emit(attempt, "bot:producerAdded", { producer });
      else if (descriptorChanged(previous, producer)) this.emit(attempt, "bot:producerUpdated", { producer });
      else if (previous.paused !== producer.paused) this.emit(attempt, producer.paused ? "bot:producerPaused" : "bot:producerResumed", { producerId, participantId: producer.participantId });
    }
    attempt.participants = nextParticipants;
    attempt.producers = nextProducers;
  }

  private emit(attempt: Attempt, type: string, data: Record<string, unknown>): void {
    attempt.revision += 1;
    if (attempt.snapshotSent) attempt.send({ type, recordingId: attempt.recordingId, revision: attempt.revision, ...data });
  }

  private require(recordingId: string, sessionId?: string): Attempt {
    const attempt = this.attempts.get(recordingId);
    if (!attempt || attempt.stopped || (sessionId !== undefined && attempt.sessionId !== sessionId)) {
      throw new Error("Recording bot session not found");
    }
    return attempt;
  }

  private assertCurrent(attempt: Attempt): void {
    if (this.attempts.get(attempt.recordingId) !== attempt || attempt.stopped) {
      this.media.closeBotSession(attempt.roomId, attempt.sessionId);
      throw new Error("Recording bot session not found");
    }
  }

  private failAttempt(attempt: Attempt): void {
    if (attempt.stopped || this.attempts.get(attempt.recordingId) !== attempt) return;
    attempt.stopped = true;
    this.attempts.delete(attempt.recordingId);
    this.media.closeBotSession(attempt.roomId, attempt.sessionId);
    this.onRecoveryRequired({ roomId: attempt.roomId, recordingId: attempt.recordingId, code: "BOT_CAPTURE_INCOMPLETE" });
  }

  private closeAttempt(attempt: Attempt): void {
    if (this.attempts.get(attempt.recordingId) !== attempt) return;
    attempt.stopped = true;
    this.attempts.delete(attempt.recordingId);
    this.media.closeBotSession(attempt.roomId, attempt.sessionId);
  }

  private fenceRoom(roomId: string): void {
    const previous = this.terminalRooms.get(roomId);
    if (previous) clearTimeout(previous.timer);
    const expiresAt = this.now() + this.terminalRoomRetentionMs;
    const timer = setTimeout(() => {
      const current = this.terminalRooms.get(roomId);
      if (current?.expiresAt === expiresAt) this.clearTerminalRoom(roomId, current);
    }, this.terminalRoomRetentionMs);
    timer.unref();
    this.terminalRooms.set(roomId, { expiresAt, timer });
  }

  private isRoomTerminal(roomId: string): boolean {
    const fence = this.terminalRooms.get(roomId);
    if (!fence) return false;
    if (fence.expiresAt > this.now()) return true;
    this.clearTerminalRoom(roomId, fence);
    return false;
  }

  private clearTerminalRoom(roomId: string, fence: { expiresAt: number; timer: NodeJS.Timeout }): void {
    clearTimeout(fence.timer);
    this.terminalRooms.delete(roomId);
  }
}

export class RecordingBotSocketGateway {
  private readonly sessions = new Map<WebSocket, { recordingId: string; sessionId: string; commandTail: Promise<void> }>();

  constructor(
    private readonly server: WebSocketServer,
    private readonly manager: RecordingBotSessionManager,
    private readonly verifyAdmission: (token: string | undefined) => Promise<RecordingBotAdmissionPayload>,
    private readonly logger?: Logger,
  ) {}

  start(): void {
    this.server.on("connection", (socket, request) => void this.connect(socket, request));
  }

  async stop(): Promise<void> {
    for (const [socket, { recordingId, sessionId }] of this.sessions) {
      this.manager.disconnect(recordingId, sessionId);
      socket.close(1001, "Server shutting down");
    }
    this.sessions.clear();
  }

  private async connect(socket: WebSocket, request: IncomingMessage): Promise<void> {
    try {
      const url = new URL(request.url ?? "", "ws://localhost");
      const payload = await this.verifyAdmission(url.searchParams.get("admission") ?? undefined);
      const sessionId = this.manager.open(payload, (event) => this.send(socket, event));
      for (const [existingSocket, existing] of this.sessions) {
        if (existing.recordingId === payload.recordingId) {
          this.sessions.delete(existingSocket);
          existingSocket.close(1000, "Bot session replaced");
        }
      }
      this.sessions.set(socket, { recordingId: payload.recordingId, sessionId, commandTail: Promise.resolve() });
      this.logger?.info("recording bot admitted", {
        operation: "recordingBot.connect",
        roomId: payload.roomId,
        recordingId: payload.recordingId,
        outcome: "ok",
      });
      socket.on("message", (raw) => {
        const current = this.sessions.get(socket);
        if (current?.sessionId !== sessionId) return;
        current.commandTail = current.commandTail.then(() => this.handleMessage(socket, payload.recordingId, sessionId, raw));
      });
      socket.once("close", (code, reason) => {
        this.logger?.warn("recording bot websocket closed", {
          operation: "recordingBot.disconnect",
          roomId: payload.roomId,
          recordingId: payload.recordingId,
          closeCode: code,
          closeReason: reason.toString() || "none",
        });
        const current = this.sessions.get(socket);
        if (current?.sessionId === sessionId) {
          this.sessions.delete(socket);
          this.manager.disconnect(payload.recordingId, sessionId);
        }
      });
      socket.on("error", () => {});
    } catch (error) {
      this.logger?.warn("recording bot admission rejected", {
        operation: "recordingBot.connect",
        errorCode: "BOT_ADMISSION_REJECTED",
        error: String(error),
        outcome: "client_error",
      });
      this.send(socket, { type: "response", requestId: "connection", ok: false, error: { code: "BOT_ADMISSION_REJECTED", message: "Bot admission rejected" } });
      socket.close(1008, "Bot admission rejected");
    }
  }

  private async handleMessage(socket: WebSocket, recordingId: string, sessionId: string, raw: WebSocket.RawData): Promise<void> {
    let requestId = "unknown";
    try {
      const parsed = JSON.parse(raw.toString()) as unknown;
      if (typeof parsed === "object" && parsed && "requestId" in parsed && typeof parsed.requestId === "string") requestId = parsed.requestId;
      const request = botRequestSchema.parse(parsed);
      const current = this.sessions.get(socket);
      if (current?.sessionId !== sessionId) throw new Error("Stale recording bot session");
      const data = await this.route(recordingId, sessionId, request);
      if (this.sessions.get(socket)?.sessionId !== sessionId) return;
      this.send(socket, { type: "response", requestId: request.requestId, ok: true, data });
      if (request.type === "bot:teardown") {
        this.sessions.delete(socket);
        socket.close(1000, "Bot teardown complete");
      }
    } catch (error) {
      this.logger?.warn("recording bot request rejected", { operation: "recordingBot.request", recordingId, errorCode: "INVALID_BOT_REQUEST", error: String(error) });
      this.send(socket, { type: "response", requestId, ok: false, error: { code: "INVALID_PAYLOAD", message: "Invalid bot request" } });
    }
  }

  private async route(recordingId: string, sessionId: string, request: BotRequest): Promise<unknown> {
    switch (request.type) {
      case "bot:getRouterRtpCapabilities": return this.manager.getRouterRtpCapabilities(recordingId, sessionId);
      case "bot:createRecvTransport": return this.manager.createRecvTransport(recordingId, sessionId);
      case "bot:syncSnapshot": await this.manager.syncSnapshot(recordingId, sessionId); return {};
      case "bot:connectRecvTransport": await this.manager.connectRecvTransport(recordingId, request.transportId, request.dtlsParameters, sessionId); return {};
      case "bot:consume": return this.manager.consume(recordingId, request.transportId, request.producerId, request.rtpCapabilities, sessionId);
      case "bot:pauseConsumer": await this.manager.setConsumerPaused(recordingId, request.producerId, true, sessionId); return {};
      case "bot:resumeConsumer": await this.manager.setConsumerPaused(recordingId, request.producerId, false, sessionId); return {};
      case "bot:closeConsumer": this.manager.closeConsumer(recordingId, request.producerId, sessionId); return {};
      case "bot:teardown": this.manager.disconnect(recordingId, sessionId); return {};
    }
  }

  private send(socket: WebSocket, event: BotEvent): void {
    if (socket.readyState !== WebSocket.OPEN) return;
    try {
      socket.send(JSON.stringify(event));
      if (event.type === "bot:producerSnapshot" || event.type === "bot:stop" || event.type.endsWith("Added")) {
        this.logger?.info("recording bot lifecycle event sent", {
          operation: "recordingBot.send",
          eventType: event.type,
          recordingId: event.recordingId,
          revision: event.revision,
          participantCount: Array.isArray(event.participants) ? event.participants.length : undefined,
          producerCount: Array.isArray(event.producers) ? event.producers.length : undefined,
          outcome: "ok",
        });
      }
    } catch (error) {
      this.logger?.warn("recording bot event delivery failed", {
        operation: "recordingBot.send",
        errorCode: "BOT_EVENT_DELIVERY_FAILED",
        error: String(error),
      });
      socket.terminate();
    }
  }
}

function mapParticipants(participants: BotParticipantDescriptor[]): Map<string, BotParticipantDescriptor> {
  return new Map(participants.map((participant) => [participant.participantId, { ...participant }]));
}

function mapProducers(producers: BotProducerDescriptor[]): Map<string, BotProducerDescriptor> {
  return new Map(producers.map((producer) => [producer.producerId, { ...producer }]));
}

function cloneAuthority(snapshot: BotAuthoritySnapshot): BotAuthoritySnapshot {
  return {
    participants: snapshot.participants.map((participant) => ({ ...participant })),
    producers: snapshot.producers.map((producer) => ({ ...producer })),
  };
}

function descriptorChanged(left: BotProducerDescriptor, right: BotProducerDescriptor): boolean {
  return left.participantId !== right.participantId || left.displayName !== right.displayName || left.kind !== right.kind || left.source !== right.source;
}
