import { createHmac, randomUUID } from "node:crypto";

import type { MediaKind, MediaSource, Participant, RoomMetadata } from "#contracts";

import type { ServiceJwtConfig, SignalingConfig } from "./config.js";
import type { Logger } from "./logger.js";
import {
  RecordingWorkerLeaseMonitor,
  RecordingWorkerPool,
  type RecordingWorkerBinding,
  type RecordingWorkerPoolSnapshot,
} from "./recordingWorkerPool.js";

type RecordingProjection = {
  recordingId: string;
  integrationId: string;
  requestedAt: string;
  status: "requested" | "capturing" | "finalizing";
};

export type RecorderProducer = {
  producerId: string;
  participantId: string;
  kind: MediaKind;
  source: MediaSource;
};

export interface RoomRecordingBridge {
  roomBecameLive(room: RoomMetadata, participants: Participant[], producers: RecorderProducer[]): Promise<void>;
  producerAdded(room: RoomMetadata, participant: Participant, producer: RecorderProducer): Promise<void>;
  producerPaused(roomId: string, producerId: string, paused: boolean): Promise<void>;
  producerClosed(roomId: string, producerId: string): Promise<void>;
  stopRoom(roomId: string): Promise<void>;
  abortPrepared?(roomId: string): Promise<void>;
  logFailure(operation: string, roomId: string, error: unknown): void;
}

type RecordingControl = {
  lookup(roomId: string): Promise<RecordingProjection | undefined>;
  activate(roomId: string): Promise<RecordingProjection | undefined>;
  closeInputs(recordingId: string): Promise<void>;
  issueBotAdmission(recordingId: string): Promise<{ protocolVersion: "recording-bot.v1"; admission: string; expiresAt: string }>;
};

type RecordingBotWorker = {
  create(binding: RecordingWorkerBinding, input: { attemptId: string; roomId: string; recordingId: string; integrationId: string; requestedAt: string; signalingUrl: string; admission: string }): Promise<void>;
  stop(binding: RecordingWorkerBinding, recordingId: string): Promise<void>;
  finalize(binding: RecordingWorkerBinding, input: {
    attemptId: string;
    roomId: string;
    recordingId: string;
    integrationId: string;
    requestedAt: string;
  }): Promise<void>;
};

type Session = {
  room: RoomMetadata;
  projection: RecordingProjection;
  binding?: RecordingWorkerBinding;
  startPromise?: Promise<void>;
  stopping?: boolean;
  recoveryPromise?: Promise<void>;
  finalizing?: boolean;
  workerFinalized?: boolean;
  finalizationPromise?: Promise<void>;
};
const TERMINAL_ROOM_TTL_MS = 15 * 60 * 1_000;
const RECOVERY_MAX_ATTEMPTS = 3;
const RECOVERY_RETRY_DELAY_MS = 100;

/**
 * Owns only bot control-plane lifecycle. Participant and producer changes flow
 * through RecordingBotSessionManager's private WebRTC protocol, never through
 * a second mediasoup router or RTP recorder inputs.
 */
export class RecordingBridge implements RoomRecordingBridge {
  private readonly sessions = new Map<string, Session>();
  private readonly terminalRooms = new Map<string, number>();
  private readonly blockedPreLiveRooms = new Map<string, number>();
  private readonly pendingFinalizations = new Set<string>();
  private readonly roomOperations = new Map<string, Promise<void>>();

  private readonly leaseMonitor: RecordingWorkerLeaseMonitor;

  constructor(
    private readonly control: RecordingControl,
    private readonly bot: RecordingBotWorker,
    private readonly config: NonNullable<SignalingConfig["recording"]>,
    private readonly logger?: Logger,
    readonly workerPool = new RecordingWorkerPool(config.bot.workers, config.bot.workerLeaseTtlMs),
  ) {
    this.leaseMonitor = new RecordingWorkerLeaseMonitor(
      this.workerPool,
      this.config.bot.workerHeartbeatMs,
      fetch,
      async (recordingIds) => {
        if (recordingIds.length > 0) {
          this.logger?.warn("recording worker bindings reported lost", {
            operation: "recording.worker.lease",
            recordingIds,
            outcome: "failure",
          });
        }
        await this.recoverLostBindings(recordingIds);
        await this.retryPendingFinalizations();
      },
    );
  }

  async start(): Promise<void> {
    await this.leaseMonitor.refresh();
    this.leaseMonitor.start();
  }

  stop(): void {
    this.leaseMonitor.stop();
  }

  getWorkerPoolSnapshot(): RecordingWorkerPoolSnapshot {
    return this.workerPool.snapshot();
  }

  async recoverRecording(recordingId: string): Promise<void> {
    const session = [...this.sessions.values()].find((candidate) => candidate.projection.recordingId === recordingId);
    if (!session) return;
    if (session.finalizing || session.recoveryPromise) return session.recoveryPromise;
    const recoveryPromise = this.runSerialized(session.room.roomId, async () => {
      if (session.stopping || this.isTerminal(session.room.roomId)) return;
      await this.workerPool.invalidate(recordingId);
      session.binding = undefined;
      let lastError: unknown;
      for (let attempt = 1; attempt <= RECOVERY_MAX_ATTEMPTS; attempt += 1) {
        try {
          await this.startBot(session);
          this.logger?.warn("recording worker binding recovered", {
            operation: "recording.worker.recover",
            roomId: session.room.roomId,
            recordingId,
            attempt,
            outcome: "ok",
          });
          return;
        } catch (error) {
          lastError = error;
          await this.workerPool.release(recordingId);
          session.binding = undefined;
          if (attempt < RECOVERY_MAX_ATTEMPTS) {
            await new Promise((resolve) => setTimeout(resolve, RECOVERY_RETRY_DELAY_MS));
          }
        }
      }
      await this.workerPool.queueLostBinding(recordingId);
      this.logger?.error("recording worker recovery failed", {
        operation: "recording.worker.recover",
        errorCode: "RECORDING_WORKER_RECOVERY_FAILED",
        roomId: session.room.roomId,
        recordingId,
        error: String(lastError),
        outcome: "failure",
      });
    });
    session.recoveryPromise = recoveryPromise;
    try {
      await recoveryPromise;
    } finally {
      if (session.recoveryPromise === recoveryPromise) session.recoveryPromise = undefined;
    }
  }

  async roomBecameLive(room: RoomMetadata, _participants: Participant[], _producers: RecorderProducer[]): Promise<void> {
    if (this.isBlockedPreLive(room.roomId)) throw new Error("Recording preparation was aborted before the first participant became live");
    return this.runSerialized(room.roomId, () => this.ensureStarted(room));
  }

  async producerAdded(room: RoomMetadata, _participant: Participant, _producer: RecorderProducer): Promise<void> {
    return this.runSerialized(room.roomId, () => this.ensureStarted(room));
  }

  async producerPaused(_roomId: string, _producerId: string, _paused: boolean): Promise<void> {}

  async producerClosed(_roomId: string, _producerId: string): Promise<void> {}

  async stopRoom(roomId: string): Promise<void> {
    this.markTerminal(roomId);
    return this.runSerialized(roomId, async () => {
      const session = this.sessions.get(roomId);
      if (!session) return;
      if (session.finalizationPromise) return session.finalizationPromise;
      if (session.stopping) return;
      session.stopping = true;
      if (session.startPromise) await session.startPromise;
      session.finalizing = true;
      const finalizationPromise = this.finalizeSession(session, true);
      session.finalizationPromise = finalizationPromise;
      try {
        await finalizationPromise;
      } finally {
        if (session.finalizationPromise === finalizationPromise) session.finalizationPromise = undefined;
        session.stopping = false;
      }
    });
  }

  async abortPrepared(roomId: string): Promise<void> {
    if (!this.sessions.has(roomId)) return;
    this.markBlockedPreLive(roomId);
    await this.stopRoom(roomId);
  }

  async retryFinalization(recordingId: string): Promise<void> {
    const session = [...this.sessions.values()].find((candidate) => candidate.projection.recordingId === recordingId);
    if (!session?.finalizing || session.finalizationPromise) return session?.finalizationPromise;
    const finalizationPromise = this.runSerialized(session.room.roomId, () => this.finalizeSession(session, false));
    session.finalizationPromise = finalizationPromise;
    try {
      await finalizationPromise;
    } finally {
      if (session.finalizationPromise === finalizationPromise) session.finalizationPromise = undefined;
    }
  }

  logFailure(operation: string, roomId: string, error: unknown): void {
    this.logger?.warn("recording bridge operation failed", {
      operation,
      errorCode: "RECORDING_BRIDGE_FAILED",
      roomId,
      error: String(error),
    });
  }

  private async ensureStarted(room: RoomMetadata): Promise<void> {
    if (this.isTerminal(room.roomId)) return;
    const existing = this.sessions.get(room.roomId);
    if (existing?.startPromise) return existing.startPromise;
    if (existing) return;

    const requested = await this.control.lookup(room.roomId);
    if (!requested) return;
    if (requested.status === "finalizing") throw new Error("Recording is already finalizing");
    const session: Session = { room, projection: requested };
    this.sessions.set(room.roomId, session);
    session.startPromise = this.prepareAndStartBot(session);
    try {
      await session.startPromise;
    } catch (error) {
      if (session.binding) await this.bot.stop(session.binding, requested.recordingId).catch(() => undefined);
      await this.workerPool.release(requested.recordingId);
      this.sessions.delete(room.roomId);
      throw error;
    } finally {
      session.startPromise = undefined;
    }
  }

  private async prepareAndStartBot(session: Session): Promise<void> {
    session.binding = await this.workerPool.reserve(session.projection.recordingId);
    try {
      const activated = await this.control.activate(session.room.roomId);
      if (!activated || activated.recordingId !== session.projection.recordingId || activated.status === "finalizing") {
        throw new Error("Recording activation did not establish a capturable attempt");
      }
      session.projection = activated;
      await this.startBot(session);
    } catch (error) {
      await this.workerPool.release(session.projection.recordingId);
      session.binding = undefined;
      throw error;
    }
  }

  private async startBot(session: Session): Promise<void> {
    const activated = session.projection;
    const binding = session.binding ?? await this.workerPool.reserve(activated.recordingId);
    session.binding = binding;
    const admission = await this.control.issueBotAdmission(activated.recordingId);
    await this.bot.create(binding, {
      attemptId: randomUUID(),
      roomId: session.room.roomId,
      recordingId: activated.recordingId,
      integrationId: activated.integrationId,
      requestedAt: activated.requestedAt,
      signalingUrl: this.config.bot.signalingUrl,
      admission: admission.admission,
    });
  }

  private async recoverLostBindings(recordingIds: string[]): Promise<void> {
    await Promise.allSettled(recordingIds.map(async (recordingId) => {
      if (this.pendingFinalizations.has(recordingId)) await this.retryFinalization(recordingId);
      else await this.recoverRecording(recordingId);
    }));
  }

  private async retryPendingFinalizations(): Promise<void> {
    await Promise.allSettled([...this.pendingFinalizations].map((recordingId) => this.retryFinalization(recordingId)));
  }

  private async finalizeSession(session: Session, tryAssignedWorker: boolean): Promise<void> {
    const recordingId = session.projection.recordingId;
    try {
      if (!session.workerFinalized && tryAssignedWorker && session.binding) {
        try {
          await this.bot.stop(session.binding, recordingId);
          session.workerFinalized = true;
        } catch {
          await this.workerPool.markWorkerUnavailable(session.binding.workerId, session.binding.generationId);
          session.binding = undefined;
        }
      }
      if (!session.workerFinalized) {
        await this.workerPool.invalidate(recordingId);
        session.binding = undefined;
        let lastError: unknown;
        for (let attempt = 1; attempt <= RECOVERY_MAX_ATTEMPTS; attempt += 1) {
          try {
            const binding = await this.workerPool.reserve(recordingId);
            session.binding = binding;
            await this.bot.finalize(binding, {
              attemptId: randomUUID(),
              roomId: session.room.roomId,
              recordingId,
              integrationId: session.projection.integrationId,
              requestedAt: session.projection.requestedAt,
            });
            session.workerFinalized = true;
            break;
          } catch (error) {
            lastError = error;
            await this.workerPool.release(recordingId);
            session.binding = undefined;
            if (attempt < RECOVERY_MAX_ATTEMPTS) {
              await new Promise((resolve) => setTimeout(resolve, RECOVERY_RETRY_DELAY_MS));
            }
          }
        }
        if (!session.workerFinalized) throw lastError;
      }
      await this.control.closeInputs(recordingId);
      this.pendingFinalizations.delete(recordingId);
      await this.workerPool.release(recordingId);
      this.sessions.delete(session.room.roomId);
    } catch (error) {
      this.pendingFinalizations.add(recordingId);
      this.logger?.error("recording finalization recovery deferred", {
        operation: "recording.worker.finalize",
        errorCode: "RECORDING_FINALIZATION_RECOVERY_FAILED",
        roomId: session.room.roomId,
        recordingId,
        error: String(error),
        outcome: "failure",
      });
    }
  }

  private runSerialized(roomId: string, action: () => Promise<void>): Promise<void> {
    const previous = this.roomOperations.get(roomId) ?? Promise.resolve();
    const next = previous.catch(() => undefined).then(action);
    this.roomOperations.set(roomId, next);
    void next.finally(() => {
      if (this.roomOperations.get(roomId) === next) this.roomOperations.delete(roomId);
    }).catch(() => undefined);
    return next;
  }

  private markTerminal(roomId: string): void {
    const expiresAt = Date.now() + TERMINAL_ROOM_TTL_MS;
    this.terminalRooms.set(roomId, expiresAt);
    const timer = setTimeout(() => {
      if (this.terminalRooms.get(roomId) === expiresAt) this.terminalRooms.delete(roomId);
    }, TERMINAL_ROOM_TTL_MS);
    timer.unref();
  }

  private markBlockedPreLive(roomId: string): void {
    const expiresAt = Date.now() + TERMINAL_ROOM_TTL_MS;
    this.blockedPreLiveRooms.set(roomId, expiresAt);
    const timer = setTimeout(() => {
      if (this.blockedPreLiveRooms.get(roomId) === expiresAt) this.blockedPreLiveRooms.delete(roomId);
    }, TERMINAL_ROOM_TTL_MS);
    timer.unref();
  }

  private isBlockedPreLive(roomId: string): boolean {
    const expiresAt = this.blockedPreLiveRooms.get(roomId);
    if (expiresAt === undefined) return false;
    if (expiresAt > Date.now()) return true;
    this.blockedPreLiveRooms.delete(roomId);
    return false;
  }

  private isTerminal(roomId: string): boolean {
    const expiresAt = this.terminalRooms.get(roomId);
    if (expiresAt === undefined) return false;
    if (expiresAt > Date.now()) return true;
    this.terminalRooms.delete(roomId);
    return false;
  }
}

export function createRecordingBridge(
  _media: unknown,
  config: SignalingConfig["recording"],
  logger?: Logger,
): RecordingBridge | undefined {
  if (!config) return undefined;
  return new RecordingBridge(
    new HttpRecordingControl(config.controlUrl, config.controlJwt),
    new HttpRecordingBot(config.bot.controlJwt),
    config,
    logger,
  );
}

class HttpRecordingControl implements RecordingControl {
  constructor(private readonly baseUrl: string, private readonly jwt: ServiceJwtConfig) {}

  async lookup(roomId: string): Promise<RecordingProjection | undefined> {
    const response = await request(`${this.baseUrl}/rooms/${encodeURIComponent(roomId)}/recording`, this.jwt, "GET");
    if (response.status === 204 || response.status === 404) return undefined;
    return readProjection(response);
  }

  async activate(roomId: string): Promise<RecordingProjection | undefined> {
    const response = await request(`${this.baseUrl}/rooms/${encodeURIComponent(roomId)}/recording/activate`, this.jwt, "POST");
    if (response.status === 204 || response.status === 404) return undefined;
    return readProjection(response);
  }

  async closeInputs(recordingId: string): Promise<void> {
    const response = await request(`${this.baseUrl}/recordings/${encodeURIComponent(recordingId)}/close-inputs`, this.jwt, "POST");
    if (response.status !== 201 && response.status !== 204) throw await responseError(response);
  }

  async issueBotAdmission(recordingId: string): Promise<{ protocolVersion: "recording-bot.v1"; admission: string; expiresAt: string }> {
    const response = await request(`${this.baseUrl}/recordings/${encodeURIComponent(recordingId)}/bot-admissions`, this.jwt, "POST");
    if (!response.ok) throw await responseError(response);
    const value: unknown = await response.json();
    if (!isBotAdmission(value)) throw new Error("Invalid recording bot admission response");
    return value;
  }
}

export class HttpRecordingBot implements RecordingBotWorker {
  constructor(private readonly jwt: ServiceJwtConfig) {}

  async create(binding: RecordingWorkerBinding, input: { attemptId: string; roomId: string; recordingId: string; integrationId: string; requestedAt: string; signalingUrl: string; admission: string }): Promise<void> {
    const response = await fetch(`${binding.controlUrl}/internal/v1/attempts/${encodeURIComponent(input.recordingId)}`, {
      method: "PUT",
      headers: this.headers(binding, true),
      body: JSON.stringify({
        ...input,
        protocolVersion: "recording-bot.v1",
        workerId: binding.workerId,
        workerGenerationId: binding.generationId,
      }),
      signal: AbortSignal.timeout(30_000),
    });
    if (response.status !== 202 && response.status !== 200) throw await responseError(response);
  }

  async stop(binding: RecordingWorkerBinding, recordingId: string): Promise<void> {
    const response = await fetch(`${binding.controlUrl}/internal/v1/attempts/${encodeURIComponent(recordingId)}`, {
      method: "DELETE",
      headers: this.headers(binding),
      signal: AbortSignal.timeout(130_000),
    });
    if (response.status !== 200 && response.status !== 404) throw await responseError(response);
  }

  async finalize(binding: RecordingWorkerBinding, input: {
    attemptId: string;
    roomId: string;
    recordingId: string;
    integrationId: string;
    requestedAt: string;
  }): Promise<void> {
    const response = await fetch(`${binding.controlUrl}/internal/v1/finalizations/${encodeURIComponent(input.recordingId)}`, {
      method: "PUT",
      headers: this.headers(binding, true),
      body: JSON.stringify({
        ...input,
        workerId: binding.workerId,
        workerGenerationId: binding.generationId,
      }),
      signal: AbortSignal.timeout(130_000),
    });
    if (response.status !== 200 && response.status !== 202) throw await responseError(response);
  }

  private headers(binding: RecordingWorkerBinding, json = false): Record<string, string> {
    return {
      Authorization: `Bearer ${createServiceJwt(this.jwt, "recording-bot-control+jwt", "conference-signaling")}`,
      ...(json ? { "Content-Type": "application/json" } : {}),
      "X-Recording-Worker-Id": binding.workerId,
      "X-Recording-Worker-Generation-Id": binding.generationId,
    };
  }
}

async function request(url: string, jwt: ServiceJwtConfig, method: "GET" | "POST"): Promise<Response> {
  return fetch(url, {
    method,
    headers: { Authorization: `Bearer ${createServiceJwt(jwt)}` },
    signal: AbortSignal.timeout(10_000),
  });
}

function createServiceJwt(config: ServiceJwtConfig, typ = "JWT", subject = "signaling"): string {
  const now = Math.floor(Date.now() / 1_000);
  const header = Buffer.from(JSON.stringify({ alg: "HS256", typ })).toString("base64url");
  const payload = Buffer.from(JSON.stringify({ iss: config.issuer, aud: config.audience, sub: subject, iat: now, exp: now + config.maxTtlSeconds })).toString("base64url");
  const signature = createHmac("sha256", config.secret).update(`${header}.${payload}`).digest("base64url");
  return `${header}.${payload}.${signature}`;
}

async function readProjection(response: Response): Promise<RecordingProjection> {
  if (!response.ok) throw await responseError(response);
  const value: unknown = await response.json();
  if (!isProjection(value)) throw new Error("Invalid recording control response");
  return value;
}

function isProjection(value: unknown): value is RecordingProjection {
  if (typeof value !== "object" || value === null) return false;
  const candidate = value as Partial<RecordingProjection>;
  return typeof candidate.recordingId === "string"
    && typeof candidate.integrationId === "string"
    && typeof candidate.requestedAt === "string"
    && (candidate.status === "requested" || candidate.status === "capturing" || candidate.status === "finalizing");
}

function isBotAdmission(value: unknown): value is { protocolVersion: "recording-bot.v1"; admission: string; expiresAt: string } {
  return typeof value === "object" && value !== null
    && (value as { protocolVersion?: unknown }).protocolVersion === "recording-bot.v1"
    && typeof (value as { admission?: unknown }).admission === "string"
    && typeof (value as { expiresAt?: unknown }).expiresAt === "string";
}

async function responseError(response: Response): Promise<Error> {
  return new Error(`Recording service request failed: ${response.status} ${(await response.text()).slice(0, 512)}`);
}
