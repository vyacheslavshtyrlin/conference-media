import { CHAT_HISTORY_LIMIT, CHAT_MESSAGE_TEXT_MAX_LENGTH } from "#contracts";
import type {
  ChatMessage,
  MediaKind,
  MediaSource,
  Participant,
  ParticipantConnectionQuality,
  RoomClosedReason,
  RoomMetadata,
} from "#contracts";
import { Mutex } from "async-mutex";
import { randomUUID } from "node:crypto";

import { SignalingError } from "./errors.js";
import type { Logger } from "./logger.js";
import type { MediasoupService } from "./mediasoupService.js";
import { getMediaMetrics, type MediaMetrics } from "./metrics.js";
import { PeerManager } from "./peerManager.js";
import type { RedisRoomRepository } from "./redisRoomRepository.js";
import type { JoinTokenPayload } from "./token.js";
import type { RecorderProducer, RoomRecordingBridge } from "./recordingBridge.js";
import type { BotAuthoritySnapshot, RecordingBotSessionManager } from "./recordingBot.js";
import { RecordingCapacityError } from "./recordingWorkerPool.js";
import type { RoomHistoryBridge } from "./roomHistoryBridge.js";

type RuntimeRoomState = {
  metadata: RoomMetadata;
  peers: PeerManager;
  cleanupTimer?: NodeJS.Timeout;
  recordingLive: boolean;
};

type CreatorControlResult = {
  actor: Participant;
  target: Participant;
  chatMessage?: ChatMessage;
  closedProducerIds?: string[];
};

const RECORDING_STOP_DISPATCH_TIMEOUT_MS = 5_000;

export class RoomManager {
  private readonly rooms = new Map<string, RuntimeRoomState>();
  private readonly roomLocks = new Map<string, Mutex>();
  private readonly recordingBootstrapSnapshots = new Map<string, BotAuthoritySnapshot>();
  private onRoomClosedCallback?: (roomId: string, reason: RoomClosedReason) => void;
  private onActiveSpeakerChangedCallback?: (roomId: string, participantId: string | null) => void;

  constructor(
    private readonly repository: RedisRoomRepository,
    private readonly mediasoup: MediasoupService,
    private readonly emptyGraceMs: number,
    private readonly logger?: Logger,
    private readonly getMediaEpoch: () => string = () => "test-media-epoch",
    private readonly isMediaLeaseHealthy: () => boolean = () => true,
    private readonly metrics: MediaMetrics = getMediaMetrics(),
    private readonly recording?: RoomRecordingBridge,
    private recordingBots?: RecordingBotSessionManager,
    private readonly history?: RoomHistoryBridge,
  ) {
    mediasoup.setRoomDeadCallback((roomId, reason) => {
      this.handleRoomDead(roomId, reason);
    });
    mediasoup.setActiveSpeakerChangedCallback((roomId, participantId) => {
      this.onActiveSpeakerChangedCallback?.(roomId, participantId);
    });
  }

  setRoomClosedCallback(callback: (roomId: string, reason: RoomClosedReason) => void): void {
    this.onRoomClosedCallback = callback;
  }

  setActiveSpeakerChangedCallback(callback: (roomId: string, participantId: string | null) => void): void {
    this.onActiveSpeakerChangedCallback = callback;
  }

  setRecordingBotSessionManager(manager: RecordingBotSessionManager): void {
    this.recordingBots = manager;
  }

  async join(payload: JoinTokenPayload, signal?: AbortSignal): Promise<{
    room: RoomMetadata;
    participant: Participant;
    participants: Participant[];
    chatMessages: ChatMessage[];
    joinMessage?: ChatMessage;
  }> {
    return this.runExclusive(payload.roomId, async () => {
      this.assertJoinActive(signal);
      this.assertMediaLeaseHealthy();
      const roomBeforeJoin = await this.awaitJoinOperation(this.repository.getRoom(payload.roomId), signal);
      this.assertJoinActive(signal);
      const shouldRestoreEmptyTtl = Boolean(roomBeforeJoin.expiresAt);
      const touchOperation = this.repository.touchActiveRoom(roomBeforeJoin);
      let metadata: RoomMetadata;
      try {
        metadata = await this.awaitJoinOperation(touchOperation, signal);
      } catch (error) {
        if (signal?.aborted) this.fenceLateJoinMutation(payload.roomId, payload.participantId, touchOperation);
        throw error;
      }
      this.assertJoinActive(signal);
      const state = await this.ensureRuntimeRoom(metadata);
      state.metadata = metadata;

      const alreadyJoined = Boolean(state.peers.getPeer(payload.participantId));
      const isFirstHuman = state.peers.count() === 0 && !alreadyJoined;
      const participant = state.peers.addPeer(payload);
      if (isFirstHuman && !state.recordingLive && metadata.recordingRequested && this.recording) {
        this.recordingBootstrapSnapshots.set(metadata.roomId, this.buildBotAuthoritySnapshot(metadata.roomId));
        try {
          await this.awaitJoinOperation(this.recording.roomBecameLive(
            metadata,
            state.peers.list(),
            this.mediasoup.listProducers(metadata.roomId),
            signal,
          ), signal);
          this.assertJoinActive(signal);
          state.recordingLive = true;
        } catch (error) {
          await this.abortPreparedRecording(metadata.roomId);
          const joinCancelled = signal?.aborted === true;
          const capacityFailure = error instanceof RecordingCapacityError || /capacity/i.test(String(error));
          if (shouldRestoreEmptyTtl) {
            state.metadata = await this.repository.markRoomEmpty(state.metadata).catch(() => state.metadata);
          }
          this.mediasoup.closeRoom(metadata.roomId);
          this.rooms.delete(metadata.roomId);
          this.metrics.recordRoomCleanup({
            operation: "join_rollback",
            outcome: "ok",
            reason: capacityFailure ? "recording_capacity" : "recording_startup",
          });
          this.recording.logFailure(
            joinCancelled
              ? "recording.joinCancelled"
              : capacityFailure ? "recording.capacityAdmission" : "recording.startup",
            metadata.roomId,
            error,
          );
          if (joinCancelled) {
            throw new SignalingError("MEDIASOUP_ERROR", "Room join timed out or was cancelled");
          }
          throw new SignalingError(
            "MEDIASOUP_ERROR",
            capacityFailure
              ? "Recording capacity is temporarily unavailable"
              : "Recording service is temporarily unavailable",
          );
        } finally {
          this.recordingBootstrapSnapshots.delete(metadata.roomId);
        }
      }
      let participantSaved = false;
      let participantSaveOperation: Promise<void> | undefined;
      let participantSaveSettled = false;
      let ensureRoomOperation: Promise<unknown> | undefined;
      let ensureRoomSettled = false;
      let sessionTransition: Promise<void> | undefined;
      let sessionTransitionSettled = false;

      if (state.cleanupTimer) {
        clearTimeout(state.cleanupTimer);
        state.cleanupTimer = undefined;
      }

      try {
        participantSaveOperation = this.repository.saveParticipant(
          metadata.roomId,
          participant,
          this.getMediaEpoch(),
        );
        await this.awaitJoinOperation(participantSaveOperation, signal);
        participantSaveSettled = true;
        participantSaved = true;
        this.assertJoinActive(signal);
        const pendingRoom = this.mediasoup.ensureRoom(metadata.roomId);
        ensureRoomOperation = pendingRoom;
        await this.awaitJoinOperation(pendingRoom, signal);
        ensureRoomSettled = true;
        this.assertJoinActive(signal);
        sessionTransition = this.repository.markRoomSessionLive?.(metadata.roomId, participant.participantId);
        if (sessionTransition) {
          await this.awaitJoinOperation(sessionTransition, signal);
          sessionTransitionSettled = true;
        }
        this.assertJoinActive(signal);
      } catch (error) {
        if (signal?.aborted) {
          if (participantSaveOperation && !participantSaveSettled) {
            this.fenceLateJoinMutation(metadata.roomId, participant.participantId, participantSaveOperation);
          }
          if (ensureRoomOperation && !ensureRoomSettled) {
            this.fenceLateJoinMutation(metadata.roomId, participant.participantId, ensureRoomOperation);
          }
          if (sessionTransition && !sessionTransitionSettled) {
            this.fenceLateJoinMutation(metadata.roomId, participant.participantId, sessionTransition);
          }
        }
        if (isFirstHuman) {
          await this.abortPreparedRecording(metadata.roomId);
        }
        if (!alreadyJoined) {
          await this.rollbackFailedJoin(
            metadata.roomId,
            participant.participantId,
            state,
            participantSaved,
            shouldRestoreEmptyTtl,
          );
        }
        throw error;
      }

      let chatMessages: ChatMessage[] = [];
      let joinMessage: ChatMessage | undefined;
      try {
        try {
          chatMessages = await this.awaitJoinOperation(this.repository.listChatMessages(metadata.roomId), signal);
        } catch (error) {
          if (signal?.aborted) throw error;
          this.logger?.warn("chat history read failed during room join", {
            operation: "room.join.chatHistory",
            errorCode: "CHAT_HISTORY_READ_FAILED",
            roomId: metadata.roomId,
            participantId: participant.participantId,
            error: String(error),
          });
        }
        if (!alreadyJoined) {
          joinMessage = await this.appendSystemChatMessageBestEffort(
            metadata.roomId,
            `${participant.displayName} подключился к конференции`,
            participant.participantId,
            "join_log",
            signal,
          );
          if (joinMessage) {
            chatMessages = [...chatMessages, joinMessage].slice(-CHAT_HISTORY_LIMIT);
          }
        }
      } catch (error) {
        if (signal?.aborted) {
          if (isFirstHuman) await this.abortPreparedRecording(metadata.roomId);
          if (!alreadyJoined) {
            await this.rollbackFailedJoin(
              metadata.roomId,
              participant.participantId,
              state,
              true,
              shouldRestoreEmptyTtl,
            );
          }
        }
        throw error;
      }

      this.logger?.info("room join completed", {
        operation: "room.join",
        roomId: metadata.roomId,
        participantId: participant.participantId,
        participantCount: state.peers.count(),
        alreadyJoined,
        outcome: "ok",
      });

      if (!alreadyJoined) this.history?.report({ type: "participant.joined", roomId: metadata.roomId, participantId: participant.participantId, displayName: participant.displayName });

      this.syncBotAuthority(metadata.roomId);

      return {
        room: metadata,
        participant,
        participants: state.peers.list(),
        chatMessages,
        joinMessage,
      };
    });
  }

  getParticipants(roomId: string): Participant[] {
    return this.requireRuntimeRoom(roomId).peers.list();
  }

  getParticipant(roomId: string, participantId: string): Participant | undefined {
    return this.requireRuntimeRoom(roomId).peers.getPeer(participantId);
  }

  async setConnectionQuality(
    roomId: string,
    participantId: string,
    connectionQuality: ParticipantConnectionQuality,
  ): Promise<Participant> {
    return this.runExclusive(roomId, async () => {
      const state = this.requireRuntimeRoom(roomId);

      const participant = state.peers.setConnectionQuality(participantId, connectionQuality);
      if (!participant) {
        throw new SignalingError("INVALID_TOKEN", "Participant is not joined");
      }

      await this.repository.saveParticipant(roomId, participant, this.getMediaEpoch());
      return participant;
    });
  }

  async getRouterRtpCapabilities(roomId: string) {
    return this.runExclusive(roomId, async () => this.mediasoup.getRouterRtpCapabilities(roomId));
  }

  async createTransport(roomId: string, participantId: string, direction: "send" | "recv") {
    return this.runExclusive(roomId, async () => {
      this.assertJoined(roomId, participantId);
      return this.mediasoup.createWebRtcTransport(roomId, participantId, direction);
    });
  }

  async connectTransport(roomId: string, participantId: string, transportId: string, dtlsParameters: unknown) {
    return this.runExclusive(roomId, async () => {
      this.assertJoined(roomId, participantId);
      return this.mediasoup.connectTransport(roomId, participantId, transportId, dtlsParameters);
    });
  }

  async produce(
    roomId: string,
    participantId: string,
    transportId: string,
    kind: MediaKind,
    rtpParameters: unknown,
    source: "mic" | "camera" | "screen",
    onClose: (result: { producerId: string; source: "mic" | "camera" | "screen"; participant: Participant }) => void,
  ) {
    return this.runExclusive(roomId, async () => {
      this.assertJoined(roomId, participantId);
      const state = this.requireRuntimeRoom(roomId);
      if ((source === "mic" && kind !== "audio") || (source !== "mic" && kind !== "video")) {
        throw new SignalingError("INVALID_PAYLOAD", "Producer kind does not match media source");
      }

      const data = await this.mediasoup.produce(
        roomId,
        participantId,
        transportId,
        kind,
        rtpParameters,
        source,
        (producerId) => {
            if (state.metadata.recordingRequested) {
              this.runRecording("recording.producerClosed", roomId, () => this.recording?.producerClosed(roomId, producerId));
            }
          void this.runExclusive(roomId, async () => {
            const runtime = this.rooms.get(roomId);
            if (!runtime) return;
            const reconciled = await this.reconcileParticipantMediaLocked(runtime, participantId);
            if (reconciled.changed) {
              onClose({ producerId, source, participant: reconciled.participant });
            }
            this.syncBotAuthority(roomId);
          }).catch((error: unknown) => {
            this.logger?.warn("producer close reconciliation failed", {
              operation: "producer.close.reconcile",
              errorCode: "PRODUCER_RECONCILIATION_FAILED",
              roomId,
              participantId,
              producerId,
              error: String(error),
            });
          });
        },
      );
      const reconciled = await this.reconcileParticipantMediaLocked(state, participantId);
      const producer: RecorderProducer = {
        producerId: data.id,
        participantId,
        kind,
        source,
      };
      if (state.metadata.recordingRequested) {
        this.runRecording("recording.producerAdded", roomId, () => this.recording?.producerAdded(state.metadata, reconciled.participant, producer));
      }
      this.syncBotAuthority(roomId);
      return { ...data, participant: reconciled.participant };
    });
  }

  async consume(roomId: string, participantId: string, transportId: string, producerId: string, rtpCapabilities: unknown) {
    return this.runExclusive(roomId, async () => {
      this.assertJoined(roomId, participantId);
      return this.mediasoup.consume(roomId, participantId, transportId, producerId, rtpCapabilities);
    });
  }

  async closeProducer(roomId: string, participantId: string, producerId: string) {
    return this.runExclusive(roomId, async () => {
      this.assertJoined(roomId, participantId);
      const closed = await this.mediasoup.closeProducer(roomId, participantId, producerId);
      const reconciled = await this.reconcileParticipantMediaLocked(
        this.requireRuntimeRoom(roomId),
        participantId,
      );
      this.syncBotAuthority(roomId);
      return { ...closed, participant: reconciled.participant };
    });
  }

  async setProducerPaused(roomId: string, participantId: string, producerId: string, paused: boolean) {
    return this.runExclusive(roomId, async () => {
      this.assertJoined(roomId, participantId);
      const changed = await this.mediasoup.setProducerPaused(roomId, participantId, producerId, paused);
      const reconciled = await this.reconcileParticipantMediaLocked(
        this.requireRuntimeRoom(roomId),
        participantId,
      );
      this.runRecording("recording.producerPaused", roomId, () => this.recording?.producerPaused(roomId, producerId, paused));
      this.syncBotAuthority(roomId);
      return { ...changed, participant: reconciled.participant };
    });
  }

  async closeRoomByCreator(roomId: string, participantId: string): Promise<void> {
    return this.runExclusive(roomId, async () => {
      const state = this.requireRuntimeRoom(roomId);
      const participant = state.peers.getPeer(participantId);
      if (!participant?.isCreator) {
        throw new SignalingError("INVALID_TOKEN", "Only the room creator can close the room");
      }

      if (state.cleanupTimer) {
        clearTimeout(state.cleanupTimer);
        state.cleanupTimer = undefined;
      }

      await this.stopRecording(roomId);
      this.mediasoup.closeRoom(roomId);
      this.rooms.delete(roomId);
      await this.repository.closeRoom(state.metadata, "closed_by_creator");
      this.history?.report({ type: "room.closed", roomId, reason: "closed_by_creator" });
      this.metrics.recordRoomCleanup({ operation: "creator_close", outcome: "ok", reason: "closed_by_creator" });
      this.logger?.info("room closed by creator", {
        operation: "room.close.creator",
        roomId,
        participantId,
        reason: "closed_by_creator",
        outcome: "ok",
      });
      this.roomLocks.delete(roomId);
    });
  }

  async kickParticipantByCreator(
    roomId: string,
    actorParticipantId: string,
    targetParticipantId: string,
  ): Promise<CreatorControlResult> {
    return this.runExclusive(roomId, async () => {
      const { state, actor, target } = this.requireCreatorControlTarget(
        roomId,
        actorParticipantId,
        targetParticipantId,
      );

      this.mediasoup.closePeer(roomId, targetParticipantId);
      state.peers.removePeer(targetParticipantId);
      await this.repository.removeParticipant(roomId, targetParticipantId);
      this.history?.report({
        type: "participant.left",
        roomId,
        participantId: target.participantId,
        displayName: target.displayName,
      });
      this.syncBotAuthority(roomId);
      await this.repository.setRoomSessionCreator(roomId, targetParticipantId, false);
      const chatMessage = await this.appendSystemChatMessageBestEffort(
        roomId,
        `${actor.displayName} removed ${target.displayName} from the conference`,
        targetParticipantId,
        "kick_log",
      );

      return { actor, target, chatMessage };
    });
  }

  async disableParticipantMediaByCreator(
    roomId: string,
    actorParticipantId: string,
    targetParticipantId: string,
    source: MediaSource,
  ): Promise<CreatorControlResult> {
    return this.runExclusive(roomId, async () => {
      const { state, actor, target } = this.requireCreatorControlTarget(
        roomId,
        actorParticipantId,
        targetParticipantId,
      );
      const closedProducerIds = await this.mediasoup.closeProducersForSource(
        roomId,
        targetParticipantId,
        source,
      );
      if (closedProducerIds.length === 0) {
        throw new SignalingError("INVALID_PAYLOAD", "Target has no published media for this source");
      }
      const nextTarget = (await this.reconcileParticipantMediaLocked(state, targetParticipantId)).participant;

      const chatMessage = await this.appendSystemChatMessageBestEffort(
        roomId,
        this.getCreatorMediaControlMessage(actor.displayName, target.displayName, source),
        targetParticipantId,
        "creator_media_log",
      );

      return { actor, target: nextTarget, chatMessage, closedProducerIds };
    });
  }

  async grantCreatorByCreator(
    roomId: string,
    actorParticipantId: string,
    targetParticipantId: string,
  ): Promise<CreatorControlResult> {
    return this.runExclusive(roomId, async () => {
      const { state, actor, target } = this.requireCreatorControlTarget(
        roomId,
        actorParticipantId,
        targetParticipantId,
      );

      const nextTarget = state.peers.setCreator(targetParticipantId, true);
      if (!nextTarget) {
        throw new SignalingError("INVALID_TOKEN", "Participant is not joined");
      }

      await this.repository.saveParticipant(roomId, nextTarget, this.getMediaEpoch());
      await this.repository.setRoomSessionCreator(roomId, targetParticipantId, true);
      const chatMessage = await this.appendSystemChatMessageBestEffort(
        roomId,
        `${actor.displayName} shared creator status with ${target.displayName}`,
        targetParticipantId,
        "creator_grant_log",
      );

      return { actor, target: nextTarget, chatMessage };
    });
  }

  listProducers(roomId: string) {
    return this.mediasoup.listProducers(roomId);
  }

  async releaseParticipantMedia(roomId: string, participantId: string): Promise<Participant | undefined> {
    return this.runExclusive(roomId, async () => {
      const state = this.requireRuntimeRoom(roomId);
      const participant = state.peers.getPeer(participantId);
      if (!participant) {
        return undefined;
      }

      this.mediasoup.closePeer(roomId, participantId);
      return (await this.reconcileParticipantMediaLocked(state, participantId)).participant;
    });
  }

  getActiveSpeakerParticipantId(roomId: string): string | null {
    return this.mediasoup.getActiveSpeakerParticipantId(roomId);
  }

  async sendChatMessage(roomId: string, participantId: string, text: string): Promise<ChatMessage> {
    return this.runExclusive(roomId, async () => {
      const state = this.requireRuntimeRoom(roomId);
      const participant = state.peers.getPeer(participantId);
      if (!participant) {
        throw new SignalingError("INVALID_TOKEN", "Participant is not joined");
      }

      const trimmed = text.trim();
      if (!trimmed || trimmed.length > CHAT_MESSAGE_TEXT_MAX_LENGTH) {
        throw new SignalingError("INVALID_PAYLOAD", "Chat message text is invalid");
      }

      const message = await this.repository.appendChatMessage(roomId, {
        messageId: this.createMessageId(),
        roomId,
        kind: "user",
        participantId: participant.participantId,
        authorName: participant.displayName,
        text: trimmed,
        createdAt: new Date().toISOString(),
      });
      this.history?.report({ type: "chat.message", roomId, messageId: message.messageId, kind: "user", participantId: participant.participantId, authorName: participant.displayName, text: message.text });
      return message;
    });
  }

  async leave(roomId: string, participantId: string): Promise<{ participant?: Participant; chatMessage?: ChatMessage }> {
    return this.runExclusive(roomId, async () => {
      const state = this.rooms.get(roomId);
      if (!state) {
        return {};
      }

      const participant = state.peers.removePeer(participantId);
      if (!participant) {
        return {};
      }

      this.mediasoup.closePeer(roomId, participantId);
      await this.repository.removeParticipant(roomId, participantId);
      this.syncBotAuthority(roomId);
      const chatMessage = await this.appendSystemChatMessageBestEffort(
        roomId,
        `${participant.displayName} покинул конференцию`,
        participantId,
        "leave_log",
      );

      if (state.peers.count() === 0) {
        state.metadata = await this.repository.markRoomEmpty(state.metadata);
        state.cleanupTimer = setTimeout(() => {
          void this.closeEmptyRoom(roomId).catch((error: unknown) => {
            this.logger?.error("empty room cleanup failed", {
              operation: "room.empty.cleanup",
              errorCode: "ROOM_CLEANUP_FAILED",
              roomId,
              error: String(error),
            });
          });
        }, this.emptyGraceMs);
      }

      this.history?.report({
        type: "participant.left",
        roomId,
        participantId: participant.participantId,
        displayName: participant.displayName,
      });

      this.logger?.info("room leave completed", {
        operation: "room.leave",
        roomId,
        participantId,
        participantCount: state.peers.count(),
        scheduledEmptyCleanup: state.peers.count() === 0,
        outcome: "ok",
      });

      return { participant, chatMessage };
    });
  }

  // Called when a mediasoup worker dies — evicts all participants in the affected room.
  private handleRoomDead(roomId: string, reason: RoomClosedReason): void {
    this.runExclusive(roomId, async () => {
      const state = this.rooms.get(roomId);
      if (!state) {
        return;
      }

      if (state.cleanupTimer) {
        clearTimeout(state.cleanupTimer);
      }

      this.rooms.delete(roomId);
      try {
        await this.stopRecording(roomId);
        await this.repository.closeRoom(state.metadata, reason);
        this.history?.report({ type: "room.closed", roomId, reason });
        this.metrics.recordRoomCleanup({ operation: "room_dead", outcome: "ok", reason });
        this.logger?.error("room closed after mediasoup failure", {
          operation: "room.dead.cleanup",
          errorCode: reason === "media_server_error" ? "MEDIA_SERVER_ERROR" : "ROOM_CLOSED_BY_SERVER",
          roomId,
          reason,
          outcome: "ok",
        });
      } finally {
        this.onRoomClosedCallback?.(roomId, reason);
        this.roomLocks.delete(roomId);
      }
    }).catch((error: unknown) => {
      this.metrics.recordRoomCleanup({ operation: "room_dead", outcome: "failure", reason });
      this.logger?.error("handleRoomDead cleanup failed", {
        operation: "room.dead.cleanup",
        errorCode: "ROOM_CLEANUP_FAILED",
        roomId,
        error: String(error),
      });
    });
  }

  private async ensureRuntimeRoom(metadata: RoomMetadata): Promise<RuntimeRoomState> {
    const existing = this.rooms.get(metadata.roomId);
    if (existing) {
      return existing;
    }

    const state: RuntimeRoomState = {
      metadata,
      peers: new PeerManager(),
      recordingLive: false,
    };
    this.rooms.set(metadata.roomId, state);
    return state;
  }

  private requireRuntimeRoom(roomId: string): RuntimeRoomState {
    const state = this.rooms.get(roomId);
    if (!state) {
      throw new SignalingError("ROOM_NOT_FOUND", "Runtime room not found");
    }

    return state;
  }

  private async reconcileParticipantMediaLocked(
    state: RuntimeRoomState,
    participantId: string,
  ): Promise<{ participant: Participant; changed: boolean }> {
    const current = state.peers.getPeer(participantId);
    if (!current) {
      throw new SignalingError("INVALID_TOKEN", "Participant is not joined");
    }

    const media = this.mediasoup.getParticipantMedia(state.metadata.roomId, participantId);
    const changed =
      current.media.mic !== media.mic ||
      current.media.camera !== media.camera ||
      current.media.screen !== media.screen;
    if (!changed) {
      return { participant: current, changed: false };
    }

    const participant = state.peers.setMedia(participantId, media);
    if (!participant) {
      throw new SignalingError("INVALID_TOKEN", "Participant is not joined");
    }
    await this.repository.saveParticipant(state.metadata.roomId, participant, this.getMediaEpoch());
    return { participant, changed: true };
  }

  private assertJoined(roomId: string, participantId: string): void {
    if (!this.getParticipant(roomId, participantId)) {
      throw new SignalingError("INVALID_TOKEN", "Participant is not joined");
    }
  }

  private requireCreatorControlTarget(
    roomId: string,
    actorParticipantId: string,
    targetParticipantId: string,
  ): {
    state: RuntimeRoomState;
    actor: Participant;
    target: Participant;
  } {
    const state = this.requireRuntimeRoom(roomId);
    const actor = state.peers.getPeer(actorParticipantId);
    if (!actor?.isCreator) {
      throw new SignalingError("INVALID_TOKEN", "Only a creator can manage participants");
    }
    if (actorParticipantId === targetParticipantId) {
      throw new SignalingError("INVALID_PAYLOAD", "Creator controls require another participant");
    }

    const target = state.peers.getPeer(targetParticipantId);
    if (!target) {
      throw new SignalingError("INVALID_PAYLOAD", "Target participant is not joined");
    }

    return { state, actor, target };
  }

  private getCreatorMediaControlMessage(
    actorDisplayName: string,
    targetDisplayName: string,
    source: MediaSource,
  ): string {
    switch (source) {
      case "mic":
        return `${actorDisplayName} muted ${targetDisplayName}'s microphone`;
      case "camera":
        return `${actorDisplayName} turned off ${targetDisplayName}'s camera`;
      case "screen":
        return `${actorDisplayName} stopped ${targetDisplayName}'s screen share`;
    }
  }

  private appendSystemChatMessage(roomId: string, text: string): Promise<ChatMessage> {
    return this.repository.appendChatMessage(roomId, {
      messageId: this.createMessageId(),
      roomId,
      kind: "system",
      text,
      createdAt: new Date().toISOString(),
    });
  }

  private async appendSystemChatMessageBestEffort(
    roomId: string,
    text: string,
    participantId: string,
    operation: string,
    signal?: AbortSignal,
  ): Promise<ChatMessage | undefined> {
    try {
      return await this.awaitJoinOperation(this.appendSystemChatMessage(roomId, text), signal);
    } catch (error) {
      if (signal?.aborted) throw error;
      this.logger?.warn("system chat append failed", {
        roomId,
        participantId,
        operation,
        errorCode: "SYSTEM_CHAT_APPEND_FAILED",
        error: String(error),
      });
      return undefined;
    }
  }

  private createMessageId(): string {
    return `msg_${randomUUID()}`;
  }

  private async rollbackFailedJoin(
    roomId: string,
    participantId: string,
    state: RuntimeRoomState,
    participantSaved: boolean,
    restoreEmptyTtl: boolean,
  ): Promise<void> {
    this.mediasoup.closePeer(roomId, participantId);
    state.peers.removePeer(participantId);

    if (participantSaved) {
      try {
        await this.repository.removeParticipant(roomId, participantId);
      } catch (error) {
        this.logger?.warn("failed to remove participant after failed join", {
          operation: "room.join.rollback",
          errorCode: "PARTICIPANT_REMOVE_FAILED",
          roomId,
          participantId,
          error: String(error),
        });
      }
    }

    if (state.peers.count() === 0) {
      if (state.cleanupTimer) {
        clearTimeout(state.cleanupTimer);
        state.cleanupTimer = undefined;
      }
      if (restoreEmptyTtl) {
        try {
          state.metadata = await this.repository.markRoomEmpty(state.metadata);
        } catch (error) {
          this.logger?.warn("failed to restore empty room TTL after failed join", {
            operation: "room.join.rollback",
            errorCode: "ROOM_EMPTY_TTL_RESTORE_FAILED",
            roomId,
            participantId,
            error: String(error),
          });
        }
      }
      this.mediasoup.closeRoom(roomId);
      this.rooms.delete(roomId);
      this.metrics.recordRoomCleanup({ operation: "join_rollback", outcome: "ok" });
      this.logger?.warn("room join rolled back", {
        operation: "room.join.rollback",
        roomId,
        participantId,
        participantSaved,
        restoreEmptyTtl,
        outcome: "ok",
      });
    }
  }

  private async closeEmptyRoom(roomId: string): Promise<void> {
    await this.runExclusive(roomId, async () => {
      const state = this.rooms.get(roomId);
      if (!state || state.peers.count() > 0) {
        return;
      }

      await this.stopRecording(roomId);
      this.mediasoup.closeRoom(roomId);
      this.rooms.delete(roomId);
      this.history?.report({ type: "room.closed", roomId, reason: "empty_timeout" });
      this.metrics.recordRoomCleanup({ operation: "empty_room", outcome: "ok" });
      this.logger?.info("empty room runtime cleaned up", {
        operation: "room.empty.cleanup",
        roomId,
        outcome: "ok",
      });
      // Fix 2: delete lock after the await so no concurrent join sneaks in via a fresh mutex
      this.roomLocks.delete(roomId);
    });
  }

  async shutdown(): Promise<void> {
    const roomIds = [...this.rooms.keys()];
    const results = await Promise.allSettled(roomIds.map((roomId) =>
      this.runExclusive(roomId, async () => {
        const state = this.rooms.get(roomId);
        if (!state) {
          return;
        }

        if (state.cleanupTimer) {
          clearTimeout(state.cleanupTimer);
        }
        for (const participant of state.peers.list()) {
          this.mediasoup.closePeer(roomId, participant.participantId);
          try {
            await this.repository.removeParticipant(roomId, participant.participantId);
          } catch (error) {
            this.logger?.warn("shutdown participant cleanup failed", {
              operation: "service.shutdown.participantCleanup",
              errorCode: "PARTICIPANT_REMOVE_FAILED",
              roomId,
              participantId: participant.participantId,
              error: String(error),
            });
          }
        }
        try {
          state.metadata = await this.repository.markRoomEmpty(state.metadata);
        } catch (error) {
          this.logger?.warn("shutdown empty-room transition failed", {
            operation: "service.shutdown.emptyRoom",
            errorCode: "ROOM_EMPTY_TRANSITION_FAILED",
            roomId,
            error: String(error),
          });
        }
        await this.stopRecording(roomId);
        this.mediasoup.closeRoom(roomId);
        this.rooms.delete(roomId);
        this.roomLocks.delete(roomId);
        this.metrics.recordRoomCleanup({ operation: "shutdown", outcome: "ok", reason: "server_shutdown" });
        this.logger?.info("shutdown room cleanup completed", {
          operation: "service.shutdown.roomCleanup",
          roomId,
          outcome: "ok",
        });
      }),
    ));
    const failures = results
      .filter((result): result is PromiseRejectedResult => result.status === "rejected")
      .map((result) => result.reason);
    if (failures.length > 0) throw new AggregateError(failures, "Room shutdown failed");
  }

  private runExclusive<T>(roomId: string, operation: () => Promise<T>): Promise<T> {
    return this.getRoomLock(roomId).runExclusive(operation);
  }

  private runRecording(operation: string, roomId: string, action: () => Promise<void> | undefined): void {
    void action()?.catch((error: unknown) => {
      this.recording?.logFailure(operation, roomId, error);
    });
  }

  private async stopRecording(roomId: string): Promise<void> {
    this.recordingBots?.stopRoom(roomId);
    if (!this.recording) return;
    try {
      await withTimeout(this.recording.stopRoom(roomId), RECORDING_STOP_DISPATCH_TIMEOUT_MS);
      const state = this.rooms.get(roomId);
      if (state) state.recordingLive = false;
    } catch (error) {
      this.recording.logFailure("recording.stop", roomId, error);
    }
  }

  private async abortPreparedRecording(roomId: string): Promise<void> {
    if (!this.recording) return;
    try {
      if (this.recording.abortPrepared) {
        await withTimeout(this.recording.abortPrepared(roomId), RECORDING_STOP_DISPATCH_TIMEOUT_MS);
      } else {
        await withTimeout(this.recording.stopRoom(roomId), RECORDING_STOP_DISPATCH_TIMEOUT_MS);
      }
    } catch (error) {
      this.recording.logFailure("recording.abortPrepared", roomId, error);
    }
  }

  private assertMediaLeaseHealthy(): void {
    if (!this.isMediaLeaseHealthy()) {
      throw new SignalingError("MEDIASOUP_ERROR", "Media process lease is unavailable");
    }
  }

  private assertJoinActive(signal?: AbortSignal): void {
    if (signal?.aborted) {
      throw new SignalingError("MEDIASOUP_ERROR", "Room join timed out or was cancelled");
    }
  }

  private async awaitJoinOperation<T>(operation: Promise<T>, signal?: AbortSignal): Promise<T> {
    if (!signal) return operation;
    this.assertJoinActive(signal);
    let onAbort: (() => void) | undefined;
    const aborted = new Promise<never>((_, reject) => {
      onAbort = () => reject(new SignalingError("MEDIASOUP_ERROR", "Room join timed out or was cancelled"));
      signal.addEventListener("abort", onAbort, { once: true });
    });
    try {
      return await Promise.race([operation, aborted]);
    } finally {
      if (onAbort) signal.removeEventListener("abort", onAbort);
    }
  }

  private fenceLateJoinMutation(roomId: string, participantId: string, operation: Promise<unknown>): void {
    void operation.then(() => this.runExclusive(roomId, async () => {
      const state = this.rooms.get(roomId);
      if (state?.peers.getPeer(participantId)) return;
      await this.repository.removeParticipant(roomId, participantId);
      if (!state || state.peers.count() === 0) {
        const metadata = state?.metadata ?? await this.repository.getRoom(roomId).catch(() => undefined);
        if (metadata && !metadata.expiresAt) await this.repository.markRoomEmpty(metadata);
        this.mediasoup.closeRoom(roomId);
        this.rooms.delete(roomId);
      }
    })).catch((error: unknown) => {
      this.logger?.warn("late join mutation compensation failed", {
        operation: "room.join.lateCompensation",
        errorCode: "JOIN_LATE_COMPENSATION_FAILED",
        roomId,
        participantId,
        error: String(error),
      });
    });
  }

  private getRoomLock(roomId: string): Mutex {
    const existing = this.roomLocks.get(roomId);
    if (existing) {
      return existing;
    }

    const lock = new Mutex();
    this.roomLocks.set(roomId, lock);
    return lock;
  }

  getRuntimeStats(): { activeRooms: number; activeParticipants: number } {
    let activeParticipants = 0;
    for (const state of this.rooms.values()) {
      activeParticipants += state.peers.count();
    }

    return {
      activeRooms: this.rooms.size,
      activeParticipants,
    };
  }

  freezeBotAuthoritySnapshot(roomId: string, accept: (snapshot: BotAuthoritySnapshot) => void): Promise<void> {
    const bootstrapSnapshot = this.recordingBootstrapSnapshots.get(roomId);
    if (bootstrapSnapshot) {
      accept(bootstrapSnapshot);
      return Promise.resolve();
    }
    return this.runExclusive(roomId, async () => {
      accept(this.buildBotAuthoritySnapshot(roomId));
    });
  }

  private buildBotAuthoritySnapshot(roomId: string): BotAuthoritySnapshot {
    const state = this.requireRuntimeRoom(roomId);
    const participants = state.peers.list().map(({ participantId, displayName }) => ({ participantId, displayName }));
    const names = new Map(participants.map((participant) => [participant.participantId, participant.displayName]));
    const producers = this.mediasoup.listProducers(roomId).flatMap((producer) => {
      const displayName = names.get(producer.participantId);
      return displayName ? [{ ...producer, displayName }] : [];
    });
    return { participants, producers };
  }

  private syncBotAuthority(roomId: string): void {
    if (!this.recordingBots) return;
    this.recordingBots.syncAuthority(roomId, this.buildBotAuthoritySnapshot(roomId));
  }
}

async function withTimeout<T>(operation: Promise<T>, timeoutMs: number): Promise<T> {
  let timeout: NodeJS.Timeout | undefined;
  try {
    return await Promise.race([
      operation,
      new Promise<T>((_, reject) => {
        timeout = setTimeout(() => reject(new Error(`Recording stop dispatch timed out after ${timeoutMs}ms`)), timeoutMs);
      }),
    ]);
  } finally {
    if (timeout) clearTimeout(timeout);
  }
}
