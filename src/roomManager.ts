import { CHAT_HISTORY_LIMIT, CHAT_MESSAGE_TEXT_MAX_LENGTH, MAX_ROOM_PARTICIPANTS } from "#contracts";
import type { ChatMessage, MediaKind, Participant, ParticipantMedia, RoomMetadata } from "#contracts";
import { Mutex } from "async-mutex";
import { randomUUID } from "node:crypto";

import { SignalingError } from "./errors.js";
import type { Logger } from "./logger.js";
import type { MediasoupService } from "./mediasoupService.js";
import { PeerManager } from "./peerManager.js";
import type { RedisRoomRepository } from "./redisRoomRepository.js";
import type { JoinTokenPayload } from "./token.js";

type RuntimeRoomState = {
  metadata: RoomMetadata;
  peers: PeerManager;
  cleanupTimer?: NodeJS.Timeout;
};

export class RoomManager {
  private readonly rooms = new Map<string, RuntimeRoomState>();
  private readonly roomLocks = new Map<string, Mutex>();
  private onParticipantEvictedCallback?: (roomId: string, participantId: string) => void;
  private onActiveSpeakerChangedCallback?: (roomId: string, participantId: string | null) => void;

  constructor(
    private readonly repository: RedisRoomRepository,
    private readonly mediasoup: MediasoupService,
    private readonly emptyGraceMs: number,
    private readonly logger?: Logger,
  ) {
    mediasoup.setRoomDeadCallback((roomId) => {
      this.handleRoomDead(roomId);
    });
    mediasoup.setActiveSpeakerChangedCallback((roomId, participantId) => {
      this.onActiveSpeakerChangedCallback?.(roomId, participantId);
    });
  }

  setParticipantEvictedCallback(callback: (roomId: string, participantId: string) => void): void {
    this.onParticipantEvictedCallback = callback;
  }

  setActiveSpeakerChangedCallback(callback: (roomId: string, participantId: string | null) => void): void {
    this.onActiveSpeakerChangedCallback = callback;
  }

  async join(payload: JoinTokenPayload): Promise<{
    room: RoomMetadata;
    participant: Participant;
    participants: Participant[];
    chatMessages: ChatMessage[];
    joinMessage?: ChatMessage;
  }> {
    return this.runExclusive(payload.roomId, async () => {
      const roomBeforeJoin = await this.repository.getRoom(payload.roomId);
      const shouldRestoreEmptyTtl = Boolean(roomBeforeJoin.expiresAt);
      const metadata = await this.repository.touchActiveRoom(roomBeforeJoin);
      const state = await this.ensureRuntimeRoom(metadata);
      state.metadata = metadata;

      if (!state.peers.getPeer(payload.participantId) && state.peers.count() >= MAX_ROOM_PARTICIPANTS) {
        throw new SignalingError("ROOM_FULL", "Room is full");
      }

      await this.repository.assertRoomCapacity(metadata.roomId, payload.participantId);

      const alreadyJoined = Boolean(state.peers.getPeer(payload.participantId));
      const participant = state.peers.addPeer(payload);
      let participantSaved = false;

      if (state.cleanupTimer) {
        clearTimeout(state.cleanupTimer);
        state.cleanupTimer = undefined;
      }

      try {
        await this.repository.saveParticipant(metadata.roomId, participant);
        participantSaved = true;
        await this.mediasoup.ensureRoom(metadata.roomId);
      } catch (error) {
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

      let chatMessages: ChatMessage[];
      let joinMessage: ChatMessage | undefined;
      try {
        chatMessages = await this.repository.listChatMessages(metadata.roomId);
        joinMessage = alreadyJoined
          ? undefined
          : await this.appendSystemChatMessage(metadata.roomId, `${participant.displayName} подключился к конференции`);
        if (joinMessage) {
          chatMessages = [...chatMessages, joinMessage].slice(-CHAT_HISTORY_LIMIT);
        }
      } catch (error) {
        if (!alreadyJoined) {
          await this.rollbackFailedJoin(
            metadata.roomId,
            participant.participantId,
            state,
            participantSaved,
            shouldRestoreEmptyTtl,
          );
        }
        this.logger?.warn("chat initialization failed during room join", {
          roomId: metadata.roomId,
          participantId: participant.participantId,
          error: String(error),
        });
        throw new SignalingError(
          "MEDIASOUP_ERROR",
          "Room chat is temporarily unavailable. Please refresh and join again.",
        );
      }

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

  async setMedia(roomId: string, participantId: string, media: ParticipantMedia): Promise<Participant> {
    return this.runExclusive(roomId, async () => {
      const state = this.requireRuntimeRoom(roomId);

      const participant = state.peers.setMedia(participantId, media);
      if (!participant) {
        throw new SignalingError("INVALID_TOKEN", "Participant is not joined");
      }

      await this.repository.saveParticipant(roomId, participant);
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
    onClose: (producerId: string) => void,
  ) {
    return this.runExclusive(roomId, async () => {
      this.assertJoined(roomId, participantId);
      if ((source === "mic" && kind !== "audio") || (source !== "mic" && kind !== "video")) {
        throw new SignalingError("INVALID_PAYLOAD", "Producer kind does not match media source");
      }

      return this.mediasoup.produce(roomId, participantId, transportId, kind, rtpParameters, source, onClose);
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
      return this.mediasoup.closeProducer(roomId, participantId, producerId);
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

      this.mediasoup.closeRoom(roomId);
      this.rooms.delete(roomId);
      await this.repository.closeRoom(state.metadata);
      this.roomLocks.delete(roomId);
    });
  }

  listProducers(roomId: string) {
    return this.mediasoup.listProducers(roomId);
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

      return this.repository.appendChatMessage(roomId, {
        messageId: this.createMessageId(),
        roomId,
        kind: "user",
        participantId: participant.participantId,
        authorName: participant.displayName,
        text: trimmed,
        createdAt: new Date().toISOString(),
      });
    });
  }

  async leave(roomId: string, participantId: string): Promise<{ participant?: Participant; chatMessage?: ChatMessage }> {
    return this.runExclusive(roomId, async () => {
      const state = this.rooms.get(roomId);
      if (!state) {
        return {};
      }

      this.mediasoup.closePeer(roomId, participantId);
      const participant = state.peers.removePeer(participantId);
      await this.repository.removeParticipant(roomId, participantId);
      const chatMessage = participant
        ? await this.appendSystemChatMessage(roomId, `${participant.displayName} покинул конференцию`)
        : undefined;

      if (state.peers.count() === 0) {
        state.metadata = await this.repository.markRoomEmpty(state.metadata);
        state.cleanupTimer = setTimeout(() => {
          void this.closeEmptyRoom(roomId);
        }, this.emptyGraceMs);
      }

      return { participant, chatMessage };
    });
  }

  // Called when a mediasoup worker dies — evicts all participants in the affected room.
  private handleRoomDead(roomId: string): void {
    this.runExclusive(roomId, async () => {
      const state = this.rooms.get(roomId);
      if (!state) {
        return;
      }

      if (state.cleanupTimer) {
        clearTimeout(state.cleanupTimer);
      }

      // Notify gateway per-participant before wiping Redis state.
      // removeParticipant per-peer is skipped: closeRoom below deletes
      // the entire participants hash in one DEL, making per-entry hDel redundant.
      for (const participant of state.peers.list()) {
        this.onParticipantEvictedCallback?.(roomId, participant.participantId);
      }

      this.rooms.delete(roomId);
      await this.repository.closeRoom(state.metadata);
      this.roomLocks.delete(roomId);
    }).catch((error: unknown) => {
      this.logger?.error("handleRoomDead cleanup failed", { roomId, error: String(error) });
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

  private assertJoined(roomId: string, participantId: string): void {
    if (!this.getParticipant(roomId, participantId)) {
      throw new SignalingError("INVALID_TOKEN", "Participant is not joined");
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
            roomId,
            participantId,
            error: String(error),
          });
        }
      }
      this.mediasoup.closeRoom(roomId);
      this.rooms.delete(roomId);
    }
  }

  private async closeEmptyRoom(roomId: string): Promise<void> {
    await this.runExclusive(roomId, async () => {
      const state = this.rooms.get(roomId);
      if (!state || state.peers.count() > 0) {
        return;
      }

      this.mediasoup.closeRoom(roomId);
      this.rooms.delete(roomId);
      // Fix 2: delete lock after the await so no concurrent join sneaks in via a fresh mutex
      this.roomLocks.delete(roomId);
    });
  }

  private runExclusive<T>(roomId: string, operation: () => Promise<T>): Promise<T> {
    return this.getRoomLock(roomId).runExclusive(operation);
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
}
