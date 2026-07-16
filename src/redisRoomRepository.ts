import {
  CHAT_HISTORY_LIMIT,
  EMPTY_ROOM_TTL_SECONDS,
  chatMessageSchema,
  participantSchema,
  roomMetadataSchema,
} from "#contracts";
import type { ChatMessage, Participant, RoomClosedReason, RoomMetadata } from "#contracts";

import { SignalingError } from "./errors.js";
import { getMediaMetrics, type MediaMetrics } from "./metrics.js";

export type RedisClient = {
  get(key: string): Promise<string | null>;
  set(key: string, value: string): Promise<string | null>;
  hExists(key: string, field: string): Promise<boolean | number>;
  hGet(key: string, field: string): Promise<string | null>;
  hLen(key: string): Promise<number>;
  hSet(key: string, field: string, value: string): Promise<number>;
  hDel(key: string, field: string): Promise<number>;
  rPush(key: string, value: string): Promise<number>;
  lRange(key: string, start: number, stop: number): Promise<string[]>;
  lTrim(key: string, start: number, stop: number): Promise<string>;
  expire(key: string, seconds: number): Promise<boolean | number>;
  persist(key: string): Promise<boolean | number>;
  del(keys: string | string[]): Promise<number>;
};

export class RedisRoomRepository {
  constructor(
    private readonly redis: RedisClient,
    private readonly metrics: MediaMetrics = getMediaMetrics(),
  ) {}

  async getRoom(roomId: string): Promise<RoomMetadata> {
    const raw = await this.recordRedis("get_room_metadata", () => this.redis.get(`room:${roomId}`));
    if (!raw) {
      throw new SignalingError("ROOM_NOT_FOUND", "Room not found");
    }

    const parsed = roomMetadataSchema.safeParse(JSON.parse(raw) as unknown);
    if (!parsed.success) {
      throw new SignalingError("ROOM_NOT_FOUND", "Room metadata is invalid");
    }

    if (parsed.data.status === "closed") {
      throw new SignalingError("ROOM_CLOSED", "Room closed");
    }

    if (parsed.data.expiresAt && new Date(parsed.data.expiresAt).getTime() <= Date.now()) {
      throw new SignalingError("ROOM_EXPIRED", "Room expired");
    }

    return parsed.data;
  }

  async touchActiveRoom(room: RoomMetadata): Promise<RoomMetadata> {
    const activeRoom = roomMetadataSchema.parse({
      ...room,
      expiresAt: null,
      status: "active",
      closedReason: null,
    });

    await this.recordRedis("set_room_metadata", () => this.redis.set(this.roomKey(activeRoom.roomId), JSON.stringify(activeRoom)));
    await this.recordRedis("set_room_slug", () => this.redis.set(this.slugKey(activeRoom.slug), activeRoom.roomId));
    await this.recordRedis("persist_room_metadata", () => this.redis.persist(this.roomKey(activeRoom.roomId)));
    await this.recordRedis("persist_room_slug", () => this.redis.persist(this.slugKey(activeRoom.slug)));
    await this.recordRedis("persist_room_chat", () => this.redis.persist(this.chatKey(activeRoom.roomId)));
    await this.recordRedis("persist_room_sessions", () => this.redis.persist(this.roomSessionsKey(activeRoom.roomId)));

    return activeRoom;
  }

  async saveParticipant(roomId: string, participant: Participant, mediaEpoch: string): Promise<void> {
    const key = this.participantsKey(roomId);

    await this.recordRedis("save_participant", () => this.redis.hSet(
      key,
      participant.participantId,
      JSON.stringify({ participant: participantSchema.parse(participant), mediaEpoch }),
    ));
  }

  async setRoomSessionCreator(roomId: string, participantId: string, isCreator: boolean): Promise<void> {
    const key = this.roomSessionsKey(roomId);
    const raw = await this.recordRedis("get_room_session", () => this.redis.hGet(key, participantId));
    if (!raw) {
      return;
    }

    let parsed: unknown;
    try {
      parsed = JSON.parse(raw) as unknown;
    } catch {
      return;
    }

    if (!isRoomSessionRecord(parsed)) {
      return;
    }

    await this.recordRedis("save_room_session", () => this.redis.hSet(key, participantId, JSON.stringify({ ...parsed, isCreator })));
  }

  async removeParticipant(roomId: string, participantId: string): Promise<void> {
    await this.recordRedis("remove_participant", () => this.redis.hDel(this.participantsKey(roomId), participantId));
  }

  async appendChatMessage(roomId: string, message: ChatMessage): Promise<ChatMessage> {
    const parsed = chatMessageSchema.parse(message);
    const key = this.chatKey(roomId);

    await this.recordRedis("append_chat_message", () => this.redis.rPush(key, JSON.stringify(parsed)));
    await this.recordRedis("trim_chat_history", () => this.redis.lTrim(key, -CHAT_HISTORY_LIMIT, -1));

    return parsed;
  }

  async listChatMessages(roomId: string): Promise<ChatMessage[]> {
    const rawMessages = await this.recordRedis("list_chat_history", () => this.redis.lRange(this.chatKey(roomId), 0, -1));
    const messages: ChatMessage[] = [];

    for (const raw of rawMessages) {
      try {
        const parsed = chatMessageSchema.safeParse(JSON.parse(raw) as unknown);
        if (parsed.success) {
          messages.push(parsed.data);
        }
      } catch {
        // Ignore malformed historical entries; live contract validation still applies on write.
      }
    }

    return messages;
  }

  async markRoomEmpty(room: RoomMetadata): Promise<RoomMetadata> {
    const expiresAt = new Date(Date.now() + EMPTY_ROOM_TTL_SECONDS * 1000).toISOString();
    const emptyRoom = roomMetadataSchema.parse({
      ...room,
      expiresAt,
      status: "active",
      closedReason: null,
    });

    await this.recordRedis("set_room_metadata", () => this.redis.set(this.roomKey(emptyRoom.roomId), JSON.stringify(emptyRoom)));
    await this.recordRedis("set_room_slug", () => this.redis.set(this.slugKey(emptyRoom.slug), emptyRoom.roomId));
    await this.recordRedis("expire_room_metadata", () => this.redis.expire(this.roomKey(emptyRoom.roomId), EMPTY_ROOM_TTL_SECONDS));
    await this.recordRedis("expire_room_slug", () => this.redis.expire(this.slugKey(emptyRoom.slug), EMPTY_ROOM_TTL_SECONDS));
    await this.recordRedis("expire_room_chat", () => this.redis.expire(this.chatKey(emptyRoom.roomId), EMPTY_ROOM_TTL_SECONDS));
    await this.recordRedis("expire_room_sessions", () => this.redis.expire(this.roomSessionsKey(emptyRoom.roomId), EMPTY_ROOM_TTL_SECONDS));
    await this.recordRedis("delete_room_participants", () => this.redis.del(this.participantsKey(emptyRoom.roomId)));

    return emptyRoom;
  }

  async closeRoom(room: RoomMetadata, reason: RoomClosedReason = "closed_by_creator"): Promise<RoomMetadata> {
    const expiresAt = new Date(Date.now() + EMPTY_ROOM_TTL_SECONDS * 1000).toISOString();
    const closedRoom = roomMetadataSchema.parse({
      ...room,
      expiresAt,
      status: "closed",
      closedReason: reason,
    });

    await this.recordRedis("set_room_metadata", () => this.redis.set(this.roomKey(closedRoom.roomId), JSON.stringify(closedRoom)));
    await this.recordRedis("set_room_slug", () => this.redis.set(this.slugKey(closedRoom.slug), closedRoom.roomId));
    await this.recordRedis("expire_room_metadata", () => this.redis.expire(this.roomKey(closedRoom.roomId), EMPTY_ROOM_TTL_SECONDS));
    await this.recordRedis("expire_room_slug", () => this.redis.expire(this.slugKey(closedRoom.slug), EMPTY_ROOM_TTL_SECONDS));
    await this.recordRedis("expire_room_chat", () => this.redis.expire(this.chatKey(closedRoom.roomId), EMPTY_ROOM_TTL_SECONDS));
    await this.recordRedis("expire_room_sessions", () => this.redis.expire(this.roomSessionsKey(closedRoom.roomId), EMPTY_ROOM_TTL_SECONDS));
    await this.recordRedis("delete_room_participants", () => this.redis.del(this.participantsKey(closedRoom.roomId)));

    return closedRoom;
  }

  private roomKey(roomId: string): string {
    return `room:${roomId}`;
  }

  private async recordRedis<T>(operation: string, command: () => Promise<T>): Promise<T> {
    const startedAt = Date.now();
    try {
      const result = await command();
      this.metrics.recordRedisOperation({
        operation,
        outcome: "ok",
        durationMs: Date.now() - startedAt,
      });
      return result;
    } catch (error) {
      this.metrics.recordRedisOperation({
        operation,
        outcome: "failure",
        durationMs: Date.now() - startedAt,
      });
      throw error;
    }
  }

  private slugKey(slug: string): string {
    return `room_slug:${slug}`;
  }

  private participantsKey(roomId: string): string {
    return `room:${roomId}:participants`;
  }

  private chatKey(roomId: string): string {
    return `room:${roomId}:chat`;
  }

  private roomSessionsKey(roomId: string): string {
    return `room:${roomId}:sessions`;
  }
}

function isRoomSessionRecord(value: unknown): value is {
  participantId: string;
  displayName: string | null;
  isCreator: boolean;
  tokenHash: string;
  createdAt: string;
} {
  if (typeof value !== "object" || value === null) {
    return false;
  }

  const record = value as Record<string, unknown>;
  return (
    typeof record.participantId === "string" &&
    record.participantId.length > 0 &&
    (record.displayName === null || typeof record.displayName === "string") &&
    typeof record.isCreator === "boolean" &&
    typeof record.tokenHash === "string" &&
    record.tokenHash.length > 0 &&
    typeof record.createdAt === "string" &&
    !Number.isNaN(Date.parse(record.createdAt))
  );
}
