import {
  CHAT_HISTORY_LIMIT,
  EMPTY_ROOM_TTL_SECONDS,
  MAX_ROOM_PARTICIPANTS,
  chatMessageSchema,
  participantSchema,
  roomMetadataSchema,
} from "#contracts";
import type { ChatMessage, Participant, RoomMetadata } from "#contracts";

import { SignalingError } from "./errors.js";

export type RedisClient = {
  get(key: string): Promise<string | null>;
  set(key: string, value: string): Promise<string | null>;
  hExists(key: string, field: string): Promise<boolean | number>;
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
  constructor(private readonly redis: RedisClient) {}

  async getRoom(roomId: string): Promise<RoomMetadata> {
    const raw = await this.redis.get(`room:${roomId}`);
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

  async assertRoomCapacity(roomId: string, participantId: string): Promise<void> {
    const participantsKey = this.participantsKey(roomId);
    const exists = await this.redis.hExists(participantsKey, participantId);
    if (exists) {
      return;
    }

    const count = await this.redis.hLen(participantsKey);
    if (count >= MAX_ROOM_PARTICIPANTS) {
      throw new SignalingError("ROOM_FULL", "Room is full");
    }
  }

  async touchActiveRoom(room: RoomMetadata): Promise<RoomMetadata> {
    const activeRoom = roomMetadataSchema.parse({
      ...room,
      expiresAt: null,
      status: "active",
    });

    await this.redis.set(this.roomKey(activeRoom.roomId), JSON.stringify(activeRoom));
    await this.redis.set(this.slugKey(activeRoom.slug), activeRoom.roomId);
    await this.redis.persist(this.roomKey(activeRoom.roomId));
    await this.redis.persist(this.slugKey(activeRoom.slug));
    await this.redis.persist(this.chatKey(activeRoom.roomId));

    return activeRoom;
  }

  async saveParticipant(roomId: string, participant: Participant): Promise<void> {
    const key = this.participantsKey(roomId);

    await this.redis.hSet(key, participant.participantId, JSON.stringify(participantSchema.parse(participant)));
  }

  async removeParticipant(roomId: string, participantId: string): Promise<void> {
    await this.redis.hDel(this.participantsKey(roomId), participantId);
  }

  async appendChatMessage(roomId: string, message: ChatMessage): Promise<ChatMessage> {
    const parsed = chatMessageSchema.parse(message);
    const key = this.chatKey(roomId);

    await this.redis.rPush(key, JSON.stringify(parsed));
    await this.redis.lTrim(key, -CHAT_HISTORY_LIMIT, -1);

    return parsed;
  }

  async listChatMessages(roomId: string): Promise<ChatMessage[]> {
    const rawMessages = await this.redis.lRange(this.chatKey(roomId), 0, -1);
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
    });

    await this.redis.set(this.roomKey(emptyRoom.roomId), JSON.stringify(emptyRoom));
    await this.redis.set(this.slugKey(emptyRoom.slug), emptyRoom.roomId);
    await this.redis.expire(this.roomKey(emptyRoom.roomId), EMPTY_ROOM_TTL_SECONDS);
    await this.redis.expire(this.slugKey(emptyRoom.slug), EMPTY_ROOM_TTL_SECONDS);
    await this.redis.expire(this.chatKey(emptyRoom.roomId), EMPTY_ROOM_TTL_SECONDS);
    await this.redis.del(this.participantsKey(emptyRoom.roomId));

    return emptyRoom;
  }

  async closeRoom(room: RoomMetadata): Promise<RoomMetadata> {
    const expiresAt = new Date(Date.now() + EMPTY_ROOM_TTL_SECONDS * 1000).toISOString();
    const closedRoom = roomMetadataSchema.parse({
      ...room,
      expiresAt,
      status: "closed",
    });

    await this.redis.set(this.roomKey(closedRoom.roomId), JSON.stringify(closedRoom));
    await this.redis.set(this.slugKey(closedRoom.slug), closedRoom.roomId);
    await this.redis.expire(this.roomKey(closedRoom.roomId), EMPTY_ROOM_TTL_SECONDS);
    await this.redis.expire(this.slugKey(closedRoom.slug), EMPTY_ROOM_TTL_SECONDS);
    await this.redis.expire(this.chatKey(closedRoom.roomId), EMPTY_ROOM_TTL_SECONDS);
    await this.redis.del(this.participantsKey(closedRoom.roomId));

    return closedRoom;
  }

  private roomKey(roomId: string): string {
    return `room:${roomId}`;
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
}
