import type { ChatMessage, Participant, RoomClosedReason, RoomMetadata } from "#contracts";
import { describe, expect, it } from "vitest";

import type { MediasoupService } from "./mediasoupService.js";
import type { RedisRoomRepository } from "./redisRoomRepository.js";
import { RoomManager } from "./roomManager.js";
import type { JoinTokenPayload } from "./token.js";

const room: RoomMetadata = {
  roomId: "room_1",
  slug: "room-one",
  mediaNodeId: "local",
  signalingUrl: "ws://localhost:4000/ws",
  createdAt: new Date(Date.now() - 1_000).toISOString(),
  expiresAt: null,
  status: "active",
  closedReason: null,
};

class FakeRepository {
  activeGetRoomCalls = 0;
  maxConcurrentGetRoomCalls = 0;
  activeTouches = 0;
  emptyMarks = 0;
  closedRooms = 0;
  closedReason: RoomClosedReason | null = null;
  failAppendChatMessage = false;
  failListChatMessages = false;
  readonly participants = new Map<string, Participant>();
  readonly chatMessages: ChatMessage[] = [];
  room: RoomMetadata = room;

  async getRoom(): Promise<RoomMetadata> {
    this.activeGetRoomCalls += 1;
    this.maxConcurrentGetRoomCalls = Math.max(this.maxConcurrentGetRoomCalls, this.activeGetRoomCalls);
    await new Promise((resolve) => setTimeout(resolve, 10));
    this.activeGetRoomCalls -= 1;
    return this.room;
  }

  async assertRoomCapacity(): Promise<void> {}

  async touchActiveRoom(roomMetadata: RoomMetadata): Promise<RoomMetadata> {
    this.activeTouches += 1;
    this.room = { ...roomMetadata, expiresAt: null, status: "active", closedReason: null };
    return this.room;
  }

  async saveParticipant(_roomId: string, participant: Participant): Promise<void> {
    this.participants.set(participant.participantId, participant);
  }

  async removeParticipant(_roomId: string, participantId: string): Promise<void> {
    this.participants.delete(participantId);
  }

  async appendChatMessage(_roomId: string, message: ChatMessage): Promise<ChatMessage> {
    if (this.failAppendChatMessage) {
      throw new Error("chat append failed");
    }
    this.chatMessages.push(message);
    return message;
  }

  async listChatMessages(): Promise<ChatMessage[]> {
    if (this.failListChatMessages) {
      throw new Error("chat history failed");
    }
    return this.chatMessages;
  }

  async markRoomEmpty(roomMetadata: RoomMetadata): Promise<RoomMetadata> {
    this.emptyMarks += 1;
    this.room = {
      ...roomMetadata,
      expiresAt: new Date(Date.now() + 15 * 60 * 1000).toISOString(),
      status: "active",
      closedReason: null,
    };
    return this.room;
  }

  async closeRoom(roomMetadata: RoomMetadata, reason: RoomClosedReason = "closed_by_creator"): Promise<RoomMetadata> {
    this.closedRooms += 1;
    this.closedReason = reason;
    this.room = {
      ...roomMetadata,
      expiresAt: new Date(Date.now() + 15 * 60 * 1000).toISOString(),
      status: "closed",
      closedReason: reason,
    };
    this.participants.clear();
    return this.room;
  }
}

class FakeMediasoupService {
  ensureRoomCalls = 0;
  closeRoomCalls = 0;
  closePeerCalls = 0;
  private roomDeadCallback?: (roomId: string, reason: RoomClosedReason) => void;

  setRoomDeadCallback(callback: (roomId: string, reason: RoomClosedReason) => void): void {
    this.roomDeadCallback = callback;
  }

  setActiveSpeakerChangedCallback(_callback: (roomId: string, participantId: string | null) => void): void {}

  async ensureRoom(): Promise<void> {
    this.ensureRoomCalls += 1;
  }

  closeRoom(): void {
    this.closeRoomCalls += 1;
  }

  closePeer(): string[] {
    this.closePeerCalls += 1;
    return [];
  }

  getActiveSpeakerParticipantId(): string | null {
    return null;
  }

  simulateWorkerDeath(roomId: string): void {
    this.roomDeadCallback?.(roomId, "media_server_error");
  }
}

function createPayload(participantId: string): JoinTokenPayload {
  return {
    roomId: room.roomId,
    participantId,
    displayName: participantId,
    isCreator: false,
  };
}

function createCreatorPayload(participantId: string): JoinTokenPayload {
  return {
    ...createPayload(participantId),
    isCreator: true,
  };
}

describe("RoomManager", () => {
  it("serializes concurrent room joins for one room", async () => {
    const repository = new FakeRepository();
    const mediasoup = new FakeMediasoupService();
    const manager = new RoomManager(
      repository as unknown as RedisRoomRepository,
      mediasoup as unknown as MediasoupService,
      60_000,
    );

    await Promise.all([
      manager.join(createPayload("peer_1")),
      manager.join(createPayload("peer_2")),
      manager.join(createPayload("peer_3")),
    ]);

    expect(repository.maxConcurrentGetRoomCalls).toBe(1);
    expect(repository.participants.size).toBe(3);
    expect(repository.chatMessages).toHaveLength(3);
    expect(repository.activeTouches).toBe(3);
    expect(mediasoup.ensureRoomCalls).toBe(3);
  });

  it("returns chat history in join result and records leave system logs", async () => {
    const repository = new FakeRepository();
    const mediasoup = new FakeMediasoupService();
    const manager = new RoomManager(
      repository as unknown as RedisRoomRepository,
      mediasoup as unknown as MediasoupService,
      60_000,
    );

    const joined = await manager.join(createPayload("peer_1"));
    const sent = await manager.sendChatMessage(room.roomId, "peer_1", " Всем привет ");
    const left = await manager.leave(room.roomId, "peer_1");

    expect(joined.chatMessages[0]).toMatchObject({
      kind: "system",
      text: "peer_1 подключился к конференции",
    });
    expect(sent).toMatchObject({
      kind: "user",
      participantId: "peer_1",
      authorName: "peer_1",
      text: "Всем привет",
    });
    expect(left.chatMessage).toMatchObject({
      kind: "system",
      text: "peer_1 покинул конференцию",
    });
    expect(repository.chatMessages.map((message) => message.kind)).toEqual(["system", "user", "system"]);
  });

  it("rolls back a new participant when chat history fails during join", async () => {
    const repository = new FakeRepository();
    repository.failListChatMessages = true;
    const mediasoup = new FakeMediasoupService();
    const manager = new RoomManager(
      repository as unknown as RedisRoomRepository,
      mediasoup as unknown as MediasoupService,
      60_000,
    );

    await expect(manager.join(createPayload("peer_1"))).rejects.toMatchObject({
      code: "MEDIASOUP_ERROR",
      message: "Room chat is temporarily unavailable. Please refresh and join again.",
    });

    expect(repository.participants.size).toBe(0);
    expect(repository.chatMessages).toEqual([]);
    expect(mediasoup.closePeerCalls).toBe(1);
    expect(mediasoup.closeRoomCalls).toBe(1);
    expect(() => manager.getParticipants(room.roomId)).toThrow("Runtime room not found");
  });

  it("does not write a false join log when chat append fails during join", async () => {
    const repository = new FakeRepository();
    repository.failAppendChatMessage = true;
    const mediasoup = new FakeMediasoupService();
    const manager = new RoomManager(
      repository as unknown as RedisRoomRepository,
      mediasoup as unknown as MediasoupService,
      60_000,
    );

    await expect(manager.join(createPayload("peer_1"))).rejects.toMatchObject({
      code: "MEDIASOUP_ERROR",
    });

    expect(repository.participants.size).toBe(0);
    expect(repository.chatMessages).toEqual([]);
    expect(mediasoup.closePeerCalls).toBe(1);
    expect(mediasoup.closeRoomCalls).toBe(1);
  });

  it("restores empty-room TTL metadata when join from an empty window fails on chat", async () => {
    const repository = new FakeRepository();
    repository.room = {
      ...room,
      expiresAt: new Date(Date.now() + 60_000).toISOString(),
      status: "active",
      closedReason: null,
    };
    repository.failListChatMessages = true;
    const mediasoup = new FakeMediasoupService();
    const manager = new RoomManager(
      repository as unknown as RedisRoomRepository,
      mediasoup as unknown as MediasoupService,
      60_000,
    );

    await expect(manager.join(createPayload("peer_1"))).rejects.toMatchObject({
      code: "MEDIASOUP_ERROR",
    });

    expect(repository.emptyMarks).toBe(1);
    expect(repository.room.expiresAt).toEqual(expect.any(String));
    expect(repository.participants.size).toBe(0);
  });

  it("marks an empty room with TTL metadata after the last participant leaves", async () => {
    const repository = new FakeRepository();
    const mediasoup = new FakeMediasoupService();
    const manager = new RoomManager(
      repository as unknown as RedisRoomRepository,
      mediasoup as unknown as MediasoupService,
      60_000,
    );

    await manager.join(createPayload("peer_1"));
    await manager.leave(room.roomId, "peer_1");

    expect(repository.emptyMarks).toBe(1);
    expect(repository.participants.size).toBe(0);
    expect(repository.room.expiresAt).toEqual(expect.any(String));
    expect(repository.room.status).toBe("active");
  });

  it("updates and persists participant connection quality", async () => {
    const repository = new FakeRepository();
    const mediasoup = new FakeMediasoupService();
    const manager = new RoomManager(
      repository as unknown as RedisRoomRepository,
      mediasoup as unknown as MediasoupService,
      60_000,
    );

    await manager.join(createPayload("peer_1"));
    const participant = await manager.setConnectionQuality(room.roomId, "peer_1", "poor");

    expect(participant.connectionQuality).toBe("poor");
    expect(repository.participants.get("peer_1")?.connectionQuality).toBe("poor");
    expect(manager.getParticipants(room.roomId)[0]?.connectionQuality).toBe("poor");
  });

  it("allows the creator participant to close the room", async () => {
    const repository = new FakeRepository();
    const mediasoup = new FakeMediasoupService();
    const manager = new RoomManager(
      repository as unknown as RedisRoomRepository,
      mediasoup as unknown as MediasoupService,
      60_000,
    );

    await manager.join(createCreatorPayload("peer_creator"));
    await manager.closeRoomByCreator(room.roomId, "peer_creator");

    expect(repository.closedRooms).toBe(1);
    expect(repository.room.status).toBe("closed");
    expect(repository.closedReason).toBe("closed_by_creator");
    expect(mediasoup.closeRoomCalls).toBe(1);
  });

  it("marks a room closed with media_server_error when the mediasoup room dies", async () => {
    const repository = new FakeRepository();
    const mediasoup = new FakeMediasoupService();
    const manager = new RoomManager(
      repository as unknown as RedisRoomRepository,
      mediasoup as unknown as MediasoupService,
      60_000,
    );
    const closedEvents: Array<{ roomId: string; reason: RoomClosedReason }> = [];
    manager.setRoomClosedCallback((roomId, reason) => {
      closedEvents.push({ roomId, reason });
    });

    await manager.join(createPayload("peer_1"));
    mediasoup.simulateWorkerDeath(room.roomId);
    await new Promise((resolve) => setTimeout(resolve, 0));

    expect(closedEvents).toEqual([{ roomId: room.roomId, reason: "media_server_error" }]);
    expect(repository.closedRooms).toBe(1);
    expect(repository.closedReason).toBe("media_server_error");
    expect(repository.room.status).toBe("closed");
    expect(repository.room.closedReason).toBe("media_server_error");
  });

  it("rejects room close from non-creator participants", async () => {
    const repository = new FakeRepository();
    const mediasoup = new FakeMediasoupService();
    const manager = new RoomManager(
      repository as unknown as RedisRoomRepository,
      mediasoup as unknown as MediasoupService,
      60_000,
    );

    await manager.join(createPayload("peer_guest"));

    await expect(manager.closeRoomByCreator(room.roomId, "peer_guest")).rejects.toMatchObject({
      code: "INVALID_TOKEN",
    });
    expect(repository.closedRooms).toBe(0);
  });
});
