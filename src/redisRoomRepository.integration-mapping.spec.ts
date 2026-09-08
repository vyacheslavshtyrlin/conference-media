import type { RoomMetadata } from "#contracts";
import { describe, expect, it } from "vitest";
import { RedisRoomRepository, type RedisClient } from "./redisRoomRepository.js";

const room: RoomMetadata = {
  roomId: "room_1",
  slug: "room-one",
  mediaNodeId: "local",
  signalingUrl: "ws://127.0.0.1/ws",
  createdAt: "2026-07-21T12:00:00.000Z",
  expiresAt: null,
  status: "active",
  closedReason: null,
  accessPolicy: "approval",
  recordingRequested: false,
};

class FakeRedis implements RedisClient {
  readonly values = new Map<string, string>();
  readonly persisted: string[] = [];
  readonly expired: string[] = [];

  async get(key: string) { return this.values.get(key) ?? null; }
  async set(key: string, value: string) { this.values.set(key, value); return "OK"; }
  async hExists() { return false; }
  async hGet() { return null; }
  async hLen() { return 0; }
  async hSet() { return 0; }
  async hDel() { return 0; }
  async rPush() { return 0; }
  async lRange() { return []; }
  async lTrim() { return "OK"; }
  async expire(key: string) { this.expired.push(key); return true; }
  async persist(key: string) { this.persisted.push(key); return true; }
  async del() { return 0; }
  async eval() { return null; }
}

function seedIntegrationOwner(redis: FakeRedis) {
  redis.values.set(
    "integration_room_owner:room_1",
    JSON.stringify({
      roomId: "room_1",
      integrationId: "crm-alpha",
      externalRoomHash: "external_hash",
      requestHash: "request_hash",
      recordingId: "rec_1",
      createdAt: room.createdAt,
    }),
  );
}

describe("RedisRoomRepository integration mappings", () => {
  it("keeps integration mappings alive while the room is live and expires them with empty or closed room metadata", async () => {
    const redis = new FakeRedis();
    seedIntegrationOwner(redis);
    const repository = new RedisRoomRepository(redis);

    await repository.touchActiveRoom(room);
    expect(redis.persisted).toEqual(expect.arrayContaining([
      "integration_room_owner:room_1",
      "integration_room:crm-alpha:external_hash",
      "integration_recording:rec_1",
      "recording_activation:rec_1",
      "recording_finalizing:rec_1",
    ]));

    await repository.markRoomEmpty(room);
    await repository.closeRoom(room);
    expect(redis.expired).toEqual(expect.arrayContaining([
      "integration_room_owner:room_1",
      "integration_room:crm-alpha:external_hash",
      "integration_recording:rec_1",
      "recording_activation:rec_1",
      "recording_finalizing:rec_1",
    ]));
  });

  it("does not touch integration mappings for a non-integration room", async () => {
    const redis = new FakeRedis();
    const repository = new RedisRoomRepository(redis);

    await repository.touchActiveRoom(room);
    await repository.markRoomEmpty(room);

    expect(redis.persisted).not.toContain("integration_room_owner:room_1");
    expect(redis.expired).not.toContain("integration_room_owner:room_1");
  });

  it("keeps a legacy integration owner record without an external hash alive", async () => {
    const redis = new FakeRedis();
    redis.values.set(
      "integration_room_owner:room_1",
      JSON.stringify({ roomId: "room_1", integrationId: "crm-alpha", requestHash: "request_hash", createdAt: room.createdAt }),
    );
    const repository = new RedisRoomRepository(redis);

    await repository.touchActiveRoom(room);

    expect(redis.persisted).toContain("integration_room_owner:room_1");
    expect(redis.persisted).not.toContain("integration_room:crm-alpha:external_hash");
  });
});
