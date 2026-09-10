import type { RoomMetadata } from "#contracts";
import { describe, expect, it, vi } from "vitest";

import { RedisRoomRepository, type RedisClient } from "./redisRoomRepository.js";

class SessionRedis {
  readonly strings = new Map<string, string>();
  readonly hashes = new Map<string, Map<string, string>>();
  readonly sets = new Map<string, Set<string>>();
  readonly persisted: string[] = [];
  readonly expirations = new Map<string, number>();
  async get(key: string) { return this.strings.get(key) ?? null; }
  async set(key: string, value: string) { this.strings.set(key, value); return "OK"; }
  async hExists(key: string, field: string) { return this.hashes.get(key)?.has(field) ?? false; }
  async hGet(key: string, field: string) { return this.hashes.get(key)?.get(field) ?? null; }
  async hLen(key: string) { return this.hashes.get(key)?.size ?? 0; }
  async hSet(key: string, field: string, value: string) { const hash = this.hashes.get(key) ?? new Map(); this.hashes.set(key, hash); hash.set(field, value); return 1; }
  async hDel(key: string, field: string) { return this.hashes.get(key)?.delete(field) ? 1 : 0; }
  async rPush() { return 1; }
  async lRange() { return []; }
  async lTrim() { return "OK"; }
  async expire(key: string, seconds: number) { this.expirations.set(key, seconds * 1_000); return true; }
  async pExpire(key: string, milliseconds: number) { this.expirations.set(key, milliseconds); return true; }
  async persist(key: string) { this.persisted.push(key); this.expirations.delete(key); return true; }
  async del(keys: string | string[]) { for (const key of Array.isArray(keys) ? keys : [keys]) { this.strings.delete(key); this.hashes.delete(key); } return 1; }
  async sAdd(key: string, member: string) { const set = this.sets.get(key) ?? new Set(); this.sets.set(key, set); const before = set.size; set.add(member); return set.size - before; }
  async sMembers(key: string) { return [...(this.sets.get(key) ?? [])]; }
  async eval(_script: string, options: { keys: string[]; arguments: string[] }) {
    if (options.keys.length === 4 && options.keys[0]?.includes(":session:")) {
      const [sessionKey, indexKey, idsKey, legacyKey] = options.keys as [string, string, string, string];
      const [sessionJson, indexJson, participantId, persist, fallbackTtl] = options.arguments;
      const sessionTtl = this.expirations.get(sessionKey);
      const indexTtl = this.expirations.get(indexKey);
      this.strings.set(sessionKey, sessionJson!);
      this.strings.set(indexKey, indexJson!);
      await this.sAdd(idsKey, participantId!);
      await this.hDel(legacyKey, participantId!);
      if (persist === "1") {
        await this.persist(sessionKey);
        await this.persist(indexKey);
      } else {
        const convergedTtl = Number(fallbackTtl) > 0
          ? Number(fallbackTtl)
          : Math.min(...[sessionTtl, indexTtl].filter((value): value is number => value !== undefined));
        this.expirations.set(sessionKey, convergedTtl);
        this.expirations.set(indexKey, convergedTtl);
      }
      return 1;
    }
    if (options.keys.length === 4) {
      const [participantsKey, sessionKey, legacyKey, idsKey] = options.keys as [string, string, string, string];
      const [participantId, indexPrefix, roomId, expiresAt, ttl] = options.arguments;
      await this.hDel(participantsKey, participantId!);
      const raw = this.strings.get(sessionKey) ?? await this.hGet(legacyKey, participantId!);
      if (!raw) return 0;
      const session = JSON.parse(raw) as typeof legacySession & { state?: string; expiresAt?: string | null };
      session.state = "reconnectable";
      session.expiresAt = expiresAt!;
      this.strings.set(sessionKey, JSON.stringify(session));
      this.strings.set(`${indexPrefix}${session.tokenHash}`, JSON.stringify({ roomId, participantId }));
      this.expirations.set(sessionKey, Number(ttl));
      this.expirations.set(`${indexPrefix}${session.tokenHash}`, Number(ttl));
      await this.sAdd(idsKey, participantId!);
      await this.hDel(legacyKey, participantId!);
      return 1;
    }
    if (options.keys.length === 2) {
      const [idsKey, legacyKey] = options.keys;
      const [sessionPrefix, indexPrefix, roomId, expiresAt, ttl] = options.arguments;
      const participantIds = new Set(await this.sMembers(idsKey!));
      for (const participantId of this.hashes.get(legacyKey!)?.keys() ?? []) participantIds.add(participantId);
      for (const participantId of participantIds) {
        const sessionKey = `${sessionPrefix}${participantId}`;
        const raw = this.strings.get(sessionKey) ?? await this.hGet(legacyKey!, participantId);
        if (!raw) continue;
        const session = JSON.parse(raw) as typeof legacySession & { state: string; expiresAt: string | null };
        session.state = "reconnectable";
        session.expiresAt = expiresAt!;
        this.strings.set(sessionKey, JSON.stringify(session));
        this.strings.set(`${indexPrefix}${session.tokenHash}`, JSON.stringify({ roomId, participantId }));
        this.expirations.set(sessionKey, Number(ttl));
        this.expirations.set(`${indexPrefix}${session.tokenHash}`, Number(ttl));
        await this.sAdd(idsKey!, participantId);
      }
      this.hashes.delete(legacyKey!);
      this.expirations.set(idsKey!, Number(ttl));
      return 1;
    }
    return 0;
  }
}

const room: RoomMetadata = {
  roomId: "room_1", slug: "room-one", mediaNodeId: "local", signalingUrl: "ws://localhost/ws",
  createdAt: "2026-01-01T00:00:00.000Z", expiresAt: null, status: "active", closedReason: null,
  accessPolicy: "link", recordingRequested: false,
};
const legacySession = {
  participantId: "peer_1", displayName: "Alice", isCreator: false, tokenHash: "hash_1",
  createdAt: "2026-01-01T00:00:00.000Z",
};

describe("RedisRoomRepository session lifecycle", () => {
  it("canonicalizes a legacy session and persists the live session and token index", async () => {
    const redis = new SessionRedis();
    await redis.hSet("room:room_1:sessions", "peer_1", JSON.stringify(legacySession));
    const repository = new RedisRoomRepository(redis as RedisClient);

    await repository.markRoomSessionLive("room_1", "peer_1");

    expect(JSON.parse(redis.strings.get("room:room_1:session:peer_1")!)).toMatchObject({ state: "live", expiresAt: null });
    expect(redis.strings.get("room_session_token:hash_1")).toBe(JSON.stringify({ roomId: "room_1", participantId: "peer_1" }));
    expect(redis.sets.get("room:room_1:session_ids")).toEqual(new Set(["peer_1"]));
    expect(redis.persisted).toEqual(expect.arrayContaining(["room:room_1:session:peer_1", "room_session_token:hash_1"]));
    expect(redis.hashes.get("room:room_1:sessions")?.has("peer_1")).toBe(false);
  });

  it("atomically removes a participant and makes a legacy-only session reconnectable", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-01-01T00:00:00.000Z"));
    const redis = new SessionRedis();
    await redis.hSet("room:room_1:participants", "peer_1", "live");
    await redis.hSet("room:room_1:sessions", "peer_1", JSON.stringify(legacySession));
    const repository = new RedisRoomRepository(redis as RedisClient);

    await repository.removeParticipant("room_1", "peer_1");

    expect(await redis.hGet("room:room_1:participants", "peer_1")).toBeNull();
    expect(await redis.hGet("room:room_1:sessions", "peer_1")).toBeNull();
    expect(JSON.parse(redis.strings.get("room:room_1:session:peer_1")!)).toMatchObject({
      state: "reconnectable",
      expiresAt: "2026-01-01T00:15:00.000Z",
    });
    expect(redis.expirations.get("room:room_1:session:peer_1")).toBe(redis.expirations.get("room_session_token:hash_1"));
    vi.useRealTimers();
  });

  it("migrates legacy-only sessions during the empty-room lifecycle", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-01-01T00:00:00.000Z"));
    const redis = new SessionRedis();
    redis.strings.set("room:room_1", JSON.stringify(room));
    await redis.hSet("room:room_1:sessions", "peer_1", JSON.stringify(legacySession));
    const repository = new RedisRoomRepository(redis as RedisClient);

    const empty = await repository.markRoomEmpty(room);

    expect(redis.hashes.has("room:room_1:sessions")).toBe(false);
    expect(redis.sets.get("room:room_1:session_ids")).toEqual(new Set(["peer_1"]));
    expect(JSON.parse(redis.strings.get("room:room_1:session:peer_1")!)).toMatchObject({
      state: "reconnectable",
      expiresAt: empty.expiresAt,
    });
    vi.useRealTimers();
  });

  it("moves live sessions and their indexes to the same reconnectable retention deadline", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-01-01T00:00:00.000Z"));
    const redis = new SessionRedis();
    redis.strings.set("room:room_1", JSON.stringify(room));
    redis.sets.set("room:room_1:session_ids", new Set(["peer_1"]));
    redis.strings.set("room:room_1:session:peer_1", JSON.stringify({ ...legacySession, state: "live", expiresAt: null }));
    redis.strings.set("room_session_token:hash_1", JSON.stringify({ roomId: "room_1", participantId: "peer_1" }));
    const repository = new RedisRoomRepository(redis as RedisClient);

    const empty = await repository.markRoomEmpty(room);

    expect(JSON.parse(redis.strings.get("room:room_1:session:peer_1")!)).toMatchObject({ state: "reconnectable", expiresAt: empty.expiresAt });
    expect(redis.expirations.get("room:room_1:session:peer_1")).toBe(redis.expirations.get("room_session_token:hash_1"));
    vi.useRealTimers();
  });

  it("preserves both session and token-index TTL while changing creator status", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-01-01T00:00:00.000Z"));
    const redis = new SessionRedis();
    const sessionKey = "room:room_1:session:peer_1";
    const indexKey = "room_session_token:hash_1";
    redis.strings.set(sessionKey, JSON.stringify({ ...legacySession, state: "reconnectable", expiresAt: "2026-01-01T00:02:03.000Z" }));
    redis.strings.set(indexKey, JSON.stringify({ roomId: "room_1", participantId: "peer_1" }));
    redis.expirations.set(sessionKey, 123_000);
    redis.expirations.set(indexKey, 123_000);
    const repository = new RedisRoomRepository(redis as RedisClient);

    await repository.setRoomSessionCreator("room_1", "peer_1", true);

    expect(JSON.parse(redis.strings.get(sessionKey)!)).toMatchObject({ isCreator: true, state: "reconnectable" });
    expect(redis.expirations.get(sessionKey)).toBe(123_000);
    expect(redis.expirations.get(indexKey)).toBe(123_000);
    vi.useRealTimers();
  });
});
