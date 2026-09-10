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
  pExpire?(key: string, milliseconds: number): Promise<boolean | number>;
  sAdd?(key: string, member: string): Promise<number>;
  sMembers?(key: string): Promise<string[]>;
  del(keys: string | string[]): Promise<number>;
  eval(
    script: string,
    options: { keys: string[]; arguments: string[] },
  ): Promise<unknown>;
};

export type RecordingV2FinalizationFence = {
  protocolVersion: "recording-capture.v2";
  captureGeneration: number;
  activatedAt: string;
  captureStartedAt: string;
  terminalObservedAt: string;
  terminalTimelineEndMs: number;
};

export type RecordingRecoveryContext = {
  roomId: string;
  integrationId: string;
  requestedAt: string;
};

export type RecordingV2Activation = {
  captureGeneration: number;
  activatedAt: string;
};

type RoomSessionRecord = {
  participantId: string;
  displayName: string | null;
  isCreator: boolean;
  tokenHash: string;
  createdAt: string;
  state: "pending" | "live" | "reconnectable";
  expiresAt: string | null;
  identity?: unknown;
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
    await this.recordRedis("persist_room_session_ids", () => this.redis.persist(this.roomSessionIdsKey(activeRoom.roomId)));
    await this.syncIntegrationMappings(activeRoom.roomId, "persist");

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

  async markRoomSessionLive(roomId: string, participantId: string): Promise<void> {
    const session = await this.getRoomSession(roomId, participantId);
    if (!session) return;
    await this.saveCanonicalRoomSession(roomId, {
      ...session,
      state: "live",
      expiresAt: null,
    }, "persist");
  }

  async setRoomSessionCreator(roomId: string, participantId: string, isCreator: boolean): Promise<void> {
    const session = await this.getRoomSession(roomId, participantId);
    if (!session) return;
    await this.saveCanonicalRoomSession(roomId, { ...session, isCreator }, session.state === "live" ? "persist" : "keepTtl");
  }

  async removeParticipant(roomId: string, participantId: string): Promise<void> {
    const expiresAt = new Date(Date.now() + EMPTY_ROOM_TTL_SECONDS * 1_000).toISOString();
    const ttlMs = Math.max(1, Date.parse(expiresAt) - Date.now());
    await this.recordRedis("remove_participant", () => this.redis.eval(REMOVE_PARTICIPANT_SCRIPT, {
      keys: [
        this.participantsKey(roomId),
        this.roomSessionKey(roomId, participantId),
        this.legacyRoomSessionsKey(roomId),
        this.roomSessionIdsKey(roomId),
      ],
      arguments: [participantId, `room_session_token:`, roomId, expiresAt, String(ttlMs)],
    }));
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
    await this.transitionRoomSessionsToReconnectable(emptyRoom.roomId, expiresAt);
    await this.syncIntegrationMappings(emptyRoom.roomId, "expire");
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
    await this.transitionRoomSessionsToReconnectable(closedRoom.roomId, expiresAt);
    await this.syncIntegrationMappings(closedRoom.roomId, "expire");
    await this.recordRedis("delete_room_participants", () => this.redis.del(this.participantsKey(closedRoom.roomId)));

    return closedRoom;
  }

  async beginV2RecordingFinalization(
    recordingId: string,
    captureGeneration: number,
  ): Promise<RecordingV2FinalizationFence> {
    const finalizingKey = `recording_finalizing:${recordingId}`;
    const existing = parseRecordingV2FinalizationFence(
      await this.recordRedis("get_recording_v2_finalization", () => this.redis.get(finalizingKey)),
    );
    if (existing) {
      if (existing.captureGeneration !== captureGeneration) {
        throw new Error("Recording finalization generation does not match");
      }
      return existing;
    }

    const activationKey = `recording_activation:${recordingId}`;
    const activationRaw = await this.recordRedis(
      "get_recording_v2_activation",
      () => this.redis.get(activationKey),
    );
    const activation = parseRecordingV2Activation(activationRaw);
    if (!activation || activation.captureGeneration !== captureGeneration) {
      throw new Error("Recording capture activation does not match");
    }

    const raw = await this.recordRedis("begin_recording_v2_finalization", () =>
      this.redis.eval(BEGIN_RECORDING_V2_FINALIZATION_SCRIPT, {
        keys: [activationKey, finalizingKey],
        arguments: [
          String(captureGeneration),
          activation.activatedAt,
          String(Date.parse(activation.activatedAt)),
          String(EMPTY_ROOM_TTL_SECONDS * 1_000),
        ],
      }),
    );
    const fence = parseRecordingV2FinalizationFence(typeof raw === "string" ? raw : null);
    if (!fence || fence.captureGeneration !== captureGeneration ||
      fence.activatedAt !== activation.activatedAt) {
      throw new Error("Recording finalization fence did not converge");
    }
    return fence;
  }

  async getRecordingRecoveryContext(recordingId: string): Promise<RecordingRecoveryContext | undefined> {
    const raw = await this.recordRedis(
      "get_recording_recovery_context",
      () => this.redis.get(`integration_recording:${recordingId}`),
    );
    if (!raw) return undefined;
    try {
      const owner = JSON.parse(raw) as Record<string, unknown>;
      if (owner.recordingId !== recordingId || typeof owner.roomId !== "string" ||
        typeof owner.integrationId !== "string" || typeof owner.createdAt !== "string" ||
        Number.isNaN(Date.parse(owner.createdAt))) {
        return undefined;
      }
      return {
        roomId: owner.roomId,
        integrationId: owner.integrationId,
        requestedAt: owner.createdAt,
      };
    } catch {
      return undefined;
    }
  }

  async getV2RecordingFinalization(recordingId: string): Promise<RecordingV2FinalizationFence | undefined> {
    return parseRecordingV2FinalizationFence(await this.recordRedis(
      "get_recording_v2_finalization",
      () => this.redis.get(`recording_finalizing:${recordingId}`),
    ));
  }

  async getV2RecordingActivation(recordingId: string): Promise<RecordingV2Activation | undefined> {
    return parseRecordingV2Activation(await this.recordRedis(
      "get_recording_v2_activation",
      () => this.redis.get(`recording_activation:${recordingId}`),
    ));
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

  private legacyRoomSessionsKey(roomId: string): string {
    return `room:${roomId}:sessions`;
  }

  private roomSessionKey(roomId: string, participantId: string): string {
    return `room:${roomId}:session:${participantId}`;
  }

  private roomSessionIdsKey(roomId: string): string {
    return `room:${roomId}:session_ids`;
  }

  private roomSessionTokenIndexKey(tokenHash: string): string {
    return `room_session_token:${tokenHash}`;
  }

  private async getRoomSession(roomId: string, participantId: string): Promise<RoomSessionRecord | undefined> {
    const canonical = await this.recordRedis("get_room_session", () =>
      this.redis.get(this.roomSessionKey(roomId, participantId)),
    );
    const legacy = canonical ?? await this.recordRedis("get_legacy_room_session", () =>
      this.redis.hGet(this.legacyRoomSessionsKey(roomId), participantId),
    );
    if (!legacy) return undefined;
    try {
      const value = JSON.parse(legacy) as unknown;
      if (!isRoomSessionRecord(value)) return undefined;
      return {
        ...value,
        state: value.state ?? "reconnectable",
        expiresAt: value.expiresAt ?? null,
      };
    } catch {
      return undefined;
    }
  }

  private async saveCanonicalRoomSession(
    roomId: string,
    session: RoomSessionRecord,
    ttl: "persist" | "keepTtl",
  ): Promise<void> {
    const sessionKey = this.roomSessionKey(roomId, session.participantId);
    const indexKey = this.roomSessionTokenIndexKey(session.tokenHash);
    const fallbackTtlMs = session.expiresAt
      ? Math.max(1, Date.parse(session.expiresAt) - Date.now())
      : 0;
    await this.recordRedis("save_room_session", () => this.redis.eval(SAVE_ROOM_SESSION_SCRIPT, {
      keys: [sessionKey, indexKey, this.roomSessionIdsKey(roomId), this.legacyRoomSessionsKey(roomId)],
      arguments: [
        JSON.stringify(session),
        JSON.stringify({ roomId, participantId: session.participantId }),
        session.participantId,
        ttl === "persist" ? "1" : "0",
        String(fallbackTtlMs),
      ],
    }));
  }

  private async transitionRoomSessionsToReconnectable(roomId: string, expiresAt: string): Promise<void> {
    const ttlMs = Math.max(1, Date.parse(expiresAt) - Date.now());
    await this.recordRedis("expire_room_sessions", () => this.redis.eval(TRANSITION_ROOM_SESSIONS_SCRIPT, {
      keys: [this.roomSessionIdsKey(roomId), this.legacyRoomSessionsKey(roomId)],
      arguments: [
        `room:${roomId}:session:`,
        "room_session_token:",
        roomId,
        expiresAt,
        String(ttlMs),
      ],
    }));
  }

  private async syncIntegrationMappings(roomId: string, action: "persist" | "expire"): Promise<void> {
    const ownerKey = `integration_room_owner:${roomId}`;
    const rawOwner = await this.recordRedis("get_integration_room_owner", () => this.redis.get(ownerKey));
    if (!rawOwner) return;

    let owner: { integrationId?: unknown; externalRoomHash?: unknown; recordingId?: unknown };
    try {
      owner = JSON.parse(rawOwner) as { integrationId?: unknown; externalRoomHash?: unknown; recordingId?: unknown };
    } catch {
      return;
    }
    if (action === "persist") {
      await this.recordRedis("persist_integration_room_owner", () => this.redis.persist(ownerKey));
      if (typeof owner.recordingId === "string" && owner.recordingId.length > 0) {
        await this.syncRecordingControlMappings(owner.recordingId, "persist");
      }
      if (typeof owner.integrationId !== "string" || typeof owner.externalRoomHash !== "string") return;
      const externalKey = `integration_room:${owner.integrationId}:${owner.externalRoomHash}`;
      await this.recordRedis("persist_integration_room_mapping", () => this.redis.persist(externalKey));
      return;
    }
    await this.recordRedis("expire_integration_room_owner", () => this.redis.expire(ownerKey, EMPTY_ROOM_TTL_SECONDS));
    if (typeof owner.recordingId === "string" && owner.recordingId.length > 0) {
      await this.syncRecordingControlMappings(owner.recordingId, "expire");
    }
    if (typeof owner.integrationId !== "string" || typeof owner.externalRoomHash !== "string") return;
    const externalKey = `integration_room:${owner.integrationId}:${owner.externalRoomHash}`;
    await this.recordRedis("expire_integration_room_mapping", () => this.redis.expire(externalKey, EMPTY_ROOM_TTL_SECONDS));
  }

  private async syncRecordingControlMappings(recordingId: string, action: "persist" | "expire"): Promise<void> {
    const keys = [
      `integration_recording:${recordingId}`,
      `recording_activation:${recordingId}`,
      `recording_finalizing:${recordingId}`,
    ];

    for (const key of keys) {
      if (action === "persist") {
        await this.recordRedis("persist_recording_control_mapping", () => this.redis.persist(key));
      } else {
        await this.recordRedis("expire_recording_control_mapping", () => this.redis.expire(key, EMPTY_ROOM_TTL_SECONDS));
      }
    }
  }
}

const SAVE_ROOM_SESSION_SCRIPT = `
local session_ttl = redis.call("PTTL", KEYS[1])
local index_ttl = redis.call("PTTL", KEYS[2])
redis.call("SET", KEYS[1], ARGV[1])
redis.call("SET", KEYS[2], ARGV[2])
redis.call("SADD", KEYS[3], ARGV[3])
redis.call("HDEL", KEYS[4], ARGV[3])
if ARGV[4] == "1" then
  redis.call("PERSIST", KEYS[1])
  redis.call("PERSIST", KEYS[2])
else
  local fallback_ttl = tonumber(ARGV[5])
  local ttl = fallback_ttl
  if ttl <= 0 then
    if session_ttl > 0 and index_ttl > 0 then ttl = math.min(session_ttl, index_ttl)
    elseif session_ttl > 0 then ttl = session_ttl
    elseif index_ttl > 0 then ttl = index_ttl end
  end
  if ttl > 0 then
    redis.call("PEXPIRE", KEYS[1], ttl)
    redis.call("PEXPIRE", KEYS[2], ttl)
  end
end
return 1
`;

const TRANSITION_ROOM_SESSIONS_SCRIPT = `
local ids = redis.call("SMEMBERS", KEYS[1])
local legacy_ids = redis.call("HKEYS", KEYS[2])
for _, participant_id in ipairs(legacy_ids) do
  redis.call("SADD", KEYS[1], participant_id)
  table.insert(ids, participant_id)
end
local ttl_ms = tonumber(ARGV[5])
for _, participant_id in ipairs(ids) do
  local session_key = ARGV[1] .. participant_id
  local raw = redis.call("GET", session_key)
  if not raw then raw = redis.call("HGET", KEYS[2], participant_id) end
  if raw then
    local session = cjson.decode(raw)
    session["state"] = "reconnectable"
    session["expiresAt"] = ARGV[4]
    redis.call("SET", session_key, cjson.encode(session), "PX", ttl_ms)
    local index_key = ARGV[2] .. session["tokenHash"]
    redis.call("SET", index_key, cjson.encode({ roomId = ARGV[3], participantId = participant_id }), "PX", ttl_ms)
    redis.call("HDEL", KEYS[2], participant_id)
  end
end
redis.call("DEL", KEYS[2])
redis.call("PEXPIRE", KEYS[1], ttl_ms)
return #ids
`;

const REMOVE_PARTICIPANT_SCRIPT = `
redis.call("HDEL", KEYS[1], ARGV[1])
local raw = redis.call("GET", KEYS[2])
if not raw then raw = redis.call("HGET", KEYS[3], ARGV[1]) end
if not raw then return 0 end
local session = cjson.decode(raw)
session["state"] = "reconnectable"
session["expiresAt"] = ARGV[4]
local ttl_ms = tonumber(ARGV[5])
redis.call("SET", KEYS[2], cjson.encode(session), "PX", ttl_ms)
local index_key = ARGV[2] .. session["tokenHash"]
redis.call("SET", index_key, cjson.encode({ roomId = ARGV[3], participantId = ARGV[1] }), "PX", ttl_ms)
redis.call("SADD", KEYS[4], ARGV[1])
redis.call("HDEL", KEYS[3], ARGV[1])
return 1
`;

const BEGIN_RECORDING_V2_FINALIZATION_SCRIPT = `
local existing = redis.call("GET", KEYS[2])
if existing then return existing end

local activation_raw = redis.call("GET", KEYS[1])
if not activation_raw then return "missing" end
local activation = cjson.decode(activation_raw)
local generation = tonumber(ARGV[1])
if activation["protocolVersion"] ~= "recording-capture.v2" or
  activation["captureGeneration"] ~= generation or
  activation["activatedAt"] ~= ARGV[2] or
  activation["captureStartedAt"] ~= ARGV[2] then
  return "conflict"
end

local redis_time = redis.call("TIME")
local observed_ms = tonumber(redis_time[1]) * 1000 + math.floor(tonumber(redis_time[2]) / 1000)
local origin_ms = tonumber(ARGV[3])
local timeline_end_ms = observed_ms - origin_ms
if timeline_end_ms < 0 then timeline_end_ms = 0 end

local days = math.floor(observed_ms / 86400000)
local day_ms = observed_ms - days * 86400000
local z = days + 719468
local era = math.floor(z / 146097)
local doe = z - era * 146097
local yoe = math.floor((doe - math.floor(doe / 1460) + math.floor(doe / 36524) - math.floor(doe / 146096)) / 365)
local year = yoe + era * 400
local doy = doe - (365 * yoe + math.floor(yoe / 4) - math.floor(yoe / 100))
local mp = math.floor((5 * doy + 2) / 153)
local day = doy - math.floor((153 * mp + 2) / 5) + 1
local month = mp + (mp < 10 and 3 or -9)
if month <= 2 then year = year + 1 end
local hour = math.floor(day_ms / 3600000)
local minute = math.floor((day_ms % 3600000) / 60000)
local second = math.floor((day_ms % 60000) / 1000)
local millisecond = day_ms % 1000
local terminal_observed_at = string.format(
  "%04d-%02d-%02dT%02d:%02d:%02d.%03dZ",
  year, month, day, hour, minute, second, millisecond
)

local marker = cjson.encode({
  protocolVersion = "recording-capture.v2",
  captureGeneration = generation,
  activatedAt = ARGV[2],
  captureStartedAt = ARGV[2],
  terminalObservedAt = terminal_observed_at,
  terminalTimelineEndMs = timeline_end_ms
})
redis.call("SET", KEYS[2], marker)
local ttl_ms = tonumber(ARGV[4])
local activation_ttl_ms = redis.call("PTTL", KEYS[1])
if activation_ttl_ms > 0 and activation_ttl_ms < ttl_ms then ttl_ms = activation_ttl_ms end
redis.call("PEXPIRE", KEYS[1], ttl_ms)
redis.call("PEXPIRE", KEYS[2], ttl_ms)
return marker
`;

function parseRecordingV2Activation(raw: string | null): RecordingV2Activation | undefined {
  if (!raw) return undefined;
  try {
    const value = JSON.parse(raw) as Record<string, unknown>;
    if (value.protocolVersion !== "recording-capture.v2" ||
      !Number.isSafeInteger(value.captureGeneration) || Number(value.captureGeneration) < 1 ||
      typeof value.activatedAt !== "string" || Number.isNaN(Date.parse(value.activatedAt)) ||
      value.captureStartedAt !== value.activatedAt) {
      return undefined;
    }
    return { captureGeneration: Number(value.captureGeneration), activatedAt: value.activatedAt };
  } catch {
    return undefined;
  }
}

function parseRecordingV2FinalizationFence(raw: string | null): RecordingV2FinalizationFence | undefined {
  if (!raw) return undefined;
  try {
    const value = JSON.parse(raw) as Record<string, unknown>;
    if (value.protocolVersion !== "recording-capture.v2" ||
      !Number.isSafeInteger(value.captureGeneration) || Number(value.captureGeneration) < 1 ||
      typeof value.activatedAt !== "string" || Number.isNaN(Date.parse(value.activatedAt)) ||
      value.captureStartedAt !== value.activatedAt ||
      typeof value.terminalObservedAt !== "string" || Number.isNaN(Date.parse(value.terminalObservedAt)) ||
      !Number.isSafeInteger(value.terminalTimelineEndMs) || Number(value.terminalTimelineEndMs) < 0 ||
      Date.parse(value.terminalObservedAt) - Date.parse(value.activatedAt) !== value.terminalTimelineEndMs) {
      return undefined;
    }
    return value as RecordingV2FinalizationFence;
  } catch {
    return undefined;
  }
}

function isRoomSessionRecord(value: unknown): value is {
  participantId: string;
  displayName: string | null;
  isCreator: boolean;
  tokenHash: string;
  createdAt: string;
  state?: "pending" | "live" | "reconnectable";
  expiresAt?: string | null;
  identity?: unknown;
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
    !Number.isNaN(Date.parse(record.createdAt)) &&
    (record.state === undefined || record.state === "pending" || record.state === "live" || record.state === "reconnectable") &&
    (record.expiresAt === undefined || record.expiresAt === null ||
      (typeof record.expiresAt === "string" && !Number.isNaN(Date.parse(record.expiresAt))))
  );
}
