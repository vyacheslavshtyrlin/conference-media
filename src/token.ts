import { createHash, createHmac, timingSafeEqual } from "node:crypto";
import { z } from "zod";

import { SignalingError } from "./errors.js";

const joinTokenPayloadSchema = z.object({
  roomId: z.string().min(1),
  participantId: z.string().min(1),
  displayName: z.string().trim().min(1).max(80),
  isCreator: z.boolean().default(false),
  exp: z.number().int().positive().optional(),
}).strict();

export type JoinTokenPayload = z.infer<typeof joinTokenPayloadSchema>;

const recordingBotAdmissionPayloadSchema = z.object({
  sub: z.literal("recording-bot"),
  aud: z.literal("conference-recording-bot-signaling"),
  iss: z.string().min(1),
  roomId: z.string().min(1),
  recordingId: z.string().min(1),
  mediaNodeId: z.string().min(1),
  jti: z.string().min(1),
  iat: z.number().int().nonnegative(),
  exp: z.number().int().positive(),
}).strict();

export type RecordingBotAdmissionPayload = z.infer<typeof recordingBotAdmissionPayloadSchema>;

export type RecordingBotAdmissionRedis = {
  eval(script: string, options: { keys: string[]; arguments: string[] }): Promise<unknown>;
};

const consumeRecordingBotAdmissionScript = `
if redis.call('EXISTS', KEYS[2]) == 1 then
  redis.call('DEL', KEYS[1])
  return -1
end
local raw = redis.call('GET', KEYS[1])
if not raw then return 0 end
local marker = cjson.decode(raw)
if marker.roomId ~= ARGV[1] or marker.recordingId ~= ARGV[2] or marker.mediaNodeId ~= ARGV[3] then return 0 end
if marker.consumedAt and marker.consumedAt ~= cjson.null then return 0 end
marker.consumedAt = ARGV[4]
redis.call('SET', KEYS[1], cjson.encode(marker), 'KEEPTTL')
return 1
`;

function base64UrlDecode(value: string): Buffer {
  return Buffer.from(value, "base64url");
}

function sign(input: string, secret: string): string {
  return createHmac("sha256", secret).update(input).digest("base64url");
}

export function verifyJoinToken(token: string | undefined, secret: string): JoinTokenPayload {
  if (!token) {
    throw new SignalingError("INVALID_TOKEN", "Missing join token");
  }

  const parts = token.split(".");
  if (parts.length !== 3) {
    throw new SignalingError("INVALID_TOKEN", "Invalid join token format");
  }

  const [encodedHeader, encodedPayload, signature] = parts;
  if (!encodedHeader || !encodedPayload || !signature) {
    throw new SignalingError("INVALID_TOKEN", "Invalid join token format");
  }

  const expectedSignature = sign(`${encodedHeader}.${encodedPayload}`, secret);
  const actual = Buffer.from(signature);
  const expected = Buffer.from(expectedSignature);

  if (actual.length !== expected.length || !timingSafeEqual(actual, expected)) {
    throw new SignalingError("INVALID_TOKEN", "Invalid join token signature");
  }

  let parsed: unknown;
  try {
    const header = JSON.parse(base64UrlDecode(encodedHeader).toString("utf8")) as unknown;
    const headerResult = z.object({ alg: z.literal("HS256") }).passthrough().safeParse(header);
    if (!headerResult.success) {
      throw new Error("Unsupported token algorithm");
    }

    parsed = JSON.parse(base64UrlDecode(encodedPayload).toString("utf8")) as unknown;
  } catch {
    throw new SignalingError("INVALID_TOKEN", "Invalid join token payload");
  }

  const payload = joinTokenPayloadSchema.safeParse(parsed);
  if (!payload.success) {
    throw new SignalingError("INVALID_TOKEN", "Invalid join token payload");
  }

  if (payload.data.exp && payload.data.exp * 1000 <= Date.now()) {
    throw new SignalingError("INVALID_TOKEN", "Join token expired");
  }

  return payload.data;
}

export async function verifyAndConsumeRecordingBotAdmission(
  token: string | undefined,
  config: { issuer: string; secret: string; mediaNodeId: string; maxTtlSeconds: number },
  redis: RecordingBotAdmissionRedis,
  now: () => number = Date.now,
): Promise<RecordingBotAdmissionPayload> {
  try {
    if (!token) throw new Error("missing token");
    const parts = token.split(".");
    if (parts.length !== 3) throw new Error("invalid token format");
    const [encodedHeader, encodedPayload, signature] = parts;
    if (!encodedHeader || !encodedPayload || !signature) throw new Error("invalid token format");

    const expectedSignature = sign(`${encodedHeader}.${encodedPayload}`, config.secret);
    const actual = Buffer.from(signature);
    const expected = Buffer.from(expectedSignature);
    if (actual.length !== expected.length || !timingSafeEqual(actual, expected)) {
      throw new Error("invalid signature");
    }

    const header = z.object({
      alg: z.literal("HS256"),
      typ: z.literal("recording-bot+jwt"),
    }).passthrough().parse(JSON.parse(base64UrlDecode(encodedHeader).toString("utf8")) as unknown);
    void header;
    const payload = recordingBotAdmissionPayloadSchema.parse(
      JSON.parse(base64UrlDecode(encodedPayload).toString("utf8")) as unknown,
    );
    const nowSeconds = Math.floor(now() / 1000);
    if (
      payload.iss !== config.issuer ||
      payload.mediaNodeId !== config.mediaNodeId ||
      payload.exp <= nowSeconds ||
      payload.iat > nowSeconds + 5 ||
      payload.exp <= payload.iat ||
      payload.exp - payload.iat > config.maxTtlSeconds ||
      config.maxTtlSeconds > 60
    ) {
      throw new Error("invalid claims");
    }

    const jtiHash = createHash("sha256").update(payload.jti).digest("hex");
    const consumed = await redis.eval(consumeRecordingBotAdmissionScript, {
      keys: [
        `recording_bot_admission:${jtiHash}`,
        `recording_finalizing:${payload.recordingId}`,
      ],
      arguments: [payload.roomId, payload.recordingId, payload.mediaNodeId, new Date(now()).toISOString()],
    });
    if (Number(consumed) !== 1) throw new Error("unknown or consumed admission");
    return payload;
  } catch {
    throw new SignalingError("INVALID_TOKEN", "BOT_ADMISSION_REJECTED");
  }
}
