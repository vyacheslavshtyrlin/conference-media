import { createHash, createHmac } from "node:crypto";
import { describe, expect, it, vi } from "vitest";

import { verifyAndConsumeRecordingBotAdmission, verifyJoinToken } from "./token.js";

const secret = "test-join-secret";

function createJoinToken(payload: Record<string, unknown>): string {
  const header = Buffer.from(JSON.stringify({ alg: "HS256", typ: "JWT" })).toString("base64url");
  const body = Buffer.from(JSON.stringify(payload)).toString("base64url");
  const signature = createHmac("sha256", secret).update(`${header}.${body}`).digest("base64url");
  return `${header}.${body}.${signature}`;
}

describe("verifyJoinToken", () => {
  it("accepts only Conference signaling claims required for media join", () => {
    const token = createJoinToken({
      roomId: "room_1",
      participantId: "peer_1",
      displayName: "Trusted Participant",
      isCreator: true,
      exp: Math.floor(Date.now() / 1000) + 60,
    });

    expect(verifyJoinToken(token, secret)).toEqual({
      roomId: "room_1",
      participantId: "peer_1",
      displayName: "Trusted Participant",
      isCreator: true,
      exp: expect.any(Number),
    });
  });

  it("rejects a join token without exp", () => {
    const token = createJoinToken({
      roomId: "room_1",
      participantId: "peer_1",
      displayName: "Trusted Participant",
      isCreator: false,
    });

    expect(() => verifyJoinToken(token, secret)).toThrow("Invalid join token payload");
  });

  it("rejects an expired join token", () => {
    const token = createJoinToken({
      roomId: "room_1",
      participantId: "peer_1",
      displayName: "Trusted Participant",
      isCreator: false,
      exp: Math.floor(Date.now() / 1000),
    });

    expect(() => verifyJoinToken(token, secret)).toThrow("Join token expired");
  });

  it.each([0, -1, 1.5, "1780401900"])("rejects malformed exp %j", (exp) => {
    const token = createJoinToken({
      roomId: "room_1",
      participantId: "peer_1",
      displayName: "Trusted Participant",
      isCreator: false,
      exp,
    });

    expect(() => verifyJoinToken(token, secret)).toThrow("Invalid join token payload");
  });

  it.each([
    ["externalSubject", "crm-user-42"],
    ["identity", { source: "integration_admission" }],
    ["admissionCode", "opaque-admission-code"],
    ["integrationCredential", "service-jwt"],
    ["roomSessionToken", "browser-private-session-value"],
  ])("rejects join tokens carrying %s", (field, value) => {
    const token = createJoinToken({
      roomId: "room_1",
      participantId: "peer_1",
      displayName: "Trusted Participant",
      isCreator: false,
      exp: Math.floor(Date.now() / 1000) + 60,
      [field]: value,
    });

    expect(() => verifyJoinToken(token, secret)).toThrow("Invalid join token payload");
  });
});

describe("verifyAndConsumeRecordingBotAdmission", () => {
  const botSecret = "recording-bot-secret-at-least-32-bytes-long";
  const now = Date.parse("2026-07-22T10:00:00.000Z");
  const claims = {
    sub: "recording-bot",
    aud: "conference-recording-bot-signaling",
    iss: "conference-api",
    roomId: "room_1",
    recordingId: "rec_1",
    mediaNodeId: "local",
    jti: "one-use-jti",
    iat: Math.floor(now / 1000),
    exp: Math.floor(now / 1000) + 60,
  };

  function token(payload: Record<string, unknown> = claims): string {
    const header = Buffer.from(JSON.stringify({ alg: "HS256", typ: "recording-bot+jwt" })).toString("base64url");
    const body = Buffer.from(JSON.stringify(payload)).toString("base64url");
    const signature = createHmac("sha256", botSecret).update(`${header}.${body}`).digest("base64url");
    return `${header}.${body}.${signature}`;
  }

  const config = { issuer: "conference-api", secret: botSecret, mediaNodeId: "local", maxTtlSeconds: 60 };

  it("atomically consumes the hashed replay marker", async () => {
    const evalCommand = vi.fn().mockResolvedValue(1);
    await expect(verifyAndConsumeRecordingBotAdmission(token(), config, { eval: evalCommand }, () => now)).resolves.toMatchObject(claims);
    expect(evalCommand).toHaveBeenCalledWith(expect.any(String), {
      keys: [
        `recording_bot_admission:${createHash("sha256").update(claims.jti).digest("hex")}`,
        "recording_finalizing:rec_1",
      ],
      arguments: ["room_1", "rec_1", "local", "2026-07-22T10:00:00.000Z"],
    });
  });

  it("atomically rejects and removes an issued admission after the recording finalizing fence", async () => {
    let admissionMarkerExists = true;
    const finalizingMarkers = new Set(["recording_finalizing:rec_1"]);
    const evalCommand = vi.fn(async (_script: string, options: { keys: string[] }) => {
      const [admissionKey, finalizingKey] = options.keys;
      if (finalizingKey && finalizingMarkers.has(finalizingKey)) {
        admissionMarkerExists = false;
        return -1;
      }
      return admissionKey && admissionMarkerExists ? 1 : 0;
    });
    const openAttempt = vi.fn();

    await expect(verifyAndConsumeRecordingBotAdmission(token(), config, { eval: evalCommand }, () => now).then(openAttempt))
      .rejects.toThrow("BOT_ADMISSION_REJECTED");
    expect(admissionMarkerExists).toBe(false);
    expect(openAttempt).not.toHaveBeenCalled();
    await expect(verifyAndConsumeRecordingBotAdmission(token(), config, { eval: evalCommand }, () => now))
      .rejects.toThrow("BOT_ADMISSION_REJECTED");
    expect(evalCommand).toHaveBeenCalledTimes(2);
    expect(evalCommand.mock.calls[0]?.[1].keys).toEqual([
      `recording_bot_admission:${createHash("sha256").update(claims.jti).digest("hex")}`,
      "recording_finalizing:rec_1",
    ]);
    expect(evalCommand.mock.calls[0]?.[0]).toContain("redis.call('EXISTS', KEYS[2])");
    expect(evalCommand.mock.calls[0]?.[0]).toContain("redis.call('DEL', KEYS[1])");
  });

  it.each([
    ["expired", { ...claims, iat: claims.iat - 61, exp: claims.iat - 1 }, 1],
    ["replayed", claims, 0],
    ["cross-node", { ...claims, mediaNodeId: "other" }, 1],
    ["cross-room marker", claims, 0],
    ["expiration not after issued-at", { ...claims, iat: claims.iat + 2, exp: claims.iat + 2 }, 1],
  ])("rejects %s admissions with one private result", async (_label, payload, redisResult) => {
    await expect(verifyAndConsumeRecordingBotAdmission(token(payload), config, { eval: vi.fn().mockResolvedValue(redisResult) }, () => now))
      .rejects.toThrow("BOT_ADMISSION_REJECTED");
  });
});
