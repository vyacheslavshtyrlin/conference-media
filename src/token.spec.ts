import { createHmac } from "node:crypto";
import { describe, expect, it } from "vitest";

import { verifyJoinToken } from "./token.js";

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
