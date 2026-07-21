import { describe, expect, it } from "vitest";

import {
  chatMessageAddedEventSchema,
  participantJoinedEventSchema,
  roomSnapshotEventSchema,
  serverWebSocketEventSchema,
} from "./contracts/index.js";

const participant = {
  participantId: "peer_1",
  displayName: "Trusted Participant",
  isCreator: false,
  joinedAt: "2026-01-01T00:00:00.000Z",
  connectionState: "online",
  connectionQuality: "good",
  media: {
    mic: "on",
    camera: "off",
    screen: "off",
  },
};

const chatMessage = {
  messageId: "msg_1",
  roomId: "room_1",
  kind: "user",
  participantId: "peer_1",
  authorName: "Trusted Participant",
  text: "hello",
  createdAt: "2026-01-01T00:00:01.000Z",
};

describe("server WebSocket event contracts", () => {
  it.each([
    [
      "room snapshot root",
      roomSnapshotEventSchema,
      {
        type: "room:snapshot",
        roomId: "room_1",
        participants: [participant],
        chatMessages: [chatMessage],
        externalSubject: "crm-user-42",
      },
    ],
    [
      "participant joined root",
      participantJoinedEventSchema,
      {
        type: "participant:joined",
        participant,
        admissionCode: "opaque-admission-code",
      },
    ],
    [
      "chat message root",
      chatMessageAddedEventSchema,
      {
        type: "chat:messageAdded",
        message: chatMessage,
        integrationCredential: "service-jwt",
      },
    ],
    [
      "server event union root",
      serverWebSocketEventSchema,
      {
        type: "participant:joined",
        participant,
        roomSessionToken: "browser-private-session-value",
      },
    ],
  ])("rejects private identity fields on %s", (_name, schema, event) => {
    expect(schema.safeParse(event).success).toBe(false);
  });
});
