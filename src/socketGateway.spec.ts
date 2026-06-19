import {
  CHAT_MESSAGE_TEXT_MAX_LENGTH,
  type Participant,
  type RoomMetadata,
} from "#contracts";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { AddressInfo } from "node:net";
import { createHmac } from "node:crypto";
import { WebSocket, WebSocketServer } from "ws";

import type { Logger } from "./logger.js";
import type { RoomManager } from "./roomManager.js";
import { SocketGateway } from "./socketGateway.js";
import type { JoinTokenPayload } from "./token.js";

const JOIN_TOKEN_SECRET = "test-secret";
const createdAt = "2026-06-18T12:00:00.000Z";

type JsonMessage = Record<string, unknown>;

const room: RoomMetadata = {
  roomId: "room_1",
  slug: "room-one",
  mediaNodeId: "local",
  signalingUrl: "ws://127.0.0.1/ws",
  creatorTokenHash: "creator-token-hash",
  createdAt,
  expiresAt: null,
  status: "active",
};

const logger: Logger = {
  info: vi.fn(),
  warn: vi.fn(),
  error: vi.fn(),
};

let wsServer: WebSocketServer | undefined;
let gateway: SocketGateway | undefined;
let clients: WebSocket[] = [];

function createParticipant(payload: JoinTokenPayload): Participant {
  return {
    participantId: payload.participantId,
    displayName: payload.displayName,
    isCreator: payload.isCreator,
    joinedAt: createdAt,
    connectionState: "online",
    media: {
      mic: "off",
      camera: "off",
      screen: "off",
    },
  };
}

function createRoomManagerMock() {
  const participants = new Map<string, Participant>();

  return {
    setParticipantEvictedCallback: vi.fn(),
    setActiveSpeakerChangedCallback: vi.fn(),
    join: vi.fn(async (payload: JoinTokenPayload) => {
      const participant = createParticipant(payload);
      participants.set(participant.participantId, participant);

      return {
        room,
        participant,
        participants: Array.from(participants.values()),
        chatMessages: [],
        joinMessage: undefined,
      };
    }),
    leave: vi.fn(async (_roomId: string, participantId: string) => {
      const participant = participants.get(participantId);
      participants.delete(participantId);
      return { participant };
    }),
    getActiveSpeakerParticipantId: vi.fn(() => null),
    listProducers: vi.fn(() => []),
    sendChatMessage: vi.fn(async (_roomId: string, participantId: string, text: string) => ({
      messageId: "msg_1",
      roomId: room.roomId,
      kind: "user" as const,
      participantId,
      authorName: participants.get(participantId)?.displayName ?? participantId,
      text,
      createdAt,
    })),
    getRouterRtpCapabilities: vi.fn(),
    createTransport: vi.fn(),
    connectTransport: vi.fn(),
    produce: vi.fn(),
    consume: vi.fn(),
    closeProducer: vi.fn(),
    setMedia: vi.fn(),
    closeRoomByCreator: vi.fn(),
  };
}

function createJoinToken(payload: JoinTokenPayload): string {
  const header = Buffer.from(JSON.stringify({ alg: "HS256", typ: "JWT" })).toString("base64url");
  const body = Buffer.from(
    JSON.stringify({
      ...payload,
      exp: Math.floor(Date.now() / 1000) + 60,
    }),
  ).toString("base64url");
  const signature = createHmac("sha256", JOIN_TOKEN_SECRET)
    .update(`${header}.${body}`)
    .digest("base64url");

  return `${header}.${body}.${signature}`;
}

async function createTestGateway(roomManager = createRoomManagerMock()) {
  wsServer = new WebSocketServer({ port: 0 });
  await new Promise<void>((resolve) => {
    wsServer?.once("listening", resolve);
  });

  gateway = new SocketGateway(wsServer, roomManager as unknown as RoomManager, JOIN_TOKEN_SECRET, logger, {
    heartbeatIntervalMs: 60_000,
    sendBackpressureBytes: 1024 * 1024,
    rateLimitWindowMs: 1_000,
    rateLimitMaxMessages: 100,
    allowedOrigins: [],
  });
  gateway.start();

  const address = wsServer.address() as AddressInfo;
  return {
    roomManager,
    url: `ws://127.0.0.1:${address.port}/ws`,
  };
}

async function connectClient(baseUrl: string, payload: JoinTokenPayload): Promise<WebSocket> {
  const token = createJoinToken(payload);
  const client = new WebSocket(`${baseUrl}?token=${encodeURIComponent(token)}`);
  clients.push(client);
  const snapshotPromise = waitForMessage(client, (message) => message.type === "room:snapshot");

  await waitForOpen(client);
  await snapshotPromise;

  return client;
}

function waitForOpen(client: WebSocket): Promise<void> {
  if (client.readyState === WebSocket.OPEN) {
    return Promise.resolve();
  }

  return new Promise((resolve, reject) => {
    client.once("open", resolve);
    client.once("error", reject);
  });
}

function waitForMessage(
  client: WebSocket,
  predicate: (message: JsonMessage) => boolean,
): Promise<JsonMessage> {
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => {
      client.off("message", onMessage);
      reject(new Error("Timed out waiting for websocket message"));
    }, 1_000);

    const onMessage = (raw: WebSocket.RawData) => {
      const parsed = JSON.parse(raw.toString()) as unknown;
      if (typeof parsed !== "object" || parsed === null || !predicate(parsed as JsonMessage)) {
        return;
      }

      clearTimeout(timeout);
      client.off("message", onMessage);
      resolve(parsed as JsonMessage);
    };

    client.on("message", onMessage);
  });
}

function closeClient(client: WebSocket): Promise<void> {
  if (client.readyState === WebSocket.CLOSED) {
    return Promise.resolve();
  }

  return new Promise((resolve) => {
    client.once("close", resolve);
    client.close();
  });
}

function closeServer(server: WebSocketServer | undefined): Promise<void> {
  if (!server) {
    return Promise.resolve();
  }

  return new Promise((resolve, reject) => {
    server.close((error) => {
      if (error) {
        reject(error);
        return;
      }
      resolve();
    });
  });
}

function joinPayload(participantId: string, displayName: string): JoinTokenPayload {
  return {
    roomId: room.roomId,
    participantId,
    displayName,
    isCreator: false,
  };
}

describe("SocketGateway chat protocol", () => {
  afterEach(async () => {
    gateway?.stop();
    await Promise.all(clients.map(closeClient));
    await closeServer(wsServer);
    clients = [];
    gateway = undefined;
    wsServer = undefined;
    vi.restoreAllMocks();
  });

  it("returns the chat message and broadcasts it to room participants", async () => {
    const { roomManager, url } = await createTestGateway();
    const alice = await connectClient(url, joinPayload("peer_a", "Alice"));
    const bob = await connectClient(url, joinPayload("peer_b", "Bob"));
    const responsePromise = waitForMessage(
      alice,
      (message) => message.type === "response" && message.requestId === "req_chat",
    );
    const broadcastPromise = waitForMessage(
      bob,
      (message) => message.type === "chat:messageAdded",
    );

    alice.send(JSON.stringify({ type: "chat:send", requestId: "req_chat", text: " hello " }));

    const response = await responsePromise;
    const broadcast = await broadcastPromise;

    expect(roomManager.sendChatMessage).toHaveBeenCalledWith(room.roomId, "peer_a", "hello");
    expect(response).toMatchObject({
      type: "response",
      requestId: "req_chat",
      ok: true,
      data: {
        messageId: "msg_1",
        roomId: room.roomId,
        kind: "user",
        participantId: "peer_a",
        authorName: "Alice",
        text: "hello",
        createdAt,
      },
    });
    expect(broadcast).toMatchObject({
      type: "chat:messageAdded",
      message: {
        messageId: "msg_1",
        roomId: room.roomId,
        kind: "user",
        participantId: "peer_a",
        authorName: "Alice",
        text: "hello",
        createdAt,
      },
    });
  });

  it("maps invalid chat text to INVALID_PAYLOAD without calling RoomManager", async () => {
    const { roomManager, url } = await createTestGateway();
    const alice = await connectClient(url, joinPayload("peer_a", "Alice"));
    const responsePromise = waitForMessage(
      alice,
      (message) => message.type === "response" && message.requestId === "bad_chat",
    );

    alice.send(JSON.stringify({ type: "chat:send", requestId: "bad_chat", text: "   " }));

    const response = await responsePromise;

    expect(response).toMatchObject({
      type: "response",
      requestId: "bad_chat",
      ok: false,
      error: {
        code: "INVALID_PAYLOAD",
      },
    });
    expect(roomManager.sendChatMessage).not.toHaveBeenCalled();
  });

  it("accepts chat text at the configured maximum length", async () => {
    const { roomManager, url } = await createTestGateway();
    const alice = await connectClient(url, joinPayload("peer_a", "Alice"));
    const text = "x".repeat(CHAT_MESSAGE_TEXT_MAX_LENGTH);
    const responsePromise = waitForMessage(
      alice,
      (message) => message.type === "response" && message.requestId === "max_chat",
    );

    alice.send(JSON.stringify({ type: "chat:send", requestId: "max_chat", text }));

    const response = await responsePromise;

    expect(response).toMatchObject({
      type: "response",
      requestId: "max_chat",
      ok: true,
    });
    expect(roomManager.sendChatMessage).toHaveBeenCalledWith(room.roomId, "peer_a", text);
  });

  it("rejects chat text above the configured maximum length", async () => {
    const { roomManager, url } = await createTestGateway();
    const alice = await connectClient(url, joinPayload("peer_a", "Alice"));
    const responsePromise = waitForMessage(
      alice,
      (message) => message.type === "response" && message.requestId === "too_long_chat",
    );

    alice.send(JSON.stringify({
      type: "chat:send",
      requestId: "too_long_chat",
      text: "x".repeat(CHAT_MESSAGE_TEXT_MAX_LENGTH + 1),
    }));

    const response = await responsePromise;

    expect(response).toMatchObject({
      type: "response",
      requestId: "too_long_chat",
      ok: false,
      error: {
        code: "INVALID_PAYLOAD",
      },
    });
    expect(roomManager.sendChatMessage).not.toHaveBeenCalled();
  });
});
