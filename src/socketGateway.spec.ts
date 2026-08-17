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
import { SignalingError } from "./errors.js";
import type { JoinTokenPayload } from "./token.js";

const JOIN_TOKEN_SECRET = "test-secret";
const createdAt = "2026-06-18T12:00:00.000Z";

type JsonMessage = Record<string, unknown>;

const room: RoomMetadata = {
  roomId: "room_1",
  slug: "room-one",
  mediaNodeId: "local",
  signalingUrl: "ws://127.0.0.1/ws",
  createdAt,
  expiresAt: null,
  status: "active",
  closedReason: null,
  accessPolicy: "link",
  recordingRequested: false,
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
    connectionQuality: "good",
    media: {
      mic: "off",
      camera: "off",
      screen: "off",
    },
  };
}

function createRoomManagerMock() {
  const participants = new Map<string, Participant>();
  const roomManager = {
    roomClosedCallback: undefined as
      | undefined
      | ((roomId: string, reason: "closed_by_creator" | "empty_timeout" | "server_shutdown" | "media_server_error") => void),
  };

  return Object.assign(roomManager, {
    setRoomClosedCallback: vi.fn((callback) => {
      roomManager.roomClosedCallback = callback;
    }),
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
    releaseParticipantMedia: vi.fn(async (_roomId: string, participantId: string) => {
      const participant = participants.get(participantId);
      if (!participant) return undefined;

      const next = {
        ...participant,
        media: {
          mic: "off",
          camera: "off",
          screen: "off",
        },
      } satisfies Participant;
      participants.set(participantId, next);
      return next;
    }),
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
    setProducerPaused: vi.fn(),
    setConnectionQuality: vi.fn(async (_roomId: string, participantId: string, quality: Participant["connectionQuality"]) => {
      const participant = participants.get(participantId);
      if (!participant) {
        throw new SignalingError("INVALID_TOKEN", "Participant is not joined");
      }
      const next = { ...participant, connectionQuality: quality };
      participants.set(participantId, next);
      return next;
    }),
    kickParticipantByCreator: vi.fn(async (_roomId: string, actorParticipantId: string, targetParticipantId: string) => {
      const actor = participants.get(actorParticipantId);
      const target = participants.get(targetParticipantId);
      if (!actor?.isCreator) {
        throw new SignalingError("INVALID_TOKEN", "Only a creator can manage participants");
      }
      if (!target) {
        throw new SignalingError("INVALID_PAYLOAD", "Target participant is not joined");
      }
      participants.delete(targetParticipantId);
      return {
        actor,
        target,
        chatMessage: {
          messageId: "msg_kick",
          roomId: room.roomId,
          kind: "system" as const,
          text: `${actor.displayName} removed ${target.displayName}`,
          createdAt,
        },
      };
    }),
    disableParticipantMediaByCreator: vi.fn(async (_roomId: string, actorParticipantId: string, targetParticipantId: string, source: "mic" | "camera" | "screen") => {
      const actor = participants.get(actorParticipantId);
      const target = participants.get(targetParticipantId);
      if (!actor?.isCreator) {
        throw new SignalingError("INVALID_TOKEN", "Only a creator can manage participants");
      }
      if (!target) {
        throw new SignalingError("INVALID_PAYLOAD", "Target participant is not joined");
      }
      const next = {
        ...target,
        media: {
          ...target.media,
          [source]: "off",
        },
      } satisfies Participant;
      participants.set(targetParticipantId, next);
      return {
        actor,
        target: next,
        closedProducerIds: [`producer_${source}`],
        chatMessage: {
          messageId: "msg_media_control",
          roomId: room.roomId,
          kind: "system" as const,
          text: `${actor.displayName} changed ${target.displayName}`,
          createdAt,
        },
      };
    }),
    grantCreatorByCreator: vi.fn(async (_roomId: string, actorParticipantId: string, targetParticipantId: string) => {
      const actor = participants.get(actorParticipantId);
      const target = participants.get(targetParticipantId);
      if (!actor?.isCreator) {
        throw new SignalingError("INVALID_TOKEN", "Only a creator can manage participants");
      }
      if (!target) {
        throw new SignalingError("INVALID_PAYLOAD", "Target participant is not joined");
      }
      const next = { ...target, isCreator: true } satisfies Participant;
      participants.set(targetParticipantId, next);
      return {
        actor,
        target: next,
        chatMessage: {
          messageId: "msg_creator",
          roomId: room.roomId,
          kind: "system" as const,
          text: `${actor.displayName} shared creator status`,
          createdAt,
        },
      };
    }),
    closeRoomByCreator: vi.fn(),
  });
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
  const { client } = await connectClientWithSnapshot(baseUrl, payload);

  return client;
}

async function connectClientWithSnapshot(
  baseUrl: string,
  payload: JoinTokenPayload,
): Promise<{ client: WebSocket; snapshot: JsonMessage }> {
  const token = createJoinToken(payload);
  const client = new WebSocket(`${baseUrl}?token=${encodeURIComponent(token)}`);
  clients.push(client);
  const snapshotPromise = waitForMessage(client, (message) => message.type === "room:snapshot");

  await waitForOpen(client);
  const snapshot = await snapshotPromise;

  return { client, snapshot };
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

function waitForClose(client: WebSocket): Promise<void> {
  if (client.readyState === WebSocket.CLOSED) {
    return Promise.resolve();
  }

  return new Promise((resolve) => {
    client.once("close", resolve);
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

function creatorPayload(participantId: string, displayName: string): JoinTokenPayload {
  return {
    ...joinPayload(participantId, displayName),
    isCreator: true,
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

  it("supersedes an existing socket for the same participant session without leaving the room", async () => {
    const { roomManager, url } = await createTestGateway();
    const first = await connectClient(url, joinPayload("peer_a", "Alice"));
    const firstClosed = waitForClose(first);

    const { client: second, snapshot } = await connectClientWithSnapshot(url, joinPayload("peer_a", "Alice"));
    await firstClosed;

    expect(roomManager.join).toHaveBeenCalledTimes(2);
    expect(roomManager.leave).not.toHaveBeenCalled();
    expect(roomManager.releaseParticipantMedia).toHaveBeenCalledWith(room.roomId, "peer_a");
    expect(snapshot.participants).toMatchObject([
      {
        participantId: "peer_a",
        media: {
          mic: "off",
          camera: "off",
          screen: "off",
        },
      },
    ]);

    const responsePromise = waitForMessage(
      second,
      (message) => message.type === "response" && message.requestId === "req_chat_after_supersede",
    );
    second.send(JSON.stringify({ type: "chat:send", requestId: "req_chat_after_supersede", text: "still here" }));

    await expect(responsePromise).resolves.toMatchObject({
      type: "response",
      requestId: "req_chat_after_supersede",
      ok: true,
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

  it("logs command correlation and classifies contract failures as client errors", async () => {
    const { roomManager, url } = await createTestGateway();
    const alice = await connectClient(url, joinPayload("peer_a", "Alice"));
    vi.mocked(logger.warn).mockClear();
    roomManager.sendChatMessage.mockRejectedValueOnce(
      new SignalingError("INVALID_PAYLOAD", "Chat is unavailable"),
    );
    const responsePromise = waitForMessage(
      alice,
      (message) => message.type === "response" && message.requestId === "command_correlation",
    );

    alice.send(JSON.stringify({ type: "chat:send", requestId: "command_correlation", text: "hello" }));

    await expect(responsePromise).resolves.toMatchObject({
      ok: false,
      error: { code: "INVALID_PAYLOAD" },
    });
    expect(logger.warn).toHaveBeenCalledWith(
      "websocket command failed",
      expect.objectContaining({
        operation: "chat:send",
        commandId: "command_correlation",
        roomId: room.roomId,
        participantId: "peer_a",
        errorCode: "INVALID_PAYLOAD",
        outcome: "client_error",
        durationMs: expect.any(Number),
      }),
    );
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

  it("stores and broadcasts participant connection quality updates", async () => {
    const { roomManager, url } = await createTestGateway();
    const alice = await connectClient(url, joinPayload("peer_a", "Alice"));
    const bob = await connectClient(url, joinPayload("peer_b", "Bob"));
    const responsePromise = waitForMessage(
      alice,
      (message) => message.type === "response" && message.requestId === "quality_1",
    );
    const broadcastPromise = waitForMessage(
      bob,
      (message) => message.type === "participant:connectionQualityChanged",
    );

    alice.send(JSON.stringify({
      type: "participant:setConnectionQuality",
      requestId: "quality_1",
      quality: "poor",
    }));

    await expect(responsePromise).resolves.toMatchObject({
      type: "response",
      requestId: "quality_1",
      ok: true,
    });
    await expect(broadcastPromise).resolves.toMatchObject({
      type: "participant:connectionQualityChanged",
      participantId: "peer_a",
      quality: "poor",
    });
    expect(roomManager.setConnectionQuality).toHaveBeenCalledWith(room.roomId, "peer_a", "poor");
  });

  it("lets the creator disable another participant's media and broadcasts the control event", async () => {
    const { roomManager, url } = await createTestGateway();
    const alice = await connectClient(url, creatorPayload("peer_creator", "Alice"));
    const bob = await connectClient(url, joinPayload("peer_b", "Bob"));

    const responsePromise = waitForMessage(
      alice,
      (message) => message.type === "response" && message.requestId === "disable_mic",
    );
    const controlPromise = waitForMessage(
      bob,
      (message) => message.type === "participant:mediaControlApplied",
    );

    alice.send(
      JSON.stringify({
        type: "participant:disableMedia",
        requestId: "disable_mic",
        targetParticipantId: "peer_b",
        source: "mic",
      }),
    );

    await expect(responsePromise).resolves.toMatchObject({
      type: "response",
      requestId: "disable_mic",
      ok: true,
    });
    await expect(controlPromise).resolves.toMatchObject({
      type: "participant:mediaControlApplied",
      participantId: "peer_b",
      targetDisplayName: "Bob",
      source: "mic",
      actorParticipantId: "peer_creator",
      actorDisplayName: "Alice",
        media: { mic: "off", camera: "off", screen: "off" },
    });
    expect(roomManager.disableParticipantMediaByCreator).toHaveBeenCalledWith(
      room.roomId,
      "peer_creator",
      "peer_b",
      "mic",
    );
  });

  it("does not publish creator media-control success when the target producer cannot be closed", async () => {
    const { roomManager, url } = await createTestGateway();
    const alice = await connectClient(url, creatorPayload("peer_creator", "Alice"));
    const bob = await connectClient(url, joinPayload("peer_b", "Bob"));
    const receivedByBob: JsonMessage[] = [];
    bob.on("message", (raw) => receivedByBob.push(JSON.parse(raw.toString()) as JsonMessage));
    roomManager.disableParticipantMediaByCreator.mockRejectedValueOnce(
      new SignalingError("INVALID_PAYLOAD", "Target producer is no longer available"),
    );

    const responsePromise = waitForMessage(
      alice,
      (message) => message.type === "response" && message.requestId === "disable_failed",
    );
    alice.send(JSON.stringify({
      type: "participant:disableMedia",
      requestId: "disable_failed",
      targetParticipantId: "peer_b",
      source: "mic",
    }));

    await expect(responsePromise).resolves.toMatchObject({
      type: "response",
      requestId: "disable_failed",
      ok: false,
    });
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(
      receivedByBob.some(
        (message) =>
          message.type === "participant:mediaControlApplied" || message.type === "participant:mediaChanged",
      ),
    ).toBe(false);
  });

  it("lets the creator share creator status", async () => {
    const { roomManager, url } = await createTestGateway();
    const alice = await connectClient(url, creatorPayload("peer_creator", "Alice"));
    const bob = await connectClient(url, joinPayload("peer_b", "Bob"));
    const responsePromise = waitForMessage(
      alice,
      (message) => message.type === "response" && message.requestId === "grant_creator",
    );
    const grantPromise = waitForMessage(
      bob,
      (message) => message.type === "participant:creatorChanged",
    );

    alice.send(
      JSON.stringify({
        type: "participant:grantCreator",
        requestId: "grant_creator",
        targetParticipantId: "peer_b",
      }),
    );

    await expect(responsePromise).resolves.toMatchObject({
      type: "response",
      requestId: "grant_creator",
      ok: true,
    });
    await expect(grantPromise).resolves.toMatchObject({
      type: "participant:creatorChanged",
      participantId: "peer_b",
      targetDisplayName: "Bob",
      isCreator: true,
      actorParticipantId: "peer_creator",
      actorDisplayName: "Alice",
    });
    expect(roomManager.grantCreatorByCreator).toHaveBeenCalledWith(room.roomId, "peer_creator", "peer_b");
  });

  it("lets the creator kick another participant and closes the target socket", async () => {
    const { roomManager, url } = await createTestGateway();
    const alice = await connectClient(url, creatorPayload("peer_creator", "Alice"));
    const bob = await connectClient(url, joinPayload("peer_b", "Bob"));
    const responsePromise = waitForMessage(
      alice,
      (message) => message.type === "response" && message.requestId === "kick_bob",
    );
    const kickedPromise = waitForMessage(
      bob,
      (message) => message.type === "participant:kicked",
    );
    const bobClosed = waitForClose(bob);

    alice.send(
      JSON.stringify({
        type: "participant:kick",
        requestId: "kick_bob",
        targetParticipantId: "peer_b",
      }),
    );

    await expect(responsePromise).resolves.toMatchObject({
      type: "response",
      requestId: "kick_bob",
      ok: true,
    });
    await expect(kickedPromise).resolves.toMatchObject({
      type: "participant:kicked",
      participantId: "peer_b",
      targetDisplayName: "Bob",
      actorParticipantId: "peer_creator",
      actorDisplayName: "Alice",
    });
    await bobClosed;
    expect(roomManager.kickParticipantByCreator).toHaveBeenCalledWith(room.roomId, "peer_creator", "peer_b");
    expect(roomManager.leave).not.toHaveBeenCalledWith(room.roomId, "peer_b");
  });

  it("rejects participant management from non-creators", async () => {
    const { roomManager, url } = await createTestGateway();
    const alice = await connectClient(url, joinPayload("peer_a", "Alice"));
    await connectClient(url, joinPayload("peer_b", "Bob"));
    const responsePromise = waitForMessage(
      alice,
      (message) => message.type === "response" && message.requestId === "bad_kick",
    );

    alice.send(
      JSON.stringify({
        type: "participant:kick",
        requestId: "bad_kick",
        targetParticipantId: "peer_b",
      }),
    );

    await expect(responsePromise).resolves.toMatchObject({
      type: "response",
      requestId: "bad_kick",
      ok: false,
      error: {
        code: "INVALID_TOKEN",
      },
    });
    expect(roomManager.kickParticipantByCreator).toHaveBeenCalledWith(room.roomId, "peer_a", "peer_b");
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

  it("lets the creator close the room and notifies all connected clients", async () => {
    const { roomManager, url } = await createTestGateway();
    const alice = await connectClient(url, creatorPayload("peer_creator", "Alice"));
    const bob = await connectClient(url, joinPayload("peer_b", "Bob"));
    const responsePromise = waitForMessage(
      alice,
      (message) => message.type === "response" && message.requestId === "close_room",
    );
    const aliceClosedPromise = waitForMessage(
      alice,
      (message) => message.type === "room:closed",
    );
    const bobClosedPromise = waitForMessage(
      bob,
      (message) => message.type === "room:closed",
    );

    alice.send(JSON.stringify({ type: "room:close", requestId: "close_room" }));

    await expect(responsePromise).resolves.toMatchObject({
      type: "response",
      requestId: "close_room",
      ok: true,
    });
    await expect(aliceClosedPromise).resolves.toMatchObject({
      type: "room:closed",
      reason: "closed_by_creator",
    });
    await expect(bobClosedPromise).resolves.toMatchObject({
      type: "room:closed",
      reason: "closed_by_creator",
    });
    expect(roomManager.closeRoomByCreator).toHaveBeenCalledWith(room.roomId, "peer_creator");
  });

  it("rejects room close from non-creator participants", async () => {
    const { roomManager, url } = await createTestGateway();
    roomManager.closeRoomByCreator.mockRejectedValueOnce(
      new SignalingError("INVALID_TOKEN", "Only the room creator can close the room"),
    );
    const bob = await connectClient(url, joinPayload("peer_b", "Bob"));
    const responsePromise = waitForMessage(
      bob,
      (message) => message.type === "response" && message.requestId === "close_room",
    );

    bob.send(JSON.stringify({ type: "room:close", requestId: "close_room" }));

    await expect(responsePromise).resolves.toMatchObject({
      type: "response",
      requestId: "close_room",
      ok: false,
      error: {
        code: "INVALID_TOKEN",
      },
    });
    expect(roomManager.closeRoomByCreator).toHaveBeenCalledWith(room.roomId, "peer_b");
  });

  it("broadcasts media server room close reason before closing room sockets", async () => {
    const { roomManager, url } = await createTestGateway();
    const alice = await connectClient(url, joinPayload("peer_a", "Alice"));
    const bob = await connectClient(url, joinPayload("peer_b", "Bob"));
    const aliceClosedPromise = waitForMessage(
      alice,
      (message) => message.type === "room:closed",
    );
    const bobClosedPromise = waitForMessage(
      bob,
      (message) => message.type === "room:closed",
    );

    roomManager.roomClosedCallback?.(room.roomId, "media_server_error");

    await expect(aliceClosedPromise).resolves.toMatchObject({
      type: "room:closed",
      reason: "media_server_error",
    });
    await expect(bobClosedPromise).resolves.toMatchObject({
      type: "room:closed",
      reason: "media_server_error",
    });
  });

  it("runs disconnect cleanup only once for a duplicate close notification", async () => {
    const { roomManager, url } = await createTestGateway();
    await connectClient(url, joinPayload("peer_a", "Alice"));
    const internals = gateway as unknown as {
      clients: Map<WebSocket, unknown>;
      handleDisconnect: (context: unknown) => Promise<void>;
    };
    const context = [...internals.clients.values()][0];

    await internals.handleDisconnect(context);
    await internals.handleDisconnect(context);

    expect(roomManager.leave).toHaveBeenCalledTimes(1);
  });

  it("catches a rejected async socket close cleanup", async () => {
    const roomManager = createRoomManagerMock();
    roomManager.leave.mockRejectedValueOnce(new Error("redis cleanup failed"));
    const { url } = await createTestGateway(roomManager);
    vi.mocked(logger.warn).mockClear();
    const client = await connectClient(url, joinPayload("peer_a", "Alice"));

    await closeClient(client);
    await new Promise((resolve) => setTimeout(resolve, 0));

    expect(logger.warn).toHaveBeenCalledWith(
      "websocket disconnect cleanup failed",
      expect.objectContaining({ participantId: "peer_a", roomId: room.roomId }),
    );
  });
});
