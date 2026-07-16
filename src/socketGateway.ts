import { clientWebSocketEventSchema } from "#contracts";
import type {
  ClientWebSocketEvent,
  ErrorCode,
  Participant,
  RoomClosedReason,
  ServerWebSocketEvent,
} from "#contracts";
import type { IncomingMessage } from "node:http";
import { WebSocket, WebSocketServer } from "ws";

import { SignalingError, toContractError } from "./errors.js";
import type { Logger } from "./logger.js";
import { getMediaMetrics, type MediaMetrics } from "./metrics.js";
import type { RoomManager } from "./roomManager.js";
import { verifyJoinToken } from "./token.js";

type WebSocketRuntimeConfig = {
  heartbeatIntervalMs: number;
  sendBackpressureBytes: number;
  rateLimitWindowMs: number;
  rateLimitMaxMessages: number;
  allowedOrigins: string[];
};

type ClientContext = {
  socket: WebSocket;
  roomId: string;
  participantId: string;
  joined: boolean;
  isAlive: boolean;
  disconnectMetricRecorded: boolean;
  rateLimit: {
    windowStartedAt: number;
    messageCount: number;
  };
};

export class SocketGateway {
  private readonly clients = new Map<WebSocket, ClientContext>();
  private readonly roomSockets = new Map<string, Set<WebSocket>>();
  private readonly participantSockets = new Map<string, WebSocket>();
  private heartbeatTimer?: NodeJS.Timeout;

  constructor(
    private readonly wsServer: WebSocketServer,
    private readonly roomManager: RoomManager,
    private readonly joinTokenSecret: string,
    private readonly logger: Logger,
    private readonly config: WebSocketRuntimeConfig,
    private readonly metrics: MediaMetrics = getMediaMetrics(),
  ) {}

  start(): void {
    this.roomManager.setRoomClosedCallback((roomId, reason) => {
      this.handleRoomClosedByServer(roomId, reason);
    });
    this.roomManager.setActiveSpeakerChangedCallback((roomId, participantId) => {
      this.broadcast(roomId, {
        type: "participant:activeSpeakerChanged",
        participantId,
      });
    });

    this.wsServer.on("connection", (socket, request) => {
      void this.handleConnection(socket, request);
    });

    this.heartbeatTimer = setInterval(() => {
      this.checkHeartbeats();
    }, this.config.heartbeatIntervalMs);
  }

  async stop(): Promise<void> {
    if (this.heartbeatTimer) {
      clearInterval(this.heartbeatTimer);
      this.heartbeatTimer = undefined;
    }

    for (const context of [...this.clients.values()]) {
      context.joined = false;
      this.removeRoomSocket(context.roomId, context.socket);
      this.removeParticipantSocket(context);
      context.socket.close(1001, "Server shutting down");
      this.recordDisconnectOnce(context, "server_close");
    }
    this.clients.clear();
    this.roomSockets.clear();
    this.participantSockets.clear();
  }

  forceClose(): void {
    for (const context of this.clients.values()) {
      context.joined = false;
      context.socket.terminate();
      this.recordDisconnectOnce(context, "server_close");
    }
    this.clients.clear();
    this.roomSockets.clear();
    this.participantSockets.clear();
  }

  private async handleConnection(socket: WebSocket, request: IncomingMessage): Promise<void> {
    const startedAt = Date.now();
    try {
      this.assertAllowedOrigin(request);
      const url = new URL(request.url ?? "", "ws://localhost");
      const payload = verifyJoinToken(url.searchParams.get("token") ?? undefined, this.joinTokenSecret);
      const context: ClientContext = {
        socket,
        roomId: payload.roomId,
        participantId: payload.participantId,
        joined: false,
        isAlive: true,
        disconnectMetricRecorded: false,
        rateLimit: {
          windowStartedAt: Date.now(),
          messageCount: 0,
        },
      };
      this.clients.set(socket, context);

      socket.on("pong", () => {
        context.isAlive = true;
      });
      socket.on("message", (raw) => {
        void this.handleMessage(context, raw);
      });
      socket.on("close", () => {
        void this.handleDisconnect(context).catch((error: unknown) => {
          this.logger.warn("websocket disconnect cleanup failed", {
            operation: "websocket.disconnect",
            errorCode: "DISCONNECT_CLEANUP_FAILED",
            roomId: context.roomId,
            participantId: context.participantId,
            error: String(error),
          });
        });
      });
      socket.on("error", (error) => {
        this.logger.warn("websocket error", {
          operation: "websocket.connection",
          errorCode: "WEBSOCKET_ERROR",
          error: error.message,
          roomId: context.roomId,
          participantId: context.participantId,
        });
      });

      await this.joinRoom(context, payload);
      this.logger.info("websocket connection accepted", {
        operation: "websocket.connect",
        durationMs: Date.now() - startedAt,
        roomId: context.roomId,
        participantId: context.participantId,
        outcome: "ok",
      });
      this.metrics.recordWebSocketConnect({
        outcome: "ok",
        errorCode: null,
        durationMs: Date.now() - startedAt,
      });
    } catch (error) {
      const contractError = toContractError(error);
      const expectedClientError = error instanceof SignalingError;
      this.logger[expectedClientError ? "warn" : "error"]("websocket connection rejected", {
        operation: "websocket.connect",
        durationMs: Date.now() - startedAt,
        errorCode: contractError.code,
        outcome: expectedClientError ? "client_error" : "failure",
      });
      this.send(socket, {
        type: "response",
        requestId: "connection",
        ok: false,
        error: contractError,
      });
      this.metrics.recordWebSocketConnect({
        outcome: expectedClientError ? "client_error" : "failure",
        errorCode: contractError.code,
        durationMs: Date.now() - startedAt,
      });
      socket.close(1008);
    }
  }

  private assertAllowedOrigin(request: IncomingMessage): void {
    if (this.config.allowedOrigins.length === 0) {
      return;
    }

    const origin = request.headers.origin;
    if (!origin || !this.config.allowedOrigins.includes(origin)) {
      this.logger.warn("websocket origin rejected", {
        operation: "websocket.connect",
        errorCode: "ORIGIN_NOT_ALLOWED",
        origin: origin ?? "missing",
      });
      throw new SignalingError("INVALID_TOKEN", "WebSocket origin is not allowed");
    }
  }

  private async handleMessage(context: ClientContext, raw: WebSocket.RawData): Promise<void> {
    if (this.isRateLimited(context)) {
      const requestId = this.extractRequestId(raw);
      this.logger.warn("websocket message rate limit exceeded", {
        operation: "websocket.command",
        commandId: requestId,
        participantId: context.participantId,
        roomId: context.roomId,
        limit: this.config.rateLimitMaxMessages,
        windowMs: this.config.rateLimitWindowMs,
        errorCode: "INVALID_PAYLOAD",
        outcome: "client_error",
      });
      this.metrics.recordWebSocketCommand({
        operation: "websocket.command",
        outcome: "client_error",
        errorCode: "INVALID_PAYLOAD",
        durationMs: 0,
      });
      this.sendError(context.socket, requestId, "INVALID_PAYLOAD", "Message rate limit exceeded");
      context.socket.close(1008, "Message rate limit exceeded");
      return;
    }

    let parsed: unknown;
    const startedAt = Date.now();
    try {
      parsed = JSON.parse(raw.toString());
    } catch {
      this.logger.warn("websocket command rejected", {
        operation: "websocket.command",
        commandId: "unknown",
        durationMs: Date.now() - startedAt,
        roomId: context.roomId,
        participantId: context.participantId,
        errorCode: "INVALID_PAYLOAD",
        outcome: "client_error",
      });
      this.metrics.recordWebSocketCommand({
        operation: "websocket.command",
        outcome: "client_error",
        errorCode: "INVALID_PAYLOAD",
        durationMs: Date.now() - startedAt,
      });
      this.sendError(context.socket, "unknown", "INVALID_PAYLOAD", "Payload must be valid JSON");
      return;
    }

    const result = clientWebSocketEventSchema.safeParse(parsed);
    if (!result.success) {
      const requestId =
        typeof parsed === "object" && parsed !== null && "requestId" in parsed && typeof parsed.requestId === "string"
          ? parsed.requestId
          : "unknown";
      this.logger.warn("websocket command rejected", {
        operation: "websocket.command",
        commandId: requestId,
        durationMs: Date.now() - startedAt,
        roomId: context.roomId,
        participantId: context.participantId,
        errorCode: "INVALID_PAYLOAD",
        outcome: "client_error",
      });
      this.metrics.recordWebSocketCommand({
        operation: "websocket.command",
        outcome: "client_error",
        errorCode: "INVALID_PAYLOAD",
        durationMs: Date.now() - startedAt,
      });
      this.sendError(context.socket, requestId, "INVALID_PAYLOAD", "Payload does not match WebSocket contract");
      return;
    }

    try {
      await this.routeEvent(context, result.data);
      this.logger.info("websocket command completed", {
        operation: result.data.type,
        commandId: result.data.requestId,
        durationMs: Date.now() - startedAt,
        roomId: context.roomId,
        participantId: context.participantId,
        outcome: "ok",
      });
      this.metrics.recordWebSocketCommand({
        operation: result.data.type,
        outcome: "ok",
        errorCode: null,
        durationMs: Date.now() - startedAt,
      });
    } catch (error) {
      const contractError = toContractError(error);
      const expectedClientError = error instanceof SignalingError;
      this.logger[expectedClientError ? "warn" : "error"]("websocket command failed", {
        operation: result.data.type,
        commandId: result.data.requestId,
        durationMs: Date.now() - startedAt,
        roomId: context.roomId,
        participantId: context.participantId,
        errorCode: contractError.code,
        outcome: expectedClientError ? "client_error" : "failure",
      });
      this.send(context.socket, {
        type: "response",
        requestId: result.data.requestId,
        ok: false,
        error: contractError,
      });
      this.metrics.recordWebSocketCommand({
        operation: result.data.type,
        outcome: expectedClientError ? "client_error" : "failure",
        errorCode: contractError.code,
        durationMs: Date.now() - startedAt,
      });
    }
  }

  private async routeEvent(context: ClientContext, event: ClientWebSocketEvent): Promise<void> {
    switch (event.type) {
      case "room:join": {
        this.assertJoined(context);
        this.send(context.socket, { type: "response", requestId: event.requestId, ok: true, data: {} });
        return;
      }

      case "mediasoup:getRouterRtpCapabilities": {
        this.assertJoined(context);
        const data = await this.roomManager.getRouterRtpCapabilities(context.roomId);
        this.send(context.socket, { type: "response", requestId: event.requestId, ok: true, data });
        return;
      }

      case "mediasoup:createWebRtcTransport": {
        this.assertJoined(context);
        const data = await this.roomManager.createTransport(context.roomId, context.participantId, event.direction);
        this.send(context.socket, { type: "response", requestId: event.requestId, ok: true, data });
        return;
      }

      case "mediasoup:connectTransport": {
        await this.roomManager.connectTransport(
          context.roomId,
          context.participantId,
          event.transportId,
          event.dtlsParameters,
        );
        this.send(context.socket, { type: "response", requestId: event.requestId, ok: true, data: {} });
        return;
      }

      case "mediasoup:produce": {
        const data = await this.roomManager.produce(
          context.roomId,
          context.participantId,
          event.transportId,
          event.kind,
          event.rtpParameters,
          event.appData.source,
          ({ producerId, source, participant }) => {
            this.broadcast(context.roomId, {
              type: "participant:mediaChanged",
              participantId: participant.participantId,
              media: participant.media,
            });
            this.broadcast(context.roomId, {
              type: "producer:closed",
              producerId,
              participantId: context.participantId,
              source,
            });
          },
        );
        this.send(context.socket, {
          type: "response",
          requestId: event.requestId,
          ok: true,
          data: { id: data.id },
        });
        this.broadcast(context.roomId, {
          type: "participant:mediaChanged",
          participantId: data.participant.participantId,
          media: data.participant.media,
        });
        this.broadcast(context.roomId, {
          type: "producer:added",
          producerId: data.id,
          participantId: context.participantId,
          kind: event.kind,
          source: event.appData.source,
        });
        return;
      }

      case "mediasoup:consume": {
        const data = await this.roomManager.consume(
          context.roomId,
          context.participantId,
          event.transportId,
          event.producerId,
          event.rtpCapabilities,
        );
        this.send(context.socket, { type: "response", requestId: event.requestId, ok: true, data });
        return;
      }

      case "mediasoup:closeProducer": {
        const closed = await this.roomManager.closeProducer(
          context.roomId,
          context.participantId,
          event.producerId,
        );
        this.send(context.socket, { type: "response", requestId: event.requestId, ok: true, data: {} });
        this.broadcast(context.roomId, {
          type: "participant:mediaChanged",
          participantId: closed.participant.participantId,
          media: closed.participant.media,
        });
        this.broadcast(context.roomId, {
          type: "producer:closed",
          producerId: closed.producerId,
          participantId: context.participantId,
          source: closed.source,
        });
        return;
      }

      case "mediasoup:setProducerPaused": {
        const result = await this.roomManager.setProducerPaused(
          context.roomId,
          context.participantId,
          event.producerId,
          event.paused,
        );
        this.send(context.socket, { type: "response", requestId: event.requestId, ok: true, data: {} });
        this.broadcast(context.roomId, {
          type: "participant:mediaChanged",
          participantId: result.participant.participantId,
          media: result.participant.media,
        });
        return;
      }

      case "participant:setConnectionQuality": {
        const participant = await this.roomManager.setConnectionQuality(
          context.roomId,
          context.participantId,
          event.quality,
        );
        this.send(context.socket, { type: "response", requestId: event.requestId, ok: true, data: {} });
        this.broadcast(context.roomId, {
          type: "participant:connectionQualityChanged",
          participantId: participant.participantId,
          quality: participant.connectionQuality,
        });
        return;
      }

      case "participant:kick": {
        this.assertJoined(context);
        const result = await this.roomManager.kickParticipantByCreator(
          context.roomId,
          context.participantId,
          event.targetParticipantId,
        );
        this.send(context.socket, { type: "response", requestId: event.requestId, ok: true, data: {} });
        this.broadcast(context.roomId, {
          type: "participant:kicked",
          participantId: result.target.participantId,
          targetDisplayName: result.target.displayName,
          actorParticipantId: result.actor.participantId,
          actorDisplayName: result.actor.displayName,
        });
        this.broadcast(context.roomId, {
          type: "participant:left",
          participantId: result.target.participantId,
        });
        if (result.chatMessage) {
          this.broadcast(context.roomId, { type: "chat:messageAdded", message: result.chatMessage });
        }
        this.closeParticipantSocket(context.roomId, result.target.participantId, "Removed by creator");
        return;
      }

      case "participant:disableMedia": {
        this.assertJoined(context);
        const result = await this.roomManager.disableParticipantMediaByCreator(
          context.roomId,
          context.participantId,
          event.targetParticipantId,
          event.source,
        );
        this.send(context.socket, { type: "response", requestId: event.requestId, ok: true, data: {} });
        this.broadcast(context.roomId, {
          type: "participant:mediaControlApplied",
          participantId: result.target.participantId,
          targetDisplayName: result.target.displayName,
          source: event.source,
          media: result.target.media,
          actorParticipantId: result.actor.participantId,
          actorDisplayName: result.actor.displayName,
        });
        this.broadcast(context.roomId, {
          type: "participant:mediaChanged",
          participantId: result.target.participantId,
          media: result.target.media,
        });
        for (const producerId of result.closedProducerIds ?? []) {
          this.broadcast(context.roomId, {
            type: "producer:closed",
            producerId,
            participantId: result.target.participantId,
            source: event.source,
          });
        }
        if (result.chatMessage) {
          this.broadcast(context.roomId, { type: "chat:messageAdded", message: result.chatMessage });
        }
        return;
      }

      case "participant:grantCreator": {
        this.assertJoined(context);
        const result = await this.roomManager.grantCreatorByCreator(
          context.roomId,
          context.participantId,
          event.targetParticipantId,
        );
        this.send(context.socket, { type: "response", requestId: event.requestId, ok: true, data: {} });
        this.broadcast(context.roomId, {
          type: "participant:creatorChanged",
          participantId: result.target.participantId,
          targetDisplayName: result.target.displayName,
          isCreator: result.target.isCreator,
          actorParticipantId: result.actor.participantId,
          actorDisplayName: result.actor.displayName,
        });
        if (result.chatMessage) {
          this.broadcast(context.roomId, { type: "chat:messageAdded", message: result.chatMessage });
        }
        return;
      }

      case "room:leave": {
        await this.leave(context);
        this.send(context.socket, { type: "response", requestId: event.requestId, ok: true, data: {} });
        context.socket.close(1000);
        return;
      }

      case "room:close": {
        this.assertJoined(context);
        await this.roomManager.closeRoomByCreator(context.roomId, context.participantId);
        this.send(context.socket, { type: "response", requestId: event.requestId, ok: true, data: {} });
        this.broadcast(context.roomId, { type: "room:closed", reason: "closed_by_creator" });
        this.closeRoomSockets(context.roomId, "Room closed");
        return;
      }

      case "chat:send": {
        this.assertJoined(context);
        const message = await this.roomManager.sendChatMessage(context.roomId, context.participantId, event.text);
        this.send(context.socket, { type: "response", requestId: event.requestId, ok: true, data: message });
        this.broadcast(context.roomId, { type: "chat:messageAdded", message });
        return;
      }
    }
  }

  private async joinRoom(context: ClientContext, payload: Parameters<typeof this.roomManager.join>[0]): Promise<void> {
    const { room, participant, participants, chatMessages, joinMessage } = await this.roomManager.join(payload);
    context.joined = true;
    const releasedParticipant = await this.replaceParticipantSocket(context);
    if (releasedParticipant) {
      this.broadcast(room.roomId, {
        type: "participant:mediaChanged",
        participantId: releasedParticipant.participantId,
        media: releasedParticipant.media,
      });
    }
    const snapshotParticipants = releasedParticipant
      ? participants.map((item) =>
          item.participantId === releasedParticipant.participantId ? releasedParticipant : item,
        )
      : participants;
    this.addRoomSocket(context.roomId, context.socket);

    this.send(context.socket, {
      type: "room:snapshot",
      roomId: room.roomId,
      participants: snapshotParticipants,
      chatMessages,
    });

    this.send(context.socket, {
      type: "participant:activeSpeakerChanged",
      participantId: this.roomManager.getActiveSpeakerParticipantId(room.roomId),
    });

    for (const producer of this.roomManager.listProducers(room.roomId)) {
      this.send(context.socket, {
        type: "producer:added",
        producerId: producer.producerId,
        participantId: producer.participantId,
        kind: producer.kind,
        source: producer.source,
      });
    }

    this.broadcast(room.roomId, { type: "participant:joined", participant }, context.socket);
    if (joinMessage) {
      this.broadcast(room.roomId, { type: "chat:messageAdded", message: joinMessage }, context.socket);
    }
  }

  private async leave(context: ClientContext): Promise<Participant | undefined> {
    if (!context.joined) {
      return undefined;
    }

    context.joined = false;
    // Fix 4: remove from roomSockets before broadcasting so the leaving socket
    // cannot receive subsequent events in the window before the close event fires.
    this.removeRoomSocket(context.roomId, context.socket);
    this.removeParticipantSocket(context);
    const { participant, chatMessage } = await this.roomManager.leave(context.roomId, context.participantId);
    if (participant) {
      this.broadcast(context.roomId, { type: "participant:left", participantId: context.participantId });
    }
    if (chatMessage) {
      this.broadcast(context.roomId, { type: "chat:messageAdded", message: chatMessage });
    }
    return participant;
  }

  private handleRoomClosedByServer(roomId: string, reason: RoomClosedReason): void {
    this.broadcast(roomId, { type: "room:closed", reason });
    this.closeRoomSockets(roomId, reason === "media_server_error" ? "Media server error" : "Room closed");
  }

  private async handleDisconnect(context: ClientContext): Promise<void> {
    const wasTracked = this.clients.delete(context.socket);
    if (!wasTracked) {
      return;
    }
    this.removeRoomSocket(context.roomId, context.socket);
    this.removeParticipantSocket(context);
    await this.leave(context);
    this.recordDisconnectOnce(context, "client_close");
  }

  private broadcast(roomId: string, event: ServerWebSocketEvent, except?: WebSocket): void {
    const sockets = this.roomSockets.get(roomId);
    if (!sockets) {
      return;
    }
    for (const socket of sockets) {
      if (socket !== except && socket.readyState === WebSocket.OPEN) {
        this.send(socket, event);
      }
    }
  }

  private addRoomSocket(roomId: string, socket: WebSocket): void {
    let sockets = this.roomSockets.get(roomId);
    if (!sockets) {
      sockets = new Set();
      this.roomSockets.set(roomId, sockets);
    }
    sockets.add(socket);
  }

  private async replaceParticipantSocket(context: ClientContext): Promise<Participant | undefined> {
    const key = this.participantSocketKey(context.roomId, context.participantId);
    const previousSocket = this.participantSockets.get(key);
    if (previousSocket && previousSocket !== context.socket) {
      const previousContext = this.clients.get(previousSocket);
      if (previousContext) {
        previousContext.joined = false;
        this.removeRoomSocket(previousContext.roomId, previousSocket);
        this.clients.delete(previousSocket);
      }
      const releasedParticipant = await this.roomManager.releaseParticipantMedia(
        context.roomId,
        context.participantId,
      );
      previousSocket.close(1000, "Session superseded");
      if (previousContext) {
        this.recordDisconnectOnce(previousContext, "server_close");
      }
      this.metrics.recordWebSocketReconnect({ outcome: "ok", reason: "session_superseded" });
      this.logger.info("websocket session superseded by reconnect", {
        operation: "websocket.reconnect",
        roomId: context.roomId,
        participantId: context.participantId,
        outcome: "ok",
      });
      this.participantSockets.set(key, context.socket);
      return releasedParticipant;
    }

    this.participantSockets.set(key, context.socket);
    return undefined;
  }

  private removeParticipantSocket(context: ClientContext): void {
    const key = this.participantSocketKey(context.roomId, context.participantId);
    if (this.participantSockets.get(key) === context.socket) {
      this.participantSockets.delete(key);
    }
  }

  private participantSocketKey(roomId: string, participantId: string): string {
    return `${roomId}:${participantId}`;
  }

  private removeRoomSocket(roomId: string, socket: WebSocket): void {
    const sockets = this.roomSockets.get(roomId);
    if (!sockets) {
      return;
    }
    sockets.delete(socket);
    if (sockets.size === 0) {
      this.roomSockets.delete(roomId);
    }
  }

  private closeRoomSockets(roomId: string, reason: string): void {
    const sockets = this.roomSockets.get(roomId);
    if (!sockets) {
      return;
    }

    for (const socket of Array.from(sockets)) {
      const context = this.clients.get(socket);
      if (context) {
        context.joined = false;
        this.removeParticipantSocket(context);
        this.clients.delete(socket);
      }
      sockets.delete(socket);
      socket.close(1000, reason);
      if (context) {
        this.recordDisconnectOnce(context, "server_close");
      } else {
        this.metrics.recordWebSocketDisconnect({ reason: "server_close" });
      }
    }

    this.roomSockets.delete(roomId);
  }

  private closeParticipantSocket(roomId: string, participantId: string, reason: string): void {
    const socket = this.participantSockets.get(this.participantSocketKey(roomId, participantId));
    if (!socket) {
      return;
    }

    const context = this.clients.get(socket);
    if (context) {
      context.joined = false;
      this.removeParticipantSocket(context);
      this.clients.delete(socket);
      this.removeRoomSocket(context.roomId, socket);
    }

    socket.close(1000, reason);
    if (context) {
      this.recordDisconnectOnce(context, "server_close");
    } else {
      this.metrics.recordWebSocketDisconnect({ reason: "server_close" });
    }
  }

  private send(socket: WebSocket, event: ServerWebSocketEvent): void {
    if (socket.readyState !== WebSocket.OPEN) {
      return;
    }

    if (socket.bufferedAmount > this.config.sendBackpressureBytes) {
      const context = this.clients.get(socket);
      this.logger.warn("websocket send buffer exceeded", {
        operation: "websocket.send",
        errorCode: "WEBSOCKET_BACKPRESSURE",
        bufferedAmount: socket.bufferedAmount,
        participantId: context?.participantId,
        roomId: context?.roomId,
      });
      socket.close(1013, "Backpressure limit exceeded");
      if (context) {
        this.recordDisconnectOnce(context, "backpressure");
      } else {
        this.metrics.recordWebSocketDisconnect({ reason: "backpressure" });
      }
      return;
    }

    socket.send(JSON.stringify(event), (error) => {
      if (error) {
        const context = this.clients.get(socket);
        this.logger.warn("websocket send failed", {
          operation: "websocket.send",
          errorCode: "WEBSOCKET_SEND_FAILED",
          error: error.message,
          participantId: context?.participantId,
          roomId: context?.roomId,
        });
      }
    });
  }

  private sendError(socket: WebSocket, requestId: string, code: ErrorCode, message: string): void {
    this.send(socket, {
      type: "response",
      requestId,
      ok: false,
      error: { code, message },
    });
  }

  private assertJoined(context: ClientContext): void {
    if (!context.joined) {
      throw new SignalingError("INVALID_TOKEN", "Participant is not joined");
    }
  }

  private isRateLimited(context: ClientContext): boolean {
    const now = Date.now();
    if (now - context.rateLimit.windowStartedAt >= this.config.rateLimitWindowMs) {
      context.rateLimit.windowStartedAt = now;
      context.rateLimit.messageCount = 0;
    }

    context.rateLimit.messageCount += 1;
    return context.rateLimit.messageCount > this.config.rateLimitMaxMessages;
  }

  private extractRequestId(raw: WebSocket.RawData): string {
    try {
      const parsed = JSON.parse(raw.toString()) as unknown;
      if (
        typeof parsed === "object" &&
        parsed !== null &&
        "requestId" in parsed &&
        typeof parsed.requestId === "string"
      ) {
        return parsed.requestId;
      }
    } catch {
      return "unknown";
    }

    return "unknown";
  }

  private checkHeartbeats(): void {
    for (const context of this.clients.values()) {
      if (context.socket.readyState !== WebSocket.OPEN) {
        continue;
      }

      if (!context.isAlive) {
        this.logger.warn("websocket heartbeat timeout", {
          operation: "websocket.heartbeat",
          errorCode: "WEBSOCKET_HEARTBEAT_TIMEOUT",
          participantId: context.participantId,
          roomId: context.roomId,
        });
        context.socket.terminate();
        this.recordDisconnectOnce(context, "heartbeat_timeout");
        continue;
      }

      context.isAlive = false;
      context.socket.ping();
    }
  }

  getRuntimeStats(): { activeSockets: number } {
    return { activeSockets: this.clients.size };
  }

  private recordDisconnectOnce(
    context: ClientContext,
    reason: "client_close" | "server_close" | "heartbeat_timeout" | "backpressure",
  ): void {
    if (context.disconnectMetricRecorded) {
      return;
    }

    context.disconnectMetricRecorded = true;
    this.metrics.recordWebSocketDisconnect({ reason });
  }
}
