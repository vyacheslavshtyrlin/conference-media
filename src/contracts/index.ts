import { z } from "zod";

export const EMPTY_ROOM_TTL_SECONDS = 15 * 60;
export const CHAT_HISTORY_LIMIT = 500;
export const CHAT_MESSAGE_TEXT_MAX_LENGTH = 4000;

export const serviceNameSchema = z.enum(["web", "api", "signaling"]);

export const healthStatusSchema = z.object({
  status: z.literal("ok"),
  service: serviceNameSchema,
});

export type ServiceName = z.infer<typeof serviceNameSchema>;
export type HealthStatus = z.infer<typeof healthStatusSchema>;

export const appConfigResponseSchema = z.object({
  authCookieEnabled: z.boolean(),
  integrationApiEnabled: z.boolean(),
  loginUrl: z.string().url().nullable(),
  identity: z
    .object({
      displayName: z.string().min(1).max(80),
      avatarUrl: z.string().url().max(512).optional(),
    })
    .nullable(),
});

export type AppConfigResponse = z.infer<typeof appConfigResponseSchema>;

export const roomStatusSchema = z.enum(["active", "closed"]);
export const roomAccessPolicySchema = z.enum(["link", "approval", "admission"]);
export const roomExpiresAtSchema = z.string().datetime().nullable();
export const roomClosedReasonSchema = z.enum([
  "closed_by_creator",
  "empty_timeout",
  "server_shutdown",
  "media_server_error",
]);
export const roomClosedReasonValueSchema = roomClosedReasonSchema.nullable();

export const createRoomRequestSchema = z.object({
  accessPolicy: z.enum(["link", "approval"]).optional().default("link"),
}).strict();

export const createRoomResponseSchema = z.object({
  roomId: z.string().min(1),
  slug: z.string().min(1),
  joinUrl: z.string().url(),
  accessPolicy: roomAccessPolicySchema,
  participantId: z.string().min(1),
  roomSessionToken: z.string().min(1),
  displayName: z.string().min(1).nullable(),
  mediaNodeId: z.string().min(1),
  signalingUrl: z.string().url(),
  expiresAt: roomExpiresAtSchema,
});

export const getRoomResponseSchema = z.object({
  roomId: z.string().min(1),
  slug: z.string().min(1),
  expiresAt: roomExpiresAtSchema,
  status: roomStatusSchema,
  closedReason: roomClosedReasonValueSchema.optional().default(null),
  accessPolicy: roomAccessPolicySchema.default("link"),
  mediaNodeId: z.string().min(1),
  signalingUrl: z.string().url(),
  participantCount: z.number().int().nonnegative(),
});

export const joinRoomRequestSchema = z.object({
  displayName: z.string().trim().min(1).max(80),
  roomSessionToken: z.string().min(1).optional(),
});

export const joinRoomResponseSchema = z.object({
  participantId: z.string().min(1),
  isCreator: z.boolean(),
  token: z.string().min(1),
  roomSessionToken: z.string().min(1),
  signalingUrl: z.string().url(),
});

export const roomSessionRefreshRequestSchema = z.object({
  participantId: z.string().trim().min(1),
  roomSessionToken: z.string().min(1),
});

export const roomSessionRefreshResponseSchema = z.object({
  token: z.string().min(1),
  signalingUrl: z.string().url(),
});

export const createIntegrationRoomRequestSchema = z.object({
  externalRoomId: z.string().trim().min(1).max(128),
  title: z.string().trim().max(160).optional(),
  accessPolicy: z.literal("approval"),
  recordingRequested: z.boolean().optional().default(false),
  initialUser: z
    .object({
      id: z.string().trim().min(1).max(160),
      name: z.string().trim().min(1).max(80),
    })
    .optional(),
});

export const createIntegrationRoomResponseSchema = z.object({
  roomId: z.string().min(1),
  slug: z.string().min(1),
  mediaNodeId: z.string().min(1),
  signalingUrl: z.string().url(),
  expiresAt: roomExpiresAtSchema,
  status: roomStatusSchema,
  accessPolicy: z.literal("approval"),
  recordingId: z.string().min(1).nullable(),
  entryUrl: z.string().url().optional(),
  entryExpiresAt: z.string().datetime().optional(),
});

export const recordingStatusSchema = z.enum([
  "requested",
  "capturing",
  "finalizing",
  "ready",
  "failed",
  "cancelled",
  "deleted",
]);

export const recordingProjectionSchema = z.object({
  recordingId: z.string().min(1),
  status: recordingStatusSchema,
}).strict();

export const recordingControlProjectionSchema = z.object({
  integrationId: z.string().min(1),
  recordingId: z.string().min(1),
  requestedAt: z.string().datetime(),
  status: recordingStatusSchema,
}).strict();

export const recordingBotAdmissionResponseSchema = z.object({
  protocolVersion: z.literal("recording-bot.v1"),
  admission: z.string().min(1),
  expiresAt: z.string().datetime(),
}).strict();

export const integrationAdmissionIdentitySchema = z.object({
  kind: z.enum(["guest", "authenticated"]),
  externalSubject: z.string().trim().min(1).max(160),
  displayName: z.string().trim().min(1).max(80),
  avatarUrl: z.string().url().max(512).optional(),
});

export const createIntegrationAdmissionRequestSchema = z.object({
  identity: integrationAdmissionIdentitySchema,
  roomControls: z.enum(["creator", "none"]),
  expiresAt: z.string().datetime(),
});

export const createIntegrationAdmissionResponseSchema = z.object({
  entryUrl: z.string().url(),
  expiresAt: z.string().datetime(),
});

export const createRoomAdmissionRequestSchema = z.object({
  participantId: z.string().trim().min(1),
  roomSessionToken: z.string().min(1),
});

export const createRoomAdmissionResponseSchema = createIntegrationAdmissionResponseSchema;

export const exchangeAdmissionRequestSchema = z.object({
  code: z.string().trim().min(1).max(256),
  roomId: z.string().trim().min(1).optional(),
});

export const exchangeAdmissionResponseSchema = z.object({
  roomId: z.string().min(1),
  slug: z.string().min(1),
  participantId: z.string().min(1),
  roomSessionToken: z.string().min(1),
  displayName: z.string().min(1).nullable(),
  isCreator: z.boolean(),
  mediaNodeId: z.string().min(1),
  signalingUrl: z.string().url(),
  expiresAt: roomExpiresAtSchema,
});

export const joinRequestStatusSchema = z.enum(["pending", "approved", "denied", "expired"]);

export const createJoinRequestRequestSchema = z.object({
  displayName: z.string().trim().min(1).max(80),
});

export const createJoinRequestResponseSchema = z.object({
  requestId: z.string().min(1),
  requestToken: z.string().min(1),
  status: z.literal("pending"),
  displayName: z.string().min(1),
  expiresAt: z.string().datetime(),
});

export const joinRequestStatusRequestSchema = z.object({
  requestToken: z.string().min(1),
});

export const joinRequestStatusResponseSchema = z.object({
  requestId: z.string().min(1),
  status: joinRequestStatusSchema,
  displayName: z.string().min(1),
  participantId: z.string().min(1).optional(),
  roomSessionToken: z.string().min(1).optional(),
  isCreator: z.literal(false).optional(),
  expiresAt: z.string().datetime(),
});

export const creatorJoinRequestAuthSchema = z.object({
  participantId: z.string().trim().min(1),
  roomSessionToken: z.string().min(1),
});

export const listJoinRequestsRequestSchema = creatorJoinRequestAuthSchema;

export const pendingJoinRequestSchema = z.object({
  requestId: z.string().min(1),
  displayName: z.string().min(1),
  createdAt: z.string().datetime(),
  expiresAt: z.string().datetime(),
});

export const listJoinRequestsResponseSchema = z.object({
  requests: z.array(pendingJoinRequestSchema),
});

export const decideJoinRequestRequestSchema = creatorJoinRequestAuthSchema;

export const decideJoinRequestResponseSchema = z.object({
  requestId: z.string().min(1),
  status: z.enum(["approved", "denied"]),
});

export type RoomStatus = z.infer<typeof roomStatusSchema>;
export type RoomAccessPolicy = z.infer<typeof roomAccessPolicySchema>;
export type RoomClosedReason = z.infer<typeof roomClosedReasonSchema>;
export type CreateRoomRequest = z.infer<typeof createRoomRequestSchema>;
export type CreateRoomResponse = z.infer<typeof createRoomResponseSchema>;
export type GetRoomResponse = z.infer<typeof getRoomResponseSchema>;
export type JoinRoomRequest = z.infer<typeof joinRoomRequestSchema>;
export type JoinRoomResponse = z.infer<typeof joinRoomResponseSchema>;
export type RoomSessionRefreshRequest = z.infer<typeof roomSessionRefreshRequestSchema>;
export type RoomSessionRefreshResponse = z.infer<typeof roomSessionRefreshResponseSchema>;
export type CreateIntegrationRoomRequest = z.infer<typeof createIntegrationRoomRequestSchema>;
export type CreateIntegrationRoomResponse = z.infer<typeof createIntegrationRoomResponseSchema>;
export type RecordingStatus = z.infer<typeof recordingStatusSchema>;
export type RecordingProjection = z.infer<typeof recordingProjectionSchema>;
export type RecordingControlProjection = z.infer<typeof recordingControlProjectionSchema>;
export type RecordingBotAdmissionResponse = z.infer<typeof recordingBotAdmissionResponseSchema>;
export type IntegrationAdmissionIdentity = z.infer<typeof integrationAdmissionIdentitySchema>;
export type CreateIntegrationAdmissionRequest = z.infer<typeof createIntegrationAdmissionRequestSchema>;
export type CreateIntegrationAdmissionResponse = z.infer<typeof createIntegrationAdmissionResponseSchema>;
export type CreateRoomAdmissionRequest = z.infer<typeof createRoomAdmissionRequestSchema>;
export type CreateRoomAdmissionResponse = z.infer<typeof createRoomAdmissionResponseSchema>;
export type ExchangeAdmissionRequest = z.infer<typeof exchangeAdmissionRequestSchema>;
export type ExchangeAdmissionResponse = z.infer<typeof exchangeAdmissionResponseSchema>;
export type JoinRequestStatus = z.infer<typeof joinRequestStatusSchema>;
export type CreateJoinRequestRequest = z.infer<typeof createJoinRequestRequestSchema>;
export type CreateJoinRequestResponse = z.infer<typeof createJoinRequestResponseSchema>;
export type JoinRequestStatusRequest = z.infer<typeof joinRequestStatusRequestSchema>;
export type JoinRequestStatusResponse = z.infer<typeof joinRequestStatusResponseSchema>;
export type ListJoinRequestsRequest = z.infer<typeof listJoinRequestsRequestSchema>;
export type PendingJoinRequest = z.infer<typeof pendingJoinRequestSchema>;
export type ListJoinRequestsResponse = z.infer<typeof listJoinRequestsResponseSchema>;
export type DecideJoinRequestRequest = z.infer<typeof decideJoinRequestRequestSchema>;
export type DecideJoinRequestResponse = z.infer<typeof decideJoinRequestResponseSchema>;

export const mediaStateValueSchema = z.enum(["on", "off", "muted", "error"]);
export const mediaSourceSchema = z.enum(["mic", "camera", "screen"]);
export const connectionStateSchema = z.literal("online");
export const participantConnectionQualitySchema = z.enum(["good", "unstable", "poor", "reconnecting"]);
export const transportDirectionSchema = z.enum(["send", "recv"]);
export const mediaKindSchema = z.enum(["audio", "video"]);

export const participantMediaSchema = z.object({
  mic: mediaStateValueSchema,
  camera: mediaStateValueSchema,
  screen: mediaStateValueSchema,
});

export const participantSchema = z.object({
  participantId: z.string().min(1),
  displayName: z.string().min(1),
  isCreator: z.boolean(),
  joinedAt: z.string().datetime(),
  connectionState: connectionStateSchema,
  connectionQuality: participantConnectionQualitySchema,
  media: participantMediaSchema,
}).strict();

export type MediaStateValue = z.infer<typeof mediaStateValueSchema>;
export type MediaSource = z.infer<typeof mediaSourceSchema>;
export type ConnectionState = z.infer<typeof connectionStateSchema>;
export type ParticipantConnectionQuality = z.infer<typeof participantConnectionQualitySchema>;
export type TransportDirection = z.infer<typeof transportDirectionSchema>;
export type MediaKind = z.infer<typeof mediaKindSchema>;
export type ParticipantMedia = z.infer<typeof participantMediaSchema>;
export type Participant = z.infer<typeof participantSchema>;

export const userChatMessageSchema = z.object({
  messageId: z.string().min(1),
  roomId: z.string().min(1),
  kind: z.literal("user"),
  participantId: z.string().min(1),
  authorName: z.string().min(1),
  text: z.string().min(1).max(CHAT_MESSAGE_TEXT_MAX_LENGTH),
  createdAt: z.string().datetime(),
}).strict();

export const systemChatMessageSchema = z.object({
  messageId: z.string().min(1),
  roomId: z.string().min(1),
  kind: z.literal("system"),
  text: z.string().min(1).max(CHAT_MESSAGE_TEXT_MAX_LENGTH),
  createdAt: z.string().datetime(),
}).strict();

export const chatMessageSchema = z.discriminatedUnion("kind", [
  userChatMessageSchema,
  systemChatMessageSchema,
]);

export type UserChatMessage = z.infer<typeof userChatMessageSchema>;
export type SystemChatMessage = z.infer<typeof systemChatMessageSchema>;
export type ChatMessage = z.infer<typeof chatMessageSchema>;

export const errorCodeSchema = z.enum([
  "ROOM_NOT_FOUND",
  "ROOM_EXPIRED",
  "ROOM_CLOSED",
  "INVALID_TOKEN",
  "INVALID_PAYLOAD",
  "APPROVAL_REQUIRED",
  "AUTH_NOT_CONFIGURED",
  "INTEGRATION_AUTH_NOT_CONFIGURED",
  "INTEGRATION_CONFLICT",
  "INTEGRATION_FORBIDDEN",
  "INTEGRATION_NOT_IMPLEMENTED",
  "TRANSPORT_NOT_FOUND",
  "PRODUCER_NOT_FOUND",
  "MEDIASOUP_ERROR",
  "RATE_LIMITED",
]);

export const ERROR_CODES = errorCodeSchema.options;

export const contractErrorSchema = z.object({
  code: errorCodeSchema,
  message: z.string().min(1),
});

export type ErrorCode = z.infer<typeof errorCodeSchema>;
export type ContractError = z.infer<typeof contractErrorSchema>;

const requestIdSchema = z.string().min(1);
const mediasoupParametersSchema = z.record(z.string(), z.unknown());

export const roomJoinRequestEventSchema = z.object({
  type: z.literal("room:join"),
  requestId: requestIdSchema,
});

export const getRouterRtpCapabilitiesRequestEventSchema = z.object({
  type: z.literal("mediasoup:getRouterRtpCapabilities"),
  requestId: requestIdSchema,
});

export const createWebRtcTransportRequestEventSchema = z.object({
  type: z.literal("mediasoup:createWebRtcTransport"),
  requestId: requestIdSchema,
  direction: transportDirectionSchema,
});

export const iceServerSchema = z.object({
  urls: z.union([z.string().min(1), z.array(z.string().min(1)).min(1)]),
  username: z.string().min(1).optional(),
  credential: z.string().min(1).optional(),
  credentialType: z.enum(["password", "oauth"]).optional(),
});

export const createWebRtcTransportResponseDataSchema = z.object({
  id: z.string().min(1),
  iceParameters: mediasoupParametersSchema,
  iceCandidates: z.array(mediasoupParametersSchema),
  dtlsParameters: mediasoupParametersSchema,
  iceServers: z.array(iceServerSchema).optional(),
});

export const connectTransportRequestEventSchema = z.object({
  type: z.literal("mediasoup:connectTransport"),
  requestId: requestIdSchema,
  transportId: z.string().min(1),
  dtlsParameters: mediasoupParametersSchema,
});

export const produceRequestEventSchema = z.object({
  type: z.literal("mediasoup:produce"),
  requestId: requestIdSchema,
  transportId: z.string().min(1),
  kind: mediaKindSchema,
  rtpParameters: mediasoupParametersSchema,
  appData: z.object({
    source: mediaSourceSchema,
  }),
});

export const consumeRequestEventSchema = z.object({
  type: z.literal("mediasoup:consume"),
  requestId: requestIdSchema,
  transportId: z.string().min(1),
  producerId: z.string().min(1),
  rtpCapabilities: mediasoupParametersSchema,
});

export const closeProducerRequestEventSchema = z.object({
  type: z.literal("mediasoup:closeProducer"),
  requestId: requestIdSchema,
  producerId: z.string().min(1),
});

export const setProducerPausedRequestEventSchema = z.object({
  type: z.literal("mediasoup:setProducerPaused"),
  requestId: requestIdSchema,
  producerId: z.string().min(1),
  paused: z.boolean(),
});

export const setConnectionQualityRequestEventSchema = z.object({
  type: z.literal("participant:setConnectionQuality"),
  requestId: requestIdSchema,
  quality: participantConnectionQualitySchema,
});

export const kickParticipantRequestEventSchema = z.object({
  type: z.literal("participant:kick"),
  requestId: requestIdSchema,
  targetParticipantId: z.string().min(1),
});

export const disableParticipantMediaRequestEventSchema = z.object({
  type: z.literal("participant:disableMedia"),
  requestId: requestIdSchema,
  targetParticipantId: z.string().min(1),
  source: mediaSourceSchema,
});

export const grantParticipantCreatorRequestEventSchema = z.object({
  type: z.literal("participant:grantCreator"),
  requestId: requestIdSchema,
  targetParticipantId: z.string().min(1),
});

export const roomLeaveRequestEventSchema = z.object({
  type: z.literal("room:leave"),
  requestId: requestIdSchema,
});

export const roomCloseRequestEventSchema = z.object({
  type: z.literal("room:close"),
  requestId: requestIdSchema,
});

export const chatSendRequestEventSchema = z.object({
  type: z.literal("chat:send"),
  requestId: requestIdSchema,
  text: z.string().trim().min(1).max(CHAT_MESSAGE_TEXT_MAX_LENGTH),
});

export const clientWebSocketEventSchema = z.discriminatedUnion("type", [
  roomJoinRequestEventSchema,
  getRouterRtpCapabilitiesRequestEventSchema,
  createWebRtcTransportRequestEventSchema,
  connectTransportRequestEventSchema,
  produceRequestEventSchema,
  consumeRequestEventSchema,
  closeProducerRequestEventSchema,
  setProducerPausedRequestEventSchema,
  setConnectionQualityRequestEventSchema,
  kickParticipantRequestEventSchema,
  disableParticipantMediaRequestEventSchema,
  grantParticipantCreatorRequestEventSchema,
  roomLeaveRequestEventSchema,
  roomCloseRequestEventSchema,
  chatSendRequestEventSchema,
]);

export type RoomJoinRequestEvent = z.infer<typeof roomJoinRequestEventSchema>;
export type GetRouterRtpCapabilitiesRequestEvent = z.infer<
  typeof getRouterRtpCapabilitiesRequestEventSchema
>;
export type CreateWebRtcTransportRequestEvent = z.infer<
  typeof createWebRtcTransportRequestEventSchema
>;
export type IceServer = z.infer<typeof iceServerSchema>;
export type CreateWebRtcTransportResponseData = z.infer<typeof createWebRtcTransportResponseDataSchema>;
export type ConnectTransportRequestEvent = z.infer<typeof connectTransportRequestEventSchema>;
export type ProduceRequestEvent = z.infer<typeof produceRequestEventSchema>;
export type ConsumeRequestEvent = z.infer<typeof consumeRequestEventSchema>;
export type CloseProducerRequestEvent = z.infer<typeof closeProducerRequestEventSchema>;
export type SetProducerPausedRequestEvent = z.infer<typeof setProducerPausedRequestEventSchema>;
export type SetConnectionQualityRequestEvent = z.infer<typeof setConnectionQualityRequestEventSchema>;
export type KickParticipantRequestEvent = z.infer<typeof kickParticipantRequestEventSchema>;
export type DisableParticipantMediaRequestEvent = z.infer<typeof disableParticipantMediaRequestEventSchema>;
export type GrantParticipantCreatorRequestEvent = z.infer<typeof grantParticipantCreatorRequestEventSchema>;
export type RoomLeaveRequestEvent = z.infer<typeof roomLeaveRequestEventSchema>;
export type RoomCloseRequestEvent = z.infer<typeof roomCloseRequestEventSchema>;
export type ChatSendRequestEvent = z.infer<typeof chatSendRequestEventSchema>;
export type ClientWebSocketEvent = z.infer<typeof clientWebSocketEventSchema>;

export const responseSuccessEventSchema = z.object({
  type: z.literal("response"),
  requestId: requestIdSchema,
  ok: z.literal(true),
  data: z.unknown(),
}).strict();

export const responseErrorEventSchema = z.object({
  type: z.literal("response"),
  requestId: requestIdSchema,
  ok: z.literal(false),
  error: contractErrorSchema,
}).strict();

export const responseEventSchema = z.discriminatedUnion("ok", [
  responseSuccessEventSchema,
  responseErrorEventSchema,
]);

export const roomSnapshotEventSchema = z.object({
  type: z.literal("room:snapshot"),
  roomId: z.string().min(1),
  participants: z.array(participantSchema),
  chatMessages: z.array(chatMessageSchema),
}).strict();

export const participantJoinedEventSchema = z.object({
  type: z.literal("participant:joined"),
  participant: participantSchema,
}).strict();

export const participantLeftEventSchema = z.object({
  type: z.literal("participant:left"),
  participantId: z.string().min(1),
}).strict();

export const participantMediaChangedEventSchema = z.object({
  type: z.literal("participant:mediaChanged"),
  participantId: z.string().min(1),
  media: participantMediaSchema,
}).strict();

export const participantActiveSpeakerChangedEventSchema = z.object({
  type: z.literal("participant:activeSpeakerChanged"),
  participantId: z.string().min(1).nullable(),
}).strict();

export const participantConnectionQualityChangedEventSchema = z.object({
  type: z.literal("participant:connectionQualityChanged"),
  participantId: z.string().min(1),
  quality: participantConnectionQualitySchema,
}).strict();

export const participantMediaControlAppliedEventSchema = z.object({
  type: z.literal("participant:mediaControlApplied"),
  participantId: z.string().min(1),
  targetDisplayName: z.string().min(1),
  source: mediaSourceSchema,
  media: participantMediaSchema,
  actorParticipantId: z.string().min(1),
  actorDisplayName: z.string().min(1),
}).strict();

export const participantCreatorChangedEventSchema = z.object({
  type: z.literal("participant:creatorChanged"),
  participantId: z.string().min(1),
  targetDisplayName: z.string().min(1),
  isCreator: z.boolean(),
  actorParticipantId: z.string().min(1),
  actorDisplayName: z.string().min(1),
}).strict();

export const participantKickedEventSchema = z.object({
  type: z.literal("participant:kicked"),
  participantId: z.string().min(1),
  targetDisplayName: z.string().min(1),
  actorParticipantId: z.string().min(1),
  actorDisplayName: z.string().min(1),
}).strict();

export const producerAddedEventSchema = z.object({
  type: z.literal("producer:added"),
  producerId: z.string().min(1),
  participantId: z.string().min(1),
  kind: mediaKindSchema,
  source: mediaSourceSchema,
}).strict();

export const producerClosedEventSchema = z.object({
  type: z.literal("producer:closed"),
  producerId: z.string().min(1),
  participantId: z.string().min(1),
  source: mediaSourceSchema,
}).strict();

export const roomClosedEventSchema = z.object({
  type: z.literal("room:closed"),
  reason: roomClosedReasonSchema,
}).strict();

export const chatMessageAddedEventSchema = z.object({
  type: z.literal("chat:messageAdded"),
  message: chatMessageSchema,
}).strict();

export const serverWebSocketEventSchema = z.union([
  responseEventSchema,
  roomSnapshotEventSchema,
  participantJoinedEventSchema,
  participantLeftEventSchema,
  participantMediaChangedEventSchema,
  participantActiveSpeakerChangedEventSchema,
  participantConnectionQualityChangedEventSchema,
  participantMediaControlAppliedEventSchema,
  participantCreatorChangedEventSchema,
  participantKickedEventSchema,
  producerAddedEventSchema,
  producerClosedEventSchema,
  roomClosedEventSchema,
  chatMessageAddedEventSchema,
]);

export type ResponseSuccessEvent = z.infer<typeof responseSuccessEventSchema>;
export type ResponseErrorEvent = z.infer<typeof responseErrorEventSchema>;
export type ResponseEvent = z.infer<typeof responseEventSchema>;
export type RoomSnapshotEvent = z.infer<typeof roomSnapshotEventSchema>;
export type ParticipantJoinedEvent = z.infer<typeof participantJoinedEventSchema>;
export type ParticipantLeftEvent = z.infer<typeof participantLeftEventSchema>;
export type ParticipantMediaChangedEvent = z.infer<typeof participantMediaChangedEventSchema>;
export type ParticipantActiveSpeakerChangedEvent = z.infer<typeof participantActiveSpeakerChangedEventSchema>;
export type ParticipantConnectionQualityChangedEvent = z.infer<typeof participantConnectionQualityChangedEventSchema>;
export type ParticipantMediaControlAppliedEvent = z.infer<typeof participantMediaControlAppliedEventSchema>;
export type ParticipantCreatorChangedEvent = z.infer<typeof participantCreatorChangedEventSchema>;
export type ParticipantKickedEvent = z.infer<typeof participantKickedEventSchema>;
export type ProducerAddedEvent = z.infer<typeof producerAddedEventSchema>;
export type ProducerClosedEvent = z.infer<typeof producerClosedEventSchema>;
export type RoomClosedEvent = z.infer<typeof roomClosedEventSchema>;
export type ChatMessageAddedEvent = z.infer<typeof chatMessageAddedEventSchema>;
export type ServerWebSocketEvent = z.infer<typeof serverWebSocketEventSchema>;

export const roomMetadataSchema = z.object({
  roomId: z.string().min(1),
  slug: z.string().min(1),
  mediaNodeId: z.string().min(1),
  signalingUrl: z.string().url(),
  createdAt: z.string().datetime(),
  expiresAt: roomExpiresAtSchema,
  status: roomStatusSchema,
  closedReason: roomClosedReasonValueSchema.optional().default(null),
  accessPolicy: roomAccessPolicySchema.default("link"),
});

export type RoomMetadata = z.infer<typeof roomMetadataSchema>;
