import { EventEmitter } from "node:events";
import { createServer, type Server } from "node:http";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { readSignalingConfig } from "./config.js";
import { createSignalingHttpHandler, type SignalingReadiness } from "./httpEndpoints.js";
import { createLogger } from "./logger.js";
import { MediasoupService } from "./mediasoupService.js";
import { formatMetricLabels, MediaMetrics } from "./metrics.js";

const { createWorker } = vi.hoisted(() => ({
  createWorker: vi.fn(),
}));

vi.mock("mediasoup", () => ({
  createWorker,
}));

const originalEnv = { ...process.env };
const servers: Server[] = [];

beforeEach(() => {
  process.env = { ...originalEnv };
  createWorker.mockReset();
});

afterEach(async () => {
  await Promise.all(
    servers.splice(0).map(
      (server) =>
        new Promise<void>((resolve, reject) => {
          server.close((error) => (error ? reject(error) : resolve()));
        }),
    ),
  );
  vi.restoreAllMocks();
  process.env = { ...originalEnv };
});

describe("E5-T6B media observability regression suite", () => {
  it("fails invalid startup configuration before serving traffic", () => {
    process.env.REDIS_URL = "http://localhost:6379";
    expect(readSignalingConfig).toThrow("REDIS_URL must be a valid redis:// or rediss:// URL");

    process.env.REDIS_URL = "redis://localhost:6379";
    process.env.MEDIA_LEASE_TTL_MS = "1000";
    process.env.MEDIA_LEASE_REFRESH_MS = "1000";
    expect(readSignalingConfig).toThrow("MEDIA_LEASE_REFRESH_MS must be less than MEDIA_LEASE_TTL_MS");

    process.env.MEDIA_LEASE_REFRESH_MS = "500";
    process.env.MEDIASOUP_RTC_MIN_PORT = "50000";
    process.env.MEDIASOUP_RTC_MAX_PORT = "40000";
    expect(readSignalingConfig).toThrow("MEDIASOUP_RTC_MIN_PORT must not be greater than MEDIASOUP_RTC_MAX_PORT");
  });

  it("reflects Redis readiness loss and recovery through the stable readiness envelope", async () => {
    let redisReady = false;
    const readiness: SignalingReadiness = {
      isRedisReady: async () => redisReady,
      isMediasoupReady: () => true,
      isMediaLeaseReady: () => true,
    };

    const unavailable = await request(readiness, "/ready");
    expect(unavailable.status).toBe(503);
    await expect(unavailable.json()).resolves.toMatchObject({
      status: "unavailable",
      service: "signaling",
      checks: { redis: "unavailable", mediasoup: "ok", mediaLease: "ok" },
    });

    redisReady = true;

    const ready = await request(readiness, "/ready");
    expect(ready.status).toBe(200);
    await expect(ready.json()).resolves.toEqual({
      status: "ok",
      service: "signaling",
      checks: { redis: "ok", mediasoup: "ok", mediaLease: "ok" },
    });
  });

  it("marks media readiness false when a dead worker cannot be replaced", async () => {
    const firstWorker = createFakeWorker();
    createWorker.mockResolvedValueOnce(firstWorker).mockRejectedValueOnce(new Error("replacement failed"));
    const service = new MediasoupService({
      numWorkers: 1,
      listenIp: "127.0.0.1",
      rtcMinPort: 40000,
      rtcMaxPort: 40100,
      maxTransportsPerPeer: 4,
      maxProducersPerPeer: 3,
      maxConsumersPerPeer: 32,
      iceServers: [],
    });
    await service.init();

    firstWorker.emit("died");
    await new Promise((resolve) => setTimeout(resolve, 0));

    expect(service.getRuntimeStats().workers).toBe(0);
    expect(service.isReady()).toBe(false);
  });

  it("renders low-cardinality metrics and rejects forbidden labels", () => {
    const metrics = new MediaMetrics();

    metrics.recordWebSocketCommand({
      operation: "chat:send",
      outcome: "client_error",
      errorCode: "INVALID_PAYLOAD",
      durationMs: 8,
    });
    metrics.recordRedisOperation({
      operation: "get_room_metadata",
      outcome: "failure",
      durationMs: 3,
    });
    metrics.recordRoomCleanup({
      operation: "room_dead",
      outcome: "failure",
      reason: "media_server_error",
    });

    const body = metrics.renderPrometheus({
      activeRooms: 1,
      activeParticipants: 2,
      activeSockets: 2,
      workers: 1,
      transports: 3,
      producers: 2,
      consumers: 4,
    });

    expect(body).toContain("signaling_active_rooms 1");
    expect(body).toContain("mediasoup_workers 1");
    expect(body).toContain(
      'signaling_websocket_commands_total{error_code="INVALID_PAYLOAD",operation="chat:send",outcome="client_error"} 1',
    );
    expect(body).toContain('signaling_redis_operations_total{operation="get_room_metadata",outcome="failure"} 1');
    expect(body).toContain(
      'signaling_room_cleanup_total{operation="room_dead",outcome="failure",reason="media_server_error"} 1',
    );
    expect(body).toContain('nodejs_event_loop_lag_seconds{stat="mean"}');
    expect(body).not.toContain("roomId=");
    expect(body).not.toContain("participantId=");
    expect(body).not.toContain("requestId=");
    expect(body).not.toContain("commandId=");
    expect(body).not.toContain("token=");

    expect(() => formatMetricLabels({ roomId: "room_1" })).toThrow("Forbidden high-cardinality metric label");
    expect(() => formatMetricLabels({ participantId: "peer_1" })).toThrow("Forbidden high-cardinality metric label");
    expect(() => formatMetricLabels({ requestId: "req_1" })).toThrow("Forbidden high-cardinality metric label");
    expect(() => formatMetricLabels({ commandId: "cmd_1" })).toThrow("Forbidden high-cardinality metric label");
    expect(() => formatMetricLabels({ token: "secret" })).toThrow("Forbidden high-cardinality metric label");
  });

  it("keeps command correlation in structured logs while redacting credentials", () => {
    const write = vi.spyOn(console, "info").mockImplementation(() => undefined);
    const logger = createLogger("signaling");

    logger.info("websocket command completed", {
      operation: "chat:send",
      commandId: "command-123",
      durationMs: 12,
      roomId: "room_1",
      participantId: "peer_1",
      token: "join-token",
      websocketUrl: "wss://example.test/ws?token=join-token",
      headers: { authorization: "Bearer private-token" },
      outcome: "ok",
    });

    expect(write).toHaveBeenCalledOnce();
    const serialized = write.mock.calls[0]?.[0];
    if (typeof serialized !== "string") {
      throw new Error("Logger did not write a serialized payload");
    }
    const payload = JSON.parse(serialized) as Record<string, unknown>;

    expect(payload).toMatchObject({
      service: "signaling",
      severity: "info",
      operation: "chat:send",
      commandId: "command-123",
      durationMs: 12,
      roomId: "room_1",
      participantId: "peer_1",
      token: "[REDACTED]",
      websocketUrl: "wss://example.test/ws",
      headers: { authorization: "[REDACTED]" },
      outcome: "ok",
    });
    expect(payload.timestamp).toEqual(expect.any(String));
    expect(serialized).not.toContain("join-token");
    expect(serialized).not.toContain("private-token");
  });
});

function createFakeWorker(): EventEmitter & { close: () => void; createRouter: () => Promise<never>; createWebRtcServer: () => Promise<{ close: () => void }> } {
  const worker = new EventEmitter() as EventEmitter & {
    close: () => void;
    createRouter: () => Promise<never>;
    createWebRtcServer: () => Promise<{ close: () => void }>;
  };
  worker.close = () => undefined;
  worker.createRouter = async () => {
    throw new Error("not needed for this regression test");
  };
  worker.createWebRtcServer = async () => ({ close: () => undefined });
  return worker;
}

async function request(readiness: SignalingReadiness, path: string): Promise<Response> {
  const server = createServer(createSignalingHttpHandler(readiness));
  servers.push(server);
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  if (!address || typeof address === "string") {
    throw new Error("Test server did not bind a TCP port");
  }
  return fetch(`http://127.0.0.1:${address.port}${path}`);
}
