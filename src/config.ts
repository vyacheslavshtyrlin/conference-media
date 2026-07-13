import type { IceServer } from "#contracts";
import { cpus } from "node:os";

import { readEnv, readNumberEnv } from "./env.js";

export type SignalingConfig = {
  port: number;
  redisUrl: string;
  joinTokenSecret: string;
  roomEmptyGraceMs: number;
  shutdownTimeoutMs: number;
  mediaLease: {
    ttlMs: number;
    refreshMs: number;
  };
  websocket: {
    heartbeatIntervalMs: number;
    maxPayloadBytes: number;
    sendBackpressureBytes: number;
    rateLimitWindowMs: number;
    rateLimitMaxMessages: number;
    allowedOrigins: string[];
  };
  mediasoup: {
    numWorkers: number;
    listenIp: string;
    announcedIp?: string;
    rtcMinPort: number;
    rtcMaxPort: number;
    maxTransportsPerPeer: number;
    maxProducersPerPeer: number;
    maxConsumersPerPeer: number;
    iceServers: IceServer[];
  };
};

export function readSignalingConfig(): SignalingConfig {
  const announcedIp = process.env.MEDIASOUP_ANNOUNCED_IP;

  return {
    port: readNumberEnv("PORT", 4000),
    redisUrl: readEnv("REDIS_URL", "redis://localhost:6379"),
    joinTokenSecret: readEnv(
      "JOIN_TOKEN_SECRET",
      "change-me-for-local-development",
    ),
    roomEmptyGraceMs: readNumberEnv("ROOM_EMPTY_GRACE_MS", 60_000),
    shutdownTimeoutMs: readPositiveNumberEnv("SHUTDOWN_TIMEOUT_MS", 10_000),
    mediaLease: {
      ttlMs: readPositiveNumberEnv("MEDIA_LEASE_TTL_MS", 15_000),
      refreshMs: readPositiveNumberEnv("MEDIA_LEASE_REFRESH_MS", 5_000),
    },
    websocket: {
      heartbeatIntervalMs: readPositiveNumberEnv(
        "WS_HEARTBEAT_INTERVAL_MS",
        30_000,
      ),
      maxPayloadBytes: readPositiveNumberEnv("WS_MAX_PAYLOAD_BYTES", 64 * 1024),
      sendBackpressureBytes: readPositiveNumberEnv(
        "WS_SEND_BACKPRESSURE_BYTES",
        256 * 1024,
      ),
      rateLimitWindowMs: readPositiveNumberEnv(
        "WS_RATE_LIMIT_WINDOW_MS",
        10_000,
      ),
      rateLimitMaxMessages: readPositiveNumberEnv(
        "WS_RATE_LIMIT_MAX_MESSAGES",
        60,
      ),
      allowedOrigins: readCsvEnv("WS_ALLOWED_ORIGINS", "http://localhost:5173"),
    },
    mediasoup: {
      numWorkers: readPositiveNumberEnv("MEDIASOUP_NUM_WORKERS", cpus().length),
      listenIp: readEnv("MEDIASOUP_LISTEN_IP", "0.0.0.0"),
      announcedIp:
        announcedIp && announcedIp.length > 0 ? announcedIp : undefined,
      rtcMinPort: readNumberEnv("MEDIASOUP_RTC_MIN_PORT", 40000),
      rtcMaxPort: readNumberEnv("MEDIASOUP_RTC_MAX_PORT", 49999),
      maxTransportsPerPeer: readPositiveNumberEnv(
        "MEDIASOUP_MAX_TRANSPORTS_PER_PEER",
        4,
      ),
      maxProducersPerPeer: readPositiveNumberEnv(
        "MEDIASOUP_MAX_PRODUCERS_PER_PEER",
        3,
      ),
      maxConsumersPerPeer: readPositiveNumberEnv(
        "MEDIASOUP_MAX_CONSUMERS_PER_PEER",
        32,
      ),
      iceServers: readIceServers(),
    },
  };
}

function readPositiveNumberEnv(name: string, fallback: number): number {
  const value = readNumberEnv(name, fallback);
  if (value <= 0) {
    throw new Error(`Environment variable ${name} must be greater than 0`);
  }

  return value;
}

function readCsvEnv(name: string, fallback: string): string[] {
  return readEnv(name, fallback)
    .split(",")
    .map((value) => value.trim())
    .filter((value) => value.length > 0);
}

function readIceServers(): IceServer[] {
  const urls = readCsvEnv("TURN_URLS", process.env.TURN_URL ?? "");
  if (urls.length === 0) {
    return [];
  }

  const username = process.env.TURN_USERNAME?.trim();
  const credential = process.env.TURN_PASSWORD?.trim();
  const server: IceServer = { urls };

  if (username && credential) {
    server.username = username;
    server.credential = credential;
    server.credentialType = "password";
  }

  return [server];
}
