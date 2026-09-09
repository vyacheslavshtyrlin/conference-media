import type { IceServer } from "#contracts";
import { cpus, networkInterfaces, type NetworkInterfaceInfo } from "node:os";

import { readEnv, readNumberEnv } from "./env.js";

export type SignalingConfig = {
  port: number;
  mediaNodeId: string;
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
    botAnnouncedIp?: string;
    rtcMinPort: number;
    rtcMaxPort: number;
    maxTransportsPerPeer: number;
    maxProducersPerPeer: number;
    maxConsumersPerPeer: number;
    iceServers: IceServer[];
  };
  recording?: {
    controlUrl: string;
    controlJwt: ServiceJwtConfig;
    bot: {
      workers: Array<{ workerId: string; controlUrl: string; capacity: number }>;
      workerLeaseTtlMs: number;
      workerHeartbeatMs: number;
      signalingUrl: string;
      controlJwt: ServiceJwtConfig;
    };
  };
  recordingBotAdmission?: {
    issuer: string;
    secret: string;
    mediaNodeId: string;
    maxTtlSeconds: number;
  };
};

export type ServiceJwtConfig = {
  issuer: string;
  audience: string;
  secret: string;
  maxTtlSeconds: number;
};

export function readSignalingConfig(): SignalingConfig {
  const announcedIp = process.env.MEDIASOUP_ANNOUNCED_IP;
  const resolvedAnnouncedIp =
    announcedIp && announcedIp.length > 0 ? announcedIp : undefined;
  const recording = readRecordingConfig();
  const mediaNodeId = readEnv("MEDIA_NODE_ID", "local");
  const recordingBotAdmission = readRecordingBotAdmissionConfig(mediaNodeId);
  const totalMediasoupWorkers = readPositiveNumberEnv(
    "MEDIASOUP_NUM_WORKERS",
    cpus().length,
  );

  const config: SignalingConfig = {
    port: readNumberEnv("PORT", 4000),
    mediaNodeId,
    redisUrl: readEnv("REDIS_URL", "redis://localhost:6379"),
    joinTokenSecret: readEnv(
      "JOIN_TOKEN_SECRET",
      "change-me-for-local-development",
    ),
    roomEmptyGraceMs: readPositiveNumberEnv("ROOM_EMPTY_GRACE_MS", 60_000),
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
      numWorkers: totalMediasoupWorkers,
      listenIp: readEnv("MEDIASOUP_LISTEN_IP", "0.0.0.0"),
      announcedIp: resolvedAnnouncedIp,
      botAnnouncedIp: resolveBotAnnouncedIp(
        process.env.MEDIASOUP_BOT_ANNOUNCED_IP,
        resolvedAnnouncedIp,
      ),
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

  if (recording) {
    config.recording = recording;
  }
  if (recordingBotAdmission) config.recordingBotAdmission = recordingBotAdmission;

  validateSignalingConfig(config);
  return config;
}

export function resolveBotAnnouncedIp(
  configuredValue: string | undefined,
  fallback: string | undefined,
  interfaces: NodeJS.Dict<NetworkInterfaceInfo[]> = networkInterfaces(),
): string | undefined {
  const configured = configuredValue?.trim();
  if (!configured || configured.toLowerCase() !== "auto") {
    return configured || fallback;
  }

  for (const addresses of Object.values(interfaces)) {
    const address = addresses?.find(
      (candidate) => candidate.family === "IPv4" && !candidate.internal,
    );
    if (address) return address.address;
  }
  throw new Error("MEDIASOUP_BOT_ANNOUNCED_IP=auto requires a non-loopback IPv4 address");
}

function readRecordingBotAdmissionConfig(mediaNodeId: string): SignalingConfig["recordingBotAdmission"] {
  const secret = process.env.RECORDING_BOT_ADMISSION_JWT_HS256_SECRET?.trim();
  if (!secret) return undefined;
  if (Buffer.byteLength(secret, "utf8") < 32) {
    throw new Error("RECORDING_BOT_ADMISSION_JWT_HS256_SECRET must contain at least 32 bytes");
  }
  return { issuer: "conference-api", secret, mediaNodeId, maxTtlSeconds: 60 };
}

export function validateSignalingConfig(config: SignalingConfig): void {
  assertPort("PORT", config.port);
  assertPort("MEDIASOUP_RTC_MIN_PORT", config.mediasoup.rtcMinPort);
  assertPort("MEDIASOUP_RTC_MAX_PORT", config.mediasoup.rtcMaxPort);

  if (config.mediasoup.rtcMinPort > config.mediasoup.rtcMaxPort) {
    throw new Error("MEDIASOUP_RTC_MIN_PORT must not be greater than MEDIASOUP_RTC_MAX_PORT");
  }
  if (config.mediaLease.refreshMs >= config.mediaLease.ttlMs) {
    throw new Error("MEDIA_LEASE_REFRESH_MS must be less than MEDIA_LEASE_TTL_MS");
  }
  if (config.recording && config.recording.bot.workerHeartbeatMs >= config.recording.bot.workerLeaseTtlMs) {
    throw new Error("RECORDING_WORKER_HEARTBEAT_MS must be less than RECORDING_WORKER_LEASE_TTL_MS");
  }
  try {
    const redisUrl = new URL(config.redisUrl);
    if (redisUrl.protocol !== "redis:" && redisUrl.protocol !== "rediss:") {
      throw new Error("unsupported protocol");
    }
  } catch {
    throw new Error("REDIS_URL must be a valid redis:// or rediss:// URL");
  }
}

function readRecordingConfig(): SignalingConfig["recording"] {
  const required = [
    "RECORDING_CONTROL_URL",
    "RECORDING_CONTROL_JWT_HS256_SECRET",
    "RECORDING_BOT_WORKERS_JSON",
    "RECORDING_BOT_SIGNALING_URL",
    "RECORDING_BOT_CONTROL_JWT_HS256_SECRET",
  ] as const;
  const configured = required.filter((name) => process.env[name]?.trim());
  if (configured.length === 0) {
    return undefined;
  }
  if (configured.length !== required.length) {
    throw new Error(`Recording configuration is incomplete; missing: ${required.filter((name) => !process.env[name]?.trim()).join(", ")}`);
  }

  return {
    controlUrl: readPrivateHttpUrl("RECORDING_CONTROL_URL"),
    controlJwt: readServiceJwt("RECORDING_CONTROL_JWT"),
    bot: {
      workers: readRecordingWorkers(),
      workerLeaseTtlMs: readPositiveNumberEnv("RECORDING_WORKER_LEASE_TTL_MS", 15_000),
      workerHeartbeatMs: readPositiveNumberEnv("RECORDING_WORKER_HEARTBEAT_MS", 5_000),
      signalingUrl: readPrivateWebSocketUrl("RECORDING_BOT_SIGNALING_URL"),
      controlJwt: readServiceJwt("RECORDING_BOT_CONTROL_JWT"),
    },
  };
}

function readRecordingWorkers(): Array<{ workerId: string; controlUrl: string; capacity: number }> {
  let value: unknown;
  try {
    value = JSON.parse(readEnv("RECORDING_BOT_WORKERS_JSON"));
  } catch {
    throw new Error("RECORDING_BOT_WORKERS_JSON must be valid JSON");
  }
  if (!Array.isArray(value) || value.length === 0) throw new Error("RECORDING_BOT_WORKERS_JSON must contain at least one worker");
  const ids = new Set<string>();
  return value.map((entry) => {
    if (typeof entry !== "object" || entry === null) throw new Error("Invalid recording worker");
    const candidate = entry as Record<string, unknown>;
    if (typeof candidate.workerId !== "string" || !/^[a-zA-Z0-9_.-]{1,80}$/.test(candidate.workerId) || ids.has(candidate.workerId)) {
      throw new Error("Recording worker IDs must be unique and safe");
    }
    if (typeof candidate.controlUrl !== "string") throw new Error("Recording worker controlUrl is required");
    if (!Number.isInteger(candidate.capacity) || (candidate.capacity as number) < 1) throw new Error("Recording worker capacity must be positive");
    ids.add(candidate.workerId);
    return {
      workerId: candidate.workerId,
      controlUrl: readPrivateHttpUrlValue(candidate.controlUrl, "recording worker controlUrl"),
      capacity: candidate.capacity as number,
    };
  });
}

function readPrivateHttpUrlValue(value: string, name: string): string {
  try {
    const url = new URL(value);
    if (url.protocol !== "http:" && url.protocol !== "https:") throw new Error("unsupported protocol");
    return url.toString().replace(/\/$/, "");
  } catch {
    throw new Error(`${name} must be a valid http:// or https:// URL`);
  }
}

function readServiceJwt(prefix: "RECORDING_CONTROL_JWT" | "RECORDING_BOT_CONTROL_JWT"): ServiceJwtConfig {
  const secret = readEnv(`${prefix}_HS256_SECRET`);
  if (Buffer.byteLength(secret, "utf8") < 32) {
    throw new Error(`${prefix}_HS256_SECRET must contain at least 32 bytes`);
  }

  if (prefix === "RECORDING_BOT_CONTROL_JWT") {
    return {
      issuer: "conference-signaling",
      audience: "conference-recording-bot-control",
      secret,
      maxTtlSeconds: 300,
    };
  }

  return {
    issuer: "conference-signaling",
    audience: "conference-recording-control",
    secret,
    maxTtlSeconds: 300,
  };
}
function readPrivateWebSocketUrl(name: string): string {
  const value = readEnv(name);
  try {
    const url = new URL(value);
    if (url.protocol !== "ws:" && url.protocol !== "wss:") throw new Error("unsupported protocol");
    return url.toString();
  } catch {
    throw new Error(`${name} must be a valid ws:// or wss:// URL`);
  }
}

function readPrivateHttpUrl(name: string): string {
  const value = readEnv(name);
  try {
    const url = new URL(value);
    if (url.protocol !== "http:" && url.protocol !== "https:") throw new Error("unsupported protocol");
    return url.toString().replace(/\/$/, "");
  } catch {
    throw new Error(`${name} must be a valid http:// or https:// URL`);
  }
}

function assertPort(name: string, value: number): void {
  if (!Number.isInteger(value) || value < 1 || value > 65_535) {
    throw new Error(`Environment variable ${name} must be an integer between 1 and 65535`);
  }
}

function readPositiveNumberEnv(name: string, fallback: number): number {
  const value = readNumberEnv(name, fallback);
  if (!Number.isInteger(value) || value <= 0) {
    throw new Error(`Environment variable ${name} must be a positive integer`);
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
