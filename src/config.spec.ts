import { afterEach, describe, expect, it } from "vitest";

import { readSignalingConfig, resolveBotAnnouncedIp } from "./config.js";

const originalEnv = { ...process.env };

afterEach(() => {
  process.env = { ...originalEnv };
});

describe("readSignalingConfig", () => {
  it("rejects short join-token secrets outside the intentional development default", () => {
    process.env.NODE_ENV = "development";
    process.env.JOIN_TOKEN_SECRET = "short-secret";

    expect(readSignalingConfig).toThrow("JOIN_TOKEN_SECRET must contain at least 32 bytes");
  });

  it("rejects placeholder join-token secrets in production", () => {
    process.env.NODE_ENV = "production";

    for (const secret of [
      "change-me-for-local-development",
      "prefix-change_me-suffix-at-least-32-bytes",
      "replace-with-at-least-32-random-characters",
      "production-placeholder-secret-at-least-32-bytes",
      "example-secret-that-is-at-least-32-bytes",
      "production-local-development-secret-32-bytes",
    ]) {
      process.env.JOIN_TOKEN_SECRET = secret;
      expect(readSignalingConfig).toThrow(
        "JOIN_TOKEN_SECRET must not use a placeholder or local-development value",
      );
    }
  });

  it("preserves the intentional join-token default in development", () => {
    process.env.NODE_ENV = "development";
    delete process.env.JOIN_TOKEN_SECRET;

    expect(readSignalingConfig().joinTokenSecret).toBe("change-me-for-local-development");
  });

  it("rejects the committed replace-with marker outside production", () => {
    process.env.NODE_ENV = "development";
    process.env.JOIN_TOKEN_SECRET = "replace-with-at-least-32-random-characters";

    expect(readSignalingConfig).toThrow(
      "JOIN_TOKEN_SECRET must not use a placeholder or local-development value",
    );
  });

  it("does not reject random secrets that merely contain example", () => {
    process.env.NODE_ENV = "production";
    process.env.JOIN_TOKEN_SECRET = "randomexamplevalue-with-at-least-32-bytes";

    expect(readSignalingConfig().joinTokenSecret).toBe(
      "randomexamplevalue-with-at-least-32-bytes",
    );
  });

  it("allows the root-local change-me recording secrets outside production", () => {
    Object.assign(process.env, {
      NODE_ENV: "development",
      RECORDING_CONTROL_URL: "http://api:3000/internal/v1",
      RECORDING_CONTROL_JWT_HS256_SECRET: "change-me-recording-control-secret-32-bytes",
      RECORDING_BOT_WORKERS_JSON: JSON.stringify([{ workerId: "worker-a", controlUrl: "http://worker-a:8090", capacity: 1 }]),
      RECORDING_BOT_SIGNALING_URL: "ws://127.0.0.1:4000/internal/recording-bot/v1",
      RECORDING_BOT_CONTROL_JWT_HS256_SECRET: "change-me-recording-bot-control-secret-32-bytes",
      RECORDING_BOT_ADMISSION_JWT_HS256_SECRET: "change-me-recording-bot-admission-secret-32-bytes",
    });

    expect(readSignalingConfig()).toMatchObject({
      recording: {
        controlJwt: { secret: "change-me-recording-control-secret-32-bytes" },
        bot: {
          controlJwt: { secret: "change-me-recording-bot-control-secret-32-bytes" },
        },
      },
      recordingBotAdmission: {
        secret: "change-me-recording-bot-admission-secret-32-bytes",
      },
    });
  });

  it("rejects a lease refresh interval that cannot keep the lease alive", () => {
    process.env.MEDIA_LEASE_TTL_MS = "1000";
    process.env.MEDIA_LEASE_REFRESH_MS = "1000";

    expect(readSignalingConfig).toThrow("MEDIA_LEASE_REFRESH_MS must be less than MEDIA_LEASE_TTL_MS");
  });

  it("rejects an inverted mediasoup RTC port range", () => {
    process.env.MEDIASOUP_RTC_MIN_PORT = "40001";
    process.env.MEDIASOUP_RTC_MAX_PORT = "40000";

    expect(readSignalingConfig).toThrow("MEDIASOUP_RTC_MIN_PORT must not be greater than MEDIASOUP_RTC_MAX_PORT");
  });

  it("rejects a non-Redis URL", () => {
    process.env.REDIS_URL = "http://localhost:6379";

    expect(readSignalingConfig).toThrow("REDIS_URL must be a valid redis:// or rediss:// URL");
  });

  it("rejects fractional worker and limit values", () => {
    process.env.MEDIASOUP_NUM_WORKERS = "1.5";
    expect(readSignalingConfig).toThrow("Environment variable MEDIASOUP_NUM_WORKERS must be a positive integer");

    process.env.MEDIASOUP_NUM_WORKERS = "1";
    process.env.WS_RATE_LIMIT_MAX_MESSAGES = "2.5";
    expect(readSignalingConfig).toThrow("Environment variable WS_RATE_LIMIT_MAX_MESSAGES must be a positive integer");
  });

  it("rejects an incomplete bot recording configuration", () => {
    process.env.RECORDING_BOT_WORKERS_JSON = JSON.stringify([{ workerId: "worker-a", controlUrl: "http://worker-a:8090", capacity: 1 }]);

    expect(readSignalingConfig).toThrow("Recording configuration is incomplete");
  });

  it("reads the private WebRTC bot recording configuration", () => {
    Object.assign(process.env, {
      RECORDING_CONTROL_URL: "http://api:3000/internal/v1",
      RECORDING_CONTROL_JWT_ISSUER: "conference-signaling",
      RECORDING_CONTROL_JWT_AUDIENCE: "conference-recording-control",
      RECORDING_CONTROL_JWT_HS256_SECRET: "x".repeat(32),
      RECORDING_BOT_WORKERS_JSON: JSON.stringify([{ workerId: "worker-a", controlUrl: "http://worker-a:8090", capacity: 1 }]),
      RECORDING_BOT_SIGNALING_URL: "ws://127.0.0.1:4000/internal/recording-bot/v1",
      RECORDING_BOT_CONTROL_JWT_HS256_SECRET: "y".repeat(32),
    });

    expect(readSignalingConfig().recording).toMatchObject({
      controlUrl: "http://api:3000/internal/v1",
      bot: {
        workers: [{ workerId: "worker-a", controlUrl: "http://worker-a:8090", capacity: 1 }],
        signalingUrl: "ws://127.0.0.1:4000/internal/recording-bot/v1",
        controlJwt: {
          issuer: "conference-signaling",
          audience: "conference-recording-bot-control",
          maxTtlSeconds: 300,
        },
      },
    });
  });

  it("rejects placeholder recording secrets in production", () => {
    Object.assign(process.env, {
      NODE_ENV: "production",
      JOIN_TOKEN_SECRET: "production-join-token-secret-with-32-bytes",
      RECORDING_CONTROL_URL: "http://api:3000/internal/v1",
      RECORDING_CONTROL_JWT_HS256_SECRET: "replace-with-at-least-32-random-characters",
      RECORDING_BOT_WORKERS_JSON: JSON.stringify([{ workerId: "worker-a", controlUrl: "http://worker-a:8090", capacity: 1 }]),
      RECORDING_BOT_SIGNALING_URL: "ws://127.0.0.1:4000/internal/recording-bot/v1",
      RECORDING_BOT_CONTROL_JWT_HS256_SECRET: "production-bot-control-secret-at-least-32-bytes",
    });

    expect(readSignalingConfig).toThrow(
      "RECORDING_CONTROL_JWT_HS256_SECRET must not use a placeholder or local-development value",
    );

    process.env.RECORDING_CONTROL_JWT_HS256_SECRET = "production-recording-control-secret-32-bytes";
    process.env.RECORDING_BOT_CONTROL_JWT_HS256_SECRET = "replace-with-at-least-32-random-characters";
    expect(readSignalingConfig).toThrow(
      "RECORDING_BOT_CONTROL_JWT_HS256_SECRET must not use a placeholder or local-development value",
    );
  });

  it("rejects a placeholder bot-admission secret in production", () => {
    Object.assign(process.env, {
      NODE_ENV: "production",
      JOIN_TOKEN_SECRET: "production-join-token-secret-with-32-bytes",
      RECORDING_BOT_ADMISSION_JWT_HS256_SECRET: "replace-with-at-least-32-random-characters",
    });

    expect(readSignalingConfig).toThrow(
      "RECORDING_BOT_ADMISSION_JWT_HS256_SECRET must not use a placeholder or local-development value",
    );
  });

  it("keeps every configured mediasoup worker available for live rooms", () => {
    Object.assign(process.env, {
      MEDIASOUP_NUM_WORKERS: "3",
      RECORDING_CONTROL_URL: "http://api:3000/internal/v1",
      RECORDING_CONTROL_JWT_ISSUER: "conference-signaling",
      RECORDING_CONTROL_JWT_AUDIENCE: "conference-recording-control",
      RECORDING_CONTROL_JWT_HS256_SECRET: "x".repeat(32),
      RECORDING_BOT_WORKERS_JSON: JSON.stringify([{ workerId: "worker-a", controlUrl: "http://worker-a:8090", capacity: 1 }]),
      RECORDING_BOT_SIGNALING_URL: "ws://127.0.0.1:4000/internal/recording-bot/v1",
      RECORDING_BOT_CONTROL_JWT_HS256_SECRET: "y".repeat(32),
    });

    expect(readSignalingConfig().mediasoup.numWorkers).toBe(3);
  });

  it("resolves an internal bot ICE address without changing the browser address", () => {
    expect(resolveBotAnnouncedIp("auto", "127.0.0.1", {
      lo: [{ address: "127.0.0.1", netmask: "255.0.0.0", family: "IPv4", mac: "00:00:00:00:00:00", internal: true, cidr: "127.0.0.1/8" }],
      eth0: [{ address: "172.19.0.7", netmask: "255.255.0.0", family: "IPv4", mac: "00:00:00:00:00:01", internal: false, cidr: "172.19.0.7/16" }],
    })).toBe("172.19.0.7");
    expect(resolveBotAnnouncedIp(undefined, "203.0.113.12", {})).toBe("203.0.113.12");
    expect(resolveBotAnnouncedIp("10.0.0.12", "203.0.113.12", {})).toBe("10.0.0.12");
  });

  it("reads the node-scoped recording bot admission verifier and caps its TTL at 60 seconds", () => {
    Object.assign(process.env, {
      MEDIA_NODE_ID: "media-a",
      RECORDING_BOT_ADMISSION_JWT_HS256_SECRET: "z".repeat(32),
    });

    expect(readSignalingConfig().recordingBotAdmission).toEqual({
      issuer: "conference-api",
      secret: "z".repeat(32),
      mediaNodeId: "media-a",
      maxTtlSeconds: 60,
    });

  });
});
