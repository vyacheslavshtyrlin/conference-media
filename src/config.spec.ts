import { afterEach, describe, expect, it } from "vitest";

import { readSignalingConfig, resolveBotAnnouncedIp } from "./config.js";

const originalEnv = { ...process.env };

afterEach(() => {
  process.env = { ...originalEnv };
});

describe("readSignalingConfig", () => {
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
