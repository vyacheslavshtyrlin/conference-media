import { afterEach, describe, expect, it } from "vitest";

import { readSignalingConfig } from "./config.js";

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
});
