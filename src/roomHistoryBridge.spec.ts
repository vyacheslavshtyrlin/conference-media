import { afterEach, describe, expect, it, vi } from "vitest";

import { RoomHistoryBridge } from "./roomHistoryBridge.js";

const jwt = {
  issuer: "conference-signaling",
  audience: "conference-recording-control",
  secret: "x".repeat(32),
  maxTtlSeconds: 300,
};

afterEach(() => {
  vi.unstubAllGlobals();
  vi.useRealTimers();
});

describe("RoomHistoryBridge", () => {
  it("contains delivery and outbox failures without an unhandled rejection", async () => {
    vi.stubGlobal("fetch", vi.fn().mockRejectedValue(new Error("api unavailable")));
    const redis = {
      rPush: vi.fn().mockRejectedValue(new Error("redis unavailable")),
      lPop: vi.fn().mockResolvedValue(null),
      expire: vi.fn().mockResolvedValue(true),
    };
    const bridge = new RoomHistoryBridge(redis, "http://api:3000/internal/v1", jwt);

    bridge.report({ type: "participant.left", roomId: "room_1", participantId: "peer_1" });
    await new Promise((resolve) => setTimeout(resolve, 0));

    expect(redis.rPush).toHaveBeenCalledOnce();
  });

  it("contains initial and interval outbox read failures", async () => {
    vi.useFakeTimers();
    const redis = {
      rPush: vi.fn().mockResolvedValue(1),
      lPop: vi.fn().mockRejectedValue(new Error("redis unavailable")),
      expire: vi.fn().mockResolvedValue(true),
    };
    const bridge = new RoomHistoryBridge(redis, "http://api:3000/internal/v1", jwt);

    bridge.start();
    await vi.advanceTimersByTimeAsync(2_000);
    bridge.stop();

    expect(redis.lPop).toHaveBeenCalledTimes(2);
  });
});
