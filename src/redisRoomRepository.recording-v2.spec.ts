import { describe, expect, it, vi } from "vitest";

import { RedisRoomRepository, type RedisClient } from "./redisRoomRepository.js";

function redisHarness(values: Map<string, string>) {
  const evalCommand = vi.fn(async (_script: string, options: { keys: string[]; arguments: string[] }) => {
    const activatedAt = options.arguments[1]!;
    return JSON.stringify({
      protocolVersion: "recording-capture.v2",
      captureGeneration: Number(options.arguments[0]),
      activatedAt,
      captureStartedAt: activatedAt,
      terminalObservedAt: "2026-08-25T10:00:08.123Z",
      terminalTimelineEndMs: 8_000,
    });
  });
  const redis: RedisClient = {
    get: vi.fn(async (key) => values.get(key) ?? null),
    set: vi.fn(async () => "OK"),
    hExists: vi.fn(async () => false),
    hGet: vi.fn(async () => null),
    hLen: vi.fn(async () => 0),
    hSet: vi.fn(async () => 0),
    hDel: vi.fn(async () => 0),
    rPush: vi.fn(async () => 0),
    lRange: vi.fn(async () => []),
    lTrim: vi.fn(async () => "OK"),
    expire: vi.fn(async () => true),
    persist: vi.fn(async () => true),
    del: vi.fn(async () => 0),
    eval: evalCommand,
  };
  return { repository: new RedisRoomRepository(redis), evalCommand };
}

describe("RedisRoomRepository recording-capture.v2 finalization", () => {
  it("atomically derives one terminal timeline fence from the stable activation origin", async () => {
    const activatedAt = "2026-08-25T10:00:00.123Z";
    const { repository, evalCommand } = redisHarness(new Map([
      ["recording_activation:rec_1", JSON.stringify({
        protocolVersion: "recording-capture.v2",
        captureGeneration: 4,
        activatedAt,
        captureStartedAt: activatedAt,
      })],
    ]));

    await expect(repository.beginV2RecordingFinalization("rec_1", 4)).resolves.toMatchObject({
      captureGeneration: 4,
      terminalTimelineEndMs: 8_000,
    });
    const [script, options] = evalCommand.mock.calls[0]!;
    expect(script).toContain('redis.call("TIME")');
    expect(options).toMatchObject({
      keys: ["recording_activation:rec_1", "recording_finalizing:rec_1"],
      arguments: ["4", activatedAt, String(Date.parse(activatedAt)), "900000"],
    });
  });

  it("reuses an existing exact fence without resampling time", async () => {
    const finalizing = JSON.stringify({
      protocolVersion: "recording-capture.v2",
      captureGeneration: 4,
      activatedAt: "2026-08-25T10:00:00.123Z",
      captureStartedAt: "2026-08-25T10:00:00.123Z",
      terminalObservedAt: "2026-08-25T10:00:08.123Z",
      terminalTimelineEndMs: 8_000,
    });
    const { repository, evalCommand } = redisHarness(new Map([
      ["recording_finalizing:rec_1", finalizing],
    ]));

    await expect(repository.beginV2RecordingFinalization("rec_1", 4)).resolves.toMatchObject({
      terminalTimelineEndMs: 8_000,
    });
    expect(evalCommand).not.toHaveBeenCalled();
  });

  it("rejects a stale capture generation", async () => {
    const activatedAt = "2026-08-25T10:00:00.123Z";
    const { repository, evalCommand } = redisHarness(new Map([
      ["recording_activation:rec_1", JSON.stringify({
        protocolVersion: "recording-capture.v2",
        captureGeneration: 5,
        activatedAt,
        captureStartedAt: activatedAt,
      })],
    ]));

    await expect(repository.beginV2RecordingFinalization("rec_1", 4))
      .rejects.toThrow("activation does not match");
    expect(evalCommand).not.toHaveBeenCalled();
  });
});
