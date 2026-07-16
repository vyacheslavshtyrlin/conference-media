import { describe, expect, it, vi } from "vitest";

import { RedisReadinessProbe } from "./readiness.js";

describe("RedisReadinessProbe", () => {
  it("does not ping when the Redis client is disconnected", async () => {
    const ping = vi.fn();
    const probe = new RedisReadinessProbe({ isReady: false, ping });

    await expect(probe.check()).resolves.toBe(false);
    expect(ping).not.toHaveBeenCalled();
  });

  it("reports a failed Redis command as unavailable", async () => {
    const probe = new RedisReadinessProbe({
      isReady: true,
      ping: vi.fn().mockRejectedValue(new Error("redis unavailable")),
    });

    await expect(probe.check()).resolves.toBe(false);
  });

  it("coalesces concurrent probes into one bounded Redis command", async () => {
    let resolvePing: (() => void) | undefined;
    const ping = vi.fn(
      () => new Promise<string>((resolve) => {
        resolvePing = () => resolve("PONG");
      }),
    );
    const probe = new RedisReadinessProbe({ isReady: true, ping });

    const first = probe.check();
    const second = probe.check();
    resolvePing?.();

    await expect(Promise.all([first, second])).resolves.toEqual([true, true]);
    expect(ping).toHaveBeenCalledTimes(1);
  });

  it("does not overlap Redis commands after a timed-out probe", async () => {
    vi.useFakeTimers();
    const ping = vi.fn(() => new Promise<string>(() => {}));
    const probe = new RedisReadinessProbe({ isReady: true, ping });

    const check = probe.check();
    await vi.advanceTimersByTimeAsync(250);

    await expect(check).resolves.toBe(false);
    await vi.advanceTimersByTimeAsync(251);
    await expect(probe.check()).resolves.toBe(false);
    expect(ping).toHaveBeenCalledTimes(1);
    vi.useRealTimers();
  });
});
