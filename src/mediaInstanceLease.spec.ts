import { afterEach, describe, expect, it, vi } from "vitest";

import { MEDIA_LEASE_KEY, MediaInstanceLease } from "./mediaInstanceLease.js";

class FakeRedisLeaseClient {
  value?: string;

  async set(_key: string, value: string, options: { NX?: boolean }): Promise<string | null> {
    if (options.NX && this.value) {
      return null;
    }
    this.value = value;
    return "OK";
  }

  async eval(_script: string, options: { arguments: string[] }): Promise<number> {
    if (options.arguments.length === 1) {
      if (this.value === options.arguments[0]) {
        this.value = undefined;
        return 1;
      }
      return 0;
    }
    if (this.value !== options.arguments[0]) {
      return 0;
    }
    this.value = options.arguments[2];
    return 1;
  }
}

describe("MediaInstanceLease", () => {
  afterEach(() => vi.useRealTimers());

  it("acquires a unique epoch and releases only its own lease", async () => {
    const redis = new FakeRedisLeaseClient();
    const lease = new MediaInstanceLease(redis, 1_000, 100);

    const record = await lease.acquire();

    expect(record.mediaEpoch).toMatch(/^media_/);
    expect(JSON.parse(redis.value ?? "{}")).toMatchObject({ mediaEpoch: record.mediaEpoch });
    await expect(new MediaInstanceLease(redis, 1_000, 100).acquire()).rejects.toThrow("active media process lease");

    await lease.release();
    expect(redis.value).toBeUndefined();
  });

  it("marks itself unhealthy and reports a lost lease on failed refresh", async () => {
    vi.useFakeTimers();
    const redis = new FakeRedisLeaseClient();
    const lease = new MediaInstanceLease(redis, 1_000, 100);
    const onLost = vi.fn();
    lease.setLostCallback(onLost);
    await lease.acquire();
    redis.value = "owned-by-another-process";

    await vi.advanceTimersByTimeAsync(100);

    expect(lease.isHealthy()).toBe(false);
    expect(onLost).toHaveBeenCalledOnce();
    expect(MEDIA_LEASE_KEY).toBe("media:lease");
  });

  it("reports unhealthy when its locally owned lease has expired", async () => {
    const redis = new FakeRedisLeaseClient();
    let now = 0;
    const lease = new MediaInstanceLease(redis, 1_000, 100, undefined, () => now);
    await lease.acquire();

    now = 1_000;

    expect(lease.isHealthy()).toBe(false);
  });
});
