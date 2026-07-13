import { createClient, type RedisClientType } from "redis";
import { afterEach, describe, expect, it } from "vitest";

import { MEDIA_LEASE_KEY, MediaInstanceLease } from "./mediaInstanceLease.js";

const describeRedis = process.env.TEST_REDIS_URL ? describe : describe.skip;

describeRedis("MediaInstanceLease with real Redis", () => {
  let firstClient: RedisClientType | undefined;
  let secondClient: RedisClientType | undefined;

  afterEach(async () => {
    await firstClient?.quit().catch(() => undefined);
    await secondClient?.quit().catch(() => undefined);
    firstClient = undefined;
    secondClient = undefined;
  });

  it("lets a restarted process acquire a fresh epoch after the crashed lease expires", async () => {
    const keyPrefix = `conference:e2:${crypto.randomUUID()}:`;
    firstClient = createClient({ url: process.env.TEST_REDIS_URL });
    await firstClient.connect();
    const firstLease = new MediaInstanceLease(prefixClient(firstClient, keyPrefix), 75, 25);
    const first = await firstLease.acquire();

    firstClient.disconnect();
    await new Promise((resolve) => setTimeout(resolve, 125));

    secondClient = createClient({ url: process.env.TEST_REDIS_URL });
    await secondClient.connect();
    const secondLease = new MediaInstanceLease(prefixClient(secondClient, keyPrefix), 75, 25);
    const second = await secondLease.acquire();

    expect(second.mediaEpoch).not.toBe(first.mediaEpoch);
    await secondLease.release();
  });
});

function prefixClient(client: RedisClientType, prefix: string) {
  return {
    set: (key: string, value: string, options: { NX?: boolean; PX?: number }) =>
      client.set(`${prefix}${key}`, value, options),
    eval: (script: string, options: { keys: string[]; arguments: string[] }) =>
      client.eval(script, {
        keys: options.keys.map((key) => `${prefix}${key}`),
        arguments: options.arguments,
      }),
  };
}

void MEDIA_LEASE_KEY;
