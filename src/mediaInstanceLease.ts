import { randomUUID } from "node:crypto";

import type { Logger } from "./logger.js";

export const MEDIA_LEASE_KEY = "media:lease";

export type MediaLeaseRecord = {
  mediaEpoch: string;
  issuedAt: string;
  expiresAt: string;
};

type RedisLeaseClient = {
  set(
    key: string,
    value: string,
    options: { NX?: boolean; PX?: number },
  ): Promise<string | null>;
  eval(script: string, options: { keys: string[]; arguments: string[] }): Promise<unknown>;
};

const refreshLeaseScript = `
local current = redis.call('GET', KEYS[1])
if current ~= ARGV[1] then
  return 0
end
redis.call('PSETEX', KEYS[1], ARGV[2], ARGV[3])
return 1
`;

const releaseLeaseScript = `
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
`;

export class MediaInstanceLease {
  private record?: MediaLeaseRecord;
  private serializedRecord?: string;
  private healthy = false;
  private refreshTimer?: NodeJS.Timeout;
  private onLostCallback?: (error: Error) => void;

  constructor(
    private readonly redis: RedisLeaseClient,
    private readonly ttlMs: number,
    private readonly refreshMs: number,
    private readonly logger?: Logger,
    private readonly now: () => number = Date.now,
  ) {}

  setLostCallback(callback: (error: Error) => void): void {
    this.onLostCallback = callback;
  }

  async acquire(): Promise<MediaLeaseRecord> {
    const record = this.createRecord();
    const serializedRecord = JSON.stringify(record);
    const result = await this.redis.set(MEDIA_LEASE_KEY, serializedRecord, {
      NX: true,
      PX: this.ttlMs,
    });
    if (result !== "OK") {
      throw new Error("An active media process lease already exists");
    }

    this.record = record;
    this.serializedRecord = serializedRecord;
    this.healthy = true;
    this.refreshTimer = setInterval(() => {
      void this.refresh().catch((error: unknown) => this.markLost(toError(error)));
    }, this.refreshMs);
    this.refreshTimer.unref();
    return record;
  }

  isHealthy(): boolean {
    return this.healthy && this.record !== undefined && Date.parse(this.record.expiresAt) > this.now();
  }

  getEpoch(): string {
    if (!this.record || !this.healthy) {
      throw new Error("Media process lease is not healthy");
    }
    return this.record.mediaEpoch;
  }

  async release(): Promise<void> {
    this.stopRefreshTimer();
    if (!this.serializedRecord) {
      this.healthy = false;
      return;
    }

    try {
      await this.redis.eval(releaseLeaseScript, {
        keys: [MEDIA_LEASE_KEY],
        arguments: [this.serializedRecord],
      });
    } finally {
      this.healthy = false;
      this.record = undefined;
      this.serializedRecord = undefined;
    }
  }

  private async refresh(): Promise<void> {
    if (!this.record || !this.serializedRecord || !this.healthy) {
      return;
    }

    const nextRecord: MediaLeaseRecord = {
      ...this.record,
      expiresAt: new Date(this.now() + this.ttlMs).toISOString(),
    };
    const nextSerializedRecord = JSON.stringify(nextRecord);
    const result = await this.redis.eval(refreshLeaseScript, {
      keys: [MEDIA_LEASE_KEY],
      arguments: [this.serializedRecord, String(this.ttlMs), nextSerializedRecord],
    });
    if (Number(result) !== 1) {
      throw new Error("Media process lease was lost");
    }

    this.record = nextRecord;
    this.serializedRecord = nextSerializedRecord;
  }

  private createRecord(): MediaLeaseRecord {
    const issuedAt = new Date(this.now());
    return {
      mediaEpoch: `media_${randomUUID()}`,
      issuedAt: issuedAt.toISOString(),
      expiresAt: new Date(issuedAt.getTime() + this.ttlMs).toISOString(),
    };
  }

  private markLost(error: Error): void {
    if (!this.healthy) {
      return;
    }

    this.stopRefreshTimer();
    this.healthy = false;
    this.logger?.error("media process lease lost", {
      operation: "mediaLease.refresh",
      errorCode: "MEDIA_LEASE_LOST",
      error: error.message,
    });
    this.onLostCallback?.(error);
  }

  private stopRefreshTimer(): void {
    if (this.refreshTimer) {
      clearInterval(this.refreshTimer);
      this.refreshTimer = undefined;
    }
  }
}

function toError(value: unknown): Error {
  return value instanceof Error ? value : new Error(String(value));
}
