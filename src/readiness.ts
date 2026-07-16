export type RedisReadinessClient = {
  isReady: boolean;
  ping: () => Promise<string>;
};

const REDIS_READINESS_TIMEOUT_MS = 250;
const REDIS_READINESS_CACHE_MS = 250;

/**
 * Shares a short Redis PING across concurrent readiness requests. The probe is
 * intentionally small and bounded so readiness cannot create room/runtime work.
 */
export class RedisReadinessProbe {
  private checkedAt = 0;
  private lastResult = false;
  private inFlight: Promise<boolean> | undefined;
  private pendingPing: Promise<boolean> | undefined;

  constructor(
    private readonly redis: RedisReadinessClient,
    private readonly now: () => number = Date.now,
  ) {}

  check(): Promise<boolean> {
    if (!this.redis.isReady) {
      this.lastResult = false;
      this.checkedAt = this.now();
      return Promise.resolve(false);
    }

    if (this.now() - this.checkedAt < REDIS_READINESS_CACHE_MS) {
      return Promise.resolve(this.lastResult);
    }

    if (!this.inFlight) {
      if (this.pendingPing) {
        this.lastResult = false;
        this.checkedAt = this.now();
        return Promise.resolve(false);
      }
      this.inFlight = this.probe().finally(() => {
        this.inFlight = undefined;
      });
    }

    return this.inFlight;
  }

  private async probe(): Promise<boolean> {
    this.pendingPing = this.redis.ping()
      .then(() => true)
      .catch(() => false)
      .finally(() => {
        this.pendingPing = undefined;
      });
    const result = await withTimeout(this.pendingPing, REDIS_READINESS_TIMEOUT_MS);

    this.lastResult = result;
    this.checkedAt = this.now();
    return result;
  }
}

function withTimeout(operation: Promise<boolean>, timeoutMs: number): Promise<boolean> {
  return new Promise((resolve) => {
    const timer = setTimeout(() => resolve(false), timeoutMs);
    void operation.then((result) => {
      clearTimeout(timer);
      resolve(result);
    });
  });
}
