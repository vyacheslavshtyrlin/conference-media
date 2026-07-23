import { randomUUID } from "node:crypto";

import { Mutex } from "async-mutex";

export type RecordingWorkerDescriptor = {
  workerId: string;
  controlUrl: string;
  capacity: number;
};

export type RecordingWorkerLease = RecordingWorkerDescriptor & {
  generationId: string;
  occupiedSlots: number;
  leaseExpiresAt: number;
};

export type RecordingWorkerBinding = {
  recordingId: string;
  workerId: string;
  generationId: string;
  controlUrl: string;
};

export type RecordingWorkerPoolSnapshot = {
  totalSlots: number;
  occupiedSlots: number;
  availableSlots: number;
  healthyWorkers: number;
};

export class RecordingCapacityError extends Error {
  readonly code = "RECORDING_CAPACITY_EXHAUSTED";

  constructor() {
    super("Recording capacity is temporarily unavailable");
  }
}

/**
 * The process-local implementation is safe because architecture currently
 * fences signaling to one media lease owner. The interface deliberately keeps
 * registry/reservation concerns separate so it can move to Redis unchanged
 * when media-node horizontal scaling is introduced.
 */
export class RecordingWorkerPool {
  private readonly workers = new Map<string, RecordingWorkerLease>();
  private readonly bindings = new Map<string, RecordingWorkerBinding>();
  private readonly lostBindings = new Set<string>();
  private readonly lock = new Mutex();

  constructor(
    descriptors: RecordingWorkerDescriptor[],
    private readonly leaseTtlMs: number,
    private readonly now: () => number = Date.now,
  ) {
    for (const descriptor of descriptors) {
      this.validateDescriptor(descriptor);
      this.workers.set(descriptor.workerId, {
        ...descriptor,
        generationId: randomUUID(),
        occupiedSlots: 0,
        leaseExpiresAt: this.now() + this.leaseTtlMs,
      });
    }
  }

  register(input: RecordingWorkerDescriptor & { generationId: string; occupiedSlots?: number }): Promise<void> {
    return this.lock.runExclusive(() => {
      this.validateDescriptor(input);
      const previous = this.workers.get(input.workerId);
      if (previous && previous.generationId !== input.generationId) {
        for (const [recordingId, binding] of this.bindings) {
          if (binding.workerId === input.workerId && binding.generationId === previous.generationId) {
            this.bindings.delete(recordingId);
            this.lostBindings.add(recordingId);
          }
        }
      }
      this.workers.set(input.workerId, {
        workerId: input.workerId,
        generationId: input.generationId,
        controlUrl: input.controlUrl,
        capacity: input.capacity,
        occupiedSlots: Math.min(
          input.occupiedSlots ?? (previous?.generationId === input.generationId ? previous.occupiedSlots : 0),
          input.capacity,
        ),
        leaseExpiresAt: this.now() + this.leaseTtlMs,
      });
    });
  }

  heartbeat(workerId: string, generationId: string, occupiedSlots?: number): Promise<boolean> {
    return this.lock.runExclusive(() => {
      const worker = this.workers.get(workerId);
      if (!worker || worker.generationId !== generationId) return false;
      worker.leaseExpiresAt = this.now() + this.leaseTtlMs;
      if (occupiedSlots !== undefined) {
        worker.occupiedSlots = Math.max(this.bindingCount(worker), Math.min(occupiedSlots, worker.capacity));
      }
      return true;
    });
  }

  reserve(recordingId: string): Promise<RecordingWorkerBinding> {
    return this.lock.runExclusive(() => {
      const existing = this.bindings.get(recordingId);
      if (existing) {
        const worker = this.workers.get(existing.workerId);
        if (worker && worker.generationId === existing.generationId && worker.leaseExpiresAt > this.now()) {
          return existing;
        }
        this.bindings.delete(recordingId);
      }

      const worker = [...this.workers.values()]
        .filter((candidate) => candidate.leaseExpiresAt > this.now() && candidate.occupiedSlots < candidate.capacity)
        .sort((left, right) =>
          (left.occupiedSlots / left.capacity) - (right.occupiedSlots / right.capacity)
          || left.workerId.localeCompare(right.workerId))[0];
      if (!worker) throw new RecordingCapacityError();

      worker.occupiedSlots += 1;
      const binding = {
        recordingId,
        workerId: worker.workerId,
        generationId: worker.generationId,
        controlUrl: worker.controlUrl,
      };
      this.bindings.set(recordingId, binding);
      return binding;
    });
  }

  release(recordingId: string): Promise<void> {
    return this.lock.runExclusive(() => {
      const binding = this.bindings.get(recordingId);
      if (!binding) return;
      this.bindings.delete(recordingId);
      const worker = this.workers.get(binding.workerId);
      if (worker?.generationId === binding.generationId) {
        worker.occupiedSlots = Math.max(this.bindingCount(worker), worker.occupiedSlots - 1);
      }
    });
  }

  invalidate(recordingId: string): Promise<boolean> {
    return this.lock.runExclusive(() => {
      const binding = this.bindings.get(recordingId);
      if (!binding) return false;
      this.bindings.delete(recordingId);
      const worker = this.workers.get(binding.workerId);
      if (worker?.generationId === binding.generationId) {
        worker.occupiedSlots = Math.max(this.bindingCount(worker), worker.occupiedSlots - 1);
      }
      return true;
    });
  }

  markWorkerUnavailable(workerId: string, generationId: string): Promise<string[]> {
    return this.lock.runExclusive(() => {
      const worker = this.workers.get(workerId);
      if (!worker || worker.generationId !== generationId) return [];
      worker.leaseExpiresAt = 0;
      const lost: string[] = [];
      for (const [recordingId, binding] of this.bindings) {
        if (binding.workerId !== workerId || binding.generationId !== generationId) continue;
        this.bindings.delete(recordingId);
        this.lostBindings.add(recordingId);
        lost.push(recordingId);
      }
      worker.occupiedSlots = 0;
      return lost;
    });
  }

  getBinding(recordingId: string): RecordingWorkerBinding | undefined {
    const binding = this.bindings.get(recordingId);
    if (!binding) return undefined;
    const worker = this.workers.get(binding.workerId);
    return worker && worker.generationId === binding.generationId && worker.leaseExpiresAt > this.now()
      ? { ...binding }
      : undefined;
  }

  snapshot(): RecordingWorkerPoolSnapshot {
    const healthy = [...this.workers.values()].filter((worker) => worker.leaseExpiresAt > this.now());
    const totalSlots = healthy.reduce((sum, worker) => sum + worker.capacity, 0);
    const occupiedSlots = healthy.reduce((sum, worker) => sum + worker.occupiedSlots, 0);
    return {
      totalSlots,
      occupiedSlots,
      availableSlots: Math.max(0, totalSlots - occupiedSlots),
      healthyWorkers: healthy.length,
    };
  }

  reapExpiredBindings(): Promise<string[]> {
    return this.lock.runExclusive(() => {
      const lost = [...this.lostBindings];
      this.lostBindings.clear();
      for (const [recordingId, binding] of this.bindings) {
        const worker = this.workers.get(binding.workerId);
        if (worker && worker.generationId === binding.generationId && worker.leaseExpiresAt > this.now()) continue;
        this.bindings.delete(recordingId);
        lost.push(recordingId);
      }
      return lost;
    });
  }

  queueLostBinding(recordingId: string): Promise<void> {
    return this.lock.runExclusive(() => {
      if (!this.bindings.has(recordingId)) this.lostBindings.add(recordingId);
    });
  }

  configuredWorkers(): RecordingWorkerLease[] {
    return [...this.workers.values()].map((worker) => ({ ...worker }));
  }

  private bindingCount(worker: RecordingWorkerLease): number {
    return [...this.bindings.values()].filter(
      (binding) => binding.workerId === worker.workerId && binding.generationId === worker.generationId,
    ).length;
  }

  private validateDescriptor(input: RecordingWorkerDescriptor): void {
    if (!/^[a-zA-Z0-9_.-]{1,80}$/.test(input.workerId)) throw new Error("Invalid recording workerId");
    if (!Number.isInteger(input.capacity) || input.capacity < 1) throw new Error("Recording worker capacity must be positive");
    const url = new URL(input.controlUrl);
    if (url.protocol !== "http:" && url.protocol !== "https:") throw new Error("Recording worker controlUrl must be HTTP(S)");
  }
}

export class RecordingWorkerLeaseMonitor {
  private timer?: NodeJS.Timeout;

  constructor(
    private readonly pool: RecordingWorkerPool,
    private readonly intervalMs: number,
    private readonly fetchHealth: typeof fetch = fetch,
    private readonly onBindingsLost?: (recordingIds: string[]) => Promise<void> | void,
  ) {}

  async refresh(): Promise<void> {
    const recoverable = new Set<string>();
    await Promise.allSettled(this.pool.configuredWorkers().map(async (worker) => {
      const response = await this.fetchHealth(`${worker.controlUrl}/health/ready`, {
        signal: AbortSignal.timeout(Math.min(this.intervalMs, 5_000)),
      });
      if (!response.ok) return;
      const body = await response.json() as {
        workerId?: string;
        generationId?: string;
        capacity?: number;
        occupiedSlots?: number;
        recoverableRecordingIds?: unknown;
        attempts?: unknown[];
      };
      if (body.workerId !== undefined && body.workerId !== worker.workerId) return;
      await this.pool.register({
        workerId: worker.workerId,
        generationId: body.generationId ?? worker.generationId,
        controlUrl: worker.controlUrl,
        capacity: body.capacity ?? worker.capacity,
        occupiedSlots: body.occupiedSlots ?? body.attempts?.length ?? worker.occupiedSlots,
      });
      if (Array.isArray(body.recoverableRecordingIds)) {
        for (const recordingId of body.recoverableRecordingIds) {
          if (typeof recordingId === "string" && recordingId.length > 0 && recordingId.length <= 160) {
            recoverable.add(recordingId);
          }
        }
      }
    }));
    const recoveryIds = new Set(await this.pool.reapExpiredBindings());
    for (const recordingId of recoverable) recoveryIds.add(recordingId);
    await this.onBindingsLost?.([...recoveryIds]);
  }

  start(): void {
    if (this.timer) return;
    this.timer = setInterval(() => void this.refresh(), this.intervalMs);
    this.timer.unref();
  }

  stop(): void {
    if (this.timer) clearInterval(this.timer);
    this.timer = undefined;
  }
}
