import { randomUUID } from "node:crypto";

import { Mutex } from "async-mutex";

const MAX_REPORTED_RECORDING_IDS = 1_000;

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
  consumesSlot?: boolean;
};

export type RecordingWorkerPoolSnapshot = {
  totalSlots: number;
  occupiedSlots: number;
  availableSlots: number;
  healthyWorkers: number;
};

export type RecordingWorkerRecoveryReport = {
  recoverableRecordingIds: string[];
  pendingFinalizationRecordingIds: string[];
  pendingFinalizationWorkerIds?: Record<string, string>;
};

export type RecordingWorkerHealthFailure = {
  workerId: string;
  reason: "request_failed" | "not_ready" | "invalid_payload" | "identity_mismatch" | "capacity_mismatch";
  status?: number;
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
    initiallyHealthy = false,
  ) {
    for (const descriptor of descriptors) {
      this.validateDescriptor(descriptor);
      this.workers.set(descriptor.workerId, {
        ...descriptor,
        generationId: randomUUID(),
        occupiedSlots: 0,
        leaseExpiresAt: initiallyHealthy ? this.now() + this.leaseTtlMs : 0,
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
        occupiedSlots: Math.min(input.capacity, Math.max(
          this.bindingCountFor(input.workerId, input.generationId),
          input.occupiedSlots ?? (previous?.generationId === input.generationId ? previous.occupiedSlots : 0),
        )),
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

  reserve(recordingId: string, consumesSlot = true): Promise<RecordingWorkerBinding> {
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
        .filter((candidate) => candidate.leaseExpiresAt > this.now() &&
          (!consumesSlot || candidate.occupiedSlots < candidate.capacity))
        .sort((left, right) =>
          (left.occupiedSlots / left.capacity) - (right.occupiedSlots / right.capacity)
          || left.workerId.localeCompare(right.workerId))[0];
      if (!worker) throw new RecordingCapacityError();

      if (consumesSlot) worker.occupiedSlots += 1;
      const binding = {
        recordingId,
        workerId: worker.workerId,
        generationId: worker.generationId,
        controlUrl: worker.controlUrl,
        consumesSlot,
      };
      this.bindings.set(recordingId, binding);
      return binding;
    });
  }

  reserveOnWorker(
    recordingId: string,
    workerId: string,
    consumesSlot = false,
  ): Promise<RecordingWorkerBinding> {
    return this.lock.runExclusive(() => {
      const existing = this.bindings.get(recordingId);
      if (existing) {
        const existingWorker = this.workers.get(existing.workerId);
        if (existing.workerId === workerId && existingWorker &&
          existingWorker.generationId === existing.generationId &&
          existingWorker.leaseExpiresAt > this.now()) {
          return existing;
        }
        this.bindings.delete(recordingId);
        if (existing.consumesSlot !== false && existingWorker?.generationId === existing.generationId) {
          existingWorker.occupiedSlots = Math.max(this.bindingCount(existingWorker), existingWorker.occupiedSlots - 1);
        }
      }

      const worker = this.workers.get(workerId);
      if (!worker || worker.leaseExpiresAt <= this.now() ||
        (consumesSlot && worker.occupiedSlots >= worker.capacity)) {
        throw new RecordingCapacityError();
      }
      if (consumesSlot) worker.occupiedSlots += 1;
      const binding = {
        recordingId,
        workerId: worker.workerId,
        generationId: worker.generationId,
        controlUrl: worker.controlUrl,
        consumesSlot,
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
      if (binding.consumesSlot !== false && worker?.generationId === binding.generationId) {
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
      if (binding.consumesSlot !== false && worker?.generationId === binding.generationId) {
        worker.occupiedSlots = Math.max(this.bindingCount(worker), worker.occupiedSlots - 1);
      }
      return true;
    });
  }

  invalidateUnavailable(recordingId: string): Promise<boolean> {
    return this.lock.runExclusive(() => {
      const binding = this.bindings.get(recordingId);
      if (!binding) return true;
      const worker = this.workers.get(binding.workerId);
      const bindingIsCurrent = worker?.generationId === binding.generationId &&
        worker.leaseExpiresAt > this.now();
      if (bindingIsCurrent) return false;
      this.bindings.delete(recordingId);
      if (binding.consumesSlot !== false && worker?.generationId === binding.generationId) {
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
    return this.bindingCountFor(worker.workerId, worker.generationId);
  }

  private bindingCountFor(workerId: string, generationId: string): number {
    return [...this.bindings.values()].filter(
      (binding) => binding.consumesSlot !== false && binding.workerId === workerId && binding.generationId === generationId,
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
  private stopping = false;
  private recovering = false;
  private recoveryPaused = false;
  private retryQueued = false;
  private readonly queuedRecoveries = new Set<string>();
  private readonly queuedFinalizations = new Set<string>();
  private readonly queuedFinalizationWorkers = new Map<string, string>();
  private reportedRecoveries = new Set<string>();
  private reportedFinalizations = new Set<string>();
  private reportedFinalizationWorkers = new Map<string, string>();
  private healthAbort?: AbortController;
  private readonly recoveryAbort = new AbortController();
  private refreshPromise?: Promise<void>;
  private recoveryPromise?: Promise<void>;

  constructor(
    private readonly pool: RecordingWorkerPool,
    private readonly intervalMs: number,
    private readonly fetchHealth: typeof fetch = fetch,
    private readonly onRecoveryRequired?: (
      report: RecordingWorkerRecoveryReport,
      signal: AbortSignal,
    ) => Promise<void> | void,
    private readonly authorization?: () => string,
    private readonly onHealthFailure?: (failure: RecordingWorkerHealthFailure) => void,
  ) {}

  refresh(): Promise<void> {
    if (this.stopping) return Promise.resolve();
    if (this.refreshPromise) return this.refreshPromise;

    const refresh = this.runRefresh();
    this.refreshPromise = refresh;
    void refresh.finally(() => {
      if (this.refreshPromise === refresh) this.refreshPromise = undefined;
    }).catch(() => undefined);
    return refresh;
  }

  private async runRefresh(): Promise<void> {
    const abort = new AbortController();
    this.healthAbort = abort;
    try {
      const recoverable = new Set<string>();
      const pendingFinalizations = new Set<string>();
      const pendingFinalizationWorkers = new Map<string, string>();
      await Promise.all(this.pool.configuredWorkers().map(async (worker) => {
        try {
          const response = await this.fetchHealth(`${worker.controlUrl}/health/ready`, {
            headers: this.authorization ? { Authorization: this.authorization() } : undefined,
            signal: AbortSignal.any([
              abort.signal,
              AbortSignal.timeout(Math.min(this.intervalMs, 5_000)),
            ]),
          });
          if (!response.ok) {
            this.onHealthFailure?.({ workerId: worker.workerId, reason: "not_ready", status: response.status });
            return;
          }
          const body: unknown = await response.json();
          if (!isWorkerHealth(body)) {
            this.onHealthFailure?.({ workerId: worker.workerId, reason: "invalid_payload" });
            return;
          }
          if (body.workerId !== worker.workerId) {
            this.onHealthFailure?.({ workerId: worker.workerId, reason: "identity_mismatch" });
            return;
          }
          if (body.capacity !== worker.capacity) {
            this.onHealthFailure?.({ workerId: worker.workerId, reason: "capacity_mismatch" });
            return;
          }
          await this.pool.register({
            workerId: worker.workerId,
            generationId: body.generationId,
            controlUrl: worker.controlUrl,
            capacity: body.capacity,
            occupiedSlots: body.occupiedSlots,
          });
          collectRecordingIds(body.recoverableRecordingIds, recoverable);
          const workerFinalizations = new Set<string>();
          collectRecordingIds(body.pendingFinalizationRecordingIds, workerFinalizations);
          for (const recordingId of workerFinalizations) {
            pendingFinalizations.add(recordingId);
            const currentWorker = pendingFinalizationWorkers.get(recordingId);
            if (!currentWorker || worker.workerId.localeCompare(currentWorker) < 0) {
              pendingFinalizationWorkers.set(recordingId, worker.workerId);
            }
          }
        } catch {
          if (this.stopping || abort.signal.aborted) return;
          this.onHealthFailure?.({ workerId: worker.workerId, reason: "request_failed" });
        }
      }));
      if (this.stopping) return;
      const recoveryIds = new Set(await this.pool.reapExpiredBindings());
      for (const recordingId of recoverable) {
        if (!this.reportedRecoveries.has(recordingId)) recoveryIds.add(recordingId);
      }
      const newFinalizations = [...pendingFinalizations]
        .filter((recordingId) => !this.reportedFinalizations.has(recordingId) ||
          this.reportedFinalizationWorkers.get(recordingId) !== pendingFinalizationWorkers.get(recordingId));
      this.reportedRecoveries = recoverable;
      this.reportedFinalizations = pendingFinalizations;
      this.reportedFinalizationWorkers = pendingFinalizationWorkers;
      this.queueRecovery([...recoveryIds], newFinalizations, pendingFinalizationWorkers);
    } finally {
      if (this.healthAbort === abort) this.healthAbort = undefined;
    }
  }

  start(): void {
    if (this.timer || this.stopping) return;
    this.timer = setInterval(() => {
      void this.refresh().catch(() => undefined);
    }, this.intervalMs);
    this.timer.unref();
  }

  async stop(): Promise<void> {
    this.stopping = true;
    if (this.timer) clearInterval(this.timer);
    this.timer = undefined;
    this.healthAbort?.abort();
    this.recoveryAbort.abort();
    this.queuedRecoveries.clear();
    this.queuedFinalizations.clear();
    this.queuedFinalizationWorkers.clear();
    this.retryQueued = false;
    await this.refreshPromise?.catch(() => undefined);
    await this.settleRecovery();
  }

  async settleRecovery(): Promise<void> {
    while (this.recoveryPromise) await this.recoveryPromise.catch(() => undefined);
  }

  private queueRecovery(
    recoverableRecordingIds: string[],
    pendingFinalizationRecordingIds: string[],
    pendingFinalizationWorkerIds: Map<string, string> = new Map(),
  ): void {
    if (this.stopping) return;
    this.recoveryPaused = false;
    this.retryQueued = true;
    for (const recordingId of recoverableRecordingIds) this.queuedRecoveries.add(recordingId);
    for (const recordingId of pendingFinalizationRecordingIds) {
      this.queuedRecoveries.delete(recordingId);
      this.queuedFinalizations.add(recordingId);
      const workerId = pendingFinalizationWorkerIds.get(recordingId);
      if (workerId) this.queuedFinalizationWorkers.set(recordingId, workerId);
    }
    if (!this.recovering) {
      this.startRecoveryDrain();
    }
  }

  private startRecoveryDrain(): void {
    const drain = this.drainRecoveryQueue();
    const settled = drain.catch(() => undefined).finally(() => {
      if (this.recoveryPromise === settled) this.recoveryPromise = undefined;
    });
    this.recoveryPromise = settled;
  }

  private async drainRecoveryQueue(): Promise<void> {
    this.recovering = true;
    try {
      while (!this.stopping &&
        (this.retryQueued || this.queuedRecoveries.size > 0 || this.queuedFinalizations.size > 0)) {
        this.retryQueued = false;
        const report = {
          recoverableRecordingIds: [...this.queuedRecoveries],
          pendingFinalizationRecordingIds: [...this.queuedFinalizations],
          ...(this.queuedFinalizationWorkers.size > 0
            ? { pendingFinalizationWorkerIds: Object.fromEntries(this.queuedFinalizationWorkers) }
            : {}),
        };
        this.queuedRecoveries.clear();
        this.queuedFinalizations.clear();
        this.queuedFinalizationWorkers.clear();
        try {
          await this.onRecoveryRequired?.(report, this.recoveryAbort.signal);
        } catch (error) {
          if (!this.stopping) {
            for (const recordingId of report.recoverableRecordingIds) this.queuedRecoveries.add(recordingId);
            for (const recordingId of report.pendingFinalizationRecordingIds) {
              this.queuedRecoveries.delete(recordingId);
              this.queuedFinalizations.add(recordingId);
              const workerId = report.pendingFinalizationWorkerIds?.[recordingId];
              if (workerId) this.queuedFinalizationWorkers.set(recordingId, workerId);
            }
            this.recoveryPaused = true;
          }
          throw error;
        }
      }
    } finally {
      this.recovering = false;
      if (!this.stopping && !this.recoveryPaused &&
        (this.retryQueued || this.queuedRecoveries.size > 0 || this.queuedFinalizations.size > 0)) {
        this.startRecoveryDrain();
      }
    }
  }
}

type WorkerHealth = {
  workerId: string;
  generationId: string;
  capacity: number;
  occupiedSlots: number;
  recoverableRecordingIds?: unknown;
  pendingFinalizationRecordingIds?: unknown;
};

function isWorkerHealth(value: unknown): value is WorkerHealth {
  if (typeof value !== "object" || value === null) return false;
  const health = value as Partial<WorkerHealth>;
  return typeof health.workerId === "string" && typeof health.generationId === "string" &&
    health.generationId.length > 0 && health.generationId.length <= 128 &&
    Number.isInteger(health.capacity) && Number(health.capacity) > 0 &&
    Number.isInteger(health.occupiedSlots) && Number(health.occupiedSlots) >= 0;
}

function collectRecordingIds(value: unknown, target: Set<string>): void {
  if (!Array.isArray(value)) return;
  for (const recordingId of value.slice(0, MAX_REPORTED_RECORDING_IDS)) {
    if (target.size >= MAX_REPORTED_RECORDING_IDS) return;
    if (typeof recordingId === "string" && recordingId.length > 0 && recordingId.length <= 160) {
      target.add(recordingId);
    }
  }
}
