import { describe, expect, it, vi } from "vitest";

import { RecordingCapacityError, RecordingWorkerLeaseMonitor, RecordingWorkerPool } from "./recordingWorkerPool.js";

describe("RecordingWorkerPool", () => {
  it("atomically distributes bindings and fails fast when all slots are occupied", async () => {
    let now = 1_000;
    const pool = new RecordingWorkerPool([
      { workerId: "worker-a", controlUrl: "http://worker-a:8090", capacity: 1 },
      { workerId: "worker-b", controlUrl: "http://worker-b:8090", capacity: 1 },
    ], 10_000, () => now);
    await pool.register({ workerId: "worker-a", generationId: "generation-a", controlUrl: "http://worker-a:8090", capacity: 1 });
    await pool.register({ workerId: "worker-b", generationId: "generation-b", controlUrl: "http://worker-b:8090", capacity: 1 });

    const [first, second] = await Promise.all([pool.reserve("rec-1"), pool.reserve("rec-2")]);
    expect(new Set([first.workerId, second.workerId])).toEqual(new Set(["worker-a", "worker-b"]));
    await expect(pool.reserve("rec-3")).rejects.toBeInstanceOf(RecordingCapacityError);
    expect(pool.snapshot()).toEqual({ totalSlots: 2, occupiedSlots: 2, availableSlots: 0, healthyWorkers: 2 });

    await pool.release("rec-1");
    expect((await pool.reserve("rec-3")).workerId).toBe(first.workerId);
    now += 1;
  });

  it("invalidates a binding when a worker generation changes or its lease expires", async () => {
    let now = 1_000;
    const pool = new RecordingWorkerPool([{ workerId: "worker-a", controlUrl: "http://worker-a:8090", capacity: 1 }], 100, () => now);
    await pool.register({ workerId: "worker-a", generationId: "generation-a", controlUrl: "http://worker-a:8090", capacity: 1 });
    await pool.reserve("rec-1");
    await pool.register({ workerId: "worker-a", generationId: "generation-b", controlUrl: "http://worker-a:8090", capacity: 1 });
    expect(pool.getBinding("rec-1")).toBeUndefined();
    expect(await pool.reapExpiredBindings()).toEqual(["rec-1"]);
    await pool.reserve("rec-2");
    now = 1_101;
    expect(pool.getBinding("rec-2")).toBeUndefined();
    await expect(pool.reserve("rec-3")).rejects.toBeInstanceOf(RecordingCapacityError);
  });

  it("reports a restarted generation binding as lost and rebinds it to another healthy worker", async () => {
    const pool = new RecordingWorkerPool([
      { workerId: "worker-a", controlUrl: "http://worker-a:8090", capacity: 1 },
      { workerId: "worker-b", controlUrl: "http://worker-b:8090", capacity: 1 },
    ], 1_000);
    await pool.register({ workerId: "worker-a", generationId: "generation-a", controlUrl: "http://worker-a:8090", capacity: 1 });
    await pool.register({ workerId: "worker-b", generationId: "generation-b", controlUrl: "http://worker-b:8090", capacity: 1 });
    expect((await pool.reserve("rec-1")).workerId).toBe("worker-a");

    await pool.register({ workerId: "worker-a", generationId: "generation-a2", controlUrl: "http://worker-a:8090", capacity: 1 });
    expect(await pool.reapExpiredBindings()).toEqual(["rec-1"]);
    // Keep A occupied to prove recovery can move to a different address.
    await pool.reserve("other");
    expect((await pool.reserve("rec-1")).workerId).toBe("worker-b");
  });

  it("routes worker-reported recoverable recording IDs through the lease recovery callback", async () => {
    const pool = new RecordingWorkerPool([
      { workerId: "worker-a", controlUrl: "http://worker-a:8090", capacity: 1 },
    ], 1_000);
    await pool.register({ workerId: "worker-a", generationId: "generation-a", controlUrl: "http://worker-a:8090", capacity: 1 });
    await pool.reserve("rec-recoverable");
    const recover = vi.fn().mockResolvedValue(undefined);
    const monitor = new RecordingWorkerLeaseMonitor(
      pool,
      500,
      vi.fn(async () => new Response(JSON.stringify({
        workerId: "worker-a",
        generationId: "generation-a",
        capacity: 1,
        occupiedSlots: 1,
        recoverableRecordingIds: ["rec-recoverable"],
      }), { status: 200, headers: { "Content-Type": "application/json" } })),
      recover,
    );

    await monitor.refresh();
    expect(recover).toHaveBeenCalledWith({
      recoverableRecordingIds: ["rec-recoverable"],
      pendingFinalizationRecordingIds: [],
    }, expect.any(AbortSignal));
  });

  it("routes a pending finalization back to the worker that reports its spool", async () => {
    const pool = new RecordingWorkerPool([
      { workerId: "worker-a", controlUrl: "http://worker-a:8090", capacity: 1 },
      { workerId: "worker-b", controlUrl: "http://worker-b:8090", capacity: 1 },
    ], 1_000);
    const recover = vi.fn().mockResolvedValue(undefined);
    const monitor = new RecordingWorkerLeaseMonitor(
      pool,
      500,
      vi.fn(async (url: string | URL | Request) => {
        const workerId = String(url).includes("worker-b") ? "worker-b" : "worker-a";
        return new Response(JSON.stringify({
          workerId,
          generationId: `generation-${workerId.at(-1)}`,
          capacity: 1,
          occupiedSlots: 0,
          pendingFinalizationRecordingIds: workerId === "worker-b" ? ["rec-finalizing"] : [],
        }), { status: 200, headers: { "Content-Type": "application/json" } });
      }),
      recover,
    );

    await monitor.refresh();
    await monitor.settleRecovery();

    expect(recover).toHaveBeenCalledWith(expect.objectContaining({
      pendingFinalizationRecordingIds: ["rec-finalizing"],
      pendingFinalizationWorkerIds: { "rec-finalizing": "worker-b" },
    }), expect.any(AbortSignal));
    const report = recover.mock.calls[0]![0];
    const binding = await pool.reserveOnWorker(
      "rec-finalizing",
      report.pendingFinalizationWorkerIds["rec-finalizing"],
      false,
    );
    expect(binding.workerId).toBe("worker-b");
  });

  it("rejects health capacity that differs from the configured admission ceiling", async () => {
    const pool = new RecordingWorkerPool([
      { workerId: "worker-a", controlUrl: "http://worker-a:8090", capacity: 1 },
    ], 1_000);
    const healthFailure = vi.fn();
    const monitor = new RecordingWorkerLeaseMonitor(
      pool,
      500,
      vi.fn(async () => new Response(JSON.stringify({
        workerId: "worker-a",
        generationId: "generation-a",
        capacity: 10,
        occupiedSlots: 0,
      }), { status: 200, headers: { "Content-Type": "application/json" } })),
      undefined,
      undefined,
      healthFailure,
    );

    await monitor.refresh();

    expect(healthFailure).toHaveBeenCalledWith({
      workerId: "worker-a",
      reason: "capacity_mismatch",
    });
    expect(pool.snapshot()).toEqual({
      totalSlots: 0,
      occupiedSlots: 0,
      availableSlots: 0,
      healthyWorkers: 0,
    });
  });

  it("aborts an in-flight health request during shutdown", async () => {
    const pool = new RecordingWorkerPool([
      { workerId: "worker-a", controlUrl: "http://worker-a:8090", capacity: 1 },
    ], 1_000);
    let observedSignal: AbortSignal | undefined;
    const monitor = new RecordingWorkerLeaseMonitor(
      pool,
      500,
      vi.fn(async (_url, init) => {
        observedSignal = init?.signal ?? undefined;
        return new Promise<Response>((_resolve, reject) => {
          observedSignal?.addEventListener("abort", () => reject(new Error("aborted")), { once: true });
        });
      }),
    );

    const refresh = monitor.refresh();
    await Promise.resolve();
    await monitor.stop();
    await refresh;

    expect(observedSignal?.aborted).toBe(true);
  });

  it("awaits an active recovery during shutdown", async () => {
    const pool = new RecordingWorkerPool([
      { workerId: "worker-a", controlUrl: "http://worker-a:8090", capacity: 1 },
    ], 1_000);
    let releaseRecovery: (() => void) | undefined;
    const recoveryPending = new Promise<void>((resolve) => {
      releaseRecovery = resolve;
    });
    const monitor = new RecordingWorkerLeaseMonitor(
      pool,
      500,
      vi.fn(async () => new Response(JSON.stringify({
        workerId: "worker-a",
        generationId: "generation-a",
        capacity: 1,
        occupiedSlots: 0,
        recoverableRecordingIds: ["rec-recoverable"],
      }), { status: 200, headers: { "Content-Type": "application/json" } })),
      async () => recoveryPending,
    );
    await monitor.refresh();

    let stopped = false;
    const stop = monitor.stop().then(() => {
      stopped = true;
    });
    await Promise.resolve();
    expect(stopped).toBe(false);

    releaseRecovery?.();
    await stop;
    expect(stopped).toBe(true);
  });

  it("propagates shutdown cancellation to an active recovery", async () => {
    const pool = new RecordingWorkerPool([
      { workerId: "worker-a", controlUrl: "http://worker-a:8090", capacity: 1 },
    ], 1_000);
    let recoveryAborted = false;
    const monitor = new RecordingWorkerLeaseMonitor(
      pool,
      500,
      vi.fn(async () => new Response(JSON.stringify({
        workerId: "worker-a",
        generationId: "generation-a",
        capacity: 1,
        occupiedSlots: 0,
        recoverableRecordingIds: ["rec-recoverable"],
      }), { status: 200, headers: { "Content-Type": "application/json" } })),
      async (_report, signal) => new Promise<void>((resolve) => {
        signal.addEventListener("abort", () => {
          recoveryAborted = true;
          resolve();
        }, { once: true });
      }),
    );
    await monitor.refresh();

    await monitor.stop();

    expect(recoveryAborted).toBe(true);
  });

  it("retains a failed recovery report for the next refresh without leaking a rejected promise", async () => {
    const pool = new RecordingWorkerPool([
      { workerId: "worker-a", controlUrl: "http://worker-a:8090", capacity: 1 },
    ], 1_000);
    const recover = vi.fn()
      .mockRejectedValueOnce(new Error("temporary recovery failure"))
      .mockResolvedValue(undefined);
    const monitor = new RecordingWorkerLeaseMonitor(
      pool,
      500,
      vi.fn(async () => new Response(JSON.stringify({
        workerId: "worker-a",
        generationId: "generation-a",
        capacity: 1,
        occupiedSlots: 0,
        recoverableRecordingIds: ["rec-recoverable"],
      }), { status: 200, headers: { "Content-Type": "application/json" } })),
      recover,
    );

    await monitor.refresh();
    await monitor.settleRecovery();
    await monitor.refresh();
    await monitor.settleRecovery();

    expect(recover).toHaveBeenLastCalledWith({
      recoverableRecordingIds: ["rec-recoverable"],
      pendingFinalizationRecordingIds: [],
    }, expect.any(AbortSignal));
    expect(recover).toHaveBeenCalledTimes(2);
  });
});
