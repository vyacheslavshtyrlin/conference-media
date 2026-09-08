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
    expect(recover).toHaveBeenCalledWith(["rec-recoverable"]);
  });
});
