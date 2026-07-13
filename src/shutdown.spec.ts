import { describe, expect, it, vi } from "vitest";

import { runBoundedShutdown } from "./shutdown.js";

describe("runBoundedShutdown", () => {
  it("forces close when an awaited cleanup rejects", async () => {
    const forced = vi.fn(async () => {});

    await expect(runBoundedShutdown(
      async () => { throw new Error("redis cleanup failed"); },
      forced,
      100,
    )).resolves.toEqual({ forced: true });

    expect(forced).toHaveBeenCalledWith(expect.objectContaining({ message: "redis cleanup failed" }));
  });

  it("forces close when graceful cleanup exceeds its timeout", async () => {
    const forced = vi.fn(async () => {});

    await expect(runBoundedShutdown(
      () => new Promise<void>(() => {}),
      forced,
      5,
    )).resolves.toEqual({ forced: true });

    expect(forced).toHaveBeenCalledWith(expect.objectContaining({ message: "Shutdown timed out after 5ms" }));
  });
});
