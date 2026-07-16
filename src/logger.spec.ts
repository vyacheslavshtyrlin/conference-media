import { describe, expect, it, vi } from "vitest";

import { createLogger } from "./logger.js";

describe("structured media logger", () => {
  it("adds the operational fields and recursively redacts credentials", () => {
    const write = vi.spyOn(console, "info").mockImplementation(() => undefined);
    const logger = createLogger("signaling");

    logger.info("websocket command completed", {
      operation: "chat:send",
      commandId: "command_1",
      durationMs: 12,
      token: "join-token",
      authorization: "Bearer private",
      websocketUrl: "ws://example.test/ws?token=join-token&credential=private",
      headers: { authorization: "Bearer nested-private" },
      retries: [
        { token: "nested-token" },
        "wss://user:password@example.test/ws?credential=query-secret",
      ],
      error: "upstream failed for wss://example.test/ws?token=message-token Authorization: message-secret",
    });

    expect(write).toHaveBeenCalledOnce();
    const serialized = write.mock.calls[0]?.[0];
    if (typeof serialized !== "string") {
      throw new Error("Logger did not write a serialized payload");
    }
    const payload = JSON.parse(serialized) as Record<string, unknown>;
    expect(payload).toMatchObject({
      service: "signaling",
      severity: "info",
      operation: "chat:send",
      commandId: "command_1",
      durationMs: 12,
      token: "[REDACTED]",
      authorization: "[REDACTED]",
      websocketUrl: "ws://example.test/ws",
      headers: { authorization: "[REDACTED]" },
      retries: [
        { token: "[REDACTED]" },
        "wss://example.test/ws",
      ],
      error: "upstream failed for wss://example.test/ws?token=[REDACTED] Authorization: [REDACTED]",
    });
    expect(typeof payload.timestamp).toBe("string");
  });
});
