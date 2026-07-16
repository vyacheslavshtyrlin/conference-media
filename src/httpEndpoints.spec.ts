import { createServer, type Server } from "node:http";
import { afterEach, describe, expect, it } from "vitest";

import {
  createSignalingHttpHandler,
  type SignalingMetricsEndpoint,
  type SignalingReadiness,
} from "./httpEndpoints.js";

const servers: Server[] = [];
const unavailableScenarios: Array<[string, SignalingReadiness, "redis" | "mediaLease" | "mediasoup"]> = [
  ["Redis unavailable", { isRedisReady: async (): Promise<boolean> => false, isMediasoupReady: (): boolean => true, isMediaLeaseReady: (): boolean => true }, "redis"],
  ["lease lost", { isRedisReady: async (): Promise<boolean> => true, isMediasoupReady: (): boolean => true, isMediaLeaseReady: (): boolean => false }, "mediaLease"],
  ["no usable worker", { isRedisReady: async (): Promise<boolean> => true, isMediasoupReady: (): boolean => false, isMediaLeaseReady: (): boolean => true }, "mediasoup"],
];

afterEach(async () => {
  await Promise.all(servers.splice(0).map((server) => new Promise<void>((resolve, reject) => {
    server.close((error) => error ? reject(error) : resolve());
  })));
});

describe("signaling HTTP readiness", () => {
  it("returns the full stable signaling envelope when every readiness check passes", async () => {
    const response = await requestReady({
      isRedisReady: async (): Promise<boolean> => true,
      isMediasoupReady: (): boolean => true,
      isMediaLeaseReady: (): boolean => true,
    });

    expect(response.status).toBe(200);
    await expect(response.json()).resolves.toEqual({
      status: "ok",
      service: "signaling",
      checks: { redis: "ok", mediasoup: "ok", mediaLease: "ok" },
    });
  });

  it("preserves the dependency-free health response", async () => {
    const response = await request({
      isRedisReady: async (): Promise<boolean> => false,
      isMediasoupReady: (): boolean => false,
      isMediaLeaseReady: (): boolean => false,
    }, "/health");

    expect(response.status).toBe(200);
    await expect(response.json()).resolves.toEqual({ status: "ok", service: "signaling" });
  });

  it.each(unavailableScenarios)("returns a 503 envelope when %s", async (_scenario, readiness, failedCheck) => {
    const response = await requestReady(readiness);

    expect(response.status).toBe(503);
    expect(await response.json()).toMatchObject({
      status: "unavailable",
      service: "signaling",
      checks: { [failedCheck]: "unavailable" },
    });
  });

  it("serves Prometheus metrics without running readiness checks", async () => {
    const response = await request({
      isRedisReady: async (): Promise<boolean> => false,
      isMediasoupReady: (): boolean => false,
      isMediaLeaseReady: (): boolean => false,
    }, "/metrics", {
      renderMetrics: () => "signaling_active_rooms 0\n",
    });

    expect(response.status).toBe(200);
    expect(response.headers.get("content-type")).toContain("text/plain");
    await expect(response.text()).resolves.toBe("signaling_active_rooms 0\n");
  });
});

async function requestReady(readiness: SignalingReadiness): Promise<Response> {
  return request(readiness, "/ready");
}

async function request(
  readiness: SignalingReadiness,
  path: string,
  metrics?: SignalingMetricsEndpoint,
): Promise<Response> {
  const server = createServer(createSignalingHttpHandler(readiness, metrics));
  servers.push(server);
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  if (!address || typeof address === "string") {
    throw new Error("Test server did not bind a TCP port");
  }
  return fetch(`http://127.0.0.1:${address.port}${path}`);
}
