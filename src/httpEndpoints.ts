import type { IncomingMessage, ServerResponse } from "node:http";
export type SignalingReadiness = {
  isRedisReady: () => Promise<boolean>;
  isMediasoupReady: () => boolean;
  isMediaLeaseReady: () => boolean;
};

export type SignalingMetricsEndpoint = {
  renderMetrics: () => string;
};

export function createSignalingHttpHandler(readiness: SignalingReadiness, metrics?: SignalingMetricsEndpoint) {
  return async (request: IncomingMessage, response: ServerResponse): Promise<void> => {
    const pathname = new URL(request.url ?? "/", "http://localhost").pathname;

    if (pathname === "/health") {
      writeJson(response, 200, { status: "ok", service: "signaling" });
      return;
    }

    if (pathname === "/ready") {
      const redisReady = await readiness.isRedisReady();
      const mediasoupReady = readiness.isMediasoupReady();
      const leaseReady = readiness.isMediaLeaseReady();
      const ready = redisReady && mediasoupReady && leaseReady;

      writeJson(response, ready ? 200 : 503, {
        status: ready ? "ok" : "unavailable",
        service: "signaling",
        checks: {
          redis: redisReady ? "ok" : "unavailable",
          mediasoup: mediasoupReady ? "ok" : "unavailable",
          mediaLease: leaseReady ? "ok" : "unavailable",
        },
      });
      return;
    }

    if (pathname === "/metrics" && metrics) {
      writeText(response, 200, metrics.renderMetrics());
      return;
    }

    response.writeHead(404);
    response.end();
  };
}

function writeJson(response: ServerResponse, statusCode: number, body: object): void {
  response.writeHead(statusCode, { "Content-Type": "application/json" });
  response.end(JSON.stringify(body));
}

function writeText(response: ServerResponse, statusCode: number, body: string): void {
  response.writeHead(statusCode, { "Content-Type": "text/plain; version=0.0.4; charset=utf-8" });
  response.end(body);
}
