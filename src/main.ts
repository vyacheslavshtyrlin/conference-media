import { createServer } from "node:http";
import { createClient } from "redis";
import { WebSocketServer } from "ws";

import { readSignalingConfig } from "./config.js";
import { createLogger } from "./logger.js";
import { MediaInstanceLease } from "./mediaInstanceLease.js";
import { MediasoupService } from "./mediasoupService.js";
import { RedisRoomRepository } from "./redisRoomRepository.js";
import { RoomManager } from "./roomManager.js";
import { SocketGateway } from "./socketGateway.js";
import { runBoundedShutdown } from "./shutdown.js";

const config = readSignalingConfig();
const logger = createLogger("signaling");

process.on("unhandledRejection", (reason) => {
  logger.error("unhandled rejection", { reason: String(reason) });
  process.exit(1);
});

const redis = createClient({ url: config.redisUrl });

redis.on("error", (error) => {
  logger.error("redis error", { error: error.message });
});

await redis.connect();

const repository = new RedisRoomRepository(redis);
const mediaLease = new MediaInstanceLease(
  redis,
  config.mediaLease.ttlMs,
  config.mediaLease.refreshMs,
  logger,
);
await mediaLease.acquire();
const mediasoup = new MediasoupService(config.mediasoup, logger);
try {
  await mediasoup.init();
} catch (error) {
  mediasoup.closeAll();
  await mediaLease.release();
  await redis.quit();
  throw error;
}
const roomManager = new RoomManager(
  repository,
  mediasoup,
  config.roomEmptyGraceMs,
  logger,
  () => mediaLease.getEpoch(),
  () => mediaLease.isHealthy(),
);

const server = createServer((request, response) => {
  const pathname = new URL(request.url ?? "/", "http://localhost").pathname;

  if (pathname === "/health") {
    response.writeHead(200, { "Content-Type": "application/json" });
    response.end(JSON.stringify({ status: "ok", service: "signaling" }));
    return;
  }

  if (pathname === "/ready") {
    const redisReady = redis.isReady;
    const mediasoupReady = mediasoup.isReady();
    const leaseReady = mediaLease.isHealthy();
    const ready = redisReady && mediasoupReady && leaseReady;

    response.writeHead(ready ? 200 : 503, { "Content-Type": "application/json" });
    response.end(
      JSON.stringify({
        status: ready ? "ok" : "unavailable",
        service: "signaling",
        checks: {
          redis: redisReady ? "ok" : "unavailable",
          mediasoup: mediasoupReady ? "ok" : "unavailable",
          mediaLease: leaseReady ? "ok" : "unavailable",
        },
      }),
    );
    return;
  }

  response.writeHead(404);
  response.end();
});

const wsServer = new WebSocketServer({
  server,
  path: "/ws",
  maxPayload: config.websocket.maxPayloadBytes,
});
const gateway = new SocketGateway(wsServer, roomManager, config.joinTokenSecret, logger, config.websocket);

gateway.start();

server.listen(config.port, "0.0.0.0", () => {
  logger.info("signaling service started", { port: config.port });
});

let shutdownPromise: Promise<void> | undefined;

mediaLease.setLostCallback((error) => {
  logger.error("signaling stops after media lease loss", { error: error.message });
  void shutdown("media_lease_lost").then(() => process.exit(1)).catch((shutdownError: unknown) => {
    logger.error("lease-loss shutdown failed", { error: String(shutdownError) });
    process.exit(1);
  });
});

function shutdown(reason: string): Promise<void> {
  if (shutdownPromise) {
    return shutdownPromise;
  }

  shutdownPromise = runShutdown(reason);
  return shutdownPromise;
}

async function runShutdown(reason: string): Promise<void> {
  logger.info("signaling service stopping", { reason });
  await runBoundedShutdown(
    async () => {
        await gateway.stop();
        await roomManager.shutdown();
        mediasoup.closeAll();
        await mediaLease.release();
        await closeWebSocketServer();
        await closeHttpServer();
        if (redis.isOpen) {
          await redis.quit();
        }
    },
    async (error) => {
      logger.error("signaling graceful shutdown forced", { reason, error: String(error) });
      gateway.forceClose();
      mediasoup.closeAll();
      try {
        await mediaLease.release();
      } catch (releaseError) {
        logger.warn("media lease release failed during forced shutdown", { error: String(releaseError) });
      }
      if (redis.isOpen) {
        try {
          await redis.quit();
        } catch (redisError) {
          logger.warn("redis quit failed during forced shutdown", { error: String(redisError) });
        }
      }
    },
    config.shutdownTimeoutMs,
  );
}

process.on("SIGINT", () => {
  void shutdown("SIGINT").then(() => process.exit(0)).catch(() => process.exit(1));
});

process.on("SIGTERM", () => {
  void shutdown("SIGTERM").then(() => process.exit(0)).catch(() => process.exit(1));
});


function closeWebSocketServer(): Promise<void> {
  return new Promise((resolve, reject) => {
    wsServer.close((error) => {
      if (error) {
        reject(error);
        return;
      }

      resolve();
    });
  });
}

function closeHttpServer(): Promise<void> {
  return new Promise((resolve, reject) => {
    server.close((error) => {
      if (error) {
        reject(error);
        return;
      }

      resolve();
    });
  });
}
