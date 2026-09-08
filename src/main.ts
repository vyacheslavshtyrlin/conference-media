import { createServer } from "node:http";
import { createClient } from "redis";
import { WebSocketServer } from "ws";

import { readSignalingConfig } from "./config.js";
import { createLogger } from "./logger.js";
import { MediaInstanceLease } from "./mediaInstanceLease.js";
import { MediasoupService } from "./mediasoupService.js";
import { getMediaMetrics } from "./metrics.js";
import { createSignalingHttpHandler } from "./httpEndpoints.js";
import { RedisRoomRepository } from "./redisRoomRepository.js";
import { RedisReadinessProbe } from "./readiness.js";
import { RoomManager } from "./roomManager.js";
import { createRecordingBridge } from "./recordingBridge.js";
import { RecordingBotSessionManager, RecordingBotSocketGateway } from "./recordingBot.js";
import { SocketGateway } from "./socketGateway.js";
import { createRoomHistoryBridge } from "./roomHistoryBridge.js";
import { runBoundedShutdown } from "./shutdown.js";
import { verifyAndConsumeRecordingBotAdmission } from "./token.js";
import { attachWebSocketUpgradeRouter } from "./webSocketUpgradeRouter.js";

const config = readSignalingConfig();
const logger = createLogger("signaling");
const metrics = getMediaMetrics();

process.on("unhandledRejection", (reason) => {
  logger.error("unhandled rejection", { operation: "process.unhandledRejection", errorCode: "UNHANDLED_REJECTION", error: String(reason) });
  process.exit(1);
});

const redis = createClient({ url: config.redisUrl });

redis.on("error", (error) => {
  logger.error("redis error", { operation: "redis.connection", errorCode: "REDIS_ERROR", error: error.message });
});

await redis.connect();
const redisReadiness = new RedisReadinessProbe(redis);

const repository = new RedisRoomRepository(redis, metrics);
const mediaLease = new MediaInstanceLease(
  redis,
  config.mediaLease.ttlMs,
  config.mediaLease.refreshMs,
  logger,
);
await mediaLease.acquire();
const mediasoup = new MediasoupService(
  config.mediasoup,
  logger,
  metrics,
);
try {
  await mediasoup.init();
} catch (error) {
  mediasoup.closeAll();
  await mediaLease.release();
  await redis.quit();
  throw error;
}
const recordingBridge = createRecordingBridge(repository, config.recording, logger);
await recordingBridge?.start();
const roomHistory = createRoomHistoryBridge(redis, config.recording);
roomHistory?.start();
const roomManager = new RoomManager(
  repository,
  mediasoup,
  config.roomEmptyGraceMs,
  logger,
  () => mediaLease.getEpoch(),
  () => mediaLease.isHealthy(),
  metrics,
  recordingBridge,
  undefined,
  roomHistory,
);
const recordingBots = new RecordingBotSessionManager(
  mediasoup,
  (roomId, accept) => roomManager.freezeBotAuthoritySnapshot(roomId, accept),
  ({ roomId, recordingId, code }) => {
    logger.warn("recording bot capture requires recovery", {
      operation: "recordingBot.recover",
      errorCode: code,
      roomId,
      recordingId,
      outcome: "failure",
    });
    void recordingBridge?.recoverRecording(recordingId).catch((error: unknown) => {
      recordingBridge.logFailure("recording.botRecovery", roomId, error);
    });
  },
);
roomManager.setRecordingBotSessionManager(recordingBots);

const server = createServer(createSignalingHttpHandler({
  isRedisReady: () => redisReadiness.check(),
  isMediasoupReady: () => mediasoup.isReady(),
  isMediaLeaseReady: () => mediaLease.isHealthy(),
}, {
  renderMetrics,
}));

const wsServer = new WebSocketServer({
  noServer: true,
  maxPayload: config.websocket.maxPayloadBytes,
});
const gateway = new SocketGateway(wsServer, roomManager, config.joinTokenSecret, logger, config.websocket, metrics);
const botWsServer = config.recordingBotAdmission
  ? new WebSocketServer({ noServer: true, maxPayload: config.websocket.maxPayloadBytes })
  : undefined;
const botGateway = botWsServer && config.recordingBotAdmission
  ? new RecordingBotSocketGateway(
      botWsServer,
      recordingBots,
      (token) => verifyAndConsumeRecordingBotAdmission(token, config.recordingBotAdmission!, redis),
      logger,
    )
  : undefined;

const webSocketRoutes = new Map([["/ws", wsServer]]);
if (botWsServer) webSocketRoutes.set("/internal/recording-bot/v1", botWsServer);
attachWebSocketUpgradeRouter(server, webSocketRoutes);

gateway.start();
botGateway?.start();

server.listen(config.port, "0.0.0.0", () => {
  logger.info("signaling service started", { operation: "service.start", port: config.port });
});

let shutdownPromise: Promise<void> | undefined;

function renderMetrics(): string {
  const roomStats = roomManager.getRuntimeStats();
  const mediaStats = mediasoup.getRuntimeStats();
  const socketStats = gateway.getRuntimeStats();
  return metrics.renderPrometheus({
    activeRooms: roomStats.activeRooms,
    activeParticipants: roomStats.activeParticipants,
    activeSockets: socketStats.activeSockets,
    workers: mediaStats.workers,
    transports: mediaStats.transports,
    producers: mediaStats.producers,
    consumers: mediaStats.consumers,
    recordingWorkerSlots: recordingBridge ? {
      total: recordingBridge.getWorkerPoolSnapshot().totalSlots,
      available: recordingBridge.getWorkerPoolSnapshot().availableSlots,
      occupied: recordingBridge.getWorkerPoolSnapshot().occupiedSlots,
      healthyWorkers: recordingBridge.getWorkerPoolSnapshot().healthyWorkers,
    } : undefined,
  });
}

mediaLease.setLostCallback((error) => {
  logger.error("signaling stops after media lease loss", { operation: "mediaLease.loss", errorCode: "MEDIA_LEASE_LOST", error: error.message });
  void shutdown("media_lease_lost").then(() => process.exit(1)).catch((shutdownError: unknown) => {
    logger.error("lease-loss shutdown failed", { operation: "service.shutdown", errorCode: "SHUTDOWN_FAILED", error: String(shutdownError) });
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
  logger.info("signaling service stopping", { operation: "service.shutdown", reason });
  await runBoundedShutdown(
    async () => {
        await gateway.stop();
        await botGateway?.stop();
        await roomManager.shutdown();
        recordingBridge?.stop();
        mediasoup.closeAll();
        await mediaLease.release();
        await closeWebSocketServers();
        await closeHttpServer();
        if (redis.isOpen) {
          await redis.quit();
        }
    },
    async (error) => {
      logger.error("signaling graceful shutdown forced", { operation: "service.shutdown", reason, errorCode: "SHUTDOWN_TIMEOUT", error: String(error) });
      gateway.forceClose();
      mediasoup.closeAll();
      try {
        await mediaLease.release();
      } catch (releaseError) {
        logger.warn("media lease release failed during forced shutdown", { operation: "mediaLease.release", errorCode: "MEDIA_LEASE_RELEASE_FAILED", error: String(releaseError) });
      }
      if (redis.isOpen) {
        try {
          await redis.quit();
        } catch (redisError) {
          logger.warn("redis quit failed during forced shutdown", { operation: "redis.quit", errorCode: "REDIS_QUIT_FAILED", error: String(redisError) });
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


function closeWebSocketServers(): Promise<void> {
  return Promise.all([wsServer, botWsServer].filter((value): value is WebSocketServer => Boolean(value)).map((webSocketServer) =>
    new Promise<void>((resolve, reject) => {
      webSocketServer.close((error) => error ? reject(error) : resolve());
    }),
  )).then(() => undefined);
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
