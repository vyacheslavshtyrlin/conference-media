import { describe, expect, it } from "vitest";

import { formatMetricLabels, MediaMetrics } from "./metrics.js";

const emptySnapshot = {
  activeRooms: 0,
  activeParticipants: 0,
  activeSockets: 0,
  workers: 0,
  transports: 0,
  producers: 0,
  consumers: 0,
};

describe("media metrics", () => {
  it("renders low-cardinality Prometheus metrics for signaling and mediasoup runtime state", () => {
    const metrics = new MediaMetrics();

    metrics.recordWebSocketConnect({ outcome: "ok", errorCode: null, durationMs: 12 });
    metrics.recordWebSocketCommand({
      operation: "chat:send",
      outcome: "client_error",
      errorCode: "INVALID_PAYLOAD",
      durationMs: 7,
    });
    metrics.recordWebSocketDisconnect({ reason: "heartbeat_timeout" });
    metrics.recordWebSocketReconnect({ outcome: "ok", reason: "session_superseded" });
    metrics.recordRoomCleanup({ operation: "room_dead", outcome: "failure", reason: "media_server_error" });
    metrics.recordRedisOperation({ operation: "get_room_metadata", outcome: "failure", durationMs: 3 });

    const body = metrics.renderPrometheus({
      ...emptySnapshot,
      activeRooms: 2,
      activeParticipants: 5,
      activeSockets: 4,
      workers: 2,
      transports: 8,
      producers: 6,
      consumers: 12,
      recordingWorkerSlots: { total: 4, available: 1, occupied: 3, healthyWorkers: 2 },
    });

    expect(body).toContain("signaling_active_rooms 2");
    expect(body).toContain("signaling_active_participants 5");
    expect(body).toContain("signaling_active_sockets 4");
    expect(body).toContain("mediasoup_workers 2");
    expect(body).toContain("mediasoup_transports 8");
    expect(body).toContain("recording_worker_slots_total 4");
    expect(body).toContain("recording_worker_slots_available 1");
    expect(body).toContain("recording_worker_slots_occupied 3");
    expect(body).toContain("recording_workers_healthy 2");
    expect(body).toContain('signaling_websocket_connects_total{error_code="none",outcome="ok"} 1');
    expect(body).toContain(
      'signaling_websocket_commands_total{error_code="INVALID_PAYLOAD",operation="chat:send",outcome="client_error"} 1',
    );
    expect(body).toContain('signaling_websocket_disconnects_total{reason="heartbeat_timeout"} 1');
    expect(body).toContain('signaling_websocket_reconnects_total{outcome="ok",reason="session_superseded"} 1');
    expect(body).toContain(
      'signaling_room_cleanup_total{operation="room_dead",outcome="failure",reason="media_server_error"} 1',
    );
    expect(body).toContain('signaling_redis_operations_total{operation="get_room_metadata",outcome="failure"} 1');
    expect(body).toContain('nodejs_event_loop_lag_seconds{stat="mean"}');
    expect(body).not.toContain("roomId=");
    expect(body).not.toContain("room_id=");
    expect(body).not.toContain("participantId=");
    expect(body).not.toContain("participant_id=");
    expect(body).not.toContain("token=");
  });

  it("rejects high-cardinality metric label names", () => {
    expect(() => formatMetricLabels({ roomId: "room_1" })).toThrow("Forbidden high-cardinality metric label");
    expect(() => formatMetricLabels({ participant_id: "participant_1" })).toThrow(
      "Forbidden high-cardinality metric label",
    );
    expect(() => formatMetricLabels({ token: "secret" })).toThrow("Forbidden high-cardinality metric label");
  });
});
