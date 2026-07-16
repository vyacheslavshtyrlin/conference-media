import { monitorEventLoopDelay } from "node:perf_hooks";

type MetricLabels = Record<string, string>;
type Outcome = "ok" | "client_error" | "failure";

type HistogramState = {
  buckets: number[];
  bucketCounts: Map<string, number[]>;
  counts: Map<string, number>;
  sums: Map<string, number>;
  labelValues: Map<string, MetricLabels>;
};

export type MediaRuntimeSnapshot = {
  activeRooms: number;
  activeParticipants: number;
  activeSockets: number;
  workers: number;
  transports: number;
  producers: number;
  consumers: number;
};

const forbiddenLabelNames = new Set([
  "authorization",
  "commandId",
  "command_id",
  "participantId",
  "participant_id",
  "requestId",
  "request_id",
  "roomId",
  "room_id",
  "token",
]);

const operationDurationBuckets = [0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5];
const commandDurationBuckets = [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10];

export class MediaMetrics {
  private readonly counters = new Map<string, number>();
  private readonly counterLabels = new Map<string, MetricLabels>();
  private readonly histograms = new Map<string, HistogramState>();
  private readonly eventLoopDelay = monitorEventLoopDelay({ resolution: 20 });

  constructor() {
    this.eventLoopDelay.enable();
  }

  recordWebSocketConnect(input: { outcome: Outcome; errorCode: string | null; durationMs: number }): void {
    const labels = {
      outcome: input.outcome,
      error_code: input.errorCode ?? "none",
    };
    this.incrementCounter("signaling_websocket_connects_total", labels);
    this.observeHistogram(
      "signaling_websocket_connect_duration_seconds",
      labels,
      input.durationMs / 1_000,
      operationDurationBuckets,
    );
  }

  recordWebSocketDisconnect(input: { reason: "client_close" | "server_close" | "heartbeat_timeout" | "backpressure" }): void {
    this.incrementCounter("signaling_websocket_disconnects_total", { reason: input.reason });
  }

  recordWebSocketReconnect(input: { outcome: "ok" | "failure"; reason: "session_superseded" }): void {
    this.incrementCounter("signaling_websocket_reconnects_total", {
      outcome: input.outcome,
      reason: input.reason,
    });
  }

  recordWebSocketCommand(input: {
    operation: string;
    outcome: Outcome;
    errorCode: string | null;
    durationMs: number;
  }): void {
    const labels = {
      operation: sanitizeOperation(input.operation),
      outcome: input.outcome,
      error_code: input.errorCode ?? "none",
    };
    this.incrementCounter("signaling_websocket_commands_total", labels);
    this.observeHistogram(
      "signaling_websocket_command_duration_seconds",
      labels,
      input.durationMs / 1_000,
      commandDurationBuckets,
    );
  }

  recordRoomCleanup(input: {
    operation: "empty_room" | "room_dead" | "creator_close" | "join_rollback" | "shutdown";
    outcome: "ok" | "failure";
    reason?: string;
  }): void {
    this.incrementCounter("signaling_room_cleanup_total", {
      operation: input.operation,
      outcome: input.outcome,
      reason: input.reason ? sanitizeOperation(input.reason) : "none",
    });
  }

  recordRedisOperation(input: { operation: string; outcome: "ok" | "failure"; durationMs: number }): void {
    const labels = {
      operation: sanitizeOperation(input.operation),
      outcome: input.outcome,
    };
    this.incrementCounter("signaling_redis_operations_total", labels);
    this.observeHistogram(
      "signaling_redis_operation_duration_seconds",
      labels,
      input.durationMs / 1_000,
      operationDurationBuckets,
    );
  }

  renderPrometheus(snapshot: MediaRuntimeSnapshot): string {
    const eventLoopMeanSeconds = nanosecondsToSeconds(this.eventLoopDelay.mean);
    const eventLoopMaxSeconds = nanosecondsToSeconds(this.eventLoopDelay.max);
    const cpu = process.cpuUsage();
    const memory = process.memoryUsage();

    const lines: string[] = [
      "# HELP signaling_active_rooms Active signaling runtime rooms.",
      "# TYPE signaling_active_rooms gauge",
      `signaling_active_rooms ${snapshot.activeRooms}`,
      "# HELP signaling_active_participants Active signaling participants.",
      "# TYPE signaling_active_participants gauge",
      `signaling_active_participants ${snapshot.activeParticipants}`,
      "# HELP signaling_active_sockets Active WebSocket connections.",
      "# TYPE signaling_active_sockets gauge",
      `signaling_active_sockets ${snapshot.activeSockets}`,
      "# HELP mediasoup_workers Usable mediasoup workers.",
      "# TYPE mediasoup_workers gauge",
      `mediasoup_workers ${snapshot.workers}`,
      "# HELP mediasoup_transports Active mediasoup transports.",
      "# TYPE mediasoup_transports gauge",
      `mediasoup_transports ${snapshot.transports}`,
      "# HELP mediasoup_producers Active mediasoup producers.",
      "# TYPE mediasoup_producers gauge",
      `mediasoup_producers ${snapshot.producers}`,
      "# HELP mediasoup_consumers Active mediasoup consumers.",
      "# TYPE mediasoup_consumers gauge",
      `mediasoup_consumers ${snapshot.consumers}`,
      "# HELP signaling_websocket_connects_total Total WebSocket connection attempts.",
      "# TYPE signaling_websocket_connects_total counter",
      ...this.renderCounters("signaling_websocket_connects_total"),
      "# HELP signaling_websocket_connect_duration_seconds WebSocket connection and join latency.",
      "# TYPE signaling_websocket_connect_duration_seconds histogram",
      ...this.renderHistogram("signaling_websocket_connect_duration_seconds"),
      "# HELP signaling_websocket_disconnects_total Total WebSocket disconnects.",
      "# TYPE signaling_websocket_disconnects_total counter",
      ...this.renderCounters("signaling_websocket_disconnects_total"),
      "# HELP signaling_websocket_reconnects_total Total WebSocket reconnect and supersede events.",
      "# TYPE signaling_websocket_reconnects_total counter",
      ...this.renderCounters("signaling_websocket_reconnects_total"),
      "# HELP signaling_websocket_commands_total Total WebSocket commands.",
      "# TYPE signaling_websocket_commands_total counter",
      ...this.renderCounters("signaling_websocket_commands_total"),
      "# HELP signaling_websocket_command_duration_seconds WebSocket command latency.",
      "# TYPE signaling_websocket_command_duration_seconds histogram",
      ...this.renderHistogram("signaling_websocket_command_duration_seconds"),
      "# HELP signaling_room_cleanup_total Total room cleanup and reconciliation outcomes.",
      "# TYPE signaling_room_cleanup_total counter",
      ...this.renderCounters("signaling_room_cleanup_total"),
      "# HELP signaling_redis_operations_total Total signaling Redis operations.",
      "# TYPE signaling_redis_operations_total counter",
      ...this.renderCounters("signaling_redis_operations_total"),
      "# HELP signaling_redis_operation_duration_seconds Signaling Redis operation latency.",
      "# TYPE signaling_redis_operation_duration_seconds histogram",
      ...this.renderHistogram("signaling_redis_operation_duration_seconds"),
      "# HELP nodejs_event_loop_lag_seconds Event loop delay from monitorEventLoopDelay.",
      "# TYPE nodejs_event_loop_lag_seconds gauge",
      `nodejs_event_loop_lag_seconds{stat="mean"} ${eventLoopMeanSeconds}`,
      `nodejs_event_loop_lag_seconds{stat="max"} ${eventLoopMaxSeconds}`,
      "# HELP process_cpu_user_seconds_total Total user CPU time spent in seconds.",
      "# TYPE process_cpu_user_seconds_total counter",
      `process_cpu_user_seconds_total ${cpu.user / 1_000_000}`,
      "# HELP process_cpu_system_seconds_total Total system CPU time spent in seconds.",
      "# TYPE process_cpu_system_seconds_total counter",
      `process_cpu_system_seconds_total ${cpu.system / 1_000_000}`,
      "# HELP process_resident_memory_bytes Resident memory size in bytes.",
      "# TYPE process_resident_memory_bytes gauge",
      `process_resident_memory_bytes ${memory.rss}`,
      "# HELP process_heap_used_bytes Process heap used in bytes.",
      "# TYPE process_heap_used_bytes gauge",
      `process_heap_used_bytes ${memory.heapUsed}`,
      "# HELP process_uptime_seconds Process uptime in seconds.",
      "# TYPE process_uptime_seconds gauge",
      `process_uptime_seconds ${process.uptime()}`,
      "",
    ];

    return lines.join("\n");
  }

  reset(): void {
    this.counters.clear();
    this.counterLabels.clear();
    this.histograms.clear();
    this.eventLoopDelay.reset();
  }

  private incrementCounter(name: string, labels: MetricLabels, amount = 1): void {
    const key = metricKey(name, labels);
    this.counters.set(key, (this.counters.get(key) ?? 0) + amount);
    this.counterLabels.set(key, labels);
  }

  private observeHistogram(name: string, labels: MetricLabels, value: number, buckets: number[]): void {
    const state = this.histograms.get(name) ?? {
      buckets,
      bucketCounts: new Map<string, number[]>(),
      counts: new Map<string, number>(),
      sums: new Map<string, number>(),
      labelValues: new Map<string, MetricLabels>(),
    };
    const key = metricKey(name, labels);
    const nextBucketCounts = state.bucketCounts.get(key) ?? state.buckets.map(() => 0);
    state.buckets.forEach((bucket, index) => {
      if (value <= bucket) {
        nextBucketCounts[index] = (nextBucketCounts[index] ?? 0) + 1;
      }
    });
    state.bucketCounts.set(key, nextBucketCounts);
    state.counts.set(key, (state.counts.get(key) ?? 0) + 1);
    state.sums.set(key, (state.sums.get(key) ?? 0) + value);
    state.labelValues.set(key, labels);
    this.histograms.set(name, state);
  }

  private renderCounters(name: string): string[] {
    return Array.from(this.counters.entries())
      .filter(([key]) => key.startsWith(`${name}|`))
      .map(([key, value]) => `${name}${formatMetricLabels(this.counterLabels.get(key) ?? {})} ${value}`);
  }

  private renderHistogram(name: string): string[] {
    const state = this.histograms.get(name);
    if (!state) {
      return [];
    }

    const lines: string[] = [];
    for (const [key, bucketCounts] of state.bucketCounts.entries()) {
      const labels = state.labelValues.get(key) ?? {};
      const count = state.counts.get(key) ?? 0;
      const sum = state.sums.get(key) ?? 0;

      for (const [index, bucket] of state.buckets.entries()) {
        lines.push(`${name}_bucket${formatMetricLabels({ ...labels, le: String(bucket) })} ${bucketCounts[index] ?? 0}`);
      }
      lines.push(`${name}_bucket${formatMetricLabels({ ...labels, le: "+Inf" })} ${count}`);
      lines.push(`${name}_sum${formatMetricLabels(labels)} ${sum}`);
      lines.push(`${name}_count${formatMetricLabels(labels)} ${count}`);
    }

    return lines;
  }
}

let defaultMetrics: MediaMetrics | undefined;

export function getMediaMetrics(): MediaMetrics {
  defaultMetrics ??= new MediaMetrics();
  return defaultMetrics;
}

export function resetMediaMetricsForTests(): void {
  getMediaMetrics().reset();
}

export function formatMetricLabels(labels: MetricLabels): string {
  const entries = Object.entries(labels);
  if (entries.length === 0) {
    return "";
  }

  const formatted = entries
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([key, value]) => {
      if (forbiddenLabelNames.has(key)) {
        throw new Error(`Forbidden high-cardinality metric label: ${key}`);
      }
      if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(key)) {
        throw new Error(`Invalid metric label name: ${key}`);
      }
      if (value.length > 120) {
        throw new Error(`Metric label value is too long for ${key}`);
      }
      return `${key}="${value.replace(/\\/g, "\\\\").replace(/"/g, "\\\"")}"`;
    })
    .join(",");

  return `{${formatted}}`;
}

function metricKey(name: string, labels: MetricLabels): string {
  return `${name}|${formatMetricLabels(labels)}`;
}

function sanitizeOperation(value: string): string {
  return /^[a-zA-Z0-9_.:-]{1,80}$/.test(value) ? value : "other";
}

function nanosecondsToSeconds(value: number): number {
  return Number.isFinite(value) && value > 0 ? value / 1_000_000_000 : 0;
}
