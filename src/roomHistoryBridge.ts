import { createHmac, randomUUID } from "node:crypto";
import type { ServiceJwtConfig, SignalingConfig } from "./config.js";

export type RoomHistoryEvent = { type: "participant.joined" | "participant.left" | "chat.message" | "room.closed"; eventId: string; roomId: string; occurredAt: string; participantId?: string; displayName?: string; messageId?: string; kind?: string; authorName?: string; text?: string; reason?: string };
type RedisOutbox = { rPush(key: string, value: string): Promise<number>; lPop(key: string): Promise<string | null>; expire(key: string, seconds: number): Promise<boolean | number> };
const OUTBOX_KEY = "integration_room_history:outbox";

export class RoomHistoryBridge {
  private timer?: NodeJS.Timeout;
  constructor(private readonly redis: RedisOutbox, private readonly baseUrl: string, private readonly jwt: ServiceJwtConfig) {}
  start(): void { this.timer = setInterval(() => void this.flush(), 2_000); void this.flush(); }
  stop(): void { if (this.timer) clearInterval(this.timer); }
  report(event: Omit<RoomHistoryEvent, "eventId" | "occurredAt">): void { void this.send({ ...event, eventId: `evt_${randomUUID()}`, occurredAt: new Date().toISOString() }).catch(() => this.enqueue({ ...event, eventId: `evt_${randomUUID()}`, occurredAt: new Date().toISOString() })); }
  private async enqueue(event: RoomHistoryEvent): Promise<void> { await this.redis.rPush(OUTBOX_KEY, JSON.stringify(event)); await this.redis.expire(OUTBOX_KEY, 30 * 86_400); }
  private async flush(): Promise<void> { for (;;) { const raw = await this.redis.lPop(OUTBOX_KEY); if (!raw) return; let event: RoomHistoryEvent; try { event = JSON.parse(raw) as RoomHistoryEvent; } catch { continue; } try { await this.send(event); } catch { await this.redis.rPush(OUTBOX_KEY, raw); return; } } }
  private async send(event: RoomHistoryEvent): Promise<void> { const response = await fetch(`${this.baseUrl}/room-history/events`, { method: "POST", headers: { authorization: `Bearer ${jwt(this.jwt)}`, "content-type": "application/json" }, body: JSON.stringify(event), signal: AbortSignal.timeout(5_000) }); if (!response.ok) throw new Error(`history event delivery failed: ${response.status}`); }
}
export function createRoomHistoryBridge(redis: RedisOutbox, config: SignalingConfig["recording"]): RoomHistoryBridge | undefined { return config ? new RoomHistoryBridge(redis, config.controlUrl, config.controlJwt) : undefined; }
function jwt(config: ServiceJwtConfig): string { const now = Math.floor(Date.now() / 1_000); const header = Buffer.from(JSON.stringify({ alg: "HS256", typ: "JWT" })).toString("base64url"); const payload = Buffer.from(JSON.stringify({ iss: config.issuer, aud: config.audience, sub: "signaling", iat: now, exp: now + config.maxTtlSeconds })).toString("base64url"); return `${header}.${payload}.${createHmac("sha256", config.secret).update(`${header}.${payload}`).digest("base64url")}`; }