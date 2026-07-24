# Документация проекта media

Signaling/media сервис видеоконференций. Стек: **Node.js + TypeScript + mediasoup + ws**.
Отвечает за WebRTC signaling, runtime-управление комнатами, mediasoup transports/producers/consumers, Redis state, active-speaker detection, чат и выдачу ICE/TURN-конфигурации клиентам. Контракты WS описаны **Zod** в `src/contracts/` и едины для всех сервисов.

В production репозиторий также поднимает **Redis** и **coturn (TURN)** через `docker-compose.yml` (двухсерверная модель: media-сервер хостит signaling/media, Redis и TURN).

---

## Порты

| Сервис / процесс | Порт | Где настраивается |
| --- | --- | --- |
| Signaling HTTP/WebSocket | `4000` (внутр.) | `PORT`, `docker-compose.yml` проброс `SIGNALING_PORT` |
| mediasoup WebRTC (UDP/TCP) | `40000–40100` | `MEDIASOUP_RTC_MIN_PORT` / `MEDIASOUP_RTC_MAX_PORT` |
| TURN (coturn) | `3478` (UDP/TCP) | `deploy/turnserver.prod.conf`, compose |
| TURN relay range | `50000–50100` (UDP) | `TURN_MIN_PORT` / `TURN_MAX_PORT` |
| Redis (bundled) | `6379` | `REDIS_BIND_ADDR`, том `redis-data` |

Edge-маршрутизация (на app-сервере): `wss://domain/ws` → `media:4000/ws`. На media-сервере должны быть открыты порты `4000/tcp`, `40000-40100/udp,tcp`, `3478/tcp,udp`, `50000-50100/udp`.

---

## HTTP/WS endpoints

| Метод | Путь | Назначение |
| --- | --- | --- |
| `GET` | `/health` | Liveness: `{status, service: "signaling"}`. |
| `GET` | `/ready` | Readiness: проверяет Redis + mediasoup (`200`/`503` + `checks`). |
| `GET` | `/metrics` | Prometheus text metrics для ops: агрегированные счетчики/гистограммы/gauges без roomId, participantId, requestId, commandId или token labels. |
| `WS` | `/ws` | Signaling-протокол: join, mediasoup transport/produce/consume, media state, chat, leave, close. |

WebSocket: heartbeat, max payload, backpressure, rate limit, проверка origin (`WS_ALLOWED_ORIGINS`). Авторизация — join-токен в query `?token=` (HMAC-SHA256, общий секрет с API).

---

## Переменные окружения

| Переменная | Назначение |
| --- | --- |
| `NODE_ENV` | Режим выполнения (`production`). |
| `PORT` | Порт signaling (по умолчанию `4000`). |
| `MEDIA_REDIS_URL` / `REDIS_URL` | Redis внутри compose / для внешнего запуска. |
| `JOIN_TOKEN_SECRET` | Секрет проверки join-токена. **Должен совпадать** с `conference-be`. |
| `RECORDING_CONTROL_URL` | Private API URL с суффиксом `/internal/v1` для control plane записи. |
| `RECORDING_CONTROL_JWT_HS256_SECRET` | Общий с API секрет signaling → API внутренних recording events. |
| `RECORDING_BOT_ADMISSION_JWT_HS256_SECRET` | Общий с API секрет выдачи короткоживущих admission для receive-only capture bot. |
| `RECORDING_BOT_WORKERS_JSON` | JSON-массив capture worker-ов: `workerId`, `controlUrl`, `capacity`. Production registry содержит `recording-worker-1` … `recording-worker-5` с портами `8091` … `8095` и capacity `10` у каждого. |
| `RECORDING_BOT_SIGNALING_URL` | WebSocket URL signaling для приватного capture worker: `wss://media.victoryspace.ru/internal/recording-bot/v1`. |
| `RECORDING_WORKER_LEASE_TTL_MS` / `RECORDING_WORKER_HEARTBEAT_MS` | Lease и heartbeat capacity admission; heartbeat должен быть меньше lease TTL. |
| `ROOM_EMPTY_GRACE_MS` | Задержка очистки пустой комнаты (по умолчанию `10000`). |
| `MEDIA_LEASE_TTL_MS` / `MEDIA_LEASE_REFRESH_MS` | TTL и refresh интервал single-node media lease (`15000` / `5000` ms); lease связывает Redis live participants с текущим signaling process. |
| `SHUTDOWN_TIMEOUT_MS` | Bounded graceful-shutdown timeout (`10000` ms); после него runtime закрывается принудительно. |
| `WS_HEARTBEAT_INTERVAL_MS` | Интервал heartbeat (по умолчанию `30000`). |
| `WS_MAX_PAYLOAD_BYTES` | Максимум размера WS-сообщения (по умолчанию `65536`). |
| `WS_SEND_BACKPRESSURE_BYTES` | Порог backpressure отправки (по умолчанию `262144`). |
| `WS_RATE_LIMIT_WINDOW_MS` / `WS_RATE_LIMIT_MAX_MESSAGES` | Rate limit WS (окно `10000` / `60` сообщений). |
| `WS_ALLOWED_ORIGINS` | Разрешённые browser origins (CSV). |
| `MEDIASOUP_LISTEN_IP` | IP mediasoup внутри контейнера (по умолчанию `0.0.0.0`). |
| `MEDIASOUP_ANNOUNCED_IP` | Публичный IP в ICE candidates (обязательный). |
| `MEDIASOUP_RTC_MIN_PORT` / `MEDIASOUP_RTC_MAX_PORT` | Диапазон RTC-портов (по умолчанию `40000`–`40100`). |
| `MEDIASOUP_NUM_WORKERS` | Кол-во worker-процессов mediasoup (по умолчанию `2`). |
| `MEDIASOUP_MAX_TRANSPORTS_PER_PEER` | Лимит transports на участника (по умолчанию `4`). |
| `MEDIASOUP_MAX_PRODUCERS_PER_PEER` | Лимит producers (по умолчанию `3`). |
| `MEDIASOUP_MAX_CONSUMERS_PER_PEER` | Лимит consumers (по умолчанию `32`). |
| `TURN_URLS` / `TURN_USERNAME` / `TURN_PASSWORD` / `TURN_REALM` | TURN-конфиг, отдаваемый браузерам как ICE servers. |
| `TURN_MIN_PORT` / `TURN_MAX_PORT` | Диапазон relay-портов coturn (по умолчанию `50000`–`50100`). |
| `MEDIA_IMAGE` | Docker-образ для `docker-compose.yml` (подставляется CI). |

---

## Структура `src/`

### Корень
| Файл | Что делает |
| --- | --- |
| `main.ts` | Bootstrap: создаёт Redis-клиент, `MediasoupService`, `RoomManager`, HTTP-сервер (`/health`, `/ready`, `/metrics`) и `WebSocketServer` (`/ws`), `SocketGateway`. Graceful shutdown (SIGINT/SIGTERM). |
| `config.ts` | `readSignalingConfig()` — собирает конфиг из env (port, redis, WS-лимиты, mediasoup, ICE/TURN). |
| `env.ts` | Хелперы чтения env (`readEnv`, `readNumberEnv`). |
| `logger.ts` | JSON-логгер (`createLogger`): info/warn/error с контекстом. |
| `errors.ts` | `SignalingError` (с кодом) + `toContractError` для WS-ответов. |
| `token.ts` | `verifyJoinToken()` — проверка подписи HS256, декодирование payload, проверка срока действия. |
| `contracts/index.ts` | Zod-схемы/типы WS-контрактов (клиентские и серверные события, участники, чат, коды ошибок). Единый источник истины для протокола. |

### Основные модули
| Файл | Что делает |
| --- | --- |
| `socketGateway.ts` | `SocketGateway` — граница протокола: принимает WS-соединения, проверяет join-токен и origin, валидирует сообщения (zod), роутит запросы в `RoomManager`, broadcast событий участникам, heartbeat, rate limit, backpressure. |
| `roomManager.ts` | `RoomManager` — lifecycle комнат: join/leave, медиа-операции (transport/produce/consume/closeProducer), смена media state и качества связи, чат, закрытие комнаты создателем, cleanup пустой комнаты по grace-TTL. Мьютексы на комнату. |
| `peerManager.ts` | `PeerManager` — состояние участников комнаты в памяти (добавление, media, connectionQuality, удаление, список). |
| `mediasoupService.ts` | `MediasoupService` — mediasoup runtime: workers, routers, WebRTC-транспортов (send/recv), producers, consumers, `AudioLevelObserver` (active speaker), лимиты на peer, обработка смерти комнаты. |
| `redisRoomRepository.ts` | `RedisRoomRepository` — персистентное состояние комнат: метаданные, live-участники, чат, TTL/empty/close. Только сериализуемые данные (runtime-объекты mediasoup в Redis не хранятся). |

### Спецификации
| Файл | Что делает |
| --- | --- |
| `mediasoupService.spec.ts` | Тесты mediasoup-сервиса. |
| `roomManager.spec.ts` | Тесты менеджера комнат. |
| `socketGateway.spec.ts` | Тесты WS-шлюза. |

### `deploy/`
| Файл | Что делает |
| --- | --- |
| `turnserver.prod.conf` | Базовый конфиг coturn (listening-port 3478, relay 50000–50100, lt-cred-mech). Секреты/realm/port передаются аргументами compose. |
| `turnserver.conf` | Локальный конфиг coturn. |

---

## Ключи в Redis

| Ключ | Тип | Назначение |
| --- | --- | --- |
| `room:{roomId}` | string | Метаданные комнаты (общий с API формат). |
| `room_slug:{slug}` | string | slug → roomId. |
| `room:{roomId}:participants` | hash | Live-участники (заполняется media, читается API). |
| `room:{roomId}:chat` | list | История чата (лимит `CHAT_HISTORY_LIMIT`). |
| `room:{roomId}:sessions` | hash | Сессии (общий с API). |

---

## CI/CD

`.gitlab-ci.yml` — pipeline на ветке `main`:
1. **validate** — lint, typecheck, test, build (`allow_failure: true`).
2. **build** — сборка Docker-образа (`--target production`), теги `<sha>` + `latest`.
3. **deploy** — копирование `.env`, `docker compose up -d` (signaling + redis + coturn).
4. **cleanup** — prune контейнеров/образов старше 24ч.

Runner: `158.160.10.26` (media-сервер). Продуктивная среда: `prod` (`$PROD_URL`).
