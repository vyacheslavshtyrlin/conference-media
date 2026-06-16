# Conference Media

Signaling/media сервис видеоконференций. Это Node.js + TypeScript сервис с WebSocket API и mediasoup. Он отвечает за WebRTC signaling, управление комнатами в runtime, mediasoup transports/producers/consumers, Redis state и выдачу ICE/TURN конфигурации клиентам.

В production этот репозиторий также содержит compose для Redis и coturn. В двухсерверной модели media server запускает signaling/media, Redis и TURN.

## Команды

```bash
npm ci
npm run dev
npm run build
npm run lint
npm run typecheck
npm run test
```

Docker:

```bash
docker compose --env-file .env.example config
docker compose --env-file .env up -d --build
docker compose --env-file .env down
```

## HTTP/WebSocket

Сервис слушает `PORT`, по умолчанию `4000`.

Endpoints:

```text
GET /health
GET /ready
WS  /ws
```

## Переменные окружения

| Переменная | Пример | Описание |
| --- | --- | --- |
| `NODE_ENV` | `production` | Режим выполнения Node.js приложения. |
| `PORT` | `4000` | Порт signaling HTTP/WebSocket сервиса. |
| `MEDIA_IMAGE` | `registry.git.victory-crm.ru/conference/media:latest` | Docker image, который использует `docker-compose.yml` при деплое. |
| `REDIS_URL` | `redis://localhost:6379` | Redis URL для запуска сервиса без bundled Redis. |
| `MEDIA_REDIS_URL` | `redis://redis:6379` | Redis URL внутри `docker-compose.yml`, когда Redis поднимается рядом с media сервисом. |
| `JOIN_TOKEN_SECRET` | `replace-with-shared-secret` | Секрет проверки join token. Должен совпадать с API. |
| `ROOM_EMPTY_GRACE_MS` | `60000` | Задержка перед очисткой пустой комнаты в миллисекундах. |
| `WS_HEARTBEAT_INTERVAL_MS` | `30000` | Интервал heartbeat для WebSocket соединений. |
| `WS_MAX_PAYLOAD_BYTES` | `65536` | Максимальный размер входящего WebSocket сообщения. |
| `WS_SEND_BACKPRESSURE_BYTES` | `262144` | Порог backpressure для WebSocket отправки. |
| `WS_RATE_LIMIT_WINDOW_MS` | `10000` | Окно rate limit для WebSocket сообщений. |
| `WS_RATE_LIMIT_MAX_MESSAGES` | `60` | Максимум WebSocket сообщений на окно rate limit. |
| `WS_ALLOWED_ORIGINS` | `https://conference.example.com` | Разрешенные browser origins для WebSocket. В production должен быть публичный frontend origin. |
| `MEDIASOUP_LISTEN_IP` | `0.0.0.0` | IP, на котором mediasoup слушает WebRTC transports внутри контейнера/сервера. |
| `MEDIASOUP_ANNOUNCED_IP` | `203.0.113.12` | Публичный IP media server, который будет объявлен браузерам в ICE candidates. |
| `MEDIASOUP_RTC_MIN_PORT` | `40000` | Нижняя граница UDP/TCP портов mediasoup. |
| `MEDIASOUP_RTC_MAX_PORT` | `40100` | Верхняя граница UDP/TCP портов mediasoup. |
| `MEDIASOUP_NUM_WORKERS` | `2` | Количество mediasoup worker process. |
| `MEDIASOUP_MAX_TRANSPORTS_PER_PEER` | `4` | Лимит transports на одного участника. |
| `MEDIASOUP_MAX_PRODUCERS_PER_PEER` | `3` | Лимит producers на одного участника. |
| `MEDIASOUP_MAX_CONSUMERS_PER_PEER` | `32` | Лимит consumers на одного участника. |
| `TURN_URLS` | `turn:media.example.com:3478?transport=udp,turn:media.example.com:3478?transport=tcp` | TURN URLs, которые signaling отдает браузерам как ICE servers. |
| `TURN_USERNAME` | `conference` | TURN username. Должен совпадать с coturn config/compose. |
| `TURN_PASSWORD` | `replace-with-turn-password` | TURN password. Должен совпадать с coturn config/compose. |
| `TURN_REALM` | `media.example.com` | TURN realm для coturn. |

| `TURN_MIN_PORT` | `50000` | Lower UDP relay port for coturn. |
| `TURN_MAX_PORT` | `50100` | Upper UDP relay port for coturn. |

## Production ports

На media server должны быть открыты:

```text
4000/tcp                 signaling HTTP/WebSocket
40000-40100/udp,tcp      mediasoup WebRTC
3478/tcp,udp             TURN
50000-50100/udp          TURN relay range, or TURN_MIN_PORT-TURN_MAX_PORT
```

Внешний edge на app server должен проксировать:

```text
wss://domain/ws -> media:4000/ws
```
