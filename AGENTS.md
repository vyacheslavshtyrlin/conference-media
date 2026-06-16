# Signaling/Media Agent Guide

Owner: Signaling/media implementation agent for `media/**`.
Reviewer: Reviewer agent (`../.agents/reviewer-agent.md`).

Use this file for changes under `media/**`.

Required reading:

- `../AGENTS.md`
- `../docs/agents.md`
- `../docs/architecture.md`
- `../docs/contracts.md`

Required skills:

- `docker-expert` when signaling changes touch ports, TURN, ICE, Compose or deployment.

Ownership:

- Owns WebSocket protocol boundary, live participants, mediasoup workers/routers/transports/producers/consumers, room cleanup and media runtime limits.
- Owns media-server deploy config under `media/deploy/**`, including coturn/TURN configuration.
- Does not own public room creation, product backend features, durable persistence, frontend UI or S3 upload.

Rules:

- Keep WebSocket payloads schema-aligned with `media/src/contracts` and `../docs/contracts.md`.
- `RoomManager` owns room lifecycle, `PeerManager` owns participant state, `MediasoupService` owns mediasoup runtime objects, and `SocketGateway` owns the protocol boundary.
- Never store mediasoup runtime objects in Redis.
- Validate join tokens and make the server authoritative for participant and producer state.
- Preserve room TTL and empty-room cleanup behavior.
- Keep media-node routing room-sticky when multiple signaling/media nodes are introduced.
- Do not rely on random WebSocket load balancing for participants of the same room.
- Keep TURN/ICE behavior compatible with `media/deploy/**`, `../.env.example` and browser-visible contracts.
- Enforce per-peer transport, producer and consumer limits.
- Enforce only one active screen-share producer per room.
- If signaling work needs new WebSocket, token, Redis or ICE contract fields, stop and route the contract change through the Architect first.

Verification before handoff:

- `npm run typecheck --workspace @conference/media`
- `npm run test --workspace @conference/media`
- Compose config validation when env, ports, TURN or ICE behavior changes.
