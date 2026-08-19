# Telemetry Backend - Fixed

## Install and run

```bash
npm install
cp .env.example .env
# Set real ADMIN_TOKEN and INGEST_TOKEN values in your environment
npm start
```

Render can use `npm install` as the build command and `npm start` as the start command.

## Required environment variables

- `ADMIN_TOKEN` - must match the Admin Android app token.
- `INGEST_TOKEN` - must be supplied by approved client telemetry requests.
- `PORT` - supplied by Render automatically.
- `HEARTBEAT_TIMEOUT_MS` - optional, defaults to 30000.
- `CORS_ORIGIN` - optional, defaults to `*`.

## Admin routes

All require `Authorization: Bearer <ADMIN_TOKEN>` or `X-Admin-Token: <ADMIN_TOKEN>`:

- `GET /api/healthz`
- `GET /api/admin/devices`
- `GET /api/admin/events`
- `GET /api/admin/commands`
- `POST /api/admin/command`

Command body:

```json
{"deviceId":"device-123","commandType":"REFRESH_STATE","params":{}}
```

## Client routes

All require `Authorization: Bearer <INGEST_TOKEN>` or `X-Ingest-Token: <INGEST_TOKEN>`:

- `POST /api/client/register`
- `POST /api/telemetry/register`
- `POST /api/client/event`
- `POST /api/telemetry/event`
- `POST /api/ingest`
- `GET /api/client/commands/:deviceId`
- `POST /api/client/command-result`

Commands are retained in memory and transition through `queued -> dispatched -> executed` or `failed` when the client posts a result. This avoids deleting commands during polling.

Notification events are intentionally metadata-only: package, event type, timestamp, notification id/category/channel, and flags. Titles, message text, sender names, and previews are not stored by this backend.

## WebSocket

Connect approved admin clients to `/ws?token=<ADMIN_TOKEN>`. The server sends a snapshot and live `devices`, `event`, and `command` updates.
