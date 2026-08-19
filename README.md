# Telemetry Backend API v3

Deploy this folder as the Render service. Set environment variables:

- `INGEST_TOKEN` — must match the Android main app ingest token.
- `ADMIN_TOKEN` — must match the admin panel token.
- `HEARTBEAT_TIMEOUT_MS=30000`

Routes:

- `GET /api/healthz`
- `POST /api/client/register`
- `POST /api/client/event`
- `POST /api/ingest` (compatibility alias)
- `GET /api/admin/devices`
- `GET /api/admin/events`

All API errors return JSON. Notification events are normalized to metadata only. The backend does not intentionally persist notification title, text, sender, or message-preview fields.
