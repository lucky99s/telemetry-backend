# Render deployment

Deploy this `backend` directory as the Render service root.

Build command:
`npm install`

Start command:
`npm start`

Set these Render environment variables:
- `INGEST_TOKEN` = the ingest token configured in the main Android app
- `ADMIN_TOKEN` = the admin token configured in the admin Android app
- `HEARTBEAT_TIMEOUT_MS` = `30000`

After deployment, verify these return JSON:
- `/api/healthz`
- `/healthz`

Production routes:
- `POST /api/client/register` (ingest token)
- `POST /api/client/event` (ingest token)
- `GET /api/admin/devices` (admin token)
- `GET /api/admin/events` (admin token)

The backend intentionally returns JSON for every `/api/*` 404 instead of an HTML error page.
