# Telemetry Ingestion + Operator Panel

## Exact audited mobile JSON schema

The scanned Android service emits **no device UUID, model, authentication key, heartbeat, notification text, title, or message content in the JSON body**.

### notification_posted
```json
{"schemaVersion":1,"eventType":"notification_posted","packageName":"com.google.android.gm","notificationId":123,"postedAtEpochMs":1730000000000,"isOngoing":false,"isClearable":true,"category":null,"channelId":null}
```

### notification_removed
```json
{"schemaVersion":1,"eventType":"notification_removed","packageName":"com.google.android.gm","notificationId":123,"removedAtEpochMs":1730000000000}
```

The server accepts those bodies without adding or requiring body keys. Device identity/metadata are supplied in HTTP headers by the optional bridge patch below, preserving the body schema exactly.

## Run
```bash
cp .env.example .env
npm install
set -a; . ./.env; set +a
npm start
```

Use a reverse proxy/TLS in production and set both tokens.

Dashboard: `/`
Ingest: `POST /v1/device-events`
Health: `GET /healthz`
WebSocket: `/ws?token=ADMIN_TOKEN`
