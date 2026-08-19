import http from 'node:http';
import crypto from 'node:crypto';
import express from 'express';
import { WebSocketServer } from 'ws';

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: '/ws' });

const devices = new Map();
const events = [];
const HEARTBEAT_TIMEOUT_MS = Number(process.env.HEARTBEAT_TIMEOUT_MS || 30000);
const INGEST_TOKEN = process.env.INGEST_TOKEN;
const ADMIN_TOKEN = process.env.ADMIN_TOKEN;

if (!INGEST_TOKEN || !ADMIN_TOKEN) {
  console.error('Missing required Render environment variables: INGEST_TOKEN and/or ADMIN_TOKEN');
}

// Middleware must be registered before every route.
app.use((req, res, next) => {
  res.setHeader('Access-Control-Allow-Origin', process.env.CORS_ORIGIN || '*');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization, X-Admin-Token, X-Ingest-Token');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  if (req.method === 'OPTIONS') return res.sendStatus(204);
  next();
});
app.use(express.json({ limit: '256kb' }));

function bearerToken(req) {
  const value = String(req.get('authorization') || '');
  return /^Bearer\s+/i.test(value) ? value.replace(/^Bearer\s+/i, '').trim() : '';
}
function tokenFrom(req, headerName) {
  return bearerToken(req) || String(req.get(headerName) || '').trim();
}
function requireAdmin(req, res, next) {
  if (!ADMIN_TOKEN || tokenFrom(req, 'x-admin-token') !== ADMIN_TOKEN) {
    return res.status(401).json({ ok: false, error: 'Unauthorized admin token' });
  }
  next();
}
function requireIngest(req, res, next) {
  if (!INGEST_TOKEN || tokenFrom(req, 'x-ingest-token') !== INGEST_TOKEN) {
    return res.status(401).json({ ok: false, error: 'Unauthorized ingest token' });
  }
  next();
}

function publicDevice(device) {
  return { ...device, active: Date.now() - device.lastHeartbeat < HEARTBEAT_TIMEOUT_MS };
}
function devicesView() {
  return [...devices.values()].map(publicDevice);
}
function pushEvent(event) {
  events.unshift(event);
  if (events.length > 1000) events.length = 1000;
  const message = JSON.stringify({ type: 'event', event });
  for (const ws of wss.clients) if (ws.readyState === 1) ws.send(message);
}
function broadcastDevices() {
  const message = JSON.stringify({ type: 'devices', devices: devicesView() });
  for (const ws of wss.clients) if (ws.readyState === 1) ws.send(message);
}

function normalizeDevice(body) {
  const deviceId = String(body?.deviceId || body?.deviceUuid || '').trim();
  if (!deviceId) return null;
  const info = body?.deviceInfo && typeof body.deviceInfo === 'object' && !Array.isArray(body.deviceInfo)
    ? body.deviceInfo : {};
  const previous = devices.get(deviceId) || {};
  return {
    ...previous,
    deviceId,
    uuid: deviceId,
    model: String(body?.model || info.model || previous.model || 'Unknown').slice(0, 160),
    manufacturer: String(body?.manufacturer || info.manufacturer || previous.manufacturer || 'Unknown').slice(0, 160),
    androidVersion: String(body?.androidVersion || info.osVersion || previous.androidVersion || 'Unknown').slice(0, 64),
    sdkLevel: Number(body?.sdkLevel ?? body?.sdkInt ?? info.sdkLevel ?? previous.sdkLevel ?? 0),
    batteryLevel: Number(body?.batteryLevel ?? info.batteryLevel ?? previous.batteryLevel ?? -1),
    appVersion: String(body?.appVersion || previous.appVersion || 'Unknown').slice(0, 120),
    permissions: body?.permissions && typeof body.permissions === 'object' && !Array.isArray(body.permissions)
      ? body.permissions : (previous.permissions || {}),
    deviceInfo: info,
    lastHeartbeat: Date.now(),
    lastTimestamp: Number(body?.timestamp || body?.sentAtEpochMs || Date.now())
  };
}
function makeEvent(body, deviceId, fallbackType) {
  return {
    id: crypto.randomUUID(),
    deviceId,
    deviceUuid: deviceId,
    eventType: String(body?.eventType || body?.event?.eventType || fallbackType),
    timestamp: Number(body?.timestamp || body?.sentAtEpochMs || Date.now()),
    receivedAtEpochMs: Date.now(),
    payload: body || {}
  };
}

function registerDevice(req, res) {
  const device = normalizeDevice(req.body || {});
  if (!device) return res.status(400).json({ ok: false, error: 'deviceId is required' });
  devices.set(device.deviceId, device);
  const event = makeEvent(req.body, device.deviceId, 'device_register');
  pushEvent(event);
  broadcastDevices();
  return res.status(200).json({ ok: true, deviceId: device.deviceId, eventId: event.id });
}
function receiveEvent(req, res) {
  const device = normalizeDevice(req.body || {});
  if (!device) return res.status(400).json({ ok: false, error: 'deviceId is required' });
  devices.set(device.deviceId, device);
  const event = makeEvent(req.body, device.deviceId, 'telemetry_event');
  pushEvent(event);
  broadcastDevices();
  return res.status(200).json({ ok: true, deviceId: device.deviceId, eventId: event.id });
}

// Health endpoints.
app.get('/', (_req, res) => res.json({ ok: true, service: 'telemetry-backend', apiVersion: 2 }));
app.get('/healthz', (_req, res) => res.json({ ok: true, devices: devices.size, events: events.length, apiVersion: 2 }));
app.get('/api/healthz', (_req, res) => res.json({ ok: true, devices: devices.size, events: events.length, apiVersion: 2 }));

// Exact production API routes requested by both Android applications.
app.post('/api/client/register', requireIngest, registerDevice);
app.post('/api/client/event', requireIngest, receiveEvent);
app.get('/api/admin/devices', requireAdmin, (_req, res) => res.status(200).json(devicesView()));
app.get('/api/admin/events', requireAdmin, (_req, res) => res.status(200).json(events));

// Backward-compatible aliases for earlier APKs.
app.post('/api/ingest', requireIngest, receiveEvent);
app.post('/api/telemetry/register', requireIngest, registerDevice);
app.post('/api/devices/heartbeat', requireIngest, registerDevice);
app.get('/api/devices', requireAdmin, (_req, res) => res.status(200).json(devicesView()));
app.get('/api/events', requireAdmin, (_req, res) => res.status(200).json(events));

// API 404s are always JSON, never HTML.
app.use('/api', (req, res) => res.status(404).json({ ok: false, error: `API route not found: ${req.method} ${req.originalUrl}` }));

app.use((err, _req, res, _next) => {
  if (err?.type === 'entity.parse.failed') return res.status(400).json({ ok: false, error: 'Invalid JSON request body' });
  console.error(err);
  return res.status(500).json({ ok: false, error: 'Internal server error' });
});
app.use((req, res) => res.status(404).json({ ok: false, error: `Route not found: ${req.method} ${req.originalUrl}` }));

wss.on('connection', (ws, req) => {
  const url = new URL(req.url || '/ws', 'http://localhost');
  const queryToken = url.searchParams.get('token') || '';
  const headerToken = bearerToken(req) || String(req.headers['x-admin-token'] || '');
  if (!ADMIN_TOKEN || (queryToken !== ADMIN_TOKEN && headerToken !== ADMIN_TOKEN)) {
    ws.close(1008, 'Unauthorized');
    return;
  }
  ws.send(JSON.stringify({ type: 'snapshot', devices: devicesView(), events }));
});

setInterval(broadcastDevices, 5000).unref();
const port = Number(process.env.PORT || 8080);
server.listen(port, '0.0.0.0', () => console.log(`Telemetry backend listening on :${port}`));
