import http from 'node:http';
import crypto from 'node:crypto';
import express from 'express';
import { WebSocketServer } from 'ws';

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: '/ws' });

const devices = new Map();
const events = [];
const commands = new Map();
const HEARTBEAT_TIMEOUT_MS = Number(process.env.HEARTBEAT_TIMEOUT_MS || 30000);
const INGEST_TOKEN = String(process.env.INGEST_TOKEN || '');
const ADMIN_TOKEN = String(process.env.ADMIN_TOKEN || '');

app.use((req, res, next) => {
  res.setHeader('Access-Control-Allow-Origin', process.env.CORS_ORIGIN || '*');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization, X-Admin-Token, X-Ingest-Token');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  if (req.method === 'OPTIONS') return res.status(204).end();
  next();
});
app.use(express.json({ limit: '256kb' }));

const bearer = req => {
  const value = String(req.get('authorization') || '');
  return /^Bearer\s+/i.test(value) ? value.replace(/^Bearer\s+/i, '').trim() : '';
};
const supplied = (req, header) => bearer(req) || String(req.get(header) || '').trim();
const requireToken = (expected, header, label) => (req, res, next) => {
  if (!expected || supplied(req, header) !== expected) return res.status(401).json({ ok: false, error: `Unauthorized ${label} token` });
  next();
};
const requireAdmin = requireToken(ADMIN_TOKEN, 'x-admin-token', 'admin');
const requireIngest = requireToken(INGEST_TOKEN, 'x-ingest-token', 'ingest');

function metadataOnly(raw = {}) {
  const source = raw && typeof raw === 'object' && !Array.isArray(raw) ? raw : {};
  return {
    eventType: String(source.eventType || 'notification_event').slice(0, 80),
    packageName: String(source.packageName || '').slice(0, 255),
    timestamp: Number(source.timestamp || Date.now()),
    notificationId: Number(source.notificationId || 0),
    category: source.category == null ? null : String(source.category).slice(0, 100),
    channelId: source.channelId == null ? null : String(source.channelId).slice(0, 255),
    isOngoing: Boolean(source.isOngoing),
    isClearable: Boolean(source.isClearable),
    contentCaptured: false
  };
}
function sanitizeNotificationStream(value) {
  if (!Array.isArray(value)) return [];
  return value.slice(-100).map(metadataOnly).filter(e => e.packageName || e.eventType.startsWith('notification_'));
}
function normalizeDevice(body = {}) {
  const deviceId = String(body.deviceId || body.deviceUuid || '').trim();
  if (!deviceId) return null;
  const previous = devices.get(deviceId) || {};
  const info = body.deviceInfo && typeof body.deviceInfo === 'object' && !Array.isArray(body.deviceInfo) ? body.deviceInfo : {};
  const previousStream = Array.isArray(previous.notificationStream) ? previous.notificationStream : [];
  const incomingStream = sanitizeNotificationStream(body.notificationStream);
  return {
    ...previous,
    deviceId,
    uuid: deviceId,
    model: String(body.model || info.model || previous.model || 'Unknown').slice(0, 160),
    manufacturer: String(body.manufacturer || info.manufacturer || previous.manufacturer || 'Unknown').slice(0, 160),
    androidVersion: String(body.androidVersion || info.osVersion || previous.androidVersion || 'Unknown').slice(0, 64),
    sdkLevel: Number(body.sdkLevel ?? body.sdkInt ?? info.sdkLevel ?? previous.sdkLevel ?? 0),
    batteryLevel: Number(body.batteryLevel ?? info.batteryLevel ?? previous.batteryLevel ?? -1),
    appVersion: String(body.appVersion || previous.appVersion || 'Unknown').slice(0, 120),
    permissions: body.permissions && typeof body.permissions === 'object' && !Array.isArray(body.permissions) ? body.permissions : (previous.permissions || {}),
    specialAccess: body.specialAccess && typeof body.specialAccess === 'object' && !Array.isArray(body.specialAccess) ? body.specialAccess : (previous.specialAccess || {}),
    notificationStream: [...previousStream, ...incomingStream].slice(-100),
    deviceInfo: info,
    lastHeartbeat: Date.now(),
    lastTimestamp: Number(body.timestamp || body.sentAtEpochMs || Date.now())
  };
}
const publicDevice = device => ({ ...device, active: Date.now() - device.lastHeartbeat < HEARTBEAT_TIMEOUT_MS });
const devicesView = () => [...devices.values()].map(publicDevice);
function eventPayload(body) {
  const copy = body && typeof body === 'object' && !Array.isArray(body) ? { ...body } : {};
  delete copy.title; delete copy.text; delete copy.bigText; delete copy.sender; delete copy.messagePreview;
  if (copy.event && typeof copy.event === 'object' && String(copy.event.eventType || '').startsWith('notification_')) copy.event = metadataOnly(copy.event);
  if (copy.notificationStream) copy.notificationStream = sanitizeNotificationStream(copy.notificationStream);
  return copy;
}
function broadcast(message) {
  const encoded = JSON.stringify(message);
  for (const ws of wss.clients) if (ws.readyState === 1) ws.send(encoded);
}
function pushEvent(event) {
  events.unshift(event);
  if (events.length > 1000) events.length = 1000;
  broadcast({ type: 'event', event });
}
const broadcastDevices = () => broadcast({ type: 'devices', devices: devicesView() });
function makeEvent(body, deviceId, fallback) {
  return {
    id: crypto.randomUUID(), deviceId, deviceUuid: deviceId,
    eventType: String(body?.eventType || body?.event?.eventType || fallback),
    timestamp: Number(body?.timestamp || body?.sentAtEpochMs || Date.now()),
    receivedAtEpochMs: Date.now(), payload: eventPayload(body)
  };
}
function receive(kind) {
  return (req, res) => {
    const device = normalizeDevice(req.body || {});
    if (!device) return res.status(400).json({ ok: false, error: 'deviceId is required' });
    devices.set(device.deviceId, device);
    const event = makeEvent(req.body, device.deviceId, kind);
    pushEvent(event); broadcastDevices();
    res.status(200).json({ ok: true, deviceId: device.deviceId, eventId: event.id });
  };
}
function queueCommand(req, res) {
  const body = req.body && typeof req.body === 'object' ? req.body : {};
  const deviceId = String(body.deviceId || '').trim();
  const commandType = String(body.commandType || '').trim();
  if (!deviceId || !commandType) return res.status(400).json({ ok: false, error: 'deviceId and commandType are required' });
  const command = {
    id: crypto.randomUUID(), deviceId, commandType,
    parameters: body.parameters && typeof body.parameters === 'object' ? body.parameters : {},
    createdAt: Date.now(), requiresUserConfirmation: true
  };
  const queue = commands.get(deviceId) || [];
  queue.push(command); commands.set(deviceId, queue);
  pushEvent({ id: crypto.randomUUID(), deviceId, eventType: 'admin_command_queued', timestamp: Date.now(), receivedAtEpochMs: Date.now(), payload: { commandType, requiresUserConfirmation: true } });
  res.status(202).json({ ok: true, command });
}

app.get('/', (_req, res) => res.json({ ok: true, service: 'telemetry-backend', apiVersion: 4 }));
app.get('/healthz', (_req, res) => res.json({ ok: true, devices: devices.size, events: events.length, apiVersion: 4 }));
app.get('/api/healthz', (_req, res) => res.json({ ok: true, devices: devices.size, events: events.length, apiVersion: 4 }));
app.post('/api/client/register', requireIngest, receive('device_register'));
app.post('/api/client/event', requireIngest, receive('telemetry_event'));
app.post('/api/ingest', requireIngest, receive('telemetry_event'));
app.post('/api/telemetry/register', requireIngest, receive('device_register'));
app.post('/api/devices/heartbeat', requireIngest, receive('device_heartbeat'));
app.get('/api/admin/devices', requireAdmin, (_req, res) => res.json(devicesView()));
app.get('/api/admin/events', requireAdmin, (_req, res) => res.json(events));
app.post('/api/admin/command', requireAdmin, queueCommand);
app.post('/api/admin/commands', requireAdmin, queueCommand);
app.get('/api/client/commands/:deviceId', requireIngest, (req, res) => {
  const deviceId = String(req.params.deviceId || '').trim();
  const queue = commands.get(deviceId) || [];
  commands.set(deviceId, []);
  res.json({ ok: true, commands: queue });
});

app.use('/api', (req, res) => res.status(404).json({ ok: false, error: `API route not found: ${req.method} ${req.originalUrl}` }));
app.use((err, _req, res, _next) => {
  if (err?.type === 'entity.parse.failed') return res.status(400).json({ ok: false, error: 'Invalid JSON request body' });
  console.error(err); return res.status(500).json({ ok: false, error: 'Internal server error' });
});
app.use((req, res) => res.status(404).json({ ok: false, error: `Route not found: ${req.method} ${req.originalUrl}` }));

wss.on('connection', (ws, req) => {
  const url = new URL(req.url || '/ws', 'http://localhost');
  const token = url.searchParams.get('token') || bearer(req) || String(req.headers['x-admin-token'] || '');
  if (!ADMIN_TOKEN || token !== ADMIN_TOKEN) return ws.close(1008, 'Unauthorized');
  ws.send(JSON.stringify({ type: 'snapshot', devices: devicesView(), events }));
});
setInterval(broadcastDevices, 5000).unref();
const port = Number(process.env.PORT || 8080);
server.listen(port, '0.0.0.0', () => console.log(`Telemetry backend listening on :${port}`));
