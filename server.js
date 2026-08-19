import http from 'node:http';
import crypto from 'node:crypto';
import express from 'express';
import { WebSocketServer, WebSocket } from 'ws';

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: '/ws' });

const devices = new Map();
const events = [];
const commands = new Map();

const PORT = Number(process.env.PORT || 8080);
const HEARTBEAT_TIMEOUT_MS = Number(process.env.HEARTBEAT_TIMEOUT_MS || 30_000);
const ADMIN_TOKEN = String(process.env.ADMIN_TOKEN || '');
const INGEST_TOKEN = String(process.env.INGEST_TOKEN || '');
const MAX_EVENTS = 1_000;
const MAX_NOTIFICATION_METADATA = 100;

app.use((req, res, next) => {
  res.setHeader('Access-Control-Allow-Origin', process.env.CORS_ORIGIN || '*');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization, X-Admin-Token, X-Ingest-Token');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  if (req.method === 'OPTIONS') return res.status(204).end();
  next();
});
app.use(express.json({ limit: '256kb' }));

function bearer(req) {
  const value = String(req.get('authorization') || '');
  return /^Bearer\s+/i.test(value) ? value.replace(/^Bearer\s+/i, '').trim() : '';
}
function supplied(req, headerName) {
  return bearer(req) || String(req.get(headerName) || '').trim();
}
function guard(expectedToken, headerName, label) {
  return (req, res, next) => {
    if (!expectedToken) {
      return res.status(503).json({ ok: false, error: `${label} token is not configured on the server` });
    }
    if (supplied(req, headerName) !== expectedToken) {
      return res.status(401).json({ ok: false, error: `Unauthorized ${label} token` });
    }
    next();
  };
}
const verifyAdminToken = guard(ADMIN_TOKEN, 'x-admin-token', 'admin');
const verifyIngestToken = guard(INGEST_TOKEN, 'x-ingest-token', 'ingest');

function sanitizeNotification(value) {
  const o = value && typeof value === 'object' && !Array.isArray(value) ? value : {};
  // Metadata only: do not persist notification titles, message text, sender names, or previews.
  return {
    eventType: String(o.eventType || 'notification_posted').slice(0, 80),
    packageName: String(o.packageName || '').slice(0, 255),
    timestamp: Number(o.timestamp || Date.now()),
    notificationId: Number(o.notificationId || 0),
    category: o.category == null ? null : String(o.category).slice(0, 100),
    channelId: o.channelId == null ? null : String(o.channelId).slice(0, 255),
    isOngoing: Boolean(o.isOngoing),
    isClearable: Boolean(o.isClearable),
    contentCaptured: false
  };
}
function sanitizeNotificationStream(value) {
  return Array.isArray(value) ? value.slice(-MAX_NOTIFICATION_METADATA).map(sanitizeNotification) : [];
}
function publicDevice(device) {
  return {
    ...device,
    active: Date.now() - device.lastHeartbeat < HEARTBEAT_TIMEOUT_MS
  };
}
function devicesView() {
  return [...devices.values()].map(publicDevice);
}
function send(ws, payload) {
  if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(payload));
}
function broadcast(payload) {
  for (const ws of wss.clients) send(ws, payload);
}
function pushEvent(event) {
  events.unshift(event);
  if (events.length > MAX_EVENTS) events.length = MAX_EVENTS;
  broadcast({ type: 'event', event });
}
function commandView(command) {
  return { ...command };
}
function eventRecord({ deviceId, eventType, timestamp = Date.now(), payload = {} }) {
  return {
    id: crypto.randomUUID(),
    deviceId,
    eventType: String(eventType || 'telemetry_event').slice(0, 100),
    timestamp: Number(timestamp || Date.now()),
    receivedAtEpochMs: Date.now(),
    payload
  };
}
function normalizeDevice(body = {}) {
  const deviceId = String(body.deviceId || body.deviceUuid || '').trim();
  if (!deviceId) return null;

  const previous = devices.get(deviceId) || {};
  const info = body.deviceInfo && typeof body.deviceInfo === 'object' ? body.deviceInfo : {};
  const incomingPermissions = body.permissions && typeof body.permissions === 'object' ? body.permissions : previous.permissions || {};
  const incomingSpecialAccess = body.specialAccess && typeof body.specialAccess === 'object' ? body.specialAccess : previous.specialAccess || {};
  const incomingStream = sanitizeNotificationStream(body.notificationStream);

  return {
    ...previous,
    deviceId,
    uuid: deviceId,
    model: String(body.model || info.model || previous.model || 'Unknown'),
    manufacturer: String(body.manufacturer || info.manufacturer || previous.manufacturer || 'Unknown'),
    androidVersion: String(body.androidVersion || info.osVersion || previous.androidVersion || 'Unknown'),
    sdkLevel: Number(body.sdkLevel ?? info.sdkLevel ?? previous.sdkLevel ?? 0),
    batteryLevel: Number(body.batteryLevel ?? info.batteryLevel ?? previous.batteryLevel ?? -1),
    appVersion: String(body.appVersion || info.appVersion || previous.appVersion || 'Unknown'),
    permissions: incomingPermissions,
    specialAccess: incomingSpecialAccess,
    notificationStream: [...(previous.notificationStream || []), ...incomingStream].slice(-MAX_NOTIFICATION_METADATA),
    deviceInfo: info,
    lastHeartbeat: Date.now(),
    lastTimestamp: Number(body.timestamp || Date.now())
  };
}
function broadcastDevices() {
  broadcast({ type: 'devices', devices: devicesView() });
}
function ingest(kind) {
  return (req, res) => {
    const device = normalizeDevice(req.body || {});
    if (!device) return res.status(400).json({ ok: false, error: 'deviceId is required' });

    devices.set(device.deviceId, device);

    const body = req.body || {};
    const notification = sanitizeNotification(body.notification || body.event || body);
    const isNotification = String(body.eventType || body.event?.eventType || '').startsWith('notification_');
    const event = eventRecord({
      deviceId: device.deviceId,
      eventType: body.eventType || body.event?.eventType || kind,
      timestamp: body.timestamp || body.sentAtEpochMs || Date.now(),
      payload: isNotification
        ? { event: notification }
        : { kind, fields: Object.keys(body).filter(k => !['title', 'text', 'content', 'message', 'notificationStream'].includes(k)).slice(0, 50) }
    });
    pushEvent(event);
    broadcastDevices();
    res.status(200).json({ ok: true, deviceId: device.deviceId, eventId: event.id });
  };
}

function queueCommand(req, res) {
  const body = req.body || {};
  const deviceId = String(body.deviceId || '').trim();
  const commandType = String(body.commandType || '').trim();
  if (!deviceId || !commandType) {
    return res.status(400).json({ ok: false, error: 'deviceId and commandType are required' });
  }

  const command = {
    id: crypto.randomUUID(),
    deviceId,
    commandType,
    params: body.params && typeof body.params === 'object' ? body.params : (body.parameters || {}),
    status: 'queued',
    createdAt: Date.now(),
    dispatchedAt: null,
    executedAt: null,
    completedAt: null,
    requiresUserConfirmation: ['GET_LOCATION', 'REQUEST_PERMISSIONS'].includes(commandType)
  };
  commands.set(command.id, command);
  pushEvent(eventRecord({
    deviceId,
    eventType: 'admin_command_queued',
    payload: { commandId: command.id, commandType, status: command.status, requiresUserConfirmation: command.requiresUserConfirmation }
  }));
  broadcast({ type: 'command', command: commandView(command) });
  res.status(202).json({ ok: true, command: commandView(command) });
}

function commandsForDevice(deviceId) {
  return [...commands.values()].filter(command => command.deviceId === deviceId);
}

app.get('/', (_req, res) => res.json({ ok: true, service: 'telemetry-backend', apiVersion: 6 }));
app.get('/healthz', (_req, res) => res.json({ ok: true, devices: devices.size, events: events.length, commands: commands.size, apiVersion: 6 }));
app.get('/api/healthz', (_req, res) => res.json({ ok: true, devices: devices.size, events: events.length, commands: commands.size, apiVersion: 6 }));

// Client ingestion aliases for compatibility with existing Android builds.
app.post('/api/client/register', verifyIngestToken, ingest('device_register'));
app.post('/api/telemetry/register', verifyIngestToken, ingest('device_register'));
app.post('/api/client/event', verifyIngestToken, ingest('telemetry_event'));
app.post('/api/telemetry/event', verifyIngestToken, ingest('telemetry_event'));
app.post('/api/ingest', verifyIngestToken, ingest('telemetry_event'));

// Admin API.
app.post('/api/admin/command', verifyAdminToken, queueCommand);
app.post('/api/admin/commands', verifyAdminToken, queueCommand);
app.get('/api/admin/devices', verifyAdminToken, (_req, res) => res.json(devicesView()));
app.get('/api/admin/events', verifyAdminToken, (_req, res) => res.json(events));
app.get('/api/admin/commands', verifyAdminToken, (req, res) => {
  const deviceId = String(req.query.deviceId || '').trim();
  const list = deviceId ? commandsForDevice(deviceId) : [...commands.values()];
  res.json(list.sort((a, b) => b.createdAt - a.createdAt).map(commandView));
});

// Client command synchronization. Polling does not delete commands; it changes queued -> dispatched.
app.get('/api/client/commands/:deviceId', verifyIngestToken, (req, res) => {
  const deviceId = String(req.params.deviceId || '').trim();
  const now = Date.now();
  const pending = commandsForDevice(deviceId)
    .filter(command => command.status === 'queued')
    .map(command => {
      command.status = 'dispatched';
      command.dispatchedAt = now;
      pushEvent(eventRecord({
        deviceId,
        eventType: 'command_dispatched',
        payload: { commandId: command.id, commandType: command.commandType, status: command.status }
      }));
      broadcast({ type: 'command', command: commandView(command) });
      return commandView(command);
    });
  res.json({ ok: true, commands: pending });
});

// Client acknowledges actual execution, resolving the old permanent "Queued" state.
app.post('/api/client/command-result', verifyIngestToken, (req, res) => {
  const body = req.body || {};
  const commandId = String(body.commandId || body.id || '').trim();
  const command = commands.get(commandId);
  if (!command) return res.status(404).json({ ok: false, error: 'Unknown commandId' });

  const success = body.success !== false && String(body.status || '').toLowerCase() !== 'failed';
  command.status = success ? 'executed' : 'failed';
  command.executedAt = Date.now();
  command.completedAt = command.executedAt;
  command.result = body.result && typeof body.result === 'object' ? body.result : {};
  command.error = success ? null : String(body.error || 'Command failed').slice(0, 500);

  pushEvent(eventRecord({
    deviceId: command.deviceId,
    eventType: success ? 'command_executed' : 'command_failed',
    payload: { commandId: command.id, commandType: command.commandType, status: command.status, error: command.error }
  }));
  broadcast({ type: 'command', command: commandView(command) });
  res.json({ ok: true, command: commandView(command) });
});

app.use('/api', (req, res) => res.status(404).type('application/json').json({ ok: false, error: `API route not found: ${req.method} ${req.originalUrl}` }));
app.use((err, _req, res, _next) => {
  if (err?.type === 'entity.parse.failed') return res.status(400).json({ ok: false, error: 'Invalid JSON request body' });
  console.error(err);
  res.status(500).json({ ok: false, error: 'Internal server error' });
});
app.use((req, res) => res.status(404).json({ ok: false, error: `Route not found: ${req.method} ${req.originalUrl}` }));

wss.on('connection', (ws, req) => {
  const url = new URL(req.url || '/ws', 'http://localhost');
  const token = url.searchParams.get('token') || String(req.headers['x-admin-token'] || '');
  if (!ADMIN_TOKEN || token !== ADMIN_TOKEN) return ws.close(1008, 'Unauthorized');
  send(ws, { type: 'snapshot', devices: devicesView(), events, commands: [...commands.values()].map(commandView) });
});

setInterval(() => broadcastDevices(), 5_000).unref();
server.listen(PORT, '0.0.0.0', () => console.log(`Telemetry backend listening on ${PORT}`));
