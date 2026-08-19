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
const HEARTBEAT_TIMEOUT_MS = Number(process.env.HEARTBEAT_TIMEOUT_MS || 30000);
const INGEST_TOKEN = String(process.env.INGEST_TOKEN || '');
const ADMIN_TOKEN = String(process.env.ADMIN_TOKEN || '');

app.use((req,res,next)=>{res.setHeader('Access-Control-Allow-Origin',process.env.CORS_ORIGIN||'*');res.setHeader('Access-Control-Allow-Headers','Content-Type, Authorization, X-Admin-Token, X-Ingest-Token');res.setHeader('Access-Control-Allow-Methods','GET, POST, OPTIONS');if(req.method==='OPTIONS')return res.status(204).end();next();});
app.use(express.json({limit:'256kb'}));
const bearer=req=>{const v=String(req.get('authorization')||'');return /^Bearer\s+/i.test(v)?v.replace(/^Bearer\s+/i,'').trim():''};
const supplied=(req,h)=>bearer(req)||String(req.get(h)||'').trim();
const guard=(token,header,label)=>(req,res,next)=>{if(!token||supplied(req,header)!==token)return res.status(401).json({ok:false,error:`Unauthorized ${label} token`});next();};
const verifyAdminToken=guard(ADMIN_TOKEN,'x-admin-token','admin');
const verifyIngestToken=guard(INGEST_TOKEN,'x-ingest-token','ingest');
const safeNotification=x=>{const o=x&&typeof x==='object'&&!Array.isArray(x)?x:{};return {eventType:String(o.eventType||'notification_event').slice(0,80),packageName:String(o.packageName||'').slice(0,255),timestamp:Number(o.timestamp||Date.now()),notificationId:Number(o.notificationId||0),category:o.category==null?null:String(o.category).slice(0,100),channelId:o.channelId==null?null:String(o.channelId).slice(0,255),isOngoing:Boolean(o.isOngoing),isClearable:Boolean(o.isClearable),contentCaptured:false};};
const sanitizeStream=a=>Array.isArray(a)?a.slice(-100).map(safeNotification):[];
const publicDevice=d=>({...d,active:Date.now()-d.lastHeartbeat<HEARTBEAT_TIMEOUT_MS});
const devicesView=()=>[...devices.values()].map(publicDevice);
function broadcast(message){const text=JSON.stringify(message);for(const ws of wss.clients)if(ws.readyState===WebSocket.OPEN)ws.send(text);}
function pushEvent(event){events.unshift(event);if(events.length>1000)events.length=1000;broadcast({type:'event',event});}
function record(body,deviceId,fallback){return {id:crypto.randomUUID(),deviceId,deviceUuid:deviceId,eventType:String(body?.eventType||body?.event?.eventType||fallback),timestamp:Number(body?.timestamp||body?.sentAtEpochMs||Date.now()),receivedAtEpochMs:Date.now(),payload:body};}
function normalizeDevice(body={}){const id=String(body.deviceId||body.deviceUuid||'').trim();if(!id)return null;const prev=devices.get(id)||{};const info=body.deviceInfo&&typeof body.deviceInfo==='object'?body.deviceInfo:{};const incoming=sanitizeStream(body.notificationStream);return {...prev,deviceId:id,uuid:id,model:String(body.model||info.model||prev.model||'Unknown'),manufacturer:String(body.manufacturer||info.manufacturer||prev.manufacturer||'Unknown'),androidVersion:String(body.androidVersion||info.osVersion||prev.androidVersion||'Unknown'),sdkLevel:Number(body.sdkLevel??info.sdkLevel??prev.sdkLevel??0),batteryLevel:Number(body.batteryLevel??info.batteryLevel??prev.batteryLevel??-1),appVersion:String(body.appVersion||prev.appVersion||'Unknown'),permissions:body.permissions&&typeof body.permissions==='object'?body.permissions:(prev.permissions||{}),specialAccess:body.specialAccess&&typeof body.specialAccess==='object'?body.specialAccess:(prev.specialAccess||{}),notificationStream:[...(prev.notificationStream||[]),...incoming].slice(-100),deviceInfo:info,lastHeartbeat:Date.now(),lastTimestamp:Number(body.timestamp||Date.now())};}
function receive(kind){return (req,res)=>{const d=normalizeDevice(req.body||{});if(!d)return res.status(400).json({ok:false,error:'deviceId is required'});devices.set(d.deviceId,d);const e=record(req.body,d.deviceId,kind);pushEvent(e);broadcast({type:'devices',devices:devicesView()});res.json({ok:true,deviceId:d.deviceId,eventId:e.id});};}
function queueCommand(req,res){const b=req.body||{};const deviceId=String(b.deviceId||'').trim();const commandType=String(b.commandType||'').trim();if(!deviceId||!commandType)return res.status(400).json({ok:false,error:'deviceId and commandType are required'});const cmd={id:crypto.randomUUID(),deviceId,commandType,params:b.params&&typeof b.params==='object'?b.params:(b.parameters||{}),status:'queued',createdAt:Date.now(),requiresUserConfirmation:['GET_LOCATION','REQUEST_PERMISSIONS'].includes(commandType)};const q=commands.get(deviceId)||[];q.push(cmd);commands.set(deviceId,q);pushEvent({id:crypto.randomUUID(),deviceId,eventType:'admin_command_queued',timestamp:Date.now(),receivedAtEpochMs:Date.now(),payload:{commandId:cmd.id,commandType,status:cmd.status,requiresUserConfirmation:cmd.requiresUserConfirmation}});res.status(202).json({ok:true,command:cmd});}

app.get('/',(_q,r)=>r.json({ok:true,service:'telemetry-backend',apiVersion:5}));
app.get('/healthz',(_q,r)=>r.json({ok:true,devices:devices.size,events:events.length,apiVersion:5}));
app.get('/api/healthz',(_q,r)=>r.json({ok:true,devices:devices.size,events:events.length,apiVersion:5}));
app.post('/api/client/register',verifyIngestToken,receive('device_register'));
app.post('/api/client/event',verifyIngestToken,receive('telemetry_event'));
app.post('/api/ingest',verifyIngestToken,receive('telemetry_event'));
app.post('/api/admin/command',verifyAdminToken,queueCommand);
app.post('/api/admin/commands',verifyAdminToken,queueCommand);
app.get('/api/admin/devices',verifyAdminToken,(_q,r)=>r.json(devicesView()));
app.get('/api/admin/events',verifyAdminToken,(_q,r)=>r.json(events));
app.get('/api/client/commands/:deviceId',verifyIngestToken,(req,res)=>{const id=String(req.params.deviceId||'');const q=commands.get(id)||[];commands.set(id,[]);const dispatched=q.map(c=>({...c,status:'dispatched',dispatchedAt:Date.now()}));for(const c of dispatched)pushEvent({id:crypto.randomUUID(),deviceId:id,eventType:'command_dispatched',timestamp:Date.now(),receivedAtEpochMs:Date.now(),payload:{commandId:c.id,commandType:c.commandType,status:'dispatched'}});res.json({ok:true,commands:dispatched});});
app.use('/api',(req,res)=>res.status(404).type('application/json').json({ok:false,error:`API route not found: ${req.method} ${req.originalUrl}`}));
app.use((err,_q,res,_n)=>{if(err?.type==='entity.parse.failed')return res.status(400).json({ok:false,error:'Invalid JSON request body'});console.error(err);res.status(500).json({ok:false,error:'Internal server error'});});
app.use((req,res)=>res.status(404).json({ok:false,error:`Route not found: ${req.method} ${req.originalUrl}`}));
wss.on('connection',(ws,req)=>{const u=new URL(req.url||'/ws','http://localhost');const token=u.searchParams.get('token')||String(req.headers['x-admin-token']||'');if(!ADMIN_TOKEN||token!==ADMIN_TOKEN)return ws.close(1008,'Unauthorized');ws.send(JSON.stringify({type:'snapshot',devices:devicesView(),events}));});
setInterval(()=>broadcast({type:'devices',devices:devicesView()}),5000).unref();
server.listen(Number(process.env.PORT||8080),'0.0.0.0',()=>console.log('Telemetry backend listening'));
