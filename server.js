import http from 'node:http';
import crypto from 'node:crypto';
import express from 'express';
import { WebSocketServer, WebSocket } from 'ws';

const app = express();
app.use(express.json({limit:'64kb', type:['application/json','application/*+json']}));
app.use(express.static(new URL('./public', import.meta.url).pathname));
const server=http.createServer(app);
const wss=new WebSocketServer({server,path:'/ws'});
const devices=new Map(); const events=[];
const HEARTBEAT_TIMEOUT_MS=Number(process.env.HEARTBEAT_TIMEOUT_MS||30000);
const INGEST_TOKEN=process.env.INGEST_TOKEN||'';
const ADMIN_TOKEN=process.env.ADMIN_TOKEN||'';
function active(d){return Date.now()-d.lastHeartbeat<HEARTBEAT_TIMEOUT_MS}
function view(d){const {ws,...x}=d; return {...x,active:active(d)}}
function allDevices(){return [...devices.values()].map(view).sort((a,b)=>b.lastHeartbeat-a.lastHeartbeat)}
function send(ws,x){if(ws.readyState===WebSocket.OPEN)ws.send(JSON.stringify(x))}
function broadcast(x){for(const ws of wss.clients)send(ws,x)}
function broadcastDevices(){broadcast({type:'devices',devices:allDevices()})}
function requireIngest(req,res,next){if(!INGEST_TOKEN||req.get('authorization')===`Bearer ${INGEST_TOKEN}`)return next();res.status(401).json({error:'Unauthorized'});}
function requireAdmin(req,res,next){if(!ADMIN_TOKEN||req.get('authorization')===`Bearer ${ADMIN_TOKEN}`)return next();res.status(401).json({error:'Unauthorized'});}
function str(v,max){return typeof v==='string'?v.slice(0,max):''}
function num(v){return typeof v==='number'&&Number.isFinite(v)?v:null}
function upsertDevice(req){
 const uuid=str(req.get('x-device-uuid'),128)||crypto.createHash('sha256').update(req.ip).digest('hex').slice(0,32);
 const prior=devices.get(uuid)||{};
 const d={uuid,model:str(req.get('x-device-model'),160)||prior.model||'Unknown',androidVersion:str(req.get('x-android-version'),80)||prior.androidVersion||'Unknown',appVersion:str(req.get('x-app-version'),80)||prior.appVersion||'Unknown',lastHeartbeat:Date.now(),lastSeenIp:req.ip,ws:prior.ws||null};
 devices.set(uuid,d); return d;
}
function validatePayload(p){
 if(!p||typeof p!=='object'||Array.isArray(p))return 'JSON object required';
 if(p.schemaVersion!==1)return 'schemaVersion must equal 1';
 if(!['notification_posted','notification_removed'].includes(p.eventType))return 'unsupported eventType';
 if(!str(p.packageName,201))return 'packageName required';
 if(!Number.isInteger(p.notificationId))return 'notificationId integer required';
 if(p.eventType==='notification_posted'){
  if(num(p.postedAtEpochMs)===null)return 'postedAtEpochMs required';
  if(typeof p.isOngoing!=='boolean'||typeof p.isClearable!=='boolean')return 'isOngoing and isClearable booleans required';
  if(!Object.hasOwn(p,'category')||!Object.hasOwn(p,'channelId'))return 'category and channelId required';
 }
 if(p.eventType==='notification_removed'&&num(p.removedAtEpochMs)===null)return 'removedAtEpochMs required';
 return null;
}
app.get('/healthz',(_q,r)=>r.json({ok:true,devices:devices.size,events:events.length}));
app.post('/v1/device-events',requireIngest,(req,res)=>{
 const error=validatePayload(req.body); if(error)return res.status(400).json({error});
 const d=upsertDevice(req); const e={id:crypto.randomUUID(),receivedAtEpochMs:Date.now(),deviceUuid:d.uuid,payload:req.body};
 events.unshift(e); if(events.length>1000)events.pop();
 broadcast({type:'event',event:e}); broadcastDevices();
 res.status(202).json({accepted:true,eventId:e.id,deviceUuid:d.uuid});
});
app.get('/api/devices',requireAdmin,(_q,r)=>r.json(allDevices()));
app.get('/api/events',requireAdmin,(_q,r)=>r.json(events));
app.post('/api/admin/command',requireAdmin,(req,res)=>res.status(400).json({error:'Mobile HTTP schema has no command channel; no remote command is sent.'}));
wss.on('connection',(ws,req)=>{
 const url=new URL(req.url,'http://localhost'); if(ADMIN_TOKEN&&url.searchParams.get('token')!==ADMIN_TOKEN)return ws.close(1008,'Unauthorized');
 send(ws,{type:'snapshot',devices:allDevices(),events:events.slice(0,200)});
});
setInterval(broadcastDevices,5000).unref();
const port=Number(process.env.PORT||8080); server.listen(port,()=>console.log(`Telemetry admin listening on :${port}`));
