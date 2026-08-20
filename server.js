const express = require("express");
const http = require("http");
const cors = require("cors");
const { Server } = require("socket.io");

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: "*", methods: ["GET","POST"] }
});

const ADMIN_TOKEN = process.env.ADMIN_TOKEN || "FasilAdmin-2026-ChangeThis-4nR8vT6z";
const devices = new Map();
const events = [];
const locations = new Map();
const MAX_EVENTS = 1000;

app.use(cors());
app.use(express.json({limit:"256kb"}));

function adminOnly(req,res,next) {
  const auth = req.get("authorization") || "";
  const x = req.get("x-admin-token") || "";
  const token = auth.startsWith("Bearer ") ? auth.slice(7) : (x || auth);
  if (token !== ADMIN_TOKEN) return res.status(401).json({error:"unauthorized"});
  next();
}

function upsertDevice(body) {
  const deviceId = String(body.deviceId || "").trim();
  if (!deviceId) return null;
  const previous = devices.get(deviceId) || {};
  const item = {
    ...previous,
    deviceId,
    deviceInfo: body.deviceInfo && typeof body.deviceInfo === "object" ? body.deviceInfo : previous.deviceInfo || {},
    permissionState: body.permissionState && typeof body.permissionState === "object" ? body.permissionState : previous.permissionState || {},
    batteryLevel: Number.isFinite(Number(body.batteryLevel)) ? Number(body.batteryLevel) : previous.batteryLevel ?? null,
    lastSeen: body.timestamp || Date.now()
  };
  devices.set(deviceId,item);
  return item;
}

app.get("/", (_,res)=>res.json({service:"Telemetry Backend",status:"ok"}));
app.get("/health", (_,res)=>res.json({status:"ok",devices:devices.size,events:events.length}));

app.post("/api/telemetry/register",(req,res)=>{
  const device=upsertDevice(req.body);
  if(!device) return res.status(400).json({error:"missing deviceId"});
  io.emit("device_update",device);
  res.json({status:"ok",device});
});

app.post("/api/telemetry/location",(req,res)=>{
  const {deviceId,latitude,longitude,timestamp}=req.body;
  if(!deviceId || !Number.isFinite(Number(latitude)) || !Number.isFinite(Number(longitude)))
    return res.status(400).json({error:"deviceId, latitude and longitude are required"});
  const location={deviceId:String(deviceId),latitude:Number(latitude),longitude:Number(longitude),timestamp:timestamp||Date.now()};
  locations.set(location.deviceId,location);
  const existing=devices.get(location.deviceId);
  if(existing){ existing.lastSeen=location.timestamp; devices.set(location.deviceId,existing); io.emit("device_update",existing); }
  io.emit("live_location_update",location);
  res.json({status:"ok"});
});

app.post("/api/telemetry/event",(req,res)=>{
  const {deviceId,eventType,packageName,timestamp}=req.body;
  if(!deviceId) return res.status(400).json({error:"missing deviceId"});
  // Privacy-preserving metadata only: no title/text/message fields are accepted or stored.
  const event={
    id:`${Date.now()}-${Math.random().toString(36).slice(2,8)}`,
    deviceId:String(deviceId),
    eventType:String(eventType||"notification_metadata"),
    packageName:String(packageName||""),
    timestamp:timestamp||Date.now()
  };
  events.unshift(event);
  if(events.length>MAX_EVENTS) events.pop();
  const existing=devices.get(event.deviceId);
  if(existing){existing.lastSeen=event.timestamp; devices.set(event.deviceId,existing); io.emit("device_update",existing);}
  io.emit("live_notification_stream",event);
  res.json({status:"ok"});
});

app.get("/api/admin/devices",adminOnly,(req,res)=>{
  const deviceId=req.query.deviceId;
  const list=[...devices.values()].filter(d=>!deviceId||d.deviceId===deviceId);
  res.json({count:list.length,devices:list});
});
app.get("/api/admin/events",adminOnly,(req,res)=>{
  const deviceId=req.query.deviceId;
  const list=events.filter(e=>!deviceId||e.deviceId===deviceId);
  res.json({count:list.length,events:list});
});
app.get("/api/admin/locations",adminOnly,(req,res)=>{
  const deviceId=req.query.deviceId;
  const list=[...locations.values()].filter(l=>!deviceId||l.deviceId===deviceId);
  res.json({count:list.length,locations:list});
});

io.use((socket,next)=>{
  const token=socket.handshake.auth?.token || socket.handshake.headers?.authorization?.replace(/^Bearer\s+/i,"");
  if(token===ADMIN_TOKEN) return next();
  next(new Error("unauthorized"));
});
io.on("connection",(socket)=>{
  socket.emit("device_snapshot",[...devices.values()]);
  socket.emit("event_snapshot",events);
  socket.emit("location_snapshot",[...locations.values()]);
});

app.use((req,res)=>res.status(404).json({error:"not_found",path:req.path}));
app.use((err,req,res,next)=>{
  console.error(err);
  res.status(500).json({error:"internal_server_error"});
});

const PORT=process.env.PORT||3000;
server.listen(PORT,()=>console.log(`Telemetry backend listening on ${PORT}`));
