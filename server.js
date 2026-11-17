import express from 'express';
import { createServer } from 'http';
import { Server } from 'socket.io';
import { TikTokLiveConnection, WebcastEvent } from 'tiktok-live-connector';

const USERNAME = process.env.TIKTOK_USERNAME || 'winniesforlife'; // no @, must be live
const AMEN_GOAL = Number(process.env.AMEN_GOAL) || 5;             // 5 for testing
const PORT = process.env.PORT || 3000;

const app = express();
const httpServer = createServer(app);
const io = new Server(httpServer);

let amen = 0;
let tiktokConnected = false;

// Health check for cron pings and Render
app.get('/health', (req, res) => {
  res.status(200).json({
    status: 'ok',
    tiktokConnected,
    amen,
    goal: AMEN_GOAL,
    uptimeSeconds: Math.round(process.uptime())
  });
});

app.use(express.static('public'));

function startTikTok() {
  const tiktok = new TikTokLiveConnection(USERNAME, {});

  tiktok.on(WebcastEvent.CHAT, data => {
    const comment =
      data.comment ??
      data.content ??
      data.text ??
      data.data?.comment ??
      data.data?.content ??
      '';

    const lower = String(comment).toLowerCase();

    if (lower.includes('amen') || lower.includes('hallelujah')) {
      amen++;
      console.log(`Amen counted: ${amen} / ${AMEN_GOAL}`);

      if (amen >= AMEN_GOAL) {
        io.emit('celebrate');
        amen = 0;
      }
      io.emit('amen', { amen, goal: AMEN_GOAL });
    }
  });

  tiktok.on('disconnected', () => {
    tiktokConnected = false;
    console.log('Disconnected, retrying in 15s...');
    setTimeout(startTikTok, 15000);
  });

  tiktok.on('error', e => console.error('Connection error:', e));

  tiktok.connect()
    .then(s => {
      tiktokConnected = true;
      console.log(`Connected to room ${s.roomId}`);
    })
    .catch(e => {
      tiktokConnected = false;
      console.error('Failed to connect, retrying in 15s:', e?.message || e);
      setTimeout(startTikTok, 15000);
    });
}

startTikTok();

io.on('connection', socket => {
  socket.emit('amen', { amen, goal: AMEN_GOAL });
});

httpServer.listen(PORT, () => console.log(`Running on port ${PORT}`));