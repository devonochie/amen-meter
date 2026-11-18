
import express from 'express';
import { createServer } from 'http';
import { Server } from 'socket.io';
import { TikTokLiveConnection, WebcastEvent } from 'tiktok-live-connector';

const USERNAME = process.env.TIKTOK_USERNAME || 'winniesforlife';
const AMEN_GOAL = Number(process.env.AMEN_GOAL) || 5;
const PORT = process.env.PORT || 3000;

const app = express();
const httpServer = createServer(app);
const io = new Server(httpServer);

let amen = 0;
let tiktokConnected = false;

let tiktok = null;
let connecting = false;
let retryTimer = null;
let failureCount = 0;
let rateLimitUntil = 0;

const BASE_RETRY_MS = 15000;
const MAX_RETRY_MS = 5 * 60 * 1000;

// Health check
app.get('/health', (req, res) => {
    res.status(200).json({
        status: 'ok',
        tiktokConnected,
        amen,
        goal: AMEN_GOAL,
        uptimeSeconds: Math.round(process.uptime()),
        rateLimited: Date.now() < rateLimitUntil,
        reconnectPending: retryTimer !== null,
        retryAt: rateLimitUntil
            ? new Date(rateLimitUntil).toISOString()
            : null
    });
});

app.use(express.static('public'));

// Error helpers
function getErrorText(error) {
    try {
        return [
            error?.message,
            error?.info,
            error?.reason,
            error?.code,
            error?.exception?.message,
            error?.exception?.reason,
            JSON.stringify(error)
        ].filter(Boolean).join(' ').toLowerCase();
    } catch {
        return String(error).toLowerCase();
    }
}

function isRateLimitError(error) {
    const message = getErrorText(error);

    return (
        message.includes('ratelimit') ||
        message.includes('rate limit') ||
        message.includes('rate_limited') ||
        message.includes('rate_limit_account_day')
    );
}

function findRetryAfter(error) {
    const visited = new Set();
    const queue = [error];

    while (queue.length) {
        const item = queue.shift();

        if (!item || typeof item !== 'object' || visited.has(item)) {
            continue;
        }

        visited.add(item);

        if (item.retryAfter !== undefined) {
            const value = Number(item.retryAfter);

            if (Number.isFinite(value) && value > 0) {
                return value;
            }
        }

        for (const value of Object.values(item)) {
            if (value && typeof value === 'object') {
                queue.push(value);
            }
        }
    }

    return null;
}

// Reconnection manager
function scheduleReconnect(reason, error = null) {
    if (retryTimer) {
        if (!error || !isRateLimitError(error)) {
            return;
        }

        clearTimeout(retryTimer);
        retryTimer = null;
    }

    let delay;

    if (error && isRateLimitError(error)) {
        const retryAfter = findRetryAfter(error);
        const message = getErrorText(error);

        const fallback = message.includes('rate_limit_account_day')
            ? 24 * 60 * 60 * 1000
            : 60 * 60 * 1000;

        delay = retryAfter || fallback;

        rateLimitUntil = Math.max(
            rateLimitUntil,
            Date.now() + delay
        );

        delay = Math.max(1000, rateLimitUntil - Date.now());

        console.error('Eulerstream rate limit reached.');
        console.error(
            `Waiting until ${new Date(rateLimitUntil).toISOString()}`
        );
    } else {
        delay = Math.min(
            BASE_RETRY_MS * Math.pow(2, failureCount),
            MAX_RETRY_MS
        );

        failureCount++;

        console.log(
            `${reason}. Retrying in ${Math.round(delay / 1000)} seconds.`
        );
    }

    retryTimer = setTimeout(() => {
        retryTimer = null;

        if (Date.now() >= rateLimitUntil) {
            rateLimitUntil = 0;
        }

        if (!connecting && !tiktokConnected) {
            startTikTok();
        }
    }, Math.min(delay, 2147483647));
}

// TikTok connection
async function startTikTok() {
    if (connecting || tiktokConnected) return;

    if (Date.now() < rateLimitUntil) {
        scheduleReconnect('Waiting for rate limit');
        return;
    }

    connecting = true;

    const connection = new TikTokLiveConnection(USERNAME, {});
    tiktok = connection;

    connection.on(WebcastEvent.CHAT, data => {
        const comment =
            data.comment ??
            data.content ??
            data.text ??
            data.data?.comment ??
            data.data?.content ??
            '';

        const lower = String(comment).toLowerCase();

        if (
            lower.includes('amen') ||
            lower.includes('hallelujah')
        ) {
            amen++;

            console.log(`Amen counted: ${amen} / ${AMEN_GOAL}`);

            if (amen >= AMEN_GOAL) {
                io.emit('celebrate');
                amen = 0;
            }

            io.emit('amen', {
                amen,
                goal: AMEN_GOAL
            });
        }
    });

    connection.on('disconnected', () => {
        if (tiktok !== connection) return;

        tiktokConnected = false;
        io.emit('status', { tiktokConnected });

        console.log('TikTok disconnected.');

        scheduleReconnect('TikTok disconnected');
    });

    connection.on('error', error => {
        console.error(
            'Connection error:',
            error?.info || error?.message || error
        );

        if (isRateLimitError(error)) {
            scheduleReconnect('Eulerstream rate limit', error);
        }
    });

    try {
        const session = await connection.connect();

        if (tiktok !== connection) return;

        connecting = false;
        tiktokConnected = true;
        failureCount = 0;

        io.emit('status', { tiktokConnected });

        console.log(`Connected to room ${session.roomId}`);
    } catch (error) {
        if (tiktok !== connection) return;

        connecting = false;
        tiktokConnected = false;

        io.emit('status', { tiktokConnected });

        console.error(
            'Failed to connect:',
            error?.message || error
        );

        scheduleReconnect('Connection failed', error);
    }
}

// Start TikTok
startTikTok();

// Socket.IO
io.on('connection', socket => {
    socket.emit('amen', {
        amen,
        goal: AMEN_GOAL
    });

    socket.emit('status', { tiktokConnected });
});

// Start server
httpServer.listen(PORT, () => {
    console.log(`Running on port ${PORT}`);
});
