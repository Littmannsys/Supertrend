const express = require('express');
const WebSocket = require('ws');
const path = require('path');

const app = express();
const PORT = process.env.PORT || 8080;

// ─── Telegram Configuration ───────────────────────────────────────────────────
const TELEGRAM_BOT_TOKEN = '8626868477:AAHyD9ajC4M_SYX4XbYcbAiV9nmtelVl6KA';
const TELEGRAM_CHAT_ID   = '6456659526';

// ─── Deriv WebSocket ──────────────────────────────────────────────────────────
const API_URL = 'wss://ws.derivws.com/websockets/v3?app_id=1089';
let ws;
let isConnecting = false; // guard against multiple simultaneous connection attempts

// ─── In-memory state (replaces DOM) ──────────────────────────────────────────
const indicatorState = {
  R_10:   { ema20: null, ema50: null, price: null, lastUpdate: null },
  R_25:   { ema20: null, ema50: null, price: null, lastUpdate: null },
  stpRNG: { ema20: null, ema50: null, price: null, lastUpdate: null }
};

// ─── Historical Data ──────────────────────────────────────────────────────────
const historicalData = {
  R_10:   { '5min': [] },
  R_25:   { '5min': [] },
  stpRNG: { '5min': [] }
};

const currentCandles = {
  R_10:   { '5min': null },
  R_25:   { '5min': null },
  stpRNG: { '5min': null }
};

const candleCount = {
  R_10:   { '5min': 0 },
  R_25:   { '5min': 0 },
  stpRNG: { '5min': 0 }
};

const emaNotificationState = {
  R_10:   { 20: { lastNotifCandle: null }, 50: { lastNotifCandle: null } },
  R_25:   { 20: { lastNotifCandle: null }, 50: { lastNotifCandle: null } },
  stpRNG: { 20: { lastNotifCandle: null }, 50: { lastNotifCandle: null } }
};

const emaPriceSide = {
  R_10:   { 20: null, 50: null },
  R_25:   { 20: null, 50: null },
  stpRNG: { 20: null, 50: null }
};

const emaState = {
  R_10:   { 20: null, 50: null },
  R_25:   { 20: null, 50: null },
  stpRNG: { 20: null, 50: null }
};

// Tracks whether historical candles have been fully loaded for each symbol.
// Cross notifications are suppressed until this is true, preventing false
// crosses that fire on the first few live ticks before EMA state is seeded.
const symbolReady = {
  R_10:   false,
  R_25:   false,
  stpRNG: false
};

const ema50DistanceNotifState = {
  R_10:   { lastNotifCandle: null },
  R_25:   { lastNotifCandle: null },
  stpRNG: { lastNotifCandle: null }
};

const displayNames = {
  R_10:   'Volatility 10 Index',
  R_25:   'Volatility 25 Index',
  stpRNG: 'Step Index 100',
  '5min': '5 minutes'
};

const timeframeMap = { '5min': 300 };
const MAX_HISTORICAL_CANDLES = 5000;

// ─── Telegram ─────────────────────────────────────────────────────────────────
// Two-layer duplicate guard:
//   1. Short 5-second window keyed on exact message text — blocks identical
//      messages from two server instances running simultaneously (e.g. Railway
//      deploy overlap), which was causing every notification to appear twice.
//   2. The 5-candle cooldown inside checkEMATouches controls when the next
//      legitimate cross notification is allowed.
const recentMessages = {};
const DEDUP_WINDOW_MS = 30000; // 30 seconds

async function sendTelegramNotification(message) {
  if (!TELEGRAM_BOT_TOKEN || !TELEGRAM_CHAT_ID) return;

  const now = Date.now();

  // Block if this exact message was already sent within the dedup window.
  // Key is the full message text — identical content from any code path is blocked.
  if (recentMessages[message] && now - recentMessages[message] < DEDUP_WINDOW_MS) {
    console.log('Duplicate notification blocked:', message.substring(0, 60));
    return;
  }
  recentMessages[message] = now;

  // Clean up old entries so recentMessages does not grow forever
  Object.keys(recentMessages).forEach(key => {
    if (now - recentMessages[key] > DEDUP_WINDOW_MS) delete recentMessages[key];
  });

  try {
    const res = await fetch(`https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        chat_id: TELEGRAM_CHAT_ID,
        text: message,
        parse_mode: 'Markdown'
      })
    });
    const data = await res.json();
    if (!data.ok) console.error('Telegram error:', data);
  } catch (err) {
    console.error('Telegram fetch error:', err.message);
  }
}

// ─── EMA Calculations ─────────────────────────────────────────────────────────
// Seed EMA from historical closed candles (called once at startup).
// Uses SMA of the first `period` bars as the seed, then applies the recursive
// formula forward — identical to how TradingView boots its EMA on a fresh chart load.
function initEMA(closedCandles, period) {
  const data = closedCandles
    .filter(c => isFinite(c.close) && c.close > 0)
    .sort((a, b) => a.timestamp - b.timestamp);

  if (data.length < period) return null;

  // SMA seed
  let ema = 0;
  for (let i = 0; i < period; i++) ema += data[i].close;
  ema /= period;

  // Recursive EMA through remaining bars
  const k = 2 / (period + 1);
  for (let i = period; i < data.length; i++) {
    ema = data[i].close * k + ema * (1 - k);
  }
  return ema;
}

// Advance the stored EMA by one closed bar's close price.
// Called every time a candle closes — one multiplication, no loop.
function advanceEMA(symbol, period, closedClose) {
  if (emaState[symbol][period] === null) return;
  const k = 2 / (period + 1);
  emaState[symbol][period] = closedClose * k + emaState[symbol][period] * (1 - k);
}

// Live display EMA: stored (last-bar) EMA stepped forward by current price.
// This is exactly the value TradingView shows on the current forming bar.
function getLiveEMA(symbol, period, currentPrice) {
  const stored = emaState[symbol][period];
  if (stored === null) return null;
  const k = 2 / (period + 1);
  return currentPrice * k + stored * (1 - k);
}

// ─── Replaces DOM updateEMAValue — stores in memory instead ──────────────────
function updateEMAValue(symbol, period, value, currentPrice) {
  if (value !== null && currentPrice !== null) {
    if (period === 20) indicatorState[symbol].ema20 = value;
    if (period === 50) indicatorState[symbol].ema50 = value;
    indicatorState[symbol].price      = currentPrice;
    indicatorState[symbol].lastUpdate = new Date().toISOString();
  }
}

// ─── EMA Cross Detection ──────────────────────────────────────────────────────
// Fires on every tick. Collects ALL EMA crosses that happen on the same tick
// and sends exactly ONE combined Telegram message. After a notification, 5
// candles must close before the next one is allowed for that EMA period.
function checkEMATouches(symbol, timeframe, currentPrice, ema20, ema50) {
  // Still loading historical candles — track price side but never notify.
  // This prevents false crosses that fire before EMA state is fully seeded.
  if (!symbolReady[symbol]) {
    [20, 50].forEach(period => {
      const ema = period === 20 ? ema20 : ema50;
      if (ema === null) return;
      emaPriceSide[symbol][period] = currentPrice >= ema ? 'above' : 'below';
    });
    return;
  }

  const symbolName    = displayNames[symbol];
  const timeframeName = displayNames[timeframe];
  const currentCount  = candleCount[symbol][timeframe];

  // Collect all EMA crosses that qualify this tick
  const crossedEMAs = [];

  [20, 50].forEach(period => {
    const ema = period === 20 ? ema20 : ema50;
    if (ema === null) return;

    const currentSide  = currentPrice >= ema ? 'above' : 'below';
    const previousSide = emaPriceSide[symbol][period];

    // Always update the tracked side
    emaPriceSide[symbol][period] = currentSide;

    // First tick ever — just record side, no notification
    if (previousSide === null) return;

    // Only notify on an actual crossing (side changed)
    if (previousSide === currentSide) return;

    const state     = emaNotificationState[symbol][period];
    const lastNotif = state.lastNotifCandle;

    // Enforce 5-candle cooldown
    if (lastNotif !== null && currentCount - lastNotif < 5) return;

    // This EMA qualifies — add to list
    crossedEMAs.push({ period, ema, crossedUp: currentSide === 'above', state });
  });

  // Nothing crossed — do nothing
  if (crossedEMAs.length === 0) return;

  // Build ONE message covering all crossed EMAs this tick
  const lines = crossedEMAs.map(({ period, ema, crossedUp }) => {
    const emoji     = crossedUp ? '📈' : '📉';
    const direction = crossedUp ? 'crossed above' : 'crossed below';
    return (
      `${emoji} *${period} EMA*: price ${direction}\n` +
      `EMA: ${ema.toFixed(4)} | Price: ${currentPrice.toFixed(4)}`
    );
  });

  const message =
    `*EMA TOUCH* — *${symbolName}* on *${timeframeName}*\n\n` +
    lines.join('\n\n');

  // Send the single combined notification
  sendTelegramNotification(message);

  // Update cooldown state for every EMA that was crossed
  crossedEMAs.forEach(({ state }) => {
    state.lastNotifCandle = currentCount;
  });
}

// ─── Candle Management ────────────────────────────────────────────────────────
function getCandleTimeframe(timestamp, granularity) {
  return Math.floor(timestamp / granularity) * granularity;
}

function updateCurrentCandle(symbol, price, timestamp) {
  Object.keys(timeframeMap).forEach(timeframe => {
    const granularity = timeframeMap[timeframe];
    const candleTime  = getCandleTimeframe(timestamp, granularity);

    if (!currentCandles[symbol][timeframe] ||
        currentCandles[symbol][timeframe].timestamp !== candleTime) {

      if (currentCandles[symbol][timeframe]) {
        historicalData[symbol][timeframe].push(currentCandles[symbol][timeframe]);

        if (historicalData[symbol][timeframe].length > MAX_HISTORICAL_CANDLES) {
          historicalData[symbol][timeframe].shift();
        }

        // Candle just closed — advance EMA, increment counter
        const closedClose = currentCandles[symbol][timeframe].close;
        advanceEMA(symbol, 20, closedClose);
        advanceEMA(symbol, 50, closedClose);
        candleCount[symbol][timeframe]++;
      }

      currentCandles[symbol][timeframe] = {
        timestamp: candleTime,
        open: price, high: price, low: price, close: price
      };
    } else {
      const candle  = currentCandles[symbol][timeframe];
      candle.high   = Math.max(candle.high, price);
      candle.low    = Math.min(candle.low,  price);
      candle.close  = price;
    }
  });
}

// ─── Recalculate Indicators ───────────────────────────────────────────────────
function recalculateIndicators(symbol, livePrice) {
  const ema20 = getLiveEMA(symbol, 20, livePrice);
  const ema50 = getLiveEMA(symbol, 50, livePrice);

  updateEMAValue(symbol, 20, ema20, livePrice);
  updateEMAValue(symbol, 50, ema50, livePrice);

  Object.keys(timeframeMap).forEach(timeframe => {
    checkEMATouches(symbol, timeframe, livePrice, ema20, ema50);
  });
}

// ─── Process Historical Candles ───────────────────────────────────────────────
function processCandles(symbol, timeframe, candles) {
  const data = candles
    .map(c => ({
      open:      parseFloat(c.open),
      high:      parseFloat(c.high),
      low:       parseFloat(c.low),
      close:     parseFloat(c.close),
      timestamp: c.epoch
    }))
    .filter(c => isFinite(c.close) && c.close > 0)
    .sort((a, b) => a.timestamp - b.timestamp);

  if (data.length === 0) return;

  historicalData[symbol][timeframe] = data.slice(0, -1);

  const lastCandle = data[data.length - 1];
  currentCandles[symbol][timeframe] = { ...lastCandle };

  // emaPriceSide is initialised to null at declaration and set to 'above'/'below'
  // on the first live tick. We must NOT reset it here: if historical candles are
  // re-fetched after a reconnect the side is already tracking correctly, and
  // clearing it would cause a false cross notification on the very next tick.

  // Seed EMA state from all historical CLOSED candles (one-time at startup)
  emaState[symbol][20] = initEMA(historicalData[symbol][timeframe], 20);
  emaState[symbol][50] = initEMA(historicalData[symbol][timeframe], 50);

  // Display live EMA (stored last-bar EMA stepped by current price)
  const currentPrice = lastCandle.close;
  updateEMAValue(symbol, 20, getLiveEMA(symbol, 20, currentPrice), currentPrice);
  updateEMAValue(symbol, 50, getLiveEMA(symbol, 50, currentPrice), currentPrice);

  // Mark this symbol as ready — live ticks can now fire cross notifications
  symbolReady[symbol] = true;

  console.log(`[${symbol}] Candles loaded: ${data.length} | EMA20: ${emaState[symbol][20]?.toFixed(4)} | EMA50: ${emaState[symbol][50]?.toFixed(4)}`);
}

// ─── WebSocket Messaging ──────────────────────────────────────────────────────
function sendMessage(message) {
  if (ws && ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(message));
  }
}

function requestCandles(symbol, timeframe) {
  const granularity = timeframeMap[timeframe];
  const end         = Math.floor(Date.now() / 1000);
  const start       = end - (MAX_HISTORICAL_CANDLES * granularity);

  sendMessage({
    ticks_history:     symbol,
    adjust_start_time: 1,
    count:             MAX_HISTORICAL_CANDLES,
    end:               'latest',
    start,
    style:             'candles',
    granularity
  });
}

function subscribeToTicks(symbol) {
  sendMessage({ ticks: symbol, subscribe: 1 });
}

// ─── WebSocket Message Handler ────────────────────────────────────────────────
function handleMessage(raw) {
  let data;
  try { data = JSON.parse(raw); } catch { return; }

  if (data.error) {
    console.error('Deriv WS error:', data.error.message);
    return;
  }

  if (data.candles) {
    const symbol    = data.echo_req.ticks_history;
    const gran      = data.echo_req.granularity;
    const timeframe = Object.keys(timeframeMap).find(k => timeframeMap[k] === gran);
    if (timeframe) processCandles(symbol, timeframe, data.candles);
  }

  if (data.tick) {
    const symbol    = data.tick.symbol;
    const price     = parseFloat(data.tick.quote);
    const timestamp = data.tick.epoch;

    updateCurrentCandle(symbol, price, timestamp);
    recalculateIndicators(symbol, price);

    console.log(
      `[${symbol}] Price: ${price} | ` +
      `EMA20: ${indicatorState[symbol].ema20?.toFixed(4)} | ` +
      `EMA50: ${indicatorState[symbol].ema50?.toFixed(4)}`
    );
  }
}

// ─── Initialize Deriv WebSocket ───────────────────────────────────────────────
function initializeWebSocket() {
  // Prevent multiple simultaneous connection attempts
  if (isConnecting) return;
  isConnecting = true;

  console.log('Connecting to Deriv WebSocket...');
  ws = new WebSocket(API_URL);

  ws.on('open', () => {
    isConnecting = false;
    console.log('Connected to Deriv WebSocket');
    ['R_10', 'R_25', 'stpRNG'].forEach(symbol => {
      requestCandles(symbol, '5min');
      subscribeToTicks(symbol);
    });
  });

  ws.on('message', handleMessage);

  ws.on('close', () => {
    isConnecting = false;
    // Re-arm the ready gate so live ticks after reconnect don't fire
    // cross notifications before historical candles are re-loaded.
    Object.keys(symbolReady).forEach(sym => { symbolReady[sym] = false; });
    console.log('Disconnected — reconnecting in 5s...');
    setTimeout(initializeWebSocket, 5000);
  });

  ws.on('error', err => {
    isConnecting = false;
    console.error('WebSocket error:', err.message);
  });
}

// ─── Express API ──────────────────────────────────────────────────────────────
app.use(express.static(path.join(__dirname, 'public')));

// Returns live indicator data as JSON — your HTML can poll this endpoint
app.get('/api/indicators', (req, res) => {
  res.json(indicatorState);
});

app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// ─── Start Server ─────────────────────────────────────────────────────────────
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
  initializeWebSocket();
});

process.on('SIGINT', () => {
  console.log('Shutting down...');
  if (ws) ws.close();
  process.exit(0);
});
