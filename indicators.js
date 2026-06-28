'use strict';

/**
 * Deriv EMA Indicator — Node.js (server-side)
 *
 * Converted from browser HTML/JS. All DOM manipulation removed.
 * Requires: ws  →  npm install ws
 * Usage:    node indicators.js
 */

const WebSocket = require('ws');

// ─── Telegram configuration ───────────────────────────────────────────────────
const TELEGRAM_BOT_TOKEN = '8626868477:AAHyD9ajC4M_SYX4XbYcbAiV9nmtelVl6KA';
const TELEGRAM_CHAT_ID   = '6456659526';

// ─── Deriv WebSocket ──────────────────────────────────────────────────────────
const API_URL = 'wss://ws.derivws.com/websockets/v3?app_id=1089';
let ws;

// ─── Symbols & timeframes ─────────────────────────────────────────────────────
const SYMBOLS    = ['R_10', 'R_25', 'stpRNG'];
const TIMEFRAMES = ['5min'];

const timeframeMap = {
  '5min': 300   // seconds
};

const displayNames = {
  'R_10':   'Volatility 10 Index',
  'R_25':   'Volatility 25 Index',
  'stpRNG': 'Step Index 100 stpRNG',
  '5min':   '5 minutes'
};

// Maximum historical candles (gives EMA enough warm-up to converge)
const MAX_HISTORICAL_CANDLES = 5000;

// ─── State ────────────────────────────────────────────────────────────────────

/** Closed historical candles per symbol/timeframe */
const historicalData = {};

/** Current (forming) candle per symbol/timeframe */
const currentCandles = {};

/** How many candles have closed since startup, per symbol/timeframe */
const candleCount = {};

/**
 * Notification lock state per symbol + EMA period.
 * lastNotifCandle: candleCount value when the last alert was sent
 * notifSent:       true while within the 5-candle cooldown window
 */
const emaNotificationState = {};

/** Which side of each EMA the price was on at the last tick */
const emaPriceSide = {};

/** EMA value at the close of the last COMPLETED bar (seed for live EMA) */
const emaState = {};

// Initialise state objects for every symbol
SYMBOLS.forEach(sym => {
  historicalData[sym]       = {};
  currentCandles[sym]       = {};
  candleCount[sym]          = {};
  emaNotificationState[sym] = {};
  emaPriceSide[sym]         = {};
  emaState[sym]             = {};

  TIMEFRAMES.forEach(tf => {
    historicalData[sym][tf] = [];
    currentCandles[sym][tf] = null;
    candleCount[sym][tf]    = 0;
  });

  [20, 50].forEach(period => {
    emaNotificationState[sym][period] = { lastNotifCandle: null, notifSent: false };
    emaPriceSide[sym][period]         = null;
    emaState[sym][period]             = null;
  });
});

// ─── Telegram ─────────────────────────────────────────────────────────────────

/**
 * Deduplication keyed on symbol+period (NOT the message string).
 * Prevents double-fires caused by the price changing between ticks
 * while still on the same cross event.
 * key: `${symbol}:${period}` → timestamp of last send
 */
const alertSentAt = new Map();

async function sendTelegramNotification(message, dedupKey) {
  if (!TELEGRAM_BOT_TOKEN || !TELEGRAM_CHAT_ID) {
    console.warn('[Telegram] Credentials not configured');
    return;
  }

  // Block if the same symbol+period already alerted within 10 seconds
  const now      = Date.now();
  const lastSent = alertSentAt.get(dedupKey) || 0;
  if (now - lastSent < 10_000) {
    console.warn(`[Telegram] Duplicate blocked for key: ${dedupKey}`);
    return;
  }
  alertSentAt.set(dedupKey, now);

  const url  = `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`;
  const body = JSON.stringify({
    chat_id:    TELEGRAM_CHAT_ID,
    text:       message,
    parse_mode: 'Markdown'
  });

  try {
    const res  = await fetch(url, {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body
    });
    const data = await res.json();
    if (!data.ok) console.error('[Telegram] API error:', data);
  } catch (err) {
    console.error('[Telegram] Send failed:', err.message);
  }
}

// ─── EMA helpers ──────────────────────────────────────────────────────────────

/**
 * Seed EMA from historical closed candles.
 * Uses SMA of the first `period` bars, then applies the recursive formula —
 * identical to TradingView's cold-start behaviour.
 */
function initEMA(closedCandles, period) {
  const data = closedCandles
    .filter(c => isFinite(c.close) && c.close > 0)
    .sort((a, b) => a.timestamp - b.timestamp);

  if (data.length < period) return null;

  // SMA seed
  let ema = 0;
  for (let i = 0; i < period; i++) ema += data[i].close;
  ema /= period;

  // Walk forward
  const k = 2 / (period + 1);
  for (let i = period; i < data.length; i++) {
    ema = data[i].close * k + ema * (1 - k);
  }
  return ema;
}

/**
 * Advance the stored EMA by one closed bar.
 * One multiplication — no loop needed.
 */
function advanceEMA(symbol, period, closedClose) {
  if (emaState[symbol][period] === null) return;
  const k = 2 / (period + 1);
  emaState[symbol][period] = closedClose * k + emaState[symbol][period] * (1 - k);
}

/**
 * Return the "live" EMA for the current forming bar — exactly what TradingView
 * shows on the active bar (stored last-bar EMA stepped one tick forward).
 */
function getLiveEMA(symbol, period, currentPrice) {
  const stored = emaState[symbol][period];
  if (stored === null) return null;
  const k = 2 / (period + 1);
  return currentPrice * k + stored * (1 - k);
}

// ─── EMA cross / touch detection ──────────────────────────────────────────────

/**
 * On each tick, check whether price has crossed either EMA.
 * One alert per cross; 5-candle cooldown before re-alerting on the same EMA.
 */
function checkEMATouches(symbol, timeframe, currentPrice, ema20, ema50) {
  const symbolName   = displayNames[symbol];
  const timeframeName = displayNames[timeframe];
  const currentCount  = candleCount[symbol][timeframe];

  [20, 50].forEach(period => {
    const ema = period === 20 ? ema20 : ema50;
    if (ema === null) return;

    const state = emaNotificationState[symbol][period];

    // Unlock after 5 closed candles
    if (state.notifSent && currentCount - state.lastNotifCandle >= 5) {
      state.notifSent = false;
    }

    if (state.notifSent) return;

    const currentSide  = currentPrice >= ema ? 'above' : 'below';
    const previousSide = emaPriceSide[symbol][period];

    emaPriceSide[symbol][period] = currentSide;

    if (previousSide === null) return;   // first tick — no prior side
    if (previousSide === currentSide) return; // no cross

    // Cross detected — alert and lock
    const crossedUp = currentSide === 'above';
    const emoji     = crossedUp ? '📈' : '📉';
    const message   =
      `${emoji} *${period} EMA ${symbolName} on ${timeframeName}* : price Touch\n` +
      ` EMA: ${ema.toFixed(4)} | Price: ${currentPrice.toFixed(4)}`;

    const dedupKey = `${symbol}:${period}`;
    sendTelegramNotification(message, dedupKey);
    console.log(`[Alert] ${message.replace(/\*/g, '')}`);

    state.lastNotifCandle = currentCount;
    state.notifSent       = true;
  });
}

// ─── Candle management ────────────────────────────────────────────────────────

function getCandleTimeframe(timestamp, granularity) {
  return Math.floor(timestamp / granularity) * granularity;
}

/**
 * Feed a live tick into the current forming candle.
 * When the candle closes, archive it and advance the EMA.
 */
function updateCurrentCandle(symbol, price, timestamp) {
  Object.keys(timeframeMap).forEach(timeframe => {
    const granularity = timeframeMap[timeframe];
    const candleTime  = getCandleTimeframe(timestamp, granularity);

    if (
      !currentCandles[symbol][timeframe] ||
      currentCandles[symbol][timeframe].timestamp !== candleTime
    ) {
      // Previous candle just closed
      if (currentCandles[symbol][timeframe]) {
        historicalData[symbol][timeframe].push(currentCandles[symbol][timeframe]);

        if (historicalData[symbol][timeframe].length > MAX_HISTORICAL_CANDLES) {
          historicalData[symbol][timeframe].shift();
        }

        const closedClose = currentCandles[symbol][timeframe].close;
        advanceEMA(symbol, 20, closedClose);
        advanceEMA(symbol, 50, closedClose);
        candleCount[symbol][timeframe]++;
      }

      // Open new candle
      currentCandles[symbol][timeframe] = {
        timestamp: candleTime,
        open:  price,
        high:  price,
        low:   price,
        close: price
      };
    } else {
      // Update forming candle
      const candle  = currentCandles[symbol][timeframe];
      candle.high   = Math.max(candle.high, price);
      candle.low    = Math.min(candle.low,  price);
      candle.close  = price;
    }
  });
}

// ─── Indicator recalculation ──────────────────────────────────────────────────

function recalculateIndicators(symbol, timeframe, livePrice) {
  const historicalCandles = historicalData[symbol][timeframe];
  const currentCandle     = currentCandles[symbol][timeframe];

  if (!historicalCandles || historicalCandles.length === 0 || !currentCandle) return;

  const ema20 = getLiveEMA(symbol, 20, livePrice);
  const ema50 = getLiveEMA(symbol, 50, livePrice);

  // Console output (replaces DOM table updates)
  const trend20   = ema20 !== null ? (livePrice > ema20 ? 'Uptrend' : 'Downtrend') : 'N/A';
  const trend50   = ema50 !== null ? (livePrice > ema50 ? 'Uptrend' : 'Downtrend') : 'N/A';
  const dist20    = ema20 !== null ? (livePrice - ema20).toFixed(4) : 'N/A';
  const dist50    = ema50 !== null ? (livePrice - ema50).toFixed(4) : 'N/A';
  const ema20Str  = ema20 !== null ? ema20.toFixed(4) : 'N/A';
  const ema50Str  = ema50 !== null ? ema50.toFixed(4) : 'N/A';

  console.log(
    `[${symbol}] Price: ${livePrice.toFixed(4)} | ` +
    `20 EMA: ${ema20Str} (${trend20}, dist ${dist20}) | ` +
    `50 EMA: ${ema50Str} (${trend50}, dist ${dist50})`
  );

  checkEMATouches(symbol, timeframe, livePrice, ema20, ema50);
}

// ─── Historical candle processing ─────────────────────────────────────────────

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

  // All bars except the last are closed; the last is still forming
  historicalData[symbol][timeframe] = data.slice(0, -1);

  const lastCandle = data[data.length - 1];
  currentCandles[symbol][timeframe] = {
    timestamp: lastCandle.timestamp,
    open:  lastCandle.open,
    high:  lastCandle.high,
    low:   lastCandle.low,
    close: lastCandle.close
  };

  // Seed EMA from all closed bars
  emaState[symbol][20] = initEMA(historicalData[symbol][timeframe], 20);
  emaState[symbol][50] = initEMA(historicalData[symbol][timeframe], 50);

  const currentPrice = lastCandle.close;
  const ema20 = getLiveEMA(symbol, 20, currentPrice);
  const ema50 = getLiveEMA(symbol, 50, currentPrice);

  console.log(
    `[${symbol}/${timeframe}] Loaded ${data.length} candles. ` +
    `Seed 20 EMA: ${ema20 !== null ? ema20.toFixed(4) : 'N/A'} | ` +
    `Seed 50 EMA: ${ema50 !== null ? ema50.toFixed(4) : 'N/A'}`
  );
}

// ─── WebSocket ────────────────────────────────────────────────────────────────

function sendMessage(message) {
  if (ws && ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(message));
  }
}

function requestCandles(symbol, timeframe) {
  const granularity = timeframeMap[timeframe];
  const end   = Math.floor(Date.now() / 1000);
  const start = end - MAX_HISTORICAL_CANDLES * granularity;

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

function handleMessage(raw) {
  let data;
  try {
    data = JSON.parse(raw);
  } catch {
    console.error('[WS] Invalid JSON received');
    return;
  }

  if (data.error) {
    console.error('[WS] Server error:', data.error);
    return;
  }

  if (data.candles) {
    const symbol      = data.echo_req.ticks_history;
    const granularity = data.echo_req.granularity;
    const timeframe   = Object.keys(timeframeMap).find(
      key => timeframeMap[key] === granularity
    );
    if (timeframe) processCandles(symbol, timeframe, data.candles);
  }

  if (data.tick) {
    const symbol    = data.tick.symbol;
    const price     = parseFloat(data.tick.quote);
    const timestamp = data.tick.epoch;

    updateCurrentCandle(symbol, price, timestamp);

    Object.keys(timeframeMap).forEach(timeframe => {
      recalculateIndicators(symbol, timeframe, price);
    });

    process.stdout.write(`\r[${new Date().toLocaleTimeString()}] Last tick received`);
  }
}

function initializeWebSocket() {
  console.log('[WS] Connecting to Deriv…');
  ws = new WebSocket(API_URL);

  ws.on('open', () => {
    console.log('[WS] Connected');
    SYMBOLS.forEach(symbol => {
      TIMEFRAMES.forEach(timeframe => requestCandles(symbol, timeframe));
      subscribeToTicks(symbol);
    });
  });

  ws.on('message', handleMessage);

  ws.on('close', () => {
    console.log('\n[WS] Disconnected — reconnecting in 5 s…');
    setTimeout(initializeWebSocket, 5_000);
  });

  ws.on('error', err => {
    console.error('[WS] Error:', err.message);
  });
}

// ─── Graceful shutdown ────────────────────────────────────────────────────────

function shutdown() {
  console.log('\n[App] Shutting down…');
  if (ws) ws.close();
  process.exit(0);
}

process.on('SIGINT',  shutdown);
process.on('SIGTERM', shutdown);

// ─── Start ────────────────────────────────────────────────────────────────────

initializeWebSocket();
