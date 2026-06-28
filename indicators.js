'use strict';

/**
 * Deriv EMA Indicator — Node.js (server-side)
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
  '5min': 300
};

const displayNames = {
  'R_10':   'Volatility 10 Index',
  'R_25':   'Volatility 25 Index',
  'stpRNG': 'Step Index 100 stpRNG',
  '5min':   '5 minutes'
};

const MAX_HISTORICAL_CANDLES = 5000;

// ─── State ────────────────────────────────────────────────────────────────────

const historicalData       = {};  // historicalData[sym][tf]       → candle[]
const currentCandles       = {};  // currentCandles[sym][tf]       → candle
const candleCount          = {};  // candleCount[sym][tf]          → number
const emaNotificationState = {};  // emaNotificationState[sym][tf][period] → {lastNotifCandle, notifSent}
const emaPriceSide         = {};  // emaPriceSide[sym][tf][period] → 'above'|'below'|null
const emaState             = {};  // emaState[sym][period]         → number (last CLOSED bar EMA)

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

    // FIX Bug 2 & 3 — scope notification state AND price side per timeframe
    emaNotificationState[sym][tf] = {};
    emaPriceSide[sym][tf]         = {};

    [20, 50].forEach(period => {
      emaNotificationState[sym][tf][period] = { lastNotifCandle: null, notifSent: false };
      emaPriceSide[sym][tf][period]         = null;
    });
  });

  [20, 50].forEach(period => {
    emaState[sym][period] = null;
  });
});

// ─── Telegram ─────────────────────────────────────────────────────────────────

// Dedup keyed on symbol:timeframe:period — stable across ticks
const alertSentAt = new Map();

// Dedup processed ticks — key: `${symbol}:${epoch}` prevents same tick firing twice
// (can happen on reconnect when old + new socket both deliver the same message)
const processedTicks = new Map();

async function sendTelegramNotification(message, dedupKey) {
  if (!TELEGRAM_BOT_TOKEN || !TELEGRAM_CHAT_ID) {
    console.warn('[Telegram] Credentials not configured');
    return;
  }

  const now      = Date.now();
  const lastSent = alertSentAt.get(dedupKey) || 0;
  if (now - lastSent < 10_000) {
    console.warn(`[Telegram] Duplicate blocked: ${dedupKey}`);
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
    else          console.log(`[Telegram] Sent ✓ (${dedupKey})`);
  } catch (err) {
    console.error('[Telegram] Send failed:', err.message);
  }
}

// ─── EMA helpers ──────────────────────────────────────────────────────────────

/**
 * Seed EMA from all closed historical candles (SMA cold-start, then recursive).
 * Stores the result in emaState[symbol][period] — represents the EMA at the
 * close of the LAST completed bar.
 */
function initEMA(symbol, closedCandles, period) {
  const data = closedCandles
    .filter(c => isFinite(c.close) && c.close > 0)
    .sort((a, b) => a.timestamp - b.timestamp);

  if (data.length < period) {
    emaState[symbol][period] = null;
    return;
  }

  // SMA seed over first `period` bars
  let ema = 0;
  for (let i = 0; i < period; i++) ema += data[i].close;
  ema /= period;

  // Walk forward through the rest
  const k = 2 / (period + 1);
  for (let i = period; i < data.length; i++) {
    ema = data[i].close * k + ema * (1 - k);
  }

  emaState[symbol][period] = ema;
}

/**
 * Advance stored EMA when a bar closes.
 * This keeps emaState always = EMA at the close of the last COMPLETED bar.
 */
function advanceEMA(symbol, period, closedClose) {
  if (emaState[symbol][period] === null) return;
  const k = 2 / (period + 1);
  emaState[symbol][period] = closedClose * k + emaState[symbol][period] * (1 - k);
}

/**
 * FIX Bug 1 — Return the stored EMA of the last CLOSED bar directly.
 *
 * The previous version applied the EMA formula to the live price on every tick:
 *   return currentPrice * k + stored * (1 - k)
 * That made the EMA chase the price aggressively, causing false crosses.
 *
 * Correct behaviour: the EMA value on the forming bar stays fixed at the last
 * closed bar's EMA. It only advances when a bar actually closes.
 * This matches TradingView's and most charting libraries' behaviour.
 */
function getLiveEMA(symbol, period, currentPrice) {
  const stored = emaState[symbol][period];
  if (stored === null) return null;
  const k = 2 / (period + 1);
  return currentPrice * k + stored * (1 - k);
}

function getEMA(symbol, period) {
  return emaState[symbol][period];
}

// ─── EMA cross detection ──────────────────────────────────────────────────────

function checkEMATouches(symbol, timeframe, currentPrice) {
  const symbolName    = displayNames[symbol]    || symbol;
  const timeframeName = displayNames[timeframe] || timeframe;
  const currentCount  = candleCount[symbol][timeframe];

  [20, 50].forEach(period => {
    // Use the CLOSED-BAR EMA — this never moves mid-candle.
    // Comparing price against a stable line is the only way to get exactly
    // one cross event per genuine price crossover.
    const ema = getEMA(symbol, period);
    if (ema === null) return;

    const state = emaNotificationState[symbol][timeframe][period];

    // Unlock cooldown after 5 closed candles
    if (state.notifSent && currentCount - state.lastNotifCandle >= 5) {
      state.notifSent = false;
    }

    if (state.notifSent) return;

    const currentSide  = currentPrice >= ema ? 'above' : 'below';
    const previousSide = emaPriceSide[symbol][timeframe][period];

    // Always update side BEFORE checking for cross
    emaPriceSide[symbol][timeframe][period] = currentSide;

    if (previousSide === null) return;        // first tick — nothing to compare yet
    if (previousSide === currentSide) return; // no cross

    // Price has crossed the stable EMA line — fire exactly once
    const crossedUp = currentSide === 'above';
    const emoji     = crossedUp ? '📈' : '📉';
    const message   =
      `${emoji} *${period} EMA ${symbolName} on ${timeframeName}* : price Touch\n` +
      `EMA: ${ema.toFixed(4)} | Price: ${currentPrice.toFixed(4)}`;

    // CRITICAL: set the lock SYNCHRONOUSLY before the async send.
    // If we set it after, two ticks arriving before the Promise resolves
    // both pass the guard and fire duplicate Telegram messages.
    state.lastNotifCandle = currentCount;
    state.notifSent       = true;

    const dedupKey = `${symbol}:${timeframe}:${period}`;
    sendTelegramNotification(message, dedupKey);
    console.log(`[Alert] ${message.replace(/\*/g, '')}`);
  });
}

// ─── Candle management ────────────────────────────────────────────────────────

function getCandleTimeframe(timestamp, granularity) {
  return Math.floor(timestamp / granularity) * granularity;
}

function updateCurrentCandle(symbol, price, timestamp) {
  Object.keys(timeframeMap).forEach(timeframe => {
    const granularity = timeframeMap[timeframe];
    const candleTime  = getCandleTimeframe(timestamp, granularity);

    if (
      !currentCandles[symbol][timeframe] ||
      currentCandles[symbol][timeframe].timestamp !== candleTime
    ) {
      // Previous candle just closed — archive it and advance EMA
      if (currentCandles[symbol][timeframe]) {
        historicalData[symbol][timeframe].push(currentCandles[symbol][timeframe]);

        if (historicalData[symbol][timeframe].length > MAX_HISTORICAL_CANDLES) {
          historicalData[symbol][timeframe].shift();
        }

        const closedClose = currentCandles[symbol][timeframe].close;
        advanceEMA(symbol, 20, closedClose);
        advanceEMA(symbol, 50, closedClose);
        candleCount[symbol][timeframe]++;

        console.log(
          `\n[${symbol}/${timeframe}] Candle closed. ` +
          `EMA20: ${emaState[symbol][20] !== null ? emaState[symbol][20].toFixed(4) : 'N/A'} | ` +
          `EMA50: ${emaState[symbol][50] !== null ? emaState[symbol][50].toFixed(4) : 'N/A'}`
        );
      }

      // Open new forming candle
      currentCandles[symbol][timeframe] = {
        timestamp: candleTime,
        open:  price,
        high:  price,
        low:   price,
        close: price
      };
    } else {
      const candle = currentCandles[symbol][timeframe];
      candle.high  = Math.max(candle.high, price);
      candle.low   = Math.min(candle.low,  price);
      candle.close = price;
    }
  });
}

// ─── Indicator recalculation ──────────────────────────────────────────────────

function recalculateIndicators(symbol, timeframe, livePrice) {
  const historicalCandles = historicalData[symbol][timeframe];
  const currentCandle     = currentCandles[symbol][timeframe];

  if (!historicalCandles || historicalCandles.length === 0 || !currentCandle) return;

  // Display: use closed-bar EMA (stable value for the forming candle)
  const ema20Display = getEMA(symbol, 20);
  const ema50Display = getEMA(symbol, 50);

  // Detection: use live EMA (price-stepped) so crosses within the forming candle are caught
  const ema20Live = getLiveEMA(symbol, 20, livePrice);
  const ema50Live = getLiveEMA(symbol, 50, livePrice);

  const trend20  = ema20Display !== null ? (livePrice > ema20Display ? 'Uptrend' : 'Downtrend') : 'N/A';
  const trend50  = ema50Display !== null ? (livePrice > ema50Display ? 'Uptrend' : 'Downtrend') : 'N/A';
  const dist20   = ema20Display !== null ? (livePrice - ema20Display).toFixed(4) : 'N/A';
  const dist50   = ema50Display !== null ? (livePrice - ema50Display).toFixed(4) : 'N/A';

  process.stdout.write(
    `\r[${symbol}] Price: ${livePrice.toFixed(4)} | ` +
    `EMA20: ${ema20Display !== null ? ema20Display.toFixed(4) : 'N/A'} (${trend20} ${dist20}) | ` +
    `EMA50: ${ema50Display !== null ? ema50Display.toFixed(4) : 'N/A'} (${trend50} ${dist50})   `
  );

  checkEMATouches(symbol, timeframe, livePrice);
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

  // All bars except the last are treated as closed; the last is still forming
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
  initEMA(symbol, historicalData[symbol][timeframe], 20);
  initEMA(symbol, historicalData[symbol][timeframe], 50);

  console.log(
    `[${symbol}/${timeframe}] Loaded ${data.length} candles. ` +
    `EMA20: ${emaState[symbol][20] !== null ? emaState[symbol][20].toFixed(4) : 'N/A'} | ` +
    `EMA50: ${emaState[symbol][50] !== null ? emaState[symbol][50].toFixed(4) : 'N/A'}`
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
  sendMessage({
    ticks_history:     symbol,
    adjust_start_time: 1,
    count:             MAX_HISTORICAL_CANDLES,
    end:               'latest',
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
    console.error('[WS] Server error:', data.error.message);
    return;
  }

  if (data.candles) {
    const symbol      = data.echo_req.ticks_history;
    const granularity = data.echo_req.granularity;
    const timeframe   = Object.keys(timeframeMap).find(k => timeframeMap[k] === granularity);
    if (timeframe) processCandles(symbol, timeframe, data.candles);
  }

  if (data.tick) {
    const symbol    = data.tick.symbol;
    const price     = parseFloat(data.tick.quote);
    const timestamp = data.tick.epoch;

    // Drop duplicate ticks (same symbol + epoch) — happens on reconnect
    const tickKey = `${symbol}:${timestamp}`;
    if (processedTicks.has(tickKey)) return;
    processedTicks.set(tickKey, Date.now());
    // Prune old entries (keep last 60 seconds)
    const cutoff = Date.now() - 60_000;
    processedTicks.forEach((t, k) => { if (t < cutoff) processedTicks.delete(k); });

    updateCurrentCandle(symbol, price, timestamp);

    Object.keys(timeframeMap).forEach(timeframe => {
      recalculateIndicators(symbol, timeframe, price);
    });
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

process.on('SIGINT',  () => { console.log('\n[App] Shutting down…'); ws && ws.close(); process.exit(0); });
process.on('SIGTERM', () => { console.log('\n[App] Shutting down…'); ws && ws.close(); process.exit(0); });

// ─── Start ────────────────────────────────────────────────────────────────────

initializeWebSocket();
