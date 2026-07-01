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
const SYMBOLS    = ['R_10', 'R_25', 'R_50'];
const TIMEFRAMES = ['5min'];

const timeframeMap = { '5min': 300 };

const displayNames = {
  'R_10':   'Volatility 10 Index',
  'R_25':   'Volatility 25 Index',
  'R_50':   'Volatility 50 Index',
  '5min':   '5 minutes'
};

const MAX_HISTORICAL_CANDLES = 5000;

// How many CLOSED candles must pass before the same EMA can alert again
const COOLDOWN_CANDLES = 5;

// Hard minimum seconds between any two Telegram sends for the same key
// Set to 4 minutes (240s) — well under a 5-min candle so real crosses aren't missed
// but long enough to absorb any duplicate ticks or reconnect floods
const MIN_ALERT_SECONDS = 240;

// ─── State ───────────────────────────────────────────────────────────────────
const historicalData       = {};
const currentCandles       = {};
const candleCount          = {};
const emaNotificationState = {};
const emaPriceSide         = {};
const emaState             = {};

function initState() {
  SYMBOLS.forEach(sym => {
    historicalData[sym]       = {};
    currentCandles[sym]       = {};
    candleCount[sym]          = {};
    emaNotificationState[sym] = {};
    emaPriceSide[sym]         = {};
    emaState[sym]             = {};

    TIMEFRAMES.forEach(tf => {
      historicalData[sym][tf]       = [];
      currentCandles[sym][tf]       = null;
      candleCount[sym][tf]          = 0;
      emaNotificationState[sym][tf] = {};
      emaPriceSide[sym][tf]         = {};

      [20].forEach(period => {
        emaNotificationState[sym][tf][period] = { lastNotifCandle: null, notifSent: false };
        emaPriceSide[sym][tf][period]         = null;
      });
    });

    [20].forEach(period => {
      emaState[sym][period] = null;
    });
  });
}

initState();

// ─── Telegram ────────────────────────────────────────────────────────────────
// Single source of truth for dedup — keyed on symbol:timeframe:period
// Set synchronously before the async fetch so no second tick can slip through
const alertSentAt = new Map();

async function sendTelegramNotification(message, dedupKey) {
  if (!TELEGRAM_BOT_TOKEN || !TELEGRAM_CHAT_ID) return;

  const url  = `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`;
  const body = JSON.stringify({ chat_id: TELEGRAM_CHAT_ID, text: message, parse_mode: 'Markdown' });

  try {
    const res  = await fetch(url, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body });
    const data = await res.json();
    if (!data.ok) console.error('[Telegram] API error:', data);
    else          console.log(`[Telegram] Sent ✓ (${dedupKey})`);
  } catch (err) {
    console.error('[Telegram] Send failed:', err.message);
  }
}

// ─── EMA helpers ─────────────────────────────────────────────────────────────
function initEMA(symbol, closedCandles, period) {
  const data = closedCandles
    .filter(c => isFinite(c.close) && c.close > 0)
    .sort((a, b) => a.timestamp - b.timestamp);

  if (data.length < period) { emaState[symbol][period] = null; return; }

  let ema = 0;
  for (let i = 0; i < period; i++) ema += data[i].close;
  ema /= period;

  const k = 2 / (period + 1);
  for (let i = period; i < data.length; i++) {
    ema = data[i].close * k + ema * (1 - k);
  }
  emaState[symbol][period] = ema;
}

function advanceEMA(symbol, period, closedClose) {
  if (emaState[symbol][period] === null) return;
  const k = 2 / (period + 1);
  emaState[symbol][period] = closedClose * k + emaState[symbol][period] * (1 - k);
}

function getEMA(symbol, period) {
  return emaState[symbol][period];
}

// ─── EMA cross detection ──────────────────────────────────────────────────────
function checkEMATouches(symbol, timeframe, currentPrice) {
  const symbolName    = displayNames[symbol]    || symbol;
  const timeframeName = displayNames[timeframe] || timeframe;
  const currentCount  = candleCount[symbol][timeframe];
  const now           = Date.now();

  [20].forEach(period => {
    const ema = getEMA(symbol, period);
    if (ema === null) return;

    const state      = emaNotificationState[symbol][timeframe][period];
    const dedupKey   = `${symbol}:${timeframe}:${period}`;
    const lastSentMs = alertSentAt.get(dedupKey) || 0;

    // ── Cooldown unlock: BOTH conditions must pass ───────────────────────────
    // 1. Enough candles have closed since last alert
    const candlesClear = state.lastNotifCandle === null ||
                         (currentCount - state.lastNotifCandle) >= COOLDOWN_CANDLES;
    // 2. Enough real time has passed (hard backstop against any duplicate source)
    const timeClear    = (now - lastSentMs) >= (MIN_ALERT_SECONDS * 1000);

    if (state.notifSent && candlesClear && timeClear) {
      state.notifSent = false;
      console.log(`[Lock] Released ${dedupKey}`);
    }

    if (state.notifSent) return;

    const currentSide  = currentPrice >= ema ? 'above' : 'below';
    const previousSide = emaPriceSide[symbol][timeframe][period];

    emaPriceSide[symbol][timeframe][period] = currentSide;

    if (previousSide === null) return;
    if (previousSide === currentSide) return;

    // ── Genuine cross — lock FIRST, then send ───────────────────────────────
    state.lastNotifCandle = currentCount;
    state.notifSent       = true;
    alertSentAt.set(dedupKey, now);   // set synchronously before async send

    const crossedUp = currentSide === 'above';
    const arrow     = crossedUp ? '⬆️' : '⬇️';
    const alertTime = new Date().toLocaleTimeString('en-GB', { hour: '2-digit', minute: '2-digit', second: '2-digit' });

    // Telegram message
    const message =
      `${symbolName} ${arrow}\n` +
      `EMA: ${ema.toFixed(4)} | Price: ${currentPrice.toFixed(4)}`;

    // Console log matches Telegram format
    console.log(`\n${symbolName} ${arrow}\nEMA: ${ema.toFixed(4)} | Price: ${currentPrice.toFixed(4)}  [${alertTime}]`);
    sendTelegramNotification(message, dedupKey);
  });
}

// ─── Candle management ───────────────────────────────────────────────────────
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
        if (historicalData[symbol][timeframe].length > MAX_HISTORICAL_CANDLES)
          historicalData[symbol][timeframe].shift();

        const closedClose = currentCandles[symbol][timeframe].close;
        // Check cross BEFORE advancing EMA — price must be compared against
        // the EMA that was active DURING the candle, not the next bar's EMA
        checkEMATouches(symbol, timeframe, closedClose);
        advanceEMA(symbol, 20, closedClose);
        candleCount[symbol][timeframe]++;
        console.log(`\n[${symbol}/${timeframe}] Candle #${candleCount[symbol][timeframe]} closed @ ${closedClose}`);
      }

      currentCandles[symbol][timeframe] = {
        timestamp: candleTime, open: price, high: price, low: price, close: price
      };
    } else {
      const c = currentCandles[symbol][timeframe];
      c.high  = Math.max(c.high, price);
      c.low   = Math.min(c.low,  price);
      c.close = price;
    }
  });
}

// ─── Indicator recalculation ─────────────────────────────────────────────────
function recalculateIndicators(symbol, timeframe, livePrice) {
  if (!historicalData[symbol][timeframe].length || !currentCandles[symbol][timeframe]) return;

  const ema20 = getEMA(symbol, 20);

  process.stdout.write(
    `\r[${symbol}] Price:${livePrice.toFixed(4)} ` +
    `EMA20:${ema20 !== null ? ema20.toFixed(4) : 'N/A'}   `
  );

  // Cross detection moved to candle close — no tick-by-tick false signals
}

// ─── Historical candle processing ────────────────────────────────────────────
function processCandles(symbol, timeframe, candles) {
  const data = candles
    .map(c => ({ open: parseFloat(c.open), high: parseFloat(c.high),
                 low: parseFloat(c.low), close: parseFloat(c.close), timestamp: c.epoch }))
    .filter(c => isFinite(c.close) && c.close > 0)
    .sort((a, b) => a.timestamp - b.timestamp);

  if (!data.length) return;

  historicalData[symbol][timeframe] = data.slice(0, -1);

  const lastCandle = data[data.length - 1];
  currentCandles[symbol][timeframe] = {
    timestamp: lastCandle.timestamp,
    open: lastCandle.open, high: lastCandle.high,
    low: lastCandle.low,   close: lastCandle.close
  };

  initEMA(symbol, historicalData[symbol][timeframe], 20);

  // Seed emaPriceSide from the last CLOSED candle's close so the first
  // live tick never sees a null→side transition and fires a false cross
  [20].forEach(period => {
    const ema      = emaState[symbol][period];
    const closed   = historicalData[symbol][timeframe];
    const lastClose = closed.length ? closed[closed.length - 1].close : null;
    emaPriceSide[symbol][timeframe][period] =
      (ema !== null && lastClose !== null) ? (lastClose >= ema ? 'above' : 'below') : null;
  });

  console.log(
    `[${symbol}/${timeframe}] Loaded ${data.length} candles | ` +
    `EMA20:${emaState[symbol][20]?.toFixed(4) ?? 'N/A'}`
  );
}

// ─── WebSocket ───────────────────────────────────────────────────────────────
function sendMessage(msg) {
  if (ws && ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(msg));
}

function requestCandles(symbol, timeframe) {
  sendMessage({
    ticks_history: symbol, adjust_start_time: 1,
    count: MAX_HISTORICAL_CANDLES, end: 'latest',
    style: 'candles', granularity: timeframeMap[timeframe]
  });
}

function subscribeToTicks(symbol) {
  sendMessage({ ticks: symbol, subscribe: 1 });
}

// Track last processed epoch per symbol to drop exact duplicates
const lastTickEpoch = {};

function handleMessage(raw) {
  let data;
  try { data = JSON.parse(raw); } catch { return; }

  if (data.error) { console.error('[WS] Error:', data.error.message); return; }

  if (data.candles) {
    const symbol    = data.echo_req.ticks_history;
    const tf        = Object.keys(timeframeMap).find(k => timeframeMap[k] === data.echo_req.granularity);
    if (tf) processCandles(symbol, tf, data.candles);
  }

  if (data.tick) {
    const { symbol, quote, epoch } = data.tick;
    const price = parseFloat(quote);

    // Drop if same epoch as the last tick we processed for this symbol
    if (lastTickEpoch[symbol] === epoch) return;
    lastTickEpoch[symbol] = epoch;

    updateCurrentCandle(symbol, price, epoch);
    Object.keys(timeframeMap).forEach(tf => recalculateIndicators(symbol, tf, price));
  }
}

function initializeWebSocket() {
  console.log('[WS] Connecting…');
  ws = new WebSocket(API_URL);

  ws.on('open', () => {
    console.log('[WS] Connected');
    SYMBOLS.forEach(sym => {
      TIMEFRAMES.forEach(tf => requestCandles(sym, tf));
      subscribeToTicks(sym);
    });
  });

  ws.on('message', handleMessage);

  ws.on('close', () => {
    console.log('\n[WS] Disconnected — reconnecting in 5s…');
    setTimeout(initializeWebSocket, 5_000);
  });

  ws.on('error', err => console.error('[WS] Error:', err.message));
}

process.on('SIGINT',  () => { ws?.close(); process.exit(0); });
process.on('SIGTERM', () => { ws?.close(); process.exit(0); });

initializeWebSocket();
