import axios from 'axios';
import { WSOL_MINT, JSON_HEADERS } from '../config.js';
import { now } from '../utils.js';

const jupiterAssetCache = new Map();
let jupiterAssetBackoffUntil = 0;

function jupiterAssetBackoffActive() {
  return now() < jupiterAssetBackoffUntil;
}

function setJupiterAssetBackoff(err) {
  if (err.response?.status !== 429) return;
  const resetHeader = Number(err.response?.headers?.['x-ratelimit-reset'] || 0);
  const resetMs = resetHeader > 1_000_000_000_000 ? resetHeader : resetHeader * 1000;
  jupiterAssetBackoffUntil = resetMs > now() ? resetMs : now() + 30_000;
  console.log(`[asset] backing off until ${new Date(jupiterAssetBackoffUntil).toISOString()} (429)`);
}

let quoteBackoffUntil = 0;

function quoteBackoffActive() {
  return now() < quoteBackoffUntil;
}

function setQuoteBackoff(err) {
  if (err.response?.status !== 429) return;
  const resetHeader = Number(err.response?.headers?.['x-ratelimit-reset'] || 0);
  const resetMs = resetHeader > 1_000_000_000_000 ? resetHeader : resetHeader * 1000;
  quoteBackoffUntil = resetMs > now() ? resetMs : now() + 30_000;
  console.log(`[quote] backing off until ${new Date(quoteBackoffUntil).toISOString()} (429)`);
}

let solUsdCache = { price: null, at: 0 };

function jupiterStatsForInterval(row, interval) {
  const key = `stats${interval}`;
  return row?.[key] || row?.stats5m || row?.stats1h || row?.stats24h || {};
}

function normalizeJupiterTrendingRow(row, interval, rank) {
  const stats = jupiterStatsForInterval(row, interval);
  const buyVolume = Number(stats.buyVolume ?? 0);
  const sellVolume = Number(stats.sellVolume ?? 0);
  const numBuys = Number(stats.numBuys ?? 0);
  const numSells = Number(stats.numSells ?? 0);
  const topHolders = Number(row?.audit?.topHoldersPercentage);
  const botHolders = Number(row?.audit?.botHoldersPercentage);
  return {
    ...row,
    address: row?.id,
    price: Number(row?.usdPrice ?? 0),
    volume: buyVolume + sellVolume,
    liquidity: Number(row?.liquidity ?? 0),
    market_cap: Number(row?.mcap ?? row?.fdv ?? 0),
    swaps: numBuys + numSells,
    buys: numBuys,
    sells: numSells,
    holder_count: Number(row?.holderCount ?? 0),
    top_10_holder_rate: Number.isFinite(topHolders) ? topHolders / 100 : null,
    launchpad_platform: row?.launchpad || null,
    launchpad_status: row?.graduatedAt ? '2' : null,
    smart_degen_count: Number(stats.numOrganicBuyers ?? 0),
    hot_level: Number(row?.organicScore ?? 0),
    rug_ratio: null,
    bundler_rate: Number.isFinite(botHolders) ? botHolders / 100 : null,
    source: 'jupiter_toptrending',
    interval,
    rank,
    stats,
  };
}

async function fetchJupiterAsset(mint, { useCache = true, ttlMs = 20_000 } = {}) {
  const cached = jupiterAssetCache.get(mint);
  if (useCache && cached && now() - cached.at < ttlMs) return cached.data;
  if (jupiterAssetBackoffActive()) return cached?.data || null;
  try {
    const url = new URL('https://datapi.jup.ag/v1/assets/search');
    url.searchParams.set('query', mint);
    const res = await axios.get(url.toString(), {
      timeout: 10_000,
      headers: JSON_HEADERS,
    });
    const rows = Array.isArray(res.data) ? res.data : [];
    const data = rows.find(row => row?.id === mint) || rows[0] || null;
    jupiterAssetCache.set(mint, { at: now(), data });
    return data;
  } catch (err) {
    setJupiterAssetBackoff(err);
    if (err.response?.status !== 429) console.log(`[asset] ${mint.slice(0, 8)}... ${err.response?.status || ''} ${err.message}`);
    return cached?.data || null;
  }
}

async function fetchSolUsdPrice() {
  try {
    const res = await axios.get(`https://lite-api.jup.ag/price/v3?ids=${WSOL_MINT}`, {
      timeout: 5000,
      headers: JSON_HEADERS,
    });
    const price = Number(res.data?.[WSOL_MINT]?.usdPrice);
    return Number.isFinite(price) && price > 0 ? price : null;
  } catch (err) {
    console.log(`[sol-price] ${err.response?.status || ''} ${err.message}`);
    return null;
  }
}

async function estimateTokenAmountFromSol(sizeSol, entryPrice) {
  if (!Number.isFinite(Number(entryPrice)) || Number(entryPrice) <= 0) return null;
  const solUsd = await fetchSolUsdPrice();
  if (!Number.isFinite(Number(solUsd)) || Number(solUsd) <= 0) return null;
  return Number(sizeSol) * solUsd / Number(entryPrice);
}

async function fetchJupiterHolders(mint) {
  try {
    const res = await axios.get(`https://datapi.jup.ag/v1/holders/${mint}`, {
      timeout: 10_000,
      headers: JSON_HEADERS,
    });
    const holders = Array.isArray(res.data?.holders) ? res.data.holders : [];
    const total = holders.reduce((sum, holder) => sum + Number(holder.amount || 0), 0);
    const mapped = holders.map((holder, index) => {
      const pct = total > 0 ? Number(holder.amount || 0) / total * 100 : null;
      return {
        address: holder.address,
        rank: index + 1,
        amount: Number(holder.amount || 0),
        percent: pct,
        tags: (holder.tags || []).map(tag => tag.name || tag.id).filter(Boolean),
      };
    });
    const top20 = mapped.slice(0, 20);
    return {
      count: holders.length,
      holders: mapped,
      top20,
      top20Percent: top20.reduce((sum, holder) => sum + Number(holder.percent || 0), 0),
      maxHolderPercent: Math.max(0, ...top20.map(holder => Number(holder.percent || 0))),
    };
  } catch (err) {
    console.log(`[holders] ${mint.slice(0, 8)}... ${err.response?.status || ''} ${err.message}`);
    return { count: 0, holders: [], top20: [], top20Percent: null, maxHolderPercent: null };
  }
}

function summarizeCandles(label, candles) {
  if (!candles.length) return { label, available: false };
  const first = candles[0];
  const last = candles[candles.length - 1];
  const high = Math.max(...candles.map(candle => Number(candle.high || 0)));
  const low = Math.min(...candles.map(candle => Number(candle.low || Infinity)));
  const volumeNative = candles.reduce((sum, candle) => sum + Number(candle.volume || 0), 0);
  const current = Number(last.close);
  const start = Number(first.open);
  return {
    label,
    available: true,
    purpose: label === 'ath_context_24h_5m' ? 'ath_context' : 'range_context',
    candles: candles.length,
    fromTime: first.time,
    toTime: last.time,
    current,
    high,
    low,
    volumeNative,
    changePercent: start > 0 ? (current / start - 1) * 100 : null,
    belowHighPercent: high > 0 ? (current / high - 1) * 100 : null,
    aboveLowPercent: low > 0 && Number.isFinite(low) ? (current / low - 1) * 100 : null,
  };
}

async function fetchJupiterChartWindow(mint, interval, candles, label) {
  const url = new URL(`https://datapi.jup.ag/v2/charts/${mint}`);
  url.searchParams.set('interval', interval);
  url.searchParams.set('to', String(now()));
  url.searchParams.set('candles', String(candles));
  url.searchParams.set('type', 'price');
  url.searchParams.set('quote', 'native');
  const res = await axios.get(url.toString(), {
    timeout: 10_000,
    headers: JSON_HEADERS,
  });
  return summarizeCandles(label, Array.isArray(res.data?.candles) ? res.data.candles : []);
}

async function fetchJupiterChartContext(mint) {
  const windows = [
    ['5_MINUTE', 288, 'ath_context_24h_5m'],
    ['1_HOUR', 168, 'swing_7d_1h'],
    ['4_HOUR', 180, 'long_30d_4h'],
  ];
  const results = await Promise.all(windows.map(([interval, candles, label]) => (
    fetchJupiterChartWindow(mint, interval, candles, label).catch((err) => {
      console.log(`[chart] ${mint.slice(0, 8)}... ${interval} ${err.message}`);
      return { label, available: false, error: err.message };
    })
  )));
  const available = results.filter(row => row.available);
  const currentNative = available[0]?.current ?? null;
  const rangeHigh = available.length ? Math.max(...available.map(row => Number(row.high || 0))) : null;
  const topBlastRisk = Number.isFinite(Number(currentNative)) && Number.isFinite(Number(rangeHigh)) && rangeHigh > 0
    ? currentNative / rangeHigh >= 0.85
    : null;
  return {
    quote: 'native',
    purpose: 'ATH/range context, not momentum scoring',
    currentNative,
    rangeHighNative: rangeHigh,
    belowRangeHighPercent: currentNative && rangeHigh ? (currentNative / rangeHigh - 1) * 100 : null,
    distanceFromAthPercent: currentNative && rangeHigh ? (currentNative / rangeHigh - 1) * 100 : null,
    topBlastRisk,
    windows: results,
  };
}

const IGNORED_PNL_MINTS = new Set([
  'So11111111111111111111111111111111111111111',
  'So11111111111111111111111111111111111111112',
  'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v',
]);

async function fetchSolUsdPriceCached() {
  if (solUsdCache.price != null && now() - solUsdCache.at < 60_000) return solUsdCache.price;
  const price = await fetchSolUsdPrice();
  solUsdCache = { price, at: now() };
  return price;
}

// Fixed 1000-token reference amount ignores price impact for large sizes —
// upgrade to position-sized quotes when size_sol > 1.
async function fetchTokenSpotViaQuote(mint) {
  const tag = mint.slice(0, 8);
  if (quoteBackoffActive()) {
    console.log(`[quote] ${tag}... backoff active until ${new Date(quoteBackoffUntil).toISOString()} — skipped (exit will use mark)`);
    return null;
  }
  try {
    const url = new URL('https://lite-api.jup.ag/swap/v1/quote');
    url.searchParams.set('inputMint', mint);
    url.searchParams.set('outputMint', WSOL_MINT);
    url.searchParams.set('amount', '1000000000');
    url.searchParams.set('slippageBps', '100');
    const [solUsd, quoteRes] = await Promise.all([
      fetchSolUsdPriceCached(),
      axios.get(url.toString(), { timeout: 10_000, headers: JSON_HEADERS }),
    ]);
    const outAmount = quoteRes.data?.outAmount;
    if (!outAmount) {
      console.log(`[quote] ${tag}... no route (outAmount null) — exit will fall back to mark`);
      return null;
    }
    const outSol = Number(outAmount) / 1e9;
    if (!Number.isFinite(solUsd) || solUsd <= 0) {
      console.log(`[quote] ${tag}... invalid solUsd (${solUsd}) — exit will fall back to mark`);
      return null;
    }
    const price = (outSol / 1000) * solUsd;
    console.log(`[quote] ${tag}... ok price=${price} (outAmount=${outAmount})`);
    return price;
  } catch (err) {
    setQuoteBackoff(err);
    if (err.response?.status !== 429) console.log(`[quote] ${tag}... ${err.response?.status || ''} ${err.message}`);
    else console.log(`[quote] ${tag}... 429 rate-limited`);
    return null;
  }
}

async function fetchJupiterWalletPnl(walletAddress) {
  try {
    const url = new URL('https://datapi.jup.ag/v1/pnl');
    url.searchParams.set('addresses', walletAddress);
    url.searchParams.set('includeClosed', 'false');
    const res = await axios.get(url.toString(), { timeout: 10_000, headers: JSON_HEADERS });
    const data = res.data?.[walletAddress] || {};
    for (const mint of IGNORED_PNL_MINTS) delete data[mint];
    return data;
  } catch (err) {
    console.log(`[pnl] ${err.response?.status || ''} ${err.message}`);
    return {};
  }
}

export {
  jupiterStatsForInterval,
  normalizeJupiterTrendingRow,
  fetchJupiterAsset,
  fetchSolUsdPrice,
  estimateTokenAmountFromSol,
  fetchJupiterHolders,
  summarizeCandles,
  fetchJupiterChartWindow,
  fetchJupiterChartContext,
  fetchJupiterWalletPnl,
  fetchTokenSpotViaQuote,
  jupiterAssetBackoffActive,
  setJupiterAssetBackoff,
};
