import axios from 'axios';
import { JUPITER_API_KEY, JSON_HEADERS, TRENDING_LOOKBACK_MS } from '../config.js';
import { now, json } from '../utils.js';
import { numSetting, boolSetting, setting } from '../db/settings.js';
import { db } from '../db/connection.js';
import { gmgnBackoffActive, setGmgnBackoff, gmgnFetch, normalizedTrendingRows } from '../enrichment/gmgn.js';
import { normalizeJupiterTrendingRow } from '../enrichment/jupiter.js';

export const trending = new Map();
let degenHandler = null;
let trendingCandidateHandler = null;

export function setDegenHandler(fn) {
  degenHandler = fn;
}

export function setTrendingCandidateHandler(fn) {
  trendingCandidateHandler = fn;
}

export function storeSignalEvent(mint, kind, source, payload) {
  db.prepare(`
    INSERT INTO signal_events (mint, kind, at_ms, source, payload_json)
    VALUES (?, ?, ?, ?, ?)
  `).run(mint, kind, now(), source, json(payload));
}

export function trendingSignalPass(row) {
  const volume = Number(row?.volume ?? 0);
  const swaps = Number(row?.swaps ?? 0);
  const rugRatio = Number(row?.rug_ratio ?? 0);
  const bundlerRate = Number(row?.bundler_rate ?? 0);
  const holderCount = Number(row?.holder_count ?? 0);
  const top10Rate = Number(row?.top_10_holder_rate ?? 0);
  const marketCap = Number(row?.market_cap ?? 0);
  const botDegenRate = Number(row?.bot_degen_rate ?? 0);
  const minVolume = numSetting('trending_min_volume_usd', 50000);
  const minSwaps = numSetting('trending_min_swaps', 500);
  const minHolders = numSetting('trending_min_holders', 100);
  const maxTop10Rate = numSetting('trending_max_top10_rate', 0.3);
  const minMcap = numSetting('trending_min_mcap_usd', 15000);
  const maxMcap = numSetting('trending_max_mcap_usd', 100000);
  const maxRugRatio = numSetting('trending_max_rug_ratio', 0.1);
  const maxBundlerRate = numSetting('trending_max_bundler_rate', 0.3);
  const maxBotDegenRate = numSetting('trending_max_bot_degen_rate', 0.5);
  if (minVolume > 0 && (!Number.isFinite(volume) || volume < minVolume)) return false;
  if (minSwaps > 0 && (!Number.isFinite(swaps) || swaps < minSwaps)) return false;
  if (minHolders > 0 && (!Number.isFinite(holderCount) || holderCount < minHolders)) return false;
  if (maxTop10Rate > 0 && Number.isFinite(top10Rate) && top10Rate > maxTop10Rate) return false;
  if (minMcap > 0 && (!Number.isFinite(marketCap) || marketCap < minMcap)) return false;
  if (maxMcap > 0 && Number.isFinite(marketCap) && marketCap > maxMcap) return false;
  if (maxRugRatio > 0 && Number.isFinite(rugRatio) && rugRatio > maxRugRatio) return false;
  if (maxBundlerRate > 0 && Number.isFinite(bundlerRate) && bundlerRate > maxBundlerRate) return false;
  if (maxBotDegenRate > 0 && Number.isFinite(botDegenRate) && botDegenRate > maxBotDegenRate) return false;
  if (row?.is_wash_trading === true || row?.is_wash_trading === 1) return false;
  return true;
}

export async function fetchJupiterTrendingRows(interval, limit) {
  if (!JUPITER_API_KEY) {
    console.log('[trending:jupiter] JUPITER_API_KEY missing');
    return [];
  }
  const supported = new Set(['5m', '1h', '6h', '24h']);
  const window = supported.has(interval) ? interval : '5m';
  const url = new URL(`https://api.jup.ag/tokens/v2/toptrending/${window}`);
  url.searchParams.set('limit', String(limit));
  const res = await axios.get(url.toString(), {
    timeout: 10_000,
    headers: { ...JSON_HEADERS, 'x-api-key': JUPITER_API_KEY },
  });
  const rows = Array.isArray(res.data) ? res.data : [];
  return rows.map((row, index) => normalizeJupiterTrendingRow(row, window, index + 1));
}

export async function fetchGmgnTrendingRows(interval, limit) {
  if (gmgnBackoffActive('trending')) return [];
  const payload = await gmgnFetch('/v1/market/rank', {
    kind: 'trending',
    params: {
      chain: 'sol',
      interval,
      limit,
      order_by: setting('trending_order_by', 'volume'),
      direction: 'desc',
      filters: ['renounced', 'frozen', 'not_wash_trading'],
      platforms: ['Pump.fun', 'meteora_virtual_curve', 'pool_pump_amm'],
    },
  });
  return normalizedTrendingRows(payload).map((row, index) => ({
    ...row,
    interval,
    rank: index + 1,
    source: 'gmgn_market_rank',
  }));
}

export async function fetchGmgnTrending() {
  if (!boolSetting('trending_enabled', true)) {
    trending.clear();
    return;
  }
  const interval = setting('trending_interval', '5m');
  const limit = Math.max(1, Math.min(200, Math.floor(numSetting('trending_limit', 100))));
  const source = setting('trending_source', 'jupiter');

  try {
    const rows = source === 'gmgn'
      ? await fetchGmgnTrendingRows(interval, Math.min(100, limit))
      : await fetchJupiterTrendingRows(interval, limit);
    const seenAt = now();
    const cutoff = seenAt - TRENDING_LOOKBACK_MS;
    for (const [mint, token] of trending) {
      if (Number(token.seenAt || 0) < cutoff) trending.delete(mint);
    }
    let tracked = 0;
    let skipped = 0;
    for (const [index, row] of rows.entries()) {
      const mint = row?.address || row?.mint;
      if (!mint || !String(mint).endsWith('pump') || !trendingSignalPass(row)) continue;
      // Dedup: skip if already tracked in this trending map (prevents re-trigger every poll cycle)
      if (trending.has(mint)) {
        skipped++;
        continue;
      }
      const token = { ...row, address: mint, interval, rank: index + 1, seenAt };
      trending.set(mint, token);
      tracked++;
      storeSignalEvent(mint, 'trending', token.source || source, token);
      if (degenHandler) await degenHandler(mint, token);
      if (trendingCandidateHandler) {
        trendingCandidateHandler({ mint, trendingToken: token, route: 'trending' }).catch(err =>
          console.log(`[trending] candidate trigger failed for ${mint.slice(0, 8)}: ${err.message}`),
        );
      }
    }
    console.log(`[trending:${source}] loaded ${rows.length}, accepted ${tracked}, skipped ${skipped}, tracking ${trending.size}`);
  } catch (err) {
    if (source === 'gmgn') setGmgnBackoff('trending', err);
    const status = err.response?.status || '';
    const body = err.response?.data;
    const resetAt = body?.reset_at ? ` reset_at=${body.reset_at}` : '';
    if (source !== 'gmgn' || (status !== 403 && status !== 429)) console.log(`[trending:${source}] ${status} ${body?.code || ''} ${body?.message || err.message}${resetAt}`);
  }
}
