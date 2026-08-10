import { randomUUID } from 'node:crypto';
import { GMGN_API_KEY, GMGN_CACHE_TTL_MS, GMGN_ENABLED, JSON_HEADERS } from '../config.js';
import { now, sleep } from '../utils.js';
import { numSetting, setting } from '../db/settings.js';

const gmgnCache = new Map();
let lastGmgnRequestAt = 0;
let gmgnQueue = Promise.resolve();
const gmgnBackoff = {
  tokenUntil: 0,
  tokenReason: '',
  trendingUntil: 0,
  trendingReason: '',
};

async function paceGmgnRequest() {
  const delayMs = Math.max(0, numSetting('gmgn_request_delay_ms', 2500));
  if (!delayMs) return;
  const elapsed = now() - lastGmgnRequestAt;
  if (elapsed < delayMs) await sleep(delayMs - elapsed);
  lastGmgnRequestAt = now();
}

function enqueueGmgn(work) {
  const run = gmgnQueue.then(work, work);
  gmgnQueue = run.catch(() => {});
  return run;
}

function gmgnErrorText(status, payload, fallback) {
  const raw = String(payload?.raw || payload?.message || payload?.error || fallback || '');
  if (/<title>\s*Just a moment/i.test(raw) || /challenge-platform|cf_chl/i.test(raw)) {
    return 'Cloudflare managed challenge';
  }
  return `${status || ''} ${payload?.code || ''} ${raw}`.replace(/\s+/g, ' ').trim().slice(0, 240);
}

function appendParams(url, params = {}) {
  for (const [key, value] of Object.entries(params)) {
    if (value == null) continue;
    if (Array.isArray(value)) {
      for (const entry of value.filter(item => item != null && item !== '')) {
        url.searchParams.append(key, String(entry));
      }
    } else {
      url.searchParams.set(key, String(value));
    }
  }
}

async function gmgnFetch(pathname, { method = 'GET', params = {}, body = null, kind = 'token' } = {}) {
  if (!GMGN_ENABLED) throw new Error('GMGN disabled');
  // Never send during an active ban — direct callers (trenches/smartMoney/gmgnSignal)
  // must not keep extending it. Mirrors fetchGmgnTokenInfo's pre-gate at the shared
  // entry so EVERY path honors the backoff, not just the wrapper functions.
  if (gmgnBackoffActive(kind)) return null;
  return enqueueGmgn(async () => {
    const url = new URL(`https://openapi.gmgn.ai${pathname}`);
    appendParams(url, params);
    const maxRetries = Math.max(0, Math.floor(numSetting('gmgn_max_retries', 2)));
    for (let attempt = 0; attempt <= maxRetries; attempt++) {
      if (gmgnBackoffActive(kind)) return null; // re-check after queue wait / between retries
      await paceGmgnRequest();
      url.searchParams.set('timestamp', String(Math.floor(now() / 1000)));
      url.searchParams.set('client_id', randomUUID());
      const fetchOpts = {
        method,
        headers: {
          'X-APIKEY': GMGN_API_KEY,
          'Content-Type': 'application/json',
          'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
        },
      };
      if (body) fetchOpts.body = JSON.stringify(body);
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), 8000);
      fetchOpts.signal = controller.signal;
      let res;
      try {
        res = await fetch(url, fetchOpts);
      } catch (err) {
        if (err?.name === 'AbortError') {
          throw new Error('gmgn fetch timeout 8000ms');
        }
        throw err;
      } finally {
        clearTimeout(timeoutId);
      }
      const text = await res.text().catch(() => '');
      let payload = {};
      try {
        payload = text ? JSON.parse(text) : {};
      } catch {
        payload = { raw: text };
      }
      if (res.ok) return payload;
      const message = gmgnErrorText(res.status, payload, `GMGN ${pathname} ${res.status}`);
      const rateLimited = res.status === 429 || /rate limit|temporarily banned/i.test(String(message));
      // Register the ban as soon as we see 403/429 — not only after retries exhaust.
      // Direct callers (trenches/smartMoney/gmgnSignal) never call setGmgnBackoff
      // themselves; without this, a 429 from those paths re-fires every poll cycle
      // and self-extends the ban. setGmgnBackoff no-ops unless status is 403/429.
      if (res.status === 403 || res.status === 429) {
        const banError = new Error(message);
        banError.response = { status: res.status, data: payload, headers: Object.fromEntries(res.headers.entries()) };
        setGmgnBackoff(kind, banError);
      }
      if (rateLimited && attempt < maxRetries) {
        const retryAfter = Number(res.headers.get('retry-after'));
        const backoffMs = Number.isFinite(retryAfter)
          ? retryAfter * 1000
          : /temporarily banned/i.test(String(message))
            ? 60_000
            : Math.min(30_000, 3000 * 2 ** attempt);
        await sleep(backoffMs);
        continue;
      }
      const error = new Error(message);
      error.response = { status: res.status, data: payload, headers: Object.fromEntries(res.headers.entries()) };
      throw error;
    }
    throw new Error(`GMGN ${pathname} failed`);
  });
}

function gmgnBackoffKey(kind) {
  return kind === 'trending' ? 'trendingUntil' : 'tokenUntil';
}

function gmgnReasonKey(kind) {
  return kind === 'trending' ? 'trendingReason' : 'tokenReason';
}

function gmgnBackoffActive(kind) {
  return now() < Number(gmgnBackoff[gmgnBackoffKey(kind)] || 0);
}

function setGmgnBackoff(kind, err) {
  const status = err.response?.status;
  if (status !== 403 && status !== 429) return;
  const body = err.response?.data || {};
  const resetAtMs = Number(body.reset_at || 0) * 1000;
  const challenge = /Cloudflare managed challenge/i.test(String(err.message));
  const fallbackMs = challenge ? 30 * 60 * 1000 : status === 403 ? 10 * 60 * 1000 : 60 * 1000;
  const until = resetAtMs > now() ? resetAtMs : now() + fallbackMs;
  const reason = gmgnErrorText(status, body, err.message);
  gmgnBackoff[gmgnBackoffKey(kind)] = until;
  gmgnBackoff[gmgnReasonKey(kind)] = reason;
  console.log(`[gmgn:${kind}] backing off until ${new Date(until).toISOString()} (${reason})`);
}

function gmgnStatusText(kind) {
  if (!GMGN_ENABLED) return 'off';
  const key = gmgnBackoffKey(kind);
  if (!gmgnBackoffActive(kind)) return 'ok';
  const seconds = Math.max(1, Math.ceil((Number(gmgnBackoff[key]) - now()) / 1000));
  return `blocked ${seconds}s`;
}

function marketCapFromGmgn(info) {
  const direct = Number(info?.market_cap ?? info?.mcap);
  if (Number.isFinite(direct) && direct > 0) return direct;
  const price = Number(info?.price);
  const supply = Number(info?.circulating_supply ?? info?.total_supply);
  return Number.isFinite(price) && Number.isFinite(supply) ? price * supply : null;
}

function tokenPriceFromGmgn(info) {
  const price = Number(info?.price);
  return Number.isFinite(price) ? price : null;
}

async function fetchGmgnTokenInfo(mint, useCache = true) {
  if (!GMGN_ENABLED) return null;
  const cached = gmgnCache.get(mint);
  if (useCache && cached && now() - cached.at < GMGN_CACHE_TTL_MS) return cached.data;
  if (gmgnBackoffActive('token')) {
    gmgnCache.set(mint, { at: now(), data: null });
    return null;
  }

  try {
    const payload = await gmgnFetch('/v1/token/info', {
      params: { chain: 'sol', address: mint },
    });
    const data = payload?.data?.data || payload?.data || payload;
    gmgnCache.set(mint, { at: now(), data });
    return data;
  } catch (err) {
    setGmgnBackoff('token', err);
    if (err.response?.status !== 403 && err.response?.status !== 429) {
      console.log(`[gmgn] ${mint.slice(0, 8)}... ${err.response?.status || ''} ${err.message}`);
    }
    gmgnCache.set(mint, { at: now(), data: null });
    return null;
  }
}

function normalizedTrendingRows(payload) {
  const rows = payload?.data?.data?.rank
    || payload?.data?.rank
    || payload?.rank
    || payload?.data?.data
    || payload?.data
    || [];
  return Array.isArray(rows) ? rows : [];
}

export {
  gmgnFetch,
  fetchGmgnTokenInfo,
  gmgnBackoffActive,
  setGmgnBackoff,
  gmgnStatusText,
  marketCapFromGmgn,
  tokenPriceFromGmgn,
  normalizedTrendingRows,
};
