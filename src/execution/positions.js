import { now, json } from '../utils.js';
import { numSetting, boolSetting, strategyById, slippageAdjustedMcap } from '../db/settings.js';
import { db } from '../db/connection.js';
import { firstPositiveNumber, marketCapFromGmgn, tokenPriceFromGmgn, computeAtrPercent, dynamicStopLossPercent } from '../utils.js';
import { fetchGmgnTokenInfo } from '../enrichment/gmgn.js';
import { fetchJupiterAsset, fetchJupiterHolders, fetchJupiterChartContext, fetchJupiterWalletPnl, fetchTokenSpotViaQuote } from '../enrichment/jupiter.js';
import { liveWalletPubkey } from '../liveExecutor.js';
import { fetchSavedWalletExposure } from '../enrichment/wallets.js';
import { filterCandidate } from '../pipeline/candidateBuilder.js';
import { openPositions, createDryRunPosition } from '../db/positions.js';
import { updateCandidateSnapshot } from '../db/candidates.js';
import { trending } from '../signals/trending.js';
import { executeLiveSell } from './router.js';
import { sendPositionExit } from '../telegram/send.js';

export async function freshEntryMarket(mint, candidate) {
  const gmgn = await fetchGmgnTokenInfo(mint, false);
  const asset = await fetchJupiterAsset(mint, { useCache: false });
  const priceUsd = firstPositiveNumber(tokenPriceFromGmgn(gmgn), asset?.usdPrice, candidate.metrics?.priceUsd);
  const marketCapUsd = firstPositiveNumber(
    marketCapFromGmgn(gmgn),
    asset?.mcap,
    asset?.fdv,
    candidate.metrics?.marketCapUsd,
    candidate.metrics?.graduatedMarketCapUsd,
  );
  return { gmgn, asset, priceUsd, marketCapUsd, refreshedAtMs: now() };
}

export async function refreshCandidateForExecution(row) {
  const candidate = row.candidate;
  const mint = candidate.token.mint;
  const route = candidate.signals?.route || '';
  const isFresh = route.includes('pumpportal_graduated');

  let gmgn, asset, holders, chart, qp;

  if (isFresh) {
    // Fast path: skip GMGN (Cloudflare blocked) and chart (no data for freshly graduated).
    // Entry anchor (2026-08-05): also fetch the executable quote — the asset mark lags the
    // migration reprice (BULLMOJI 3.65x / PEW 1.78x stale); the quote is what the bot can
    // actually transact at and matched the fill in every observed case. Falls back to the
    // mark when the quote fails (400/no-route on fresh grads — E2a class).
    // Quote retry (2026-08-06): fresh-grad routes often 400/no-route for a beat before the
    // lite-api can route them — retry 3x/750ms so the dry twin gets the quote anchor instead
    // of the stale mark (Bulls 08-05: single attempt failed -> entryAnchor=mark -> dry +107% phantom).
    // Gap A parity (2026-08-07): buildCandidate now fetches gmgn on the fresh path,
    // so the execution refresh must too — otherwise the fresh-check filter sees a
    // different liquidity source (Jupiter vs GMGN) and rejects at the $10K band
    // (observed: build soft 30 PASS, refresh soft 20 < 30 REJECT on GMGN 11.3K vs
    // Jupiter 8.3K liquidity). Use the CACHE (default true) — same 5-min object build
    // used, zero extra GMGN calls, zero pacing latency. Chart stays null: the ATH
    // distance penalty is !isFreshGrad-gated, so it cannot affect fresh-grad scores.
    const gmgnP = fetchGmgnTokenInfo(mint);
    const assetP = fetchJupiterAsset(mint, { useCache: false });
    const holdersP = fetchJupiterHolders(mint);
    qp = null;
    for (let attempt = 1; attempt <= 3; attempt++) {
      qp = await fetchTokenSpotViaQuote(mint).catch(() => null);
      if (Number.isFinite(qp) && qp > 0) break;
      console.log(`[candidate] quote attempt ${attempt}/3 failed for ${mint.slice(0, 8)}...`);
      if (attempt < 3) await new Promise((r) => setTimeout(r, 750));
    }
    [gmgn, asset, holders] = await Promise.all([gmgnP, assetP, holdersP]);
    chart = null;
  } else {
    [gmgn, asset, holders] = await Promise.all([
      fetchGmgnTokenInfo(mint, false),
      fetchJupiterAsset(mint, { useCache: false }),
      fetchJupiterHolders(mint),
    ]);
    chart = null;  // chart not used in buy path — saves 10s timeout
  }
  const quotePrice = (Number.isFinite(qp) && qp > 0) ? qp : null;
  const selectedTrending = trending.get(mint) || candidate.trending || null;
  const selectedHolders = holders?.holders?.length ? holders : candidate.holders;
  const selectedSavedWalletExposure = selectedHolders
    ? await fetchSavedWalletExposure(mint, selectedHolders)
    : candidate.savedWalletExposure;
  const priceUsd = firstPositiveNumber(tokenPriceFromGmgn(gmgn), asset?.usdPrice, selectedTrending?.price, candidate.metrics?.priceUsd);
  const marketCapUsd = firstPositiveNumber(
    marketCapFromGmgn(gmgn),
    asset?.mcap,
    asset?.fdv,
    selectedTrending?.market_cap,
    candidate.metrics?.marketCapUsd,
    candidate.metrics?.graduatedMarketCapUsd,
  );
  // Entry anchor (2026-08-05): for fresh grads, prefer the executable-quote-derived
  // price/mcap over the asset mark (stale right after migration). quoteMcap = quotePrice x supply.
  let entryAnchorSrc = 'mark';
  let effectivePriceUsd = priceUsd;
  let effectiveMarketCapUsd = marketCapUsd;
  if (isFresh && quotePrice != null) {
    const supplyTokens = Number(asset?.totalSupply || asset?.circSupply || 0);
    const quoteMcap = supplyTokens > 0 ? quotePrice * supplyTokens : null;
    if (quoteMcap != null && Number.isFinite(quoteMcap) && quoteMcap > 0) {
      entryAnchorSrc = 'quote';
      effectivePriceUsd = quotePrice;
      effectiveMarketCapUsd = quoteMcap;
    }
  }
  if (isFresh) {
    console.log(`[candidate] ${mint.slice(0, 8)}... entry anchor: ${entryAnchorSrc}${entryAnchorSrc === 'quote' ? ` (mcap ${effectiveMarketCapUsd.toFixed(0)}, mark was ${marketCapUsd.toFixed(0)})` : ' (quote unavailable after retries — live pre-fill uses mark, dry twin will be SKIPPED)'}`);
  }
  const refreshed = {
    ...candidate,
    token: {
      ...candidate.token,
      name: gmgn?.name || asset?.name || selectedTrending?.name || candidate.token.name,
      symbol: gmgn?.symbol || asset?.symbol || selectedTrending?.symbol || candidate.token.symbol,
      twitter: candidate.token.twitter || asset?.twitter || gmgn?.link?.twitter_username || selectedTrending?.twitter || '',
      website: candidate.token.website || asset?.website || gmgn?.link?.website || '',
      telegram: candidate.token.telegram || gmgn?.link?.telegram || '',
    },
    metrics: {
      ...candidate.metrics,
      priceUsd: effectivePriceUsd,
      marketCapUsd: effectiveMarketCapUsd,
      liquidityUsd: Number(gmgn?.liquidity ?? asset?.liquidity ?? selectedTrending?.liquidity ?? candidate.metrics?.liquidityUsd ?? 0),
      holderCount: Number(gmgn?.holder_count ?? asset?.holderCount ?? selectedTrending?.holder_count ?? candidate.metrics?.holderCount ?? 0),
      gmgnTotalFeesSol: Number(gmgn?.total_fee ?? asset?.fees ?? candidate.metrics?.gmgnTotalFeesSol ?? 0),
      gmgnTradeFeesSol: Number(gmgn?.trade_fee ?? candidate.metrics?.gmgnTradeFeesSol ?? 0),
      trendingVolumeUsd: Number(selectedTrending?.volume ?? candidate.metrics?.trendingVolumeUsd ?? 0),
      trendingSwaps: Number(selectedTrending?.swaps ?? candidate.metrics?.trendingSwaps ?? 0),
      trendingHotLevel: Number(selectedTrending?.hot_level ?? candidate.metrics?.trendingHotLevel ?? 0),
      trendingSmartDegenCount: Number(selectedTrending?.smart_degen_count ?? candidate.metrics?.trendingSmartDegenCount ?? 0),
    },
    gmgn,
    jupiterAsset: asset,
    trending: selectedTrending,
    holders: selectedHolders,
    chart,
    savedWalletExposure: selectedSavedWalletExposure,
    executionRefresh: {
      refreshedAtMs: now(),
      source: 'pre_execution',
      entryAnchor: entryAnchorSrc,
      marketCapUsd: effectiveMarketCapUsd,
      priceUsd: effectivePriceUsd,
      liquidityUsd: Number(gmgn?.liquidity ?? asset?.liquidity ?? selectedTrending?.liquidity ?? 0),
      holdersRefreshed: Boolean(holders?.holders?.length),
    },
  };
  refreshed.filters = filterCandidate(refreshed);
  const executionFailures = [];
  if (!Number.isFinite(Number(refreshed.metrics.marketCapUsd)) || Number(refreshed.metrics.marketCapUsd) <= 0) {
    executionFailures.push('execution mcap: missing');
  }
  if (!Number.isFinite(Number(refreshed.metrics.priceUsd)) || Number(refreshed.metrics.priceUsd) <= 0) {
    executionFailures.push('execution price: missing');
  }
  if (executionFailures.length) {
    refreshed.filters = {
      ...refreshed.filters,
      passed: false,
      failures: [...(refreshed.filters?.failures || []), ...executionFailures],
    };
  }
  updateCandidateSnapshot(row.id, refreshed, refreshed.filters.passed ? 'candidate' : 'filtered');
  return { ...row, candidate: refreshed };
}

const sellInProgress = new Set();

export async function refreshPosition(position, { autoExit = true, jupiterPnl = null, forceExit = false, prefetched = null } = {}) {
  // Bug2 fix (2026-06-19): bypass 20s cache for live monitoring — flash crash detection requires fresh data
  // Quote-first (2026-07-24): exit decisions use executable Jupiter quote (live pool
  // reserves) as primary price — datapi mark is stale by design. Mark = fallback on 429/backoff.
  // E2b (2026-08-05): quote for BOTH dry and live so both legs decide on the same price
  // signal (shadow parity). The live fill still overrides reported PnL at close.
  // Poll-parity (2026-08-05): when monitorPositions passes a prefetched per-mint price
  // (twin pairs), use the SAME sample for both legs — identical high-water/trailing state.
  const useQuote = numSetting('exit_quote_enabled', 1);
  const [asset, qp] = prefetched
    ? [prefetched.asset, prefetched.qp]
    : await Promise.all([
        fetchJupiterAsset(position.mint, { useCache: false, ttlMs: 3000 }),
        useQuote ? fetchTokenSpotViaQuote(position.mint) : Promise.resolve(null),
      ]);
  const quotePrice = (Number.isFinite(qp) && qp > 0) ? qp : null;
  const quoteMcap = quotePrice && Number(position.entry_price) > 0
    ? Number(position.entry_mcap) * (quotePrice / Number(position.entry_price))
    : null;
  const jupiterPrice = Number(asset?.usdPrice);
  const jupiterMcap = firstPositiveNumber(asset?.mcap, asset?.fdv);
  // Guard 1 DISABLED (2026-07-17): can't distinguish crash vs stale data — single source (Jupiter) is unreliable
  let price = firstPositiveNumber(quotePrice, jupiterPrice || null, position.high_water_price, position.entry_price);
  let mcap = firstPositiveNumber(quoteMcap, jupiterMcap, position.high_water_mcap, position.entry_mcap);
  if (!Number.isFinite(Number(mcap)) || !Number.isFinite(Number(position.entry_mcap)) || Number(position.entry_mcap) <= 0) {
    return null;
  }
  // Guard 2 DISABLED (2026-07-17): drop >80% heuristic can't distinguish crash vs stale bonding curve data
  const highWaterMcap = Math.max(Number(position.high_water_mcap || 0), Number(mcap));
  const highWaterPrice = Math.max(Number(position.high_water_price || 0), Number(price || 0));
  let pnlPercent = (Number(mcap) / Number(position.entry_mcap) - 1) * 100;
  const markPnlPercent = pnlPercent;
  let pnlSol = Number(position.size_sol) * pnlPercent / 100;
  // E2b (2026-08-05): jupiterPnl (Jupiter wallet-portfolio valuation) is REPORTING-ONLY.
  // It must NOT override pnlPercent — exit decisions (TP/SL/trailing) run on the real
  // price path (quote/mark) so dry and live legs are comparable. The live fill still
  // overrides stored pnl at close. Kept only as a cross-check for logs/payload.
  const jupiterPnlPercent = (jupiterPnl && Number.isFinite(Number(jupiterPnl.totalPnlPercentageNative)))
    ? Number(jupiterPnl.totalPnlPercentageNative)
    : null;
  // Dynamic ATR-based stop loss: fetch chart context and compute ATR% to widen/narrow the static sl_percent.
  const stratForSl = strategyById(position.strategy_id);
  const useDynamicSl = (stratForSl?.use_dynamic_sl ?? numSetting('use_dynamic_sl', 1)) ? true : false;
  let effectiveSlPercent = Number(position.sl_percent);
  let atrPercent = null;
  if (useDynamicSl) {
    try {
      const chart = await fetchJupiterChartContext(position.mint);
      const windows = Array.isArray(chart?.windows) ? chart.windows : [];
      atrPercent = computeAtrPercent(windows, numSetting('atr_period', 14));
      effectiveSlPercent = dynamicStopLossPercent({
        baseSlPercent: Number(position.sl_percent),
        atrPercent,
        multiplier: Number(stratForSl?.atr_sl_multiplier ?? numSetting('atr_sl_multiplier', 1.5)),
        floorPercent: Number(stratForSl?.atr_sl_floor_percent ?? numSetting('atr_sl_floor_percent', -50)),
        ceilingPercent: Number(stratForSl?.atr_sl_ceiling_percent ?? numSetting('atr_sl_ceiling_percent', -8)),
        minAtrPercent: Number(stratForSl?.atr_sl_min_atr_percent ?? numSetting('atr_sl_min_atr_percent', 4)),
        maxAtrPercent: Number(stratForSl?.atr_sl_max_atr_percent ?? numSetting('atr_sl_max_atr_percent', 30)),
      });
    } catch (err) {
      console.log(`[atr] chart refresh failed for ${position.mint.slice(0, 8)}... ${err.message}`);
    }
  }
  const tpHit = pnlPercent >= Number(position.tp_percent);
  const slHit = pnlPercent <= effectiveSlPercent && pnlPercent < 0; // Lesson 3: don't SL if PnL positive
  const armThreshold = numSetting('trailing_arm_percent', Number(position.tp_percent));
  const armHit = pnlPercent >= armThreshold;
  const trailingArmed = position.trailing_armed || (position.trailing_enabled && armHit);
  const trailDrop = highWaterMcap > 0 ? (Number(mcap) / highWaterMcap - 1) * 100 : 0;
  // EXIT-FIX 2026-07-25 (backtest 933 trades 07-22..25: base +1,685% -> +8,766% ideal / +6,314% gap).
  // (1) TIGHT TRAIL: once peak pnl >= trailing_tight_from_percent (40), trail tightens from
  //     trailing_percent (10) to trailing_tight_percent (5). Rescues armed winners that round-trip
  //     to SL (97 armed+SL trades = -6,497% pnl in window).
  // (2) FLOOR: once armed, trailing may not exit below trailing_floor_percent (+8). Kills the
  //     +1.7% "gap-dump between 3s ticks" exits (dump lands below arm before next check).
  // Partial@arm REJECTED by backtest (-867%): caps the runners that carry total profit.
  const peakPnl = Number(position.entry_mcap) > 0
    ? (highWaterMcap / Number(position.entry_mcap) - 1) * 100
    : pnlPercent;
  const tightFrom = numSetting('trailing_tight_from_percent', 40);
  const effectiveTrailPct = peakPnl >= tightFrom
    ? numSetting('trailing_tight_percent', 5)
    : Math.abs(Number(position.trailing_percent));
  const trailingFloor = numSetting('trailing_floor_percent', 8);
  const trailingHit = trailingArmed && position.trailing_enabled && pnlPercent >= trailingFloor && trailDrop <= -effectiveTrailPct;
  let exitReason = null;
  let closed = false;

  // Max hold time check — tiered by entry mcap
  const strat = strategyById(position.strategy_id);
  const entryMcap = Number(position.entry_mcap) || 0;
  const isMicrocap = entryMcap > 0 && entryMcap < 15000;
  const isHighcap = entryMcap >= 60000;
  let effectiveMaxHold = strat?.max_hold_ms ?? 0;
  // === AUDIT MODE: tiered max_hold disabled for 3-day data collection (2026-07-05) ===
  // if (isMicrocap) {
  //   effectiveMaxHold = 600000; // 10 min for microcap <15K
  //   console.log(`[position] microcap <15K — max_hold reduced to 10min`);
  // } else if (isHighcap) {
  //   effectiveMaxHold = 900000; // 15 min for highcap >60K
  //   console.log(`[position] highcap >60K — max_hold reduced to 15min`);
  // }
  if (effectiveMaxHold > 0 && (now() - position.opened_at_ms) >= effectiveMaxHold) {
    exitReason = 'MAX_HOLD';
  }

  // Sideways timeout: if open too long with negligible PnL, exit to free up capital.
  if (!exitReason) {
    const sidewaysMinutes = Number(strat?.sideways_timeout_minutes ?? numSetting('sideways_timeout_minutes', 0));
    if (sidewaysMinutes > 0) {
      const ageSeconds = (now() - position.opened_at_ms) / 1000;
      if (ageSeconds > sidewaysMinutes * 60 && Math.abs(pnlPercent) < 2) {
        exitReason = 'SIDEWAYS_TIMEOUT';
      }
    }
  }

  // Partial TP check
  if (!exitReason && strat?.partial_tp && !position.partial_tp_done && pnlPercent >= strat.partial_tp_at_percent) {
    db.prepare('UPDATE dry_run_positions SET partial_tp_done = 1 WHERE id = ?').run(position.id);
    console.log(`[position] ${position.id} partial TP at ${pnlPercent.toFixed(1)}% (${strat.partial_tp_sell_percent}% sell)`);
    if (position.execution_mode === 'live' && position.token_amount_raw) {
      try {
        const sellAmount = Math.floor(Number(position.token_amount_raw) * (strat.partial_tp_sell_percent / 100));
        if (sellAmount > 0) {
          const sell = await executeLiveSell({ ...position, token_amount_raw: String(sellAmount) }, 'PARTIAL_TP');
          const remaining = Number(position.token_amount_raw) - sellAmount;
          db.prepare('UPDATE dry_run_positions SET token_amount_raw = ? WHERE id = ?').run(String(remaining), position.id);
          db.prepare(`
            INSERT INTO dry_run_trades (position_id, mint, side, at_ms, price, mcap, size_sol, token_amount_est, reason, payload_json)
            VALUES (?, ?, 'sell', ?, ?, ?, ?, ?, 'PARTIAL_TP', ?)
          `).run(position.id, position.mint, now(), price, mcap,
            position.size_sol * (strat.partial_tp_sell_percent / 100), sellAmount,
            json({ pnlPercent, sell, partialSellPercent: strat.partial_tp_sell_percent, remaining }));
          console.log(`[position] ${position.id} partial TP sold ${sellAmount} tokens, ${remaining} remaining`);
        }
      } catch (err) {
        console.log(`[position] ${position.id} partial sell failed: ${err.message}`);
      }
    }
  }

  // Standard exit checks
  if (!exitReason) {
    if (slHit) exitReason = 'SL';
    else if (tpHit && !position.trailing_enabled) exitReason = 'TP';
    else if (trailingHit) exitReason = 'TRAILING_TP';
  }

  // FLUSH override (2026-08-05): /flush force-closes regardless of TP/SL state.
  // Reuses the exact same close paths below (live = real sell + realized PnL,
  // dry = marked-to-market PnL), so the audit trail stays consistent.
  if (forceExit) exitReason = 'FLUSH';

  // Tick observability (2026-08-05): log the exact price source + exit decision each poll.
  // Needed because silent quote-null paths left the dry engine on a stale mark with zero
  // logs (KAKAO 08-05: -20% SL fired at -92.5%, undiagnosable from logs).
  if (autoExit) {
    const srcLabel = quotePrice != null ? 'quote' : (jupiterPrice > 0 ? 'mark' : 'fallback');
    console.log(`[position] #${position.id} ${position.symbol} ${position.execution_mode || 'dry_run'} tick price=${price} mcap=${mcap} pnl=${pnlPercent.toFixed(2)}% src=${srcLabel} sl=${effectiveSlPercent}% tp=${position.tp_percent}% exit=${exitReason || '-'} jpnl=${jupiterPnlPercent != null ? jupiterPnlPercent.toFixed(2) + '%' : '-'}`);
  }

  // Live exits will override these with realized SOL values
  let finalPnlPercent = pnlPercent;
  let finalPnlSol = pnlSol;

  db.prepare(`
    UPDATE dry_run_positions
    SET high_water_mcap = ?, high_water_price = ?, trailing_armed = ?
    WHERE id = ?
  `).run(highWaterMcap, highWaterPrice, trailingArmed ? 1 : 0, position.id);

  if (exitReason && autoExit && position.execution_mode === 'live') {
    if (sellInProgress.has(position.id)) return { ...position, exitReason: null };
    sellInProgress.add(position.id);
    let sell;
    try {
      sell = await executeLiveSell(position, exitReason);
    } finally {
      sellInProgress.delete(position.id);
    }
    // A (2026-08-06): when the sell was reconciled from a zero-balance account (the
    // wallet already exited on-chain but the close write was lost), label the row with
    // RECONCILED_ZERO_BALANCE instead of the trigger reason so it's auditable as a
    // recovery, not a normal SL/TP exit. PnL below uses the recovered realized fill.
    if (sell?.reconciled) exitReason = 'RECONCILED_ZERO_BALANCE';
    const receivedLamports = Number(sell.outputAmount || 0);
    const receivedSol = receivedLamports > 0 ? receivedLamports / 1_000_000_000 : null;
    if (receivedSol != null) {
      finalPnlSol = receivedSol - Number(position.size_sol);
      finalPnlPercent = (receivedSol / Number(position.size_sol) - 1) * 100;
    }
    // E1 (2026-08-05): record the realized-equivalent exit price/mcap so the row is
    // internally consistent with the realized PnL (previously the stored mark could be
    // far off: CATEAI exit_mcap +30.9% with realized -95.6%). Mark stays in trade payload.
    if (receivedSol != null && Number.isFinite(Number(finalPnlPercent))) {
      if (Number(position.entry_mcap) > 0) mcap = Number(position.entry_mcap) * (1 + Number(finalPnlPercent) / 100);
      if (Number(position.entry_price) > 0) price = Number(position.entry_price) * (1 + Number(finalPnlPercent) / 100);
    }
    db.prepare(`
      UPDATE dry_run_positions
      SET status = 'closed', closed_at_ms = ?, exit_price = ?, exit_mcap = ?, exit_reason = ?,
          pnl_percent = ?, pnl_sol = ?, exit_signature = ?
      WHERE id = ?
    `).run(now(), price, mcap, exitReason, finalPnlPercent, finalPnlSol, sell.signature, position.id);
    db.prepare(`
      INSERT INTO dry_run_trades (position_id, mint, side, at_ms, price, mcap, size_sol, token_amount_est, reason, payload_json)
      VALUES (?, ?, 'sell', ?, ?, ?, ?, ?, ?, ?)
    `).run(position.id, position.mint, now(), price, mcap, position.size_sol, position.token_amount_est, exitReason, json({ pnlPercent: finalPnlPercent, pnlSol: finalPnlSol, receivedSol: receivedSol ?? null, sell, effectiveSlPercent, atrPercent, baseSlPercent: Number(position.sl_percent) }));
    closed = true;
  } else if (exitReason && autoExit) {
    // Apply exit slippage for dry_run PnL
    const exitMcap = slippageAdjustedMcap(quotePrice ? Number(position.entry_mcap) * (quotePrice / Number(position.entry_price)) : mcap, 'exit');
    const dryExitPrice = quotePrice || price;
    const dryExitMcap = quotePrice ? Number(position.entry_mcap) * (quotePrice / Number(position.entry_price)) : mcap;
    const dryPnlPercent = (Number(exitMcap) / Number(position.entry_mcap) - 1) * 100;
    const dryPnlSol = Number(position.size_sol) * dryPnlPercent / 100;
    db.prepare(`
      UPDATE dry_run_positions
      SET status = 'closed', closed_at_ms = ?, exit_price = ?, exit_mcap = ?, exit_reason = ?, pnl_percent = ?, pnl_sol = ?
      WHERE id = ?
    `).run(now(), dryExitPrice, dryExitMcap, exitReason, dryPnlPercent, dryPnlSol, position.id);
    db.prepare(`
      INSERT INTO dry_run_trades (position_id, mint, side, at_ms, price, mcap, size_sol, token_amount_est, reason, payload_json)
      VALUES (?, ?, 'sell', ?, ?, ?, ?, ?, ?, ?)
    `).run(position.id, position.mint, now(), dryExitPrice, dryExitMcap, position.size_sol, position.token_amount_est, exitReason, json({ pnlPercent: dryPnlPercent, pnlSol: dryPnlSol, effectiveSlPercent, atrPercent, baseSlPercent: Number(position.sl_percent), slippage_pct: numSetting('dry_run_slippage_percent', 0) }));
    finalPnlPercent = dryPnlPercent;
    finalPnlSol = dryPnlSol;
    closed = true;
  }
  if (closed) {
    console.log(`[position] #${position.id} ${position.symbol} ${position.execution_mode || 'dry_run'} EXIT reason=${exitReason} pnl=${finalPnlPercent?.toFixed(2)}% pnlSol=${finalPnlSol?.toFixed(4)} price=${price} mcap=${mcap}`);
  }
  return {
    ...position,
    status: closed ? 'closed' : position.status,
    closed_at_ms: closed ? now() : position.closed_at_ms,
    asset,
    price,
    mcap,
    highWaterMcap,
    high_water_mcap: highWaterMcap,
    high_water_price: highWaterPrice,
    pnlPercent: finalPnlPercent,
    pnl_percent: finalPnlPercent,
    pnlSol: finalPnlSol,
    pnl_sol: finalPnlSol,
    exitReason: closed ? exitReason : null,
    exit_reason: closed ? exitReason : position.exit_reason,
    exit_mcap: closed ? mcap : position.exit_mcap,
    exit_price: closed ? price : position.exit_price,
  };
}

// Deferred dry-entry backfill (2026-08-06, pure-dry): when a pure-dry entry is skipped
// because the candidate-time quote anchor failed (fail-closed), queue it here and open it
// at the first per-tick quote that succeeds — honest quote anchor, no stale marks.
const deferredDryEntries = new Map(); // mint -> { candidateId, candidate, decision, symbol, batchId, skippedAtMs }
const DEFERRED_DRY_TTL_MS = 10 * 60 * 1000;
const DEFERRED_DRY_MAX = 25;

export function queueDeferredDryEntry(mint, entry) {
  if (deferredDryEntries.size >= DEFERRED_DRY_MAX) {
    const oldest = deferredDryEntries.keys().next().value;
    deferredDryEntries.delete(oldest);
  }
  deferredDryEntries.set(mint, { ...entry, skippedAtMs: now() });
}

async function scanDeferredDryEntries() {
  if (deferredDryEntries.size === 0) return;
  for (const [mint, pending] of [...deferredDryEntries]) {
    if (now() - pending.skippedAtMs > DEFERRED_DRY_TTL_MS) {
      console.log(`[dry] deferred entry expired for ${pending.symbol} ${mint.slice(0, 8)}... (quote never recovered within ${DEFERRED_DRY_TTL_MS / 60000} min)`);
      deferredDryEntries.delete(mint);
      continue;
    }
    // Dedup: stop retrying if a dry position now exists on the mint.
    const existing = db.prepare(`SELECT id FROM dry_run_positions WHERE mint = ? AND status = 'open' AND (execution_mode IS NULL OR execution_mode = 'dry_run') LIMIT 1`).get(mint);
    if (existing) { deferredDryEntries.delete(mint); continue; }
    const qp = await fetchTokenSpotViaQuote(mint).catch(() => null);
    if (!(Number.isFinite(qp) && qp > 0)) continue;
    const asset = await fetchJupiterAsset(mint, { useCache: false });
    const supply = Number(asset?.totalSupply || asset?.circSupply || 0);
    const quoteMcap = supply > 0 ? qp * supply : null;
    if (!quoteMcap || !Number.isFinite(quoteMcap) || quoteMcap <= 0) continue;
    const corrected = {
      ...pending.candidate,
      metrics: { ...pending.candidate.metrics, priceUsd: qp, marketCapUsd: quoteMcap },
      executionRefresh: { ...(pending.candidate.executionRefresh || {}), entryAnchor: 'quote', deferredTwin: true },
    };
    const result = await createDryRunPosition(pending.candidateId, corrected, pending.decision, `deferred_dry_${pending.batchId || mint.slice(0, 8)}`).catch((err) => {
      console.log(`[dry] deferred entry failed for ${pending.symbol}: ${err.message}`);
      return null;
    });
    if (result) {
      if (result.isNew) {
        console.log(`[dry] deferred entry #${result.id} opened for ${pending.symbol} ${mint.slice(0, 8)}... — quote anchor recovered, entry mcap ${quoteMcap.toFixed(0)}`);
      }
      deferredDryEntries.delete(mint); // created or dedup-blocked — stop retrying
    }
  }
}

// Deferred twin (2026-08-06, option 3): when the candidate-time quote anchor fails
// (fail-closed: dry twin skipped rather than mark-anchored), backfill the twin at the
// first per-tick quote that succeeds — the twin enters at the recovered executable quote
// (honest "signal entry as soon as pricable"), no live-buy latency cost. Dedup is handled
// by createDryRunPosition (returns isNew=false if a dry position already exists).
async function maybeCreateDeferredTwin(position, prefetched) {
  try {
    const qp = prefetched?.qp;
    const asset = prefetched?.asset;
    if (!(Number.isFinite(qp) && qp > 0)) return;
    const snap = typeof position.snapshot_json === 'string' ? JSON.parse(position.snapshot_json || '{}') : (position.snapshot_json || {});
    const candidate = snap.candidate;
    if (!candidate || !(candidate.signals?.route || '').includes('pumpportal_graduated')) return;
    const supply = Number(asset?.totalSupply || asset?.circSupply || 0);
    const quoteMcap = supply > 0 ? qp * supply : null;
    if (!quoteMcap || !Number.isFinite(quoteMcap) || quoteMcap <= 0) return;
    const corrected = {
      ...candidate,
      metrics: { ...candidate.metrics, priceUsd: qp, marketCapUsd: quoteMcap },
      executionRefresh: { ...(candidate.executionRefresh || {}), entryAnchor: 'quote', deferredTwin: true },
    };
    const twin = await createDryRunPosition(position.candidate_id, corrected, snap.decision || {}, `shadow_twin_deferred_${position.id}`);
    if (twin.isNew) {
      console.log(`[shadow] deferred twin #${twin.id} opened for ${position.symbol} (live #${position.id}) — quote anchor recovered, entry mcap ${quoteMcap.toFixed(0)}`);
    }
  } catch (err) {
    console.log(`[shadow] deferred twin failed for ${position.symbol}: ${err.message}`);
  }
}

export async function monitorPositions() {
  const positions = openPositions();
  let walletPnlData = {};
  const pubkey = liveWalletPubkey();
  if (pubkey && positions.some(p => p.execution_mode === 'live')) {
    walletPnlData = await fetchJupiterWalletPnl(pubkey);
  }
  // Poll-parity (2026-08-05): fetch price ONCE per mint per tick and share it across
  // twin positions so dry/live accumulate identical high-water/trailing state. The
  // 0.3-0.6s offset between per-position fetches made twin legs sample the same token
  // at different instants (JELON: live hw 56.4K vs dry hw 54.5K → opposite trail calls).
  const tickCache = new Map(); // mint -> { asset, qp }
  for (const position of positions) {
    const jupiterPnl = position.execution_mode === 'live'
      ? (walletPnlData[position.mint]?.pnl || null)
      : null;
    let prefetched = tickCache.get(position.mint);
    if (!prefetched) {
      const useQuote = numSetting('exit_quote_enabled', 1);
      const [asset, qp] = await Promise.all([
        fetchJupiterAsset(position.mint, { useCache: false, ttlMs: 3000 }),
        useQuote ? fetchTokenSpotViaQuote(position.mint) : Promise.resolve(null),
      ]);
      prefetched = { asset, qp };
      tickCache.set(position.mint, prefetched);
    }
    const result = await refreshPosition(position, { autoExit: true, jupiterPnl, prefetched }).catch((err) => {
      console.log(`[position] ${position.id} ${err.message}`);
      return null;
    });
    if (result?.exitReason) await sendPositionExit(result);
    // Deferred twin backfill (2026-08-06): live positions that opened without a twin
    // (candidate-time quote failed -> fail-closed skip) get their twin here once a
    // per-tick quote succeeds. Only for still-open live positions; dedup is in
    // createDryRunPosition. Skipped when the position closed this tick (result.exitReason).
    else if (position.execution_mode === 'live') {
      await maybeCreateDeferredTwin(position, prefetched);
    }
  }
  // Pure-dry deferred backfill: scan queued entries after the position loop.
  await scanDeferredDryEntries();
}
