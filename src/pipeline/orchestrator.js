import { now, pruneSeen } from '../utils.js';
import { numSetting, boolSetting } from '../db/settings.js';
import { db } from '../db/connection.js';
import { upsertCandidate, updateCandidateStatus, recentEligibleCandidates, candidateById } from '../db/candidates.js';
import { storeDecision, storeBatchDecision, logDecisionEvent, checkDecisionCache } from '../db/decisions.js';
import { buildCandidate, filterCandidate, signalLabel } from './candidateBuilder.js';
import { preScoreCandidate } from './preScorer.js';
import { momentumFilter } from './momentumFilter.js';
import { decideCandidateBatch } from './llm.js';
import { activeStrategy } from '../db/settings.js';
import { createDryRunPosition, createLivePosition, canOpenMorePositions, openPositionCount, tradingMode, effectiveModeFor } from '../db/positions.js';
import { sendBatchReveal, sendTelegram, sendPositionOpen, sendTradeIntent } from '../telegram/send.js';
import { candidateSummary } from '../telegram/format.js';
import { createTradeIntent } from '../db/intents.js';
import { refreshCandidateForExecution, queueDeferredDryEntry } from '../execution/positions.js';
import { executeLiveBuy } from '../execution/router.js';
import { graduated } from '../signals/graduated.js';
import { setDegenHandler } from '../signals/trending.js';
import { setCandidateHandler } from '../signals/feeClaim.js';
import { short } from '../format.js';
import { escapeHtml } from '../format.js';

export const seenSignalCandidates = new Map();

setDegenHandler(maybeProcessDegenCandidate);
setCandidateHandler(processCandidateFromSignals);

export async function processCandidateFromSignals(signals) {
  const globalMode = tradingMode();
  // Per-mode slot gate (2026-08-05): check the track this candidate would execute in
  // (live for pumpportal_graduated, dry_run otherwise).
  const mode = effectiveModeFor(signals, globalMode);
  // Shadow mirror (2026-08-05, Option A): in global live mode ONLY pumpportal_graduated
  // opens positions (live + dry twin). Every other source is screened but opens nothing,
  // keeping the dry track a pure mirror of live.
  if (globalMode === 'live' && mode !== 'live') {
    console.log(`[shadow] non-pumpportal source skipped in live mode (dry track reserved for twins): ${signals.route || signals.signals?.route || '?'} ${signals.mint.slice(0, 8)}...`);
    return;
  }
  // Skip if max positions reached — don't waste enrichment/LLM calls
  if (!canOpenMorePositions(mode)) {
    const max = numSetting('max_open_positions', 3);
    console.log(`[agent] max ${mode} positions reached (${openPositionCount(mode)}/${max}), skipping ${signals.mint.slice(0, 8)}...`);
    return;
  }

  // Load strategy early — needed for duplicate checks below
  const strat = activeStrategy();

  // DUPLICATE CHECKS — all run BEFORE enrichment to save API calls
  try {
    const recentMs = Date.now() - 86400000; // 24 hours

    // 1. Open position exists (same track only — live/dry twins allowed)
    const openPos = db.prepare(
      'SELECT id FROM dry_run_positions WHERE mint = ? AND status = ? AND COALESCE(execution_mode, ?) = ? LIMIT 1'
    ).get(signals.mint, 'open', 'dry_run', mode);
    if (openPos) {
      console.log(`[agent] skipping ${signals.mint.slice(0, 8)}... — already has open ${mode} position`);
      return;
    }

    // 2. Recently closed position (<72h, same track only) — BUG #3 FIX: extend to 72h to prevent re-buy after SL
    const closedPos = db.prepare(
      'SELECT id, exit_reason, closed_at_ms FROM dry_run_positions WHERE mint = ? AND status = ? AND COALESCE(execution_mode, ?) = ? AND closed_at_ms > ? ORDER BY closed_at_ms DESC LIMIT 1'
    ).get(signals.mint, 'closed', 'dry_run', mode, Date.now() - 72 * 60 * 60 * 1000);
    if (closedPos) {
      const hoursAgo = ((Date.now() - closedPos.closed_at_ms) / 3600000).toFixed(1);
      console.log(`[agent] skipping ${signals.mint.slice(0, 8)}... — recently closed (${hoursAgo}h ago, exit: ${closedPos.exit_reason})`);
      return;
    }

    // 3. LLM decision cache — only relevant when strategy uses LLM
    if (strat.use_llm) {
      const recentDecision = db.prepare(`
        SELECT id FROM llm_decisions
        WHERE mint = ? AND created_at_ms > ?
        LIMIT 1
      `).get(signals.mint, recentMs);
      if (recentDecision) {
        console.log(`[agent] skipping ${signals.mint.slice(0, 8)}... — LLM decision exists (<24h)`);
        return;
      }
    }
  } catch (err) {
    // DB check failed — proceed anyway
  }

  // Bidirectional dedup — skip ANY route if this mint already has a recent candidate entry within 10 minutes
  // (prevents same token processed via pumpportal_graduated, pumpfun_pregrad, fee_trending, dual_source, etc. simultaneously)
  try {
    const recentCandidate = db.prepare(`
      SELECT id FROM candidates
      WHERE mint = ? AND created_at_ms > ?
      LIMIT 1
    `).get(signals.mint, Date.now() - 600000); // 10 minutes
    if (recentCandidate) {
      console.log(`[agent] skipping ${signals.mint.slice(0, 8)}... — recent candidate (<10min) for any route`);
      return;
    }
  } catch (err) {
    // DB check failed — proceed anyway
  }

  // FIX #1: Check decision cache BEFORE expensive buildCandidate() API calls
  // Lightweight check with just mint — full enrichment happens only if cache miss
  const cachedDecision = checkDecisionCache(signals.mint);
  if (cachedDecision) {
    const ageMin = ((now() - cachedDecision.cachedAt) / 60000).toFixed(1);
    console.log(`[cache-hit] ${signals.mint.slice(0, 8)}... — verdict ${cachedDecision.verdict} (cached ${ageMin}m ago, reason: ${cachedDecision.reason?.slice(0, 60) || 'n/a'})`);
    return;
  }

  const candidate = await buildCandidate(signals);
  const signature = signals.signature || null;
  const candidateId = upsertCandidate(candidate, signature);

  // Symbol-based dedup — skip copycat tokens with same symbol traded in last 24h
  try {
    const symbol = candidate.token?.symbol;
    if (symbol) {
      const symbolPos = db.prepare(
        'SELECT id FROM dry_run_positions WHERE symbol = ? AND closed_at_ms > ? LIMIT 1'
      ).get(symbol, Date.now() - 86400000);
      if (symbolPos) {
        console.log(`[agent] skipping ${symbol} (${candidate.token.mint.slice(0, 8)}) — same symbol traded <24h ago`);
        return;
      }
    }
  } catch (err) {
    // DB check failed — proceed anyway
  }

  if (!candidate.filters.passed) {
    console.log(`[candidate] filtered ${candidate.token.mint.slice(0, 8)}... ${candidate.filters.failures.join('; ')}`);
    return;
  }

  // Pre-score: rule-based check before LLM (saves LLM credits)
  const preScore = preScoreCandidate(candidate);
  if (!preScore.passed) {
    console.log(`[prescore] filtered ${candidate.token.mint.slice(0, 8)}... score ${preScore.score}/${preScore.threshold} (${preScore.reasons.slice(0, 2).join('; ')})`);
    return;
  }
  console.log(`[prescore] passed ${candidate.token.mint.slice(0, 8)}... score ${preScore.score}/${preScore.threshold}`);
  
  // FIX #2: Re-check filters BEFORE LLM call (prevent wasted LLM calls on stale data)
  // This catches cases where market conditions changed between initial filter and LLM batch eval
  filterCandidate(candidate);
  if (!candidate.filters.passed) {
    console.log(`[pre-llm-guard] filtered ${candidate.token.mint.slice(0, 8)}... ${candidate.filters.failures.join('; ')}`);
    return;
  }

  // Momentum filter — ML-based prediction of runner vs sideways.
  // REPORTING-ONLY (2026-08-07): log the score, never gate. Gap F: the author's
  // 0.5 threshold is calibrated on HIS distribution; gating on it would silently
  // reject ~70% of candidates on an unvalidated black box. Score + reason are
  // recorded on the candidate (persisted via snapshot_json at entry) and validated
  // against outcomes before any gate is flipped on.
  const momentumThreshold = strat.momentum_threshold ?? 0.5;
  const momentumResult = await momentumFilter(candidate, momentumThreshold);
  candidate.filters.momentumScore = momentumResult.score;
  candidate.filters.momentumReason = momentumResult.reason || 'ok';
  candidate.filters.momentumLatencyMs = momentumResult.latency ?? null;
  console.log(`[momentum] ${candidate.token.mint.slice(0, 8)}... REPORT score=${momentumResult.score} threshold=${momentumThreshold} reason=${candidate.filters.momentumReason}${momentumResult.latency != null ? ` (${momentumResult.latency}ms)` : ''}`);

  let rows, batchDecision, batchId;

  if (!strat.use_llm) {
    const selfRow = candidateById(candidateId);
    rows = selfRow ? [selfRow] : [];
    batchId = null;
    batchDecision = {
      verdict: 'BUY',
      confidence: 100,
      selected_candidate_id: candidateId,
      selected_mint: candidate.token.mint,
      selected_row: selfRow,
      reason: `Strategy '${strat.id}' is rule-based (use_llm: false); filters passed.`,
      risks: [],
      suggested_tp_percent: strat.tp_percent ?? numSetting('default_tp_percent', 50),
      suggested_sl_percent: strat.sl_percent ?? numSetting('default_sl_percent', -25),
      raw: null,
    };
  } else {
    rows = recentEligibleCandidates(numSetting('llm_candidate_pick_count', 10));
    batchDecision = await decideCandidateBatch(rows, candidateId);
    batchId = storeBatchDecision(candidateId, rows, batchDecision);
  }
  const selectedRow = batchDecision.selected_row;
  const selectedThisCandidate = selectedRow?.id === candidateId;
  const currentDecision = selectedThisCandidate
    ? batchDecision
    : {
        ...batchDecision,
        verdict: 'WATCH',
        reason: selectedRow
          ? `Batch #${batchId} screened ${rows.length}; selected ${short(selectedRow.candidate.token.mint)} instead. ${batchDecision.reason || ''}`.trim()
          : `Batch #${batchId} screened ${rows.length}; no buy selected. ${batchDecision.reason || ''}`.trim(),
      };
  const currentDecisionId = storeDecision(candidateId, candidate, currentDecision);
  currentDecision.id = currentDecisionId;
  updateCandidateStatus(candidateId, currentDecision.verdict.toLowerCase());

  if (selectedRow && !selectedThisCandidate) {
    const selectedDecisionId = storeDecision(selectedRow.id, selectedRow.candidate, batchDecision);
    batchDecision.id = selectedDecisionId;
    updateCandidateStatus(selectedRow.id, batchDecision.verdict.toLowerCase());
  } else if (selectedThisCandidate) {
    batchDecision.id = currentDecisionId;
  }

  if (batchId) await sendBatchReveal(batchId, rows, batchDecision, candidateId);

  // #6: Buy the LLM's selected candidate regardless of which candidate triggered the batch
  if (selectedRow && boolSetting('agent_enabled', true) && batchDecision.verdict === 'BUY' && batchDecision.confidence >= numSetting('llm_min_confidence')) {
    const mode = effectiveModeFor(selectedRow.candidate, tradingMode());
    if (!canOpenMorePositions(mode)) {
      const max = numSetting('max_open_positions', 3);
      console.log(`[agent] max ${mode} positions reached (${openPositionCount(mode)}/${max}), skipping buy ${selectedRow.candidate.token.mint}`);
      logDecisionEvent({
        batchId,
        triggerCandidateId: candidateId,
        selectedRow,
        rows,
        decision: batchDecision,
        action: 'entry_skipped_max_positions',
        guardrails: { maxOpenPositions: max, openPositions: openPositionCount(mode) },
      });
      return;
    }
    try {
      await handleApprovedBuy(selectedRow, batchDecision, batchId, rows, candidateId);
    } catch (err) {
      console.error(`[orchestrator] handleApprovedBuy failed for ${selectedRow.candidate.token.mint}: ${err.message}`);
      logDecisionEvent({
        batchId,
        triggerCandidateId: candidateId,
        selectedRow,
        rows,
        decision: batchDecision,
        action: 'handle_buy_error',
        guardrails: { error: err.message, stack: err.stack?.slice(0, 500) },
      });
      await sendTelegram([
        '🛑 <b>Buy execution failed</b>',
        '',
        candidateSummary(selectedRow.candidate, batchDecision),
        '',
        `Error: ${escapeHtml(err.message)}`,
      ].join('\n'));
    }
  } else {
    logDecisionEvent({
      batchId,
      triggerCandidateId: candidateId,
      selectedRow,
      rows,
      decision: batchDecision,
      action: selectedRow ? 'entry_not_approved' : 'no_candidate_selected',
      guardrails: {
        agentEnabled: boolSetting('agent_enabled', true),
        confidenceThreshold: numSetting('llm_min_confidence'),
        openPositions: openPositionCount(),
        maxOpenPositions: numSetting('max_open_positions', 3),
      },
    });
  }
}

export async function handleApprovedBuy(selectedRow, decision, batchId, rows = [], triggerCandidateId = null) {
  // Per-source execution gate (2026-08-05): in global live mode, only pumpportal_graduated
  // candidates execute live; every other source falls back to dry_run. confirm is untouched.
  const mode = effectiveModeFor(selectedRow.candidate, tradingMode());
  // Shadow mirror (2026-08-05, Option A): in live mode ONLY pumpportal_graduated opens
  // ANY position. Stale non-pumpportal candidates (built before this gate) are screened
  // but not traded — the dry track stays a pure mirror of live.
  if (tradingMode() === 'live' && mode !== 'live') {
    console.log(`[shadow] buy skipped — non-pumpportal source in live mode (${selectedRow.candidate.token.symbol} ${selectedRow.candidate.token.mint.slice(0, 8)})`);
    return;
  }
  // Fire-and-forget refresh — start now, await later. Wrapped so a refresh failure
  // doesn't kill the trade — we just fall back to the unrefreshed row.
  const refreshPromise = refreshCandidateForExecution(selectedRow).catch(err => {
    console.error('[handleApprovedBuy] refresh failed, using stale row:', err.message);
    return { ...selectedRow, refreshError: err.message, candidate: { ...selectedRow.candidate, filters: selectedRow.candidate.filters || {} } };
  });
  const freshSelectedRow = await refreshPromise;
  const executionRows = rows.map(row => row.id === freshSelectedRow.id ? freshSelectedRow : row);
  if (!freshSelectedRow.candidate.filters?.passed) {
    updateCandidateStatus(freshSelectedRow.id, 'stale_rejected');
    logDecisionEvent({
      batchId,
      triggerCandidateId,
      selectedRow: freshSelectedRow,
      rows: executionRows,
      decision,
      mode,
      action: 'entry_rejected_fresh_filters',
      guardrails: {
        failures: freshSelectedRow.candidate.filters?.failures || [],
        refreshedAtMs: freshSelectedRow.candidate.executionRefresh?.refreshedAtMs,
      },
    });
    await sendTelegram([
      '🛑 <b>Execution rejected on fresh check</b>',
      '',
      candidateSummary(freshSelectedRow.candidate, decision),
      '',
      `Failures: ${escapeHtml((freshSelectedRow.candidate.filters?.failures || []).join('; ') || 'fresh execution guard failed')}`,
    ].join('\n'));
    return;
  }

  if (mode === 'dry_run') {
    // FIX #3: Wrap position creation in try-catch to capture execution failures
    let positionId, isNew, pastWinPnlSol, pastWinClosedAtMs;
    // No-stale-mark guard (2026-08-06): fail-closed for fresh-grad dry entries when the
    // executable-quote anchor is unavailable after retries (entryAnchor=mark) — mirrors
    // live's FAILED_ENTRY semantics. A mark-anchored fresh-grad dry entry is stale-mark
    // fiction (Bulls +107% phantom, BULLMOJI +284%, PEW +46%). Only fires on
    // pumpportal_graduated (entryAnchor is set only there); non-fresh routes keep the mark.
    if (freshSelectedRow.candidate.executionRefresh?.entryAnchor === 'mark') {
      console.log(`[dry] entry DEFERRED for ${freshSelectedRow.candidate.token.symbol} ${freshSelectedRow.candidate.token.mint.slice(0, 8)}... — quote anchor unavailable after retries (stale-mark avoidance); will backfill on first successful quote`);
      queueDeferredDryEntry(freshSelectedRow.candidate.token.mint, {
        candidateId: freshSelectedRow.id,
        candidate: freshSelectedRow.candidate,
        decision,
        symbol: freshSelectedRow.candidate.token.symbol,
        batchId,
      });
      logDecisionEvent({
        batchId,
        triggerCandidateId,
        selectedRow: freshSelectedRow,
        rows: executionRows,
        decision,
        mode,
        action: 'dry_entry_deferred_no_quote',
        guardrails: { entryAnchor: 'mark', retries: 3 },
        execution: { reason: 'quote anchor unavailable after retries (stale-mark avoidance); will backfill on first successful quote' },
      });
      return;
    }
    try {
      const result = await createDryRunPosition(freshSelectedRow.id, freshSelectedRow.candidate, decision, `llm_batch_${batchId}`);
      positionId = result.id;
      isNew = result.isNew;
      pastWinPnlSol = result.pastWinPnlSol;
      pastWinClosedAtMs = result.pastWinClosedAtMs;
    } catch (err) {
      console.error(`[orchestrator] createDryRunPosition failed for ${freshSelectedRow.candidate.token.mint}: ${err.message}`);
      logDecisionEvent({
        batchId,
        triggerCandidateId,
        selectedRow: freshSelectedRow,
        rows: executionRows,
        decision,
        mode,
        action: 'dry_run_position_create_failed',
        guardrails: {
          maxOpenPositions: numSetting('max_open_positions', 3),
          openPositions: openPositionCount(mode),
        },
        execution: { 
          error: err.message,
          stack: err.stack?.slice(0, 500),
        },
      });
      await sendTelegram([
        '🛑 <b>Position creation failed</b>',
        '',
        candidateSummary(freshSelectedRow.candidate, decision),
        '',
        `Error: ${escapeHtml(err.message)}`,
      ].join('\n'));
      return;
    }
    
    // FIX #4: Enhanced past-win guard logging with context
    const guardrails = {
      maxOpenPositions: numSetting('max_open_positions', 3),
      openPositions: openPositionCount(mode),
      pastWinPnlSol: pastWinPnlSol ?? null,
      pastWinClosedAtMs: pastWinClosedAtMs ?? null,
    };
    
    if (!isNew && pastWinClosedAtMs) {
      // Fetch past position details for audit
      try {
        const pastPos = db.prepare(`
          SELECT exit_reason, opened_at_ms, closed_at_ms, entry_mcap, pnl_percent 
          FROM dry_run_positions 
          WHERE mint = ? AND closed_at_ms = ?
          LIMIT 1
        `).get(freshSelectedRow.candidate.token.mint, pastWinClosedAtMs);
        
        if (pastPos) {
          const holdDurationMin = ((pastPos.closed_at_ms - pastPos.opened_at_ms) / 60000).toFixed(1);
          const currentMcap = freshSelectedRow.candidate.metrics?.marketCapUsd || 0;
          guardrails.pastWinExitReason = pastPos.exit_reason;
          guardrails.pastWinHoldDurationMin = Number(holdDurationMin);
          guardrails.pastWinPnlPercent = pastPos.pnl_percent;
          guardrails.wouldHaveBeenProfit = currentMcap > (pastPos.entry_mcap || 0);
        }
      } catch (err) {
        // Past position lookup failed — proceed with basic guardrails
      }
    }
    
    logDecisionEvent({
      batchId,
      triggerCandidateId,
      selectedRow: freshSelectedRow,
      rows: executionRows,
      decision,
      mode,
      action: isNew ? 'dry_run_entry' : 'dry_run_blocked_past_win',
      guardrails,
      execution: { positionId, isNew },
    });
    if (isNew) {
      await sendPositionOpen(positionId);
    } else {
      const daysAgo = Math.max(0, Math.floor((now() - (pastWinClosedAtMs || now())) / 86400000));
      await sendTelegram(`⏸️ Re-entry blocked: ${escapeHtml(freshSelectedRow.candidate.token.symbol)} won +${pastWinPnlSol ?? '?'} SOL ${daysAgo}d ago — guard prevented new position`);
    }
    return;
  }

  if (mode === 'confirm') {
    const intentId = createTradeIntent(freshSelectedRow.id, freshSelectedRow.candidate, decision, mode, 'pending_confirmation');
    logDecisionEvent({
      batchId,
      triggerCandidateId,
      selectedRow: freshSelectedRow,
      rows: executionRows,
      decision,
      mode,
      action: 'confirm_intent_created',
      guardrails: { maxOpenPositions: numSetting('max_open_positions', 3), openPositions: openPositionCount('live') },
      execution: { intentId },
    });
    await sendTradeIntent(intentId, freshSelectedRow.candidate, decision);
    return;
  }

  // Shadow twin (2026-08-05): in live mode, pumpportal_graduated also opens a dry-run
  // twin so live and dry outcomes can be compared. Twin is created FIRST so it stands
  // even if the live swap fails (pure counterfactual). Twin failures never block live.
  // No-stale-mark guard (2026-08-06): the twin is SKIPPED when the executable-quote
  // anchor is unavailable (entryAnchor=mark after 3 retries) — a mark-anchored dry entry
  // is stale-mark fiction (Bulls +107% phantom, BULLMOJI +284%, PEW +46%). This mirrors
  // live's FAILED_ENTRY semantics: no honest price -> no dry entry. Live still proceeds
  // (the E1 fill override records its true entry).
  if (freshSelectedRow.candidate.executionRefresh?.entryAnchor === 'mark') {
    console.log(`[shadow] twin DEFERRED for ${freshSelectedRow.candidate.token.symbol} ${freshSelectedRow.candidate.token.mint.slice(0, 8)}... — quote anchor unavailable after retries (stale-mark avoidance); will backfill on first successful quote`);
  } else {
    try {
      const twin = await createDryRunPosition(freshSelectedRow.id, freshSelectedRow.candidate, decision, `shadow_twin_${batchId}`);
      if (twin.isNew) {
        console.log(`[shadow] dry twin #${twin.id} opened for ${freshSelectedRow.candidate.token.symbol} — live execution pending`);
      }
    } catch (twinErr) {
      console.error(`[shadow] dry twin failed for ${freshSelectedRow.candidate.token.symbol}: ${twinErr.message}`);
    }
  }

  try {
    await executeLiveBuy(freshSelectedRow, decision, batchId, executionRows, triggerCandidateId);
  } catch (err) {
    const intentId = createTradeIntent(freshSelectedRow.id, freshSelectedRow.candidate, decision, mode, 'execution_failed');
    logDecisionEvent({
      batchId,
      triggerCandidateId,
      selectedRow: freshSelectedRow,
      rows: executionRows,
      decision,
      mode,
      action: 'live_entry_failed',
      guardrails: { maxOpenPositions: numSetting('max_open_positions', 3), openPositions: openPositionCount('live') },
      execution: { intentId, error: err.message },
    });
    await sendTelegram([
      '🛑 <b>Live trade failed</b>',
      '',
      candidateSummary(freshSelectedRow.candidate, decision),
      '',
      `Intent #${intentId} stored.`,
      `Error: ${escapeHtml(err.message)}`,
    ].join('\n'));
  }
}

export async function maybeProcessDegenCandidate(mint, trendingToken) {
  if (!boolSetting('trending_allow_degen', false)) return;
  const graduatedCoin = graduated.get(mint);
  if (!graduatedCoin) return;
  pruneSeen(seenSignalCandidates, 10 * 60 * 1000);
  const bucket = Math.floor(now() / (5 * 60 * 1000));
  const key = `graduated_trending:${mint}:${bucket}`;
  if (seenSignalCandidates.has(key)) return;
  seenSignalCandidates.set(key, now());
  await processCandidateFromSignals({
    mint,
    graduatedCoin,
    trendingToken,
    route: 'graduated_trending',
  });
}
