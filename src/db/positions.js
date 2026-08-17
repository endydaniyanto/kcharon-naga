import { db } from './connection.js';
import { now, json } from '../utils.js';
import { numSetting, boolSetting, setting, activeStrategy, slippageAdjustedMcap } from './settings.js';

export function openPositions() {
  return db.prepare('SELECT * FROM dry_run_positions WHERE status = ? ORDER BY opened_at_ms DESC').all('open');
}

export function openPositionCount(executionMode = null) {
  // Per-mode slot counting (2026-08-05): pass 'live' or 'dry_run' to count only that
  // track's open positions; null counts all modes (legacy behavior).
  if (executionMode) {
    return db.prepare('SELECT COUNT(*) AS count FROM dry_run_positions WHERE status = ? AND COALESCE(execution_mode, ?) = ?').get('open', 'dry_run', executionMode).count;
  }
  return db.prepare('SELECT COUNT(*) AS count FROM dry_run_positions WHERE status = ?').get('open').count;
}

export function hasClosedPosition(mint) {
  const row = db.prepare(`
    SELECT 1 FROM dry_run_positions WHERE mint = ? AND status = 'closed' LIMIT 1
  `).get(mint);
  return !!row;
}

export function canOpenMorePositions(executionMode = null) {
  const strat = activeStrategy();
  const max = strat.max_open_positions ?? numSetting('max_open_positions', 3);
  if (max <= 0) return true;
  return openPositionCount(executionMode) < max;
}

export function tradingMode() {
  const mode = setting('trading_mode', 'dry_run');
  return ['dry_run', 'confirm', 'live'].includes(mode) ? mode : 'dry_run';
}

// Route-based execution gating (2026-08-05, user decision): the global mode applies to
// all sources, EXCEPT when the global mode is 'live' — then only pumpportal_graduated
// candidates execute live and everything else falls back to dry_run.
// confirm mode is intentionally untouched: approve = live for any source.
export function candidateRoute(candidate) {
  if (!candidate) return '';
  const sig = candidate.signals || candidate.signal || {};
  const route = Array.isArray(sig) ? (sig[0] && sig[0].route) : (sig.route || candidate.route);
  return String(route || '');
}

export function effectiveModeFor(candidate, globalMode) {
  if (globalMode !== 'live') return globalMode;
  return candidateRoute(candidate).includes('pumpportal_graduated') ? 'live' : 'dry_run';
}

export function allPositions(limit = 10) {
  return db.prepare('SELECT * FROM dry_run_positions ORDER BY id DESC LIMIT ?').all(limit);
}

export async function createDryRunPosition(candidateId, candidate, decision, reason = 'llm_buy') {
  const strat = activeStrategy();
  let sizeSol = strat.position_size_sol ?? numSetting('dry_run_buy_sol', 0.1);
  
  // OPTION C HYBRID: Risk-based position sizing
  // Calculate total risk severity from candidate.riskFlags
  const riskFlags = candidate.riskFlags || [];
  const totalRiskSeverity = riskFlags.reduce((sum, flag) => sum + (flag.severity || 0), 0);
  
  if (totalRiskSeverity >= 2) {
    // High risk (severity ≥2) → cut size to 50%
    const originalSize = sizeSol;
    sizeSol *= 0.5;
    console.log(`[position] risk-adjusted size: ${originalSize} → ${sizeSol} SOL (total risk severity: ${totalRiskSeverity}, flags: ${riskFlags.map(f => f.type).join(', ')})`);
  }
  
  const entryPrice = Number(candidate.metrics.priceUsd || 0) || null;
  let entryMcap = Number(candidate.metrics.marketCapUsd || candidate.metrics.graduatedMarketCapUsd || 0) || null;
  entryMcap = slippageAdjustedMcap(entryMcap, 'entry');
  const tp = Number(decision.suggested_tp_percent || strat.tp_percent || numSetting('default_tp_percent', 50));
  const sl = Number(decision.suggested_sl_percent || strat.sl_percent || numSetting('default_sl_percent', -25));
  const trailingEnabled = (strat.trailing_enabled ?? boolSetting('default_trailing_enabled', true)) ? 1 : 0;
  const trailingPercent = strat.trailing_percent ?? numSetting('default_trailing_percent', 20);

  return db.transaction(() => {
    const existing = db.prepare(`
      SELECT id FROM dry_run_positions WHERE mint = ? AND status = 'open' AND COALESCE(execution_mode, 'dry_run') = 'dry_run' LIMIT 1
    `).get(candidate.token.mint);
    if (existing) return { id: existing.id, isNew: false };

    // Dedup: block re-entry if this token has been closed within 24 hours (dry track only)
    const recentClosed = db.prepare(`
      SELECT id FROM dry_run_positions WHERE mint = ? AND status = 'closed' AND closed_at_ms > ? AND COALESCE(execution_mode, 'dry_run') = 'dry_run' LIMIT 1
    `).get(candidate.token.mint, now() - 86400000);
    if (recentClosed) {
      console.log(`[positions] blocked re-entry ${candidate.token.symbol} (${candidate.token.mint.slice(0, 8)}) — closed <24h ago`);
      return { id: recentClosed.id, isNew: false };
    }

    // Block re-entry if this mint had a winning trade in the last WIN_BLOCK_DAYS days (avoid round-trip losses)
    const WIN_BLOCK_DAYS = 7;
    const pastWin = db.prepare(`
      SELECT id, pnl_sol, closed_at_ms FROM dry_run_positions
      WHERE mint = ? AND status = 'closed' AND pnl_percent > 0
        AND closed_at_ms > ?
        AND COALESCE(execution_mode, 'dry_run') = 'dry_run'
      ORDER BY closed_at_ms DESC LIMIT 1
    `).get(candidate.token.mint, now() - WIN_BLOCK_DAYS * 86400000);
    if (pastWin) {
      console.log(`[positions] blocked re-entry ${candidate.token.symbol} (${candidate.token.mint.slice(0, 8)}) — past WIN exists`);
      return { id: pastWin.id, isNew: false, blockedBy: 'past_win', pastWinPnlSol: pastWin.pnl_sol, pastWinClosedAtMs: pastWin.closed_at_ms };
    }

    const result = db.prepare(`
      INSERT INTO dry_run_positions (
        candidate_id, mint, symbol, status, opened_at_ms, size_sol, entry_price, entry_mcap,
        token_amount_est, high_water_price, high_water_mcap, tp_percent, sl_percent,
        trailing_enabled, trailing_percent, trailing_armed, llm_decision_id, strategy_id, snapshot_json
      ) VALUES (?, ?, ?, 'open', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?)
    `).run(
      candidateId,
      candidate.token.mint,
      candidate.token.symbol,
      now(),
      sizeSol,
      entryPrice,
      entryMcap,
      null,
      entryPrice,
      entryMcap,
      tp,
      sl,
      trailingEnabled,
      trailingPercent,
      decision.id || null,
      strat.id,
      json({ candidate, decision, reason, strategy: strat.id }),
    );
    const positionId = Number(result.lastInsertRowid);
    db.prepare(`
      INSERT INTO dry_run_trades (position_id, mint, side, at_ms, price, mcap, size_sol, token_amount_est, reason, payload_json)
      VALUES (?, ?, 'buy', ?, ?, ?, ?, ?, ?, ?)
    `).run(positionId, candidate.token.mint, now(), entryPrice, entryMcap, sizeSol, null, reason, json({ candidateId, decision }));
    db.prepare(`
      INSERT INTO tp_sl_rules (position_id, tp_percent, sl_percent, trailing_enabled, trailing_percent, updated_at_ms)
      VALUES (?, ?, ?, ?, ?, ?)
    `).run(positionId, tp, sl, trailingEnabled, trailingPercent, now());
    return { id: positionId, isNew: true };
  })();
}

export function createLivePosition(candidateId, candidate, decision, swap, reason = 'live_buy', entryOverrides = null) {
  const strat = activeStrategy();
  const sizeSol = strat.position_size_sol ?? numSetting('dry_run_buy_sol', 0.1);
  // E1 (2026-08-05): entryOverrides carry the actual fill-derived entry price/mcap
  // (computed in executeLiveBuy from swap.outputAmount + token decimals); falls back
  // to the candidate mark when unavailable.
  const entryPrice = entryOverrides?.entryPrice ?? (Number(candidate.metrics.priceUsd || 0) || null);
  const entryMcap = entryOverrides?.entryMcap ?? (Number(candidate.metrics.marketCapUsd || candidate.metrics.graduatedMarketCapUsd || 0) || null);
  const tp = Number(decision.suggested_tp_percent || strat.tp_percent || numSetting('default_tp_percent', 50));
  const sl = Number(decision.suggested_sl_percent || strat.sl_percent || numSetting('default_sl_percent', -25));
  const trailingEnabled = (strat.trailing_enabled ?? boolSetting('default_trailing_enabled', true)) ? 1 : 0;
  const trailingPercent = strat.trailing_percent ?? numSetting('default_trailing_percent', 20);

  return db.transaction(() => {
    const existing = db.prepare(`
      SELECT id FROM dry_run_positions WHERE mint = ? AND status = 'open' AND execution_mode = 'live' LIMIT 1
    `).get(candidate.token.mint);
    if (existing) return { id: existing.id, isNew: false };

    // Dedup: block re-entry if this token has been closed within 24 hours (live track only)
    const recentClosed = db.prepare(`
      SELECT id FROM dry_run_positions WHERE mint = ? AND status = 'closed' AND closed_at_ms > ? AND execution_mode = 'live' LIMIT 1
    `).get(candidate.token.mint, now() - 86400000);
    if (recentClosed) {
      console.log(`[positions] blocked re-entry ${candidate.token.symbol} (${candidate.token.mint.slice(0, 8)}) — closed <24h ago (live)`);
      return { id: recentClosed.id, isNew: false };
    }

    // Block re-entry if this mint ever had a winning live trade (avoid round-trip losses)
    const pastWin = db.prepare(`
      SELECT id FROM dry_run_positions WHERE mint = ? AND status = 'closed' AND pnl_percent > 0 AND execution_mode = 'live' LIMIT 1
    `).get(candidate.token.mint);
    if (pastWin) {
      console.log(`[positions] blocked re-entry ${candidate.token.symbol} (${candidate.token.mint.slice(0, 8)}) — past WIN exists (live)`);
      return { id: pastWin.id, isNew: false };
    }

    const result = db.prepare(`
      INSERT INTO dry_run_positions (
        candidate_id, mint, symbol, status, opened_at_ms, size_sol, entry_price, entry_mcap,
        token_amount_est, high_water_price, high_water_mcap, tp_percent, sl_percent,
        trailing_enabled, trailing_percent, trailing_armed, llm_decision_id,
        execution_mode, entry_signature, token_amount_raw, strategy_id, snapshot_json
      ) VALUES (?, ?, ?, 'open', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, 'live', ?, ?, ?, ?)
    `).run(
      candidateId,
      candidate.token.mint,
      candidate.token.symbol,
      now(),
      sizeSol,
      entryPrice,
      entryMcap,
      null,
      entryPrice,
      entryMcap,
      tp,
      sl,
      trailingEnabled,
      trailingPercent,
      decision.id || null,
      swap.signature,
      swap.outputAmount || null,
      strat.id,
      json({ candidate, decision, reason, swap, strategy: strat.id }),
    );
    const positionId = Number(result.lastInsertRowid);
    db.prepare(`
      INSERT INTO dry_run_trades (position_id, mint, side, at_ms, price, mcap, size_sol, token_amount_est, reason, payload_json)
      VALUES (?, ?, 'buy', ?, ?, ?, ?, ?, ?, ?)
    `).run(positionId, candidate.token.mint, now(), entryPrice, entryMcap, sizeSol, null, reason, json({ candidateId, decision, swap }));
    db.prepare(`
      INSERT INTO tp_sl_rules (position_id, tp_percent, sl_percent, trailing_enabled, trailing_percent, updated_at_ms)
      VALUES (?, ?, ?, ?, ?, ?)
    `).run(positionId, tp, sl, trailingEnabled, trailingPercent, now());
    return { id: positionId, isNew: true };
  })();
}
