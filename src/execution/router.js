import { now, json } from '../utils.js';
import { numSetting, boolSetting } from '../db/settings.js';
import { db } from '../db/connection.js';
import { WSOL_MINT, LIVE_MIN_SOL_RESERVE_LAMPORTS } from '../config.js';
import { escapeHtml, fmtSol } from '../format.js';
import { executeJupiterSwap, liveWalletBalanceLamports, fetchLiveTokenBalance } from '../liveExecutor.js';
import { activeStrategy } from '../db/settings.js';
import { createLivePosition, canOpenMorePositions, openPositionCount } from '../db/positions.js';
import { intentById } from '../db/intents.js';
import { logDecisionEvent } from '../db/decisions.js';
import { refreshCandidateForExecution } from './positions.js';
import { bot } from '../telegram/bot.js';
import { candidateSummary } from '../telegram/format.js';
import { sendPositionOpen, sendTelegram } from '../telegram/send.js';
import { updateCandidateStatus } from '../db/candidates.js';
import { createTradeIntent } from '../db/intents.js';

const ENTRY_MAX_ATTEMPTS = 3;

export async function executeLiveBuy(selectedRow, decision, batchId, rows = [], triggerCandidateId = null) {
  const strat = activeStrategy();
  const amountLamports = Math.floor((strat.position_size_sol ?? numSetting('dry_run_buy_sol', 0.1)) * 1_000_000_000);
  const balance = await liveWalletBalanceLamports();
  if (balance < amountLamports + LIVE_MIN_SOL_RESERVE_LAMPORTS) {
    throw new Error(`Insufficient SOL balance. Need ${fmtSol((amountLamports + LIVE_MIN_SOL_RESERVE_LAMPORTS) / 1_000_000_000)} SOL including reserve.`);
  }
  const candidate = selectedRow.candidate;
  let swap = null;
  let lastError = null;
  for (let attempt = 1; attempt <= ENTRY_MAX_ATTEMPTS; attempt++) {
    try {
      swap = await executeJupiterSwap({
        inputMint: WSOL_MINT,
        outputMint: candidate.token.mint,
        amount: amountLamports,
      });
      if (!swap.outputAmount) {
        swap.outputAmount = await fetchLiveTokenBalance(candidate.token.mint) || swap.outputAmount;
      }
      lastError = null;
      break;
    } catch (err) {
      lastError = err;
      console.log(`[executeLiveBuy] attempt ${attempt}/${ENTRY_MAX_ATTEMPTS} failed for ${candidate.token.mint.slice(0, 8)}... ${err.message}`);
      if (attempt < ENTRY_MAX_ATTEMPTS) {
        await new Promise(r => setTimeout(r, 1500 * attempt));
      }
    }
  }
  if (!swap) {
    // Record the failed attempt as a closed position so the failure is auditable.
    const failedSwap = { signature: null, outputAmount: null, error: lastError?.message || 'unknown' };
    const { id: positionId } = createLivePosition(selectedRow.id, candidate, decision, failedSwap, 'FAILED_ENTRY');
    db.prepare(`
      UPDATE dry_run_positions
      SET status = 'closed', closed_at_ms = ?, exit_reason = 'FAILED_ENTRY', pnl_percent = 0, pnl_sol = 0
      WHERE id = ?
    `).run(now(), positionId);
    db.prepare(`
      INSERT INTO dry_run_trades (position_id, mint, side, at_ms, price, mcap, size_sol, token_amount_est, reason, payload_json)
      VALUES (?, ?, 'buy', ?, ?, ?, ?, ?, 'FAILED_ENTRY', ?)
    `).run(positionId, candidate.token.mint, now(), null, null, numSetting('dry_run_buy_sol', 0.1), null, 'FAILED_ENTRY',
      json({ attempts: ENTRY_MAX_ATTEMPTS, error: lastError?.message || 'unknown' }));
    logDecisionEvent({
      batchId,
      triggerCandidateId,
      selectedRow,
      rows,
      decision,
      mode: 'live',
      action: 'live_entry_failed',
      guardrails: { balanceLamports: balance, amountLamports, minReserveLamports: LIVE_MIN_SOL_RESERVE_LAMPORTS, attempts: ENTRY_MAX_ATTEMPTS },
      execution: { positionId, error: lastError?.message || 'unknown' },
    });
    await sendTelegram([
      '🛑 <b>Live entry failed after retries</b>',
      '',
      candidateSummary(candidate, decision),
      '',
      `Attempts: ${ENTRY_MAX_ATTEMPTS}`,
      `Error: ${escapeHtml(lastError?.message || 'unknown')}`,
      `Position #${positionId} recorded as FAILED_ENTRY.`,
    ].join('\n'));
    console.log(`[executeLiveBuy] FAILED_ENTRY ${candidate.token.symbol} ${candidate.token.mint.slice(0, 8)}... after ${ENTRY_MAX_ATTEMPTS} attempts: ${lastError?.message || 'unknown'}`);
    throw lastError || new Error('Live buy failed without exception');
  }
  const { id: positionId, isNew } = createLivePosition(selectedRow.id, candidate, decision, swap, `live_batch_${batchId}`);
  console.log(`[executeLiveBuy] ok #${positionId} ${candidate.token.symbol} ${candidate.token.mint.slice(0, 8)}... size=${amountLamports / 1_000_000_000} SOL sig=${swap.signature || '-'} isNew=${isNew}`);
  logDecisionEvent({
    batchId,
    triggerCandidateId,
    selectedRow,
    rows,
    decision,
    mode: 'live',
    action: 'live_entry_executed',
    guardrails: { balanceLamports: balance, amountLamports, minReserveLamports: LIVE_MIN_SOL_RESERVE_LAMPORTS },
    execution: { positionId, isNew, swap },
  });
  if (isNew) await sendPositionOpen(positionId);
}

export async function executeLiveSell(position, reason) {
  const amount = position.token_amount_raw || position.token_amount_est;
  if (!amount || Number(amount) <= 0) throw new Error('Live position has no token amount to sell.');
  return executeJupiterSwap({
    inputMint: position.mint,
    outputMint: WSOL_MINT,
    amount,
  });
}

export async function executeConfirmedIntent(chatId, intentId) {
  const intent = intentById(intentId);
  if (!intent || intent.status !== 'pending_confirmation') return bot.sendMessage(chatId, 'Pending intent not found.');
  // Confirm approval always executes live (2026-08-05 decision) — check the live track's slots.
  if (!canOpenMorePositions('live')) {
    return bot.sendMessage(chatId, `Max live positions reached (${openPositionCount('live')}/${numSetting('max_open_positions', 3)}).`);
  }
  const { decision } = intent.payload;
  try {
    const freshRow = await refreshCandidateForExecution({
      id: intent.candidate_id,
      candidate: intent.payload.candidate,
    });
    if (!freshRow.candidate.filters?.passed) {
      db.prepare('UPDATE trade_intents SET status = ?, updated_at_ms = ? WHERE id = ?').run('rejected_stale', now(), intentId);
      return bot.sendMessage(chatId, [
        '🛑 <b>Trade intent rejected on fresh check</b>',
        '',
        candidateSummary(freshRow.candidate, decision),
        '',
        `Failures: ${escapeHtml((freshRow.candidate.filters?.failures || []).join('; ') || 'fresh execution guard failed')}`,
      ].join('\n'), { parse_mode: 'HTML', disable_web_page_preview: true });
    }
    const strat = activeStrategy();
    const amountLamports = Math.floor((strat.position_size_sol ?? numSetting('dry_run_buy_sol', 0.1)) * 1_000_000_000);
    const balance = await liveWalletBalanceLamports();
    if (balance < amountLamports + LIVE_MIN_SOL_RESERVE_LAMPORTS) {
      db.prepare('UPDATE trade_intents SET status = ?, updated_at_ms = ? WHERE id = ?').run('rejected_insufficient_balance', now(), intentId);
      return bot.sendMessage(chatId, `Insufficient SOL balance. Need ${fmtSol((amountLamports + LIVE_MIN_SOL_RESERVE_LAMPORTS) / 1_000_000_000)} SOL.`, { parse_mode: 'HTML' });
    }
    const swap = await executeJupiterSwap({
      inputMint: WSOL_MINT,
      outputMint: freshRow.candidate.token.mint,
      amount: amountLamports,
    });
    if (!swap.outputAmount) {
      swap.outputAmount = await fetchLiveTokenBalance(freshRow.candidate.token.mint) || swap.outputAmount;
    }
    const { id: positionId, isNew } = createLivePosition(intent.candidate_id, freshRow.candidate, decision, swap, `confirmed_intent_${intentId}`);
    db.prepare('UPDATE trade_intents SET status = ?, updated_at_ms = ? WHERE id = ?').run('executed_live', now(), intentId);
    logDecisionEvent({
      batchId: null,
      triggerCandidateId: intent.candidate_id,
      selectedRow: freshRow,
      rows: [],
      decision,
      mode: 'live',
      action: 'confirmed_intent_executed',
      guardrails: { balanceLamports: balance, amountLamports, intentId },
      execution: { positionId, isNew, swap },
    });
    if (isNew) return sendPositionOpen(positionId);
  } catch (err) {
    db.prepare('UPDATE trade_intents SET status = ?, updated_at_ms = ? WHERE id = ?').run('execution_failed', now(), intentId);
    return bot.sendMessage(chatId, `Live execution failed: ${escapeHtml(err.message)}`, { parse_mode: 'HTML' });
  }
}

export async function rejectIntent(chatId, intentId) {
  const intent = intentById(intentId);
  if (!intent) return bot.sendMessage(chatId, 'Intent not found.');
  db.prepare('UPDATE trade_intents SET status = ?, updated_at_ms = ? WHERE id = ?').run('rejected', now(), intentId);
  return bot.sendMessage(chatId, `Rejected trade intent #${intentId}.`);
}
