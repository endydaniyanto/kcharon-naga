import { bot } from './bot.js';
import { TELEGRAM_CHAT_ID } from '../config.js';
import { now } from '../utils.js';
import { openPositions } from '../db/positions.js';
import { refreshPosition } from '../execution/positions.js';
import { logDecisionEvent } from '../db/decisions.js';
import { escapeHtml, fmtSol } from '../format.js';

// /flush — close ALL open positions (live + dry) for a clean-slate shadow experiment.
// Two-step: /flush shows a summary and arms a 60s confirmation; /flush confirm executes.
// Manual-only by design — never automatic on redeploy (that would force-sell live at market).
// 2026-08-05: reuses refreshPosition(forceExit) so closing follows the exact same paths as
// the exit engine (live = real Jupiter sell + realized PnL; dry = marked-to-market PnL).

const CONFIRM_WINDOW_MS = 60_000;
const flushPending = new Map(); // chatId -> armed-at timestamp

export async function handleFlush(chatId, text) {
  if (Number(chatId) !== Number(TELEGRAM_CHAT_ID)) {
    console.log(`[flush] rejected from unauthorized chat ${chatId}`);
    return;
  }
  const isConfirm = /^\/flush\s+confirm/i.test(text);

  if (!isConfirm) {
    const positions = openPositions();
    if (!positions.length) {
      flushPending.delete(chatId);
      return bot.sendMessage(chatId, '🧹 Nothing to flush — no open positions.', { parse_mode: 'HTML' });
    }
    const live = positions.filter(p => p.execution_mode === 'live');
    const dry = positions.filter(p => p.execution_mode !== 'live');
    flushPending.set(chatId, now());
    return bot.sendMessage(chatId, [
      `⚠️ <b>Flush will close ALL ${positions.length} open positions</b>`,
      '',
      `🔴 Live (${live.length}) — will SELL tokens at market:`,
      ...live.map(p => `• ${escapeHtml(p.symbol)} ${p.mint.slice(0, 8)}...`),
      '',
      `⚪ Dry (${dry.length}) — will close with marked-to-market PnL:`,
      ...dry.map(p => `• ${escapeHtml(p.symbol)} ${p.mint.slice(0, 8)}...`),
      '',
      `Reply <code>/flush confirm</code> within 60s to execute.`,
    ].join('\n'), { parse_mode: 'HTML' });
  }

  const armedAt = flushPending.get(chatId);
  if (!armedAt || (now() - armedAt) > CONFIRM_WINDOW_MS) {
    flushPending.delete(chatId);
    return bot.sendMessage(chatId, '⏳ No pending flush (or it expired). Send /flush again to arm it.', { parse_mode: 'HTML' });
  }
  flushPending.delete(chatId);
  return runFlush(chatId);
}

async function runFlush(chatId) {
  const positions = openPositions();
  if (!positions.length) {
    return bot.sendMessage(chatId, '🧹 Nothing to flush — no open positions.', { parse_mode: 'HTML' });
  }
  let liveClosed = 0;
  let dryClosed = 0;
  let livePnl = 0;
  let dryPnl = 0;
  let skipped = 0;
  const failures = [];
  for (const position of positions) {
    try {
      const result = await refreshPosition(position, { autoExit: true, forceExit: true });
      if (!result || result.exitReason !== 'FLUSH') {
        skipped += 1;
        failures.push(`${position.symbol}: ${result?.exitReason || 'skipped (already closing?)'}`);
        continue;
      }
      const pnl = Number(result.pnl_sol || 0);
      if (position.execution_mode === 'live') {
        liveClosed += 1;
        livePnl += pnl;
      } else {
        dryClosed += 1;
        dryPnl += pnl;
      }
    } catch (err) {
      failures.push(`${position.symbol}: ${err.message}`);
      console.error(`[flush] close failed for ${position.symbol} ${position.mint.slice(0, 8)}...: ${err.message}`);
    }
  }
  logDecisionEvent({
    action: 'flush',
    mode: 'live',
    guardrails: { total: positions.length, liveClosed, dryClosed, skipped },
    execution: { livePnl, dryPnl, failures },
  });
  const lines = [
    '🧹 <b>Flush done</b>',
    '',
    `🔴 Live: ${liveClosed} closed · realized ${fmtSol(livePnl)}`,
    `⚪ Dry: ${dryClosed} closed · marked ${fmtSol(dryPnl)}`,
    skipped ? `⏭ Skipped: ${skipped}` : null,
    failures.length ? `❌ Failures:\n${failures.map(f => `• ${escapeHtml(f)}`).join('\n')}` : null,
  ].filter(Boolean);
  await bot.sendMessage(chatId, lines.join('\n'), { parse_mode: 'HTML' });
}
