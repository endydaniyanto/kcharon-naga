import { bot } from './bot.js';
import { TELEGRAM_CHAT_ID, TELEGRAM_TOPIC_ID, DB_PATH } from '../config.js';
import { generateDailyCard } from '../visuals/dailyCard.js';
import { writeFileSync, unlinkSync } from 'fs';
import Database from 'better-sqlite3';

function computeStats(rows) {
  const totalTrades = rows.length;
  const wins = rows.filter(p => (p.pnl_sol || 0) > 0).length;
  const losses = rows.filter(p => (p.pnl_sol || 0) <= 0).length;
  const winRate = totalTrades > 0 ? (wins / totalTrades * 100) : 0;
  const pnlSol = rows.reduce((sum, p) => sum + (p.pnl_sol || 0), 0);
  const pnlPercent = totalTrades > 0
    ? rows.reduce((sum, p) => sum + (p.pnl_percent || 0), 0) / totalTrades
    : 0;

  const sorted = [...rows].sort((a, b) => (b.pnl_percent || 0) - (a.pnl_percent || 0));
  const bestTrade = totalTrades > 0
    ? { pnlPercent: sorted[0].pnl_percent || 0, symbol: sorted[0].symbol || sorted[0].mint }
    : null;
  const worstTrade = totalTrades > 0
    ? { pnlPercent: sorted[sorted.length - 1].pnl_percent || 0, symbol: sorted[sorted.length - 1].symbol || sorted[sorted.length - 1].mint }
    : null;

  const winTrades = rows.filter(p => (p.pnl_sol || 0) > 0);
  const lossTrades = rows.filter(p => (p.pnl_sol || 0) <= 0);
  const avgWin = winTrades.length > 0
    ? winTrades.reduce((s, p) => s + (p.pnl_percent || 0), 0) / winTrades.length
    : 0;
  const avgLoss = lossTrades.length > 0
    ? lossTrades.reduce((s, p) => s + (p.pnl_percent || 0), 0) / lossTrades.length
    : 0;
  const riskReward = avgLoss !== 0 ? Math.abs(avgWin / avgLoss) : 0;

  return {
    totalTrades,
    wins,
    losses,
    winRate,
    pnlSol,
    pnlPercent,
    bestTrade,
    worstTrade,
    avgWin,
    avgLoss,
    riskReward,
    positions: rows.map(p => ({
      pnlPercent: p.pnl_percent || 0,
      symbol: p.symbol || '',
    })),
  };
}

export async function buildDailyReport() {
  const db = new Database(DB_PATH);
  try {
    const now = Date.now();
    // Start of today in Jakarta time (UTC+7): midnight WIB = UTC midnight - 7h
    const wibOffset = 7 * 60 * 60 * 1000;
    const nowWIB = now + wibOffset;
    const todayWIBStart = Math.floor(nowWIB / 86400000) * 86400000; // midnight WIB in UTC+7 frame
    const startWIBmidnight = todayWIBStart - wibOffset; // convert back to UTC ms

    const closed = db.prepare(`
      SELECT * FROM dry_run_positions
      WHERE status = 'closed' AND closed_at_ms >= ?
      ORDER BY closed_at_ms DESC
    `).all(startWIBmidnight);

    // 2026-08-05: split by track — live is the headline (real money), dry is the sim twin.
    // COALESCE default 'dry_run' keeps pre-twin rows in the dry bucket. Top-level fields
    // stay headline-compatible for the card renderer.
    const live = computeStats(closed.filter(p => (p.execution_mode || 'dry_run') === 'live'));
    const dry = computeStats(closed.filter(p => (p.execution_mode || 'dry_run') === 'dry_run'));
    const headline = live.totalTrades > 0 ? live : dry;
    return {
      ...headline,
      live,
      dry,
      strategy: 'sniper',
    };
  } finally {
    db.close();
  }
}

export function buildReportCaption(report) {
  const bucketLines = (label, r) => {
    const lines = [
      `${label}: ${r.totalTrades} (${r.wins}W / ${r.losses}L) \u00B7 WR: ${r.winRate.toFixed(1)}%`,
      `PnL: ${r.pnlSol >= 0 ? '+' : ''}${r.pnlSol.toFixed(4)} SOL (avg ${r.pnlPercent >= 0 ? '+' : ''}${r.pnlPercent.toFixed(2)}%)`,
    ];
    if (r.bestTrade) lines.push(`Best: ${r.bestTrade.symbol} ${r.bestTrade.pnlPercent >= 0 ? '+' : ''}${r.bestTrade.pnlPercent.toFixed(2)}%`);
    if (r.worstTrade) lines.push(`Worst: ${r.worstTrade.symbol} ${r.worstTrade.pnlPercent >= 0 ? '+' : ''}${r.worstTrade.pnlPercent.toFixed(2)}%`);
    return lines;
  };
  const rr = report.live.bestTrade && report.live.worstTrade ? `RR: \u22481:${report.live.riskReward.toFixed(2)}` : '';
  return [
    `\u{1F4CA} <b>Daily Report</b>`,
    '',
    ...bucketLines('\u{1F7E2} Live (realized)', report.live),
    '',
    ...bucketLines('\u{1F535} Dry sim (mark-to-market)', report.dry),
    '',
    rr,
  ].filter(Boolean).join('\n');
}

export async function sendDailyReport(chatId = TELEGRAM_CHAT_ID) {
  let tmpPath = '';
  try {
    const report = await buildDailyReport();
    const buffer = await generateDailyCard(report);
    tmpPath = `/tmp/charon_daily_${new Date().toISOString().slice(0, 10)}.png`;
    writeFileSync(tmpPath, buffer);

    await bot.sendPhoto(chatId, tmpPath, {
      caption: buildReportCaption(report),
      parse_mode: 'HTML',
      disable_web_page_preview: true,
      ...(TELEGRAM_TOPIC_ID ? { message_thread_id: Number(TELEGRAM_TOPIC_ID) } : {}),
    });
    console.log('[dailyReport] sent successfully');
  } catch (err) {
    console.error('[dailyReport] failed:', err.message);
    // fallback text
    const report = await buildDailyReport().catch(() => ({ totalTrades: 0, pnlSol: 0 }));
    const fallbackText = [
      `\u{1F4CA} <b>Daily Report (text fallback)</b>`,
      '',
      `Trades: ${report.totalTrades} | PnL: ${report.pnlSol >= 0 ? '+' : ''}${report.pnlSol.toFixed(4)} SOL`,
    ].filter(Boolean).join('\n');
    try {
      await bot.sendMessage(chatId, fallbackText, {
        parse_mode: 'HTML',
        disable_web_page_preview: true,
        ...(TELEGRAM_TOPIC_ID ? { message_thread_id: Number(TELEGRAM_TOPIC_ID) } : {}),
      });
    } catch { /* give up */ }
  } finally {
    if (tmpPath) {
      try { unlinkSync(tmpPath); } catch { /* ignore */ }
    }
  }
}
