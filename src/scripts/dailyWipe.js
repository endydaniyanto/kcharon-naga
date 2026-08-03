// Daily maintenance: wipe candidates + llm_decisions at 00:00 UTC.
// Policy: keep screening/LLM data for 24h, delete on the new day. No snapshots.
// P&L history (dry_run_positions/trades) and decision_logs are untouched —
// the win-block guard (7d) and re-buy block (72h) read positions, not candidates.
// Runs in-process via scheduleDailyWipe() from app.js — the volume is only
// reachable from inside the kcharon-naga container.
import Database from 'better-sqlite3';
import { DB_PATH } from '../config.js';

const WIPE_TABLES = ['candidates', 'llm_decisions'];

function dbSizeBytes(db) {
  return db.pragma('page_count', { simple: true }) * db.pragma('page_size', { simple: true });
}

export async function runDailyWipe() {
  const started = Date.now();
  const db = new Database(DB_PATH);
  const beforeBytes = dbSizeBytes(db);

  try {
    // Wipe trading bloat tables (transactional, same DB handle discipline
    // as the bot — never rm the file under a live process).
    const wipe = db.transaction(() => {
      for (const t of WIPE_TABLES) {
        db.prepare(`DELETE FROM ${t}`).run();
      }
    });
    wipe();

    // Reclaim file space. SQLite would reuse freed pages anyway (file would
    // plateau at peak), but VACUUM keeps the file compact and the volume math
    // predictable. Runs post-delete on a ~460MB DB, so the stall is small.
    // NOTE: must use db.exec() — db.pragma('VACUUM') silently no-ops.
    // In WAL mode the vacuum lands in the WAL, so checkpoint(TRUNCATE) after
    // to physically shrink the main file and truncate the WAL.
    db.exec('VACUUM');
    // TRUNCATE can return busy while the bot is mid-write; retry until clean
    // so the WAL doesn't balloon after VACUUM.
    for (let attempt = 0; attempt < 10; attempt++) {
      const res = db.pragma('wal_checkpoint(TRUNCATE)')[0];
      if (res.busy === 0 && res.log === 0) break;
      await new Promise(r => setTimeout(r, 2000));
    }

    const afterBytes = dbSizeBytes(db);
    const result = {
      date: new Date().toISOString().slice(0, 10),
      beforeMB: Math.round(beforeBytes / 1e6),
      afterMB: Math.round(afterBytes / 1e6),
      freedMB: Math.round((beforeBytes - afterBytes) / 1e6),
      durationSec: Math.round((Date.now() - started) / 1000),
    };
    console.log(`[dailyWipe] ${JSON.stringify(result)}`);
    return result;
  } finally {
    db.close();
  }
}

// In-process scheduler: fires once per day in the 00:00-00:05 UTC window.
// Runs inside the bot because only this service can reach /data.
export function scheduleDailyWipe(onComplete) {
  let lastWipeDay = null;
  setInterval(async () => {
    const now = new Date();
    const todayKey = now.toISOString().slice(0, 10);
    const utcMin = now.getUTCHours() * 60 + now.getUTCMinutes();
    if (utcMin >= 0 && utcMin < 5 && lastWipeDay !== todayKey) {
      lastWipeDay = todayKey;
      try {
        const result = await runDailyWipe();
        if (typeof onComplete === 'function') onComplete(null, result);
      } catch (err) {
        console.error('[dailyWipe] failed:', err.message);
        if (typeof onComplete === 'function') onComplete(err);
      }
    }
  }, 60_000);
  console.log('[dailyWipe] scheduler armed (fires 00:00-00:05 UTC daily)');
}
