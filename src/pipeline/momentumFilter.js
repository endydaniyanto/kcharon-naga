/**
 * Momentum filter — calls Python ML model to predict if a candidate
 * will be a runner (TRAILING_TP) or sideways (MAX_HOLD).
 * 
 * SAFETY: If the model fails or times out, returns PASS so Charon
 * never gets stuck waiting for ML.
 */

import { spawn } from 'child_process';
import { fileURLToPath } from 'url';
import path from 'path';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const PYTHON_SCRIPT = path.join(__dirname, 'predict_momentum.py');
const TIMEOUT_MS = 8000; // 8 seconds max for Python
const DEFAULT_THRESHOLD = 0.5;

/**
 * Score a candidate using the momentum model.
 * Spawns a fresh Python process per request (model is small, loads fast).
 */
export async function momentumFilter(candidate, threshold = DEFAULT_THRESHOLD) {
  const startTime = Date.now();
  const mint = candidate.token?.mint?.slice(0, 8) || 'unknown';
  
  // Check if we have price data
  const price = candidate.gmgn?.price || {};
  if (!price.price && !price.price_1h) {
    console.log(`[momentum] ${mint}... no price data — pass`);
    return { passed: true, score: 1.0, reason: 'no_price_data' };
  }
  
  return new Promise((resolve) => {
    const proc = spawn('python3', [PYTHON_SCRIPT], {
      stdio: ['pipe', 'pipe', 'pipe'],
      timeout: TIMEOUT_MS,
      // Gap python (2026-08-07): railpack installs numpy/sklearn into /app/pylib
      // (deployOutputs from the python-deps step); the spawned python needs
      // PYTHONPATH to find them. Env override merges with inherited process.env.
      env: { ...process.env, PYTHONPATH: '/app/pylib' },
    });
    
    let stdout = '';
    let stderr = '';
    let resolved = false;
    
    const finish = (result) => {
      if (resolved) return;
      resolved = true;
      try { proc.kill(); } catch (e) { /* ignore */ }
      resolve(result);
    };
    
    const timeout = setTimeout(() => {
      stderr && console.error(`[momentum] ${mint}... stderr: ${stderr.trim()}`);
      console.error(`[momentum] ${mint}... timeout after ${TIMEOUT_MS}ms — pass`);
      finish({ passed: true, score: -1, reason: 'timeout' });
    }, TIMEOUT_MS);
    
    proc.stdout.on('data', (data) => {
      stdout += data.toString();
    });
    
    proc.stderr.on('data', (data) => {
      stderr += data.toString();
    });
    
    proc.on('error', (err) => {
      clearTimeout(timeout);
      console.error(`[momentum] ${mint}... spawn error: ${err.message} — pass`);
      finish({ passed: true, score: -1, reason: err.message });
    });
    
    proc.on('close', (code) => {
      clearTimeout(timeout);
      
      if (code !== 0) {
        console.error(`[momentum] ${mint}... exit code ${code} — pass`);
        finish({ passed: true, score: -1, reason: `exit_${code}` });
        return;
      }
      
      try {
        const result = JSON.parse(stdout.trim());
        const score = result.momentum_score;
        
        if (score < 0) {
          console.error(`[momentum] ${mint}... model error: ${result.error} — pass`);
          finish({ passed: true, score: -1, reason: result.error });
          return;
        }
        
        const passed = score >= threshold;
        const latency = Date.now() - startTime;
        
        if (!passed) {
          console.log(`[momentum] ${mint}... REJECTED score=${score.toFixed(3)} < ${threshold} (${latency}ms)`);
        } else {
          console.log(`[momentum] ${mint}... PASSED score=${score.toFixed(3)} (${latency}ms)`);
        }
        
        finish({ passed, score, latency });
      } catch (e) {
        console.error(`[momentum] ${mint}... JSON parse error: ${e.message} — pass`);
        finish({ passed: true, score: -1, reason: 'parse_error' });
      }
    });
    
    // Send candidate JSON to Python
    const input = JSON.stringify({ candidate });
    proc.stdin.write(input);
    proc.stdin.end();
  });
}

/**
 * Synchronous fallback — always passes.
 */
export function momentumScoreSync(candidate) {
  return { passed: true, score: 1.0, reason: 'sync_fallback' };
}

/**
 * Get the current momentum threshold from strategy.
 */
export function getMomentumThreshold(strat) {
  return strat?.momentum_threshold ?? DEFAULT_THRESHOLD;
}