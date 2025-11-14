// realistic-simulation.mjs
// Ultra-realistic simulation of your volume-bot RPC + Jupiter usage
// No keys, no on-chain transactions. Pure simulation and counting.
//
// Run: node realistic-simulation.mjs

import { performance } from "perf_hooks";

/* ========== CONFIG ========== */
// Simulation parameters
const DURATION_MS = 60 * 1000; // 1 minute simulation
const WALLET_COUNT = 4;       // simulate N wallets concurrently
const SEED = Date.now();       // seed for randomness (not deterministic here)

// Failure / rate-limit model
const RATE_LIMIT_PROB = 0.10;  // 10% chance an RPC/Jupiter call initially returns rate-limited
const RETRY_MAX = 3;           // max retries on rate-limit per call
const RETRY_BASE_MS = 300;     // base backoff
const RETRY_JITTER = 200;      // jitter in ms

// Operation probabilities/structure
const SELL_AFTER_BUY_PROB = 0.9; // probability a wallet sells after a buy
const VALIDATION_INTERVAL_MS = 6_000; // each wallet validates balances roughly every 6s

// Latency / duration distributions - ULTRA-REALISTIC (ms)
const LATENCIES = {
  jupiter_quote: () => randRange(200, 600),
  getLatestBlockhash: () => randRange(80, 150),
  sendTransaction: () => randRange(40, 120),
  confirmTransaction: () => randRange(300, 900), // confirmation time
  getBalance: () => randRange(40, 80),
  getTokenAccountsByOwner: () => randRange(100, 250),
  getTokenAccountBalance: () => randRange(40, 100),
};

/* ========== TRACKER ========== */
const tracker = {
  // counters
  totals: 0,
  durationMs: DURATION_MS,
  ops: { buy: 0, sell: 0, validate: 0 },
  jupiter: { total: 0, byType: {} }, // e.g. quote/build/execute
  rpc: { total: 0, byMethod: {} },   // e.g. getBalance, getLatestBlockhash, sendTransaction, confirm
  failures: 0,
  latencies: { buy: [], sell: [], validate: [] },

  logRpc(method, elapsed) {
    this.rpc.total++;
    this.rpc.byMethod[method] = (this.rpc.byMethod[method] || 0) + 1;
    if (elapsed != null) this.totals += elapsed;
  },
  logJupiter(type, elapsed) {
    this.jupiter.total++;
    this.jupiter.byType[type] = (this.jupiter.byType[type] || 0) + 1;
    if (elapsed != null) this.totals += elapsed;
  },
  logOp(op, elapsed) {
    this.ops[op] = (this.ops[op] || 0) + 1;
    if (elapsed != null) this.latencies[op].push(elapsed);
  },
  logFailure() { this.failures++; },

  report() {
    const totalRequests = this.rpc.total + this.jupiter.total;
    const durationMin = this.durationMs / 60000;
    const rps = totalRequests / (this.durationMs / 1000);
    const rpm = totalRequests / durationMin;

    function avg(arr) {
      if (!arr || arr.length === 0) return 0;
      return arr.reduce((a,b)=>a+b,0)/arr.length;
    }

    console.log("\n==============================");
    console.log("ULTRA-REALISTIC SIMULATION REPORT");
    console.log("==============================");
    console.log(`Simulated duration: ${(this.durationMs/1000).toFixed(1)}s`);
    console.log(`Simulated wallets: ${WALLET_COUNT}`);
    console.log("");
    console.log("Operations:");
    console.log(`  Buys simulated: ${this.ops.buy}`);
    console.log(`    Avg latency per buy: ${avg(this.latencies.buy).toFixed(0)} ms`);
    console.log(`  Sells simulated: ${this.ops.sell}`);
    console.log(`    Avg latency per sell: ${avg(this.latencies.sell).toFixed(0)} ms`);
    console.log(`  Validations simulated: ${this.ops.validate}`);
    console.log(`    Avg latency per validation: ${avg(this.latencies.validate).toFixed(0)} ms`);
    console.log("");
    console.log("RPC Calls (total):", this.rpc.total);
    console.log(this.rpc.byMethod);
    console.log("");
    console.log("Jupiter Calls (total):", this.jupiter.total);
    console.log(this.jupiter.byType);
    console.log("");
    console.log("Failures (rate-limited simulated):", this.failures);
    console.log("");
    console.log(`Estimated Requests/sec: ${rps.toFixed(2)}`);
    console.log(`Estimated Requests/min: ${rpm.toFixed(0)}`);
    console.log("");
    // Stress level heuristic
    let stress = "LOW";
    if (rpm > 1000) stress = "CRITICAL";
    else if (rpm > 400) stress = "HIGH";
    else if (rpm > 200) stress = "MEDIUM";
    console.log(`Estimated RPC Stress Level: ${stress}`);
    console.log("==============================\n");
  }
};

/* ========== UTILS ========== */
function randRange(min, max) {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}
function sleep(ms) {
  return new Promise((res)=>setTimeout(res, ms));
}
function jitteredBackoff(attempt) {
  // attempt starts at 1
  const base = Math.min(RETRY_BASE_MS * 2 ** (attempt - 1), 5000);
  return base + Math.floor(Math.random() * RETRY_JITTER);
}
function chance(p) { return Math.random() < p; }

/* ========== SIMULATED RPC & JUPITER CALLS ========== */
/*
 Each wrapper simulates:
  - latency (await sleep(latency))
  - possible rate-limit failure (chance), which triggers retries with backoff
  - counts the call in tracker
*/
async function simulatedRpcCall(methodName, latencyFn) {
  // attempt + retry on simulated rate-limit
  for (let attempt=1; attempt<=RETRY_MAX; attempt++) {
    const latency = latencyFn();
    await sleep(latency);
    // simulate rate-limit
    if (chance(RATE_LIMIT_PROB)) {
      tracker.logRpc(methodName, latency);
      tracker.logFailure();
      if (attempt < RETRY_MAX) {
        const backoff = jitteredBackoff(attempt);
        await sleep(backoff);
        continue;
      } else {
        // final failure, still counted
        return { ok: false, latency };
      }
    } else {
      tracker.logRpc(methodName, latency);
      return { ok: true, latency };
    }
  }
  return { ok: false, latency: 0 };
}

async function simulatedJupiterCall(type, latencyFn) {
  for (let attempt=1; attempt<=RETRY_MAX; attempt++) {
    const latency = latencyFn();
    await sleep(latency);
    if (chance(RATE_LIMIT_PROB)) {
      tracker.logJupiter(type, latency);
      tracker.logFailure();
      if (attempt < RETRY_MAX) {
        const backoff = jitteredBackoff(attempt);
        await sleep(backoff);
        continue;
      } else {
        return { ok: false, latency };
      }
    } else {
      tracker.logJupiter(type, latency);
      return { ok: true, latency };
    }
  }
  return { ok: false, latency: 0 };
}

/* ========== HIGH-LEVEL Operation Simulations ========== */

async function simulateBuyFlow(walletId) {
  const start = performance.now();
  // 1) Jupiter: quote/build (counts as Jupiter)
  const jresp = await simulatedJupiterCall("quote", LATENCIES.jupiter_quote || LATENCIES.jupiter_quote);
  if (!jresp.ok) {
    // treat as buy failure (counts logged)
    tracker.logOp("buy", performance.now() - start);
    return false;
  }

  // 2) getLatestBlockhash (RPC)
  const bh = await simulatedRpcCall("getLatestBlockhash", LATENCIES.getLatestBlockhash);
  if (!bh.ok) { tracker.logOp("buy", performance.now() - start); return false; }

  // 3) sendTransaction (RPC)
  const send = await simulatedRpcCall("sendTransaction", LATENCIES.sendTransaction);
  if (!send.ok) { tracker.logOp("buy", performance.now() - start); return false; }

  // 4) confirmTransaction (RPC) - often multiple RPC checks; simulate as 1 call yielding confirmation latency
  const conf = await simulatedRpcCall("confirmTransaction", LATENCIES.confirmTransaction);
  if (!conf.ok) { tracker.logOp("buy", performance.now() - start); return false; }

  // 5) post-check getBalance
  await simulatedRpcCall("getBalance", LATENCIES.getBalance);

  tracker.logOp("buy", performance.now() - start);
  return true;
}

async function simulateSellFlow(walletId) {
  const start = performance.now();
  // 1) find token account (RPC)
  const acc = await simulatedRpcCall("getTokenAccountsByOwner", LATENCIES.getTokenAccountsByOwner);
  if (!acc.ok) { tracker.logOp("sell", performance.now() - start); return false; }

  // 2) get token account balance RPC
  const tokBal = await simulatedRpcCall("getTokenAccountBalance", LATENCIES.getTokenAccountBalance);
  if (!tokBal.ok) { tracker.logOp("sell", performance.now() - start); return false; }

  // 3) Jupiter: quote/build for sell
  const jresp = await simulatedJupiterCall("quote", LATENCIES.jupiter_quote);
  if (!jresp.ok) { tracker.logOp("sell", performance.now() - start); return false; }

  // 4) getLatestBlockhash
  const bh = await simulatedRpcCall("getLatestBlockhash", LATENCIES.getLatestBlockhash);
  if (!bh.ok) { tracker.logOp("sell", performance.now() - start); return false; }

  // 5) sendTransaction
  const send = await simulatedRpcCall("sendTransaction", LATENCIES.sendTransaction);
  if (!send.ok) { tracker.logOp("sell", performance.now() - start); return false; }

  // 6) confirmTransaction
  const conf = await simulatedRpcCall("confirmTransaction", LATENCIES.confirmTransaction);
  if (!conf.ok) { tracker.logOp("sell", performance.now() - start); return false; }

  // 7) post-check getBalance
  await simulatedRpcCall("getBalance", LATENCIES.getBalance);

  tracker.logOp("sell", performance.now() - start);
  return true;
}

async function simulateValidation(walletId) {
  const start = performance.now();
  await simulatedRpcCall("getBalance", LATENCIES.getBalance);
  await simulatedRpcCall("getTokenAccountBalance", LATENCIES.getTokenAccountBalance);
  tracker.logOp("validate", performance.now() - start);
}

/* ========== Wallet Worker ========== */

async function walletWorker(id, deadline) {
  // each wallet keeps validating periodically and performs buy/sell cycles
  let lastValidation = 0;
  while (performance.now() < deadline) {
    const now = performance.now();
    // validation schedule
    if (now - lastValidation >= VALIDATION_INTERVAL_MS) {
      await simulateValidation(id);
      lastValidation = now;
    }
    // attempt a buy -> maybe sell
    // introduce a small randomized gap between cycles, similar to your BUY_INTERVAL logic
    await sleep(randRange(100, 800));
    const buyOk = await simulateBuyFlow(id);
    if (buyOk && chance(SELL_AFTER_BUY_PROB)) {
      // small delay before selling
      await sleep(randRange(300, 1500));
      await simulateSellFlow(id);
    }
    // short rest to avoid tight loop
    await sleep(randRange(200, 1200));
  }
}

/* ========== Simulation Runner ========== */

(async () => {
  console.log("Starting ULTRA-REALISTIC simulation");
  console.log(`Duration: ${DURATION_MS / 1000}s | Wallets: ${WALLET_COUNT}`);
  console.log(`Rate-limit prob: ${(RATE_LIMIT_PROB*100).toFixed(1)}% | Max retries: ${RETRY_MAX}`);
  console.log("Running... (this will simulate realistic latencies & rate-limit retries)\n");

  const start = performance.now();
  const deadline = start + DURATION_MS;
  const workers = [];
  for (let i = 0; i < WALLET_COUNT; i++) {
    workers.push(walletWorker(i+1, deadline));
    // stager start slightly
    await sleep(randRange(50, 150));
  }

  await Promise.all(workers);

  // done
  tracker.durationMs = DURATION_MS;
  tracker.report();
})();
