import fetch from "node-fetch";
import { performance } from "perf_hooks";

const JUPITER_QUOTE = "https://quote-api.jup.ag/v6/quote";
const SOLANA_RPC = "https://api.mainnet-beta.solana.com";

// Counters
let counts = {
  jupiterSuccess: 0,
  jupiterFail: 0,
  solanaSuccess: 0,
  solanaFail: 0,
  buy: 0,
  sell: 0,
  validate: 0,
};

async function simulateBuy() {
  const url = `${JUPITER_QUOTE}?inputMint=So11111111111111111111111111111111111111112&outputMint=EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v&amount=1000000&slippageBps=50`;
  try {
    const res = await fetch(url);
    if (res.ok) counts.jupiterSuccess++;
    else counts.jupiterFail++;
  } catch {
    counts.jupiterFail++;
  }
  counts.buy++;
}

async function simulateSell() {
  const url = `${JUPITER_QUOTE}?inputMint=EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v&outputMint=So11111111111111111111111111111111111111112&amount=1000000&slippageBps=50`;
  try {
    const res = await fetch(url);
    if (res.ok) counts.jupiterSuccess++;
    else counts.jupiterFail++;
  } catch {
    counts.jupiterFail++;
  }
  counts.sell++;
}

async function simulateValidate() {
  const body = JSON.stringify({
    jsonrpc: "2.0",
    id: 1,
    method: "getRecentBlockhash",
  });
  try {
    const res = await fetch(SOLANA_RPC, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body,
    });
    if (res.ok) counts.solanaSuccess++;
    else counts.solanaFail++;
  } catch {
    counts.solanaFail++;
  }
  counts.validate++;
}

// Simulate realistic delays per transaction
async function realisticDelay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function runSimulation(durationMs = 60000) {
  console.log("🚀 Simulating 1-minute bot activity (with realistic delays)...\n");

  const start = performance.now();
  const end = start + durationMs;

  while (performance.now() < end) {
    // Fire one batch
    await Promise.all([
      simulateBuy(),
      simulateSell(),
      simulateValidate(),
      simulateValidate(),
    ]);

    // Add small realistic delay per loop to mimic network latency
    await realisticDelay(100); // 100ms between batches
  }

  const total = counts.jupiterSuccess + counts.jupiterFail + counts.solanaSuccess + counts.solanaFail;
  console.log(`\n=== 📊 Simulation Complete ===`);
  console.log(`Total Duration: ${(durationMs / 1000).toFixed(1)} sec`);
  console.log(`Total Requests: ${total}`);
  console.log(`  - Jupiter API Success: ${counts.jupiterSuccess}`);
  console.log(`  - Jupiter API Fail: ${counts.jupiterFail}`);
  console.log(`  - Solana RPC Success: ${counts.solanaSuccess}`);
  console.log(`  - Solana RPC Fail: ${counts.solanaFail}`);
  console.log(`\nBreakdown by Function:`);
  console.log(`  🟢 Buy: ${counts.buy}`);
  console.log(`  🔴 Sell: ${counts.sell}`);
  console.log(`  🧩 Validate: ${counts.validate}`);
  console.log(`\nRequests per Second: ${(total / (durationMs / 1000)).toFixed(2)}`);
  console.log(`Estimated Realistic Trades per Minute: ${(counts.buy + counts.sell) * (1000 / 100)} approx`);
}

// Run for 1 minute
runSimulation(60000);
    