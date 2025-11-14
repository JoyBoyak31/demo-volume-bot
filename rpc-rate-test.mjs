// jupiter-rate-test.mjs
import fetch from "node-fetch";

const JUP_BASE = "https://api.jup.ag/ultra"; // can change to https://lite-api.jup.ag
const API_KEY = "06694964-4eee-44c6-aa22-926b4b9309cf";
const TEST_DURATION = 60 * 1000; // 1 minute
const CONCURRENCY = 10;

let total = 0, success = 0, fail = 0;

async function jupCall() {
  const url = `${JUP_BASE}/v1/quote?inputMint=So11111111111111111111111111111111111111112&outputMint=EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v&amount=1000000&slippageBps=50`;
  const headers = { "Authorization": `Bearer ${API_KEY}` };
  try {
    const res = await fetch(url, { headers });
    if (res.ok) success++;
    else fail++;
  } catch {
    fail++;
  } finally {
    total++;
  }
}

async function start() {
  console.log("🚀 Testing Jupiter API Rate Limit...\n");
  const start = Date.now();
  while (Date.now() - start < TEST_DURATION) {
    const batch = Array.from({ length: CONCURRENCY }, jupCall);
    await Promise.all(batch);
  }
  console.log(`\n=== Jupiter Test Complete ===`);
  console.log(`Total Requests: ${total}`);
  console.log(`Success: ${success}`);
  console.log(`Fail: ${fail}`);
  console.log(`Requests/sec: ${(total / 60).toFixed(2)}`);
}

start();
