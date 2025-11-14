// real-usage-test.mjs
import fetch from "node-fetch";
import {
  Connection,
  PublicKey,
  VersionedTransaction,
  Keypair
} from "@solana/web3.js";

console.log("🚀 Starting REAL usage test (no SOL required)\n");

// --------------------------------------
// CONFIG
// --------------------------------------

const RPC_URL = "https://api.mainnet-beta.solana.com"; // ← change to your bot’s RPC
const TEST_MINT = new PublicKey("So11111111111111111111111111111111111111112"); // SOL mint
const WALLET = Keypair.generate(); // Safe mock wallet

const connection = new Connection(RPC_URL, "confirmed");

const stats = {
  rpc: {},
  jup: {},
};

// Utility to time anything
async function measure(label, fn) {
  const t0 = performance.now();
  try {
    const result = await fn();
    const t1 = performance.now();
    stats.rpc[label] = (stats.rpc[label] || []);
    stats.rpc[label].push(t1 - t0);
    return result;
  } catch (e) {
    const t1 = performance.now();
    stats.rpc[label] = (stats.rpc[label] || []);
    stats.rpc[label].push(t1 - t0);
    console.log(`❌ Error during ${label}`, e);
    return null;
  }
}

// Jupiter measurement helper
async function measureJup(label, fn) {
  const t0 = performance.now();
  try {
    const res = await fn();
    const t1 = performance.now();
    stats.jup[label] = (stats.jup[label] || []);
    stats.jup[label].push(t1 - t0);
    return res;
  } catch (e) {
    const t1 = performance.now();
    stats.jup[label] = (stats.jup[label] || []);
    stats.jup[label].push(t1 - t0);
    console.log(`❌ JUP error during ${label}`, e);
    return null;
  }
}

// --------------------------------------
// REAL TESTING
// --------------------------------------
async function runTest() {
  // ---- RPC SECTION ----

  console.log("📡 Testing RPC endpoints...");

  await measure("getLatestBlockhash", async () => {
    return await connection.getLatestBlockhash();
  });

  await measure("getBalance", async () => {
    return await connection.getBalance(WALLET.publicKey);
  });

  await measure("getTokenAccountsByOwner", async () => {
    return await connection.getTokenAccountsByOwner(WALLET.publicKey, {
      programId: new PublicKey("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"),
    });
  });

  await measure("getTokenAccountBalance (dummy)", async () => {
    return await connection.getTokenAccountBalance(
      new PublicKey("11111111111111111111111111111111")
    );
  });

  // ---- JUPITER SECTION ----

  console.log("\n⚡ Testing Jupiter quote API...");

  const quoteUrl =
    `https://lite-api.jup.ag/swap/v1/quote` +
    `?inputMint=${TEST_MINT.toBase58()}` +
    `&outputMint=${TEST_MINT.toBase58()}` +
    `&amount=1000000&slippageBps=50`;

  const quote = await measureJup("quote", async () => {
    return await fetch(quoteUrl).then((r) => r.json());
  });

  console.log("⚡ Jupiter quote returned keys:", Object.keys(quote));

  console.log("\n⚡ Testing Jupiter swap builder...");
  const swap = await measureJup("swap", async () => {
    return await fetch("https://lite-api.jup.ag/swap/v1/swap", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        accept: "application/json",
        origin: "https://jup.ag",
      },
      body: JSON.stringify({
        userPublicKey: WALLET.publicKey.toString(),
        wrapAndUnwrapSol: true,
        dynamicComputeUnitLimit: true,
        quoteResponse: quote,
        prioritizationFeeLamports: 10000,
      }),
    }).then((r) => r.json());
  });

  // Transaction handling
  if (swap && swap.swapTransaction) {
    await measureJup("transaction_deserialize", async () => {
      VersionedTransaction.deserialize(
        new Uint8Array(Buffer.from(swap.swapTransaction, "base64"))
      );
    });
  }

  // --------------------------------------
  // REPORT
  // --------------------------------------

  console.log("\n==============================");
  console.log(" REAL-WORLD BOT USAGE REPORT ");
  console.log("==============================");

  console.log("\nRPC Timing:");
  for (const key in stats.rpc) {
    const arr = stats.rpc[key];
    console.log(
      `  ${key}: avg ${avg(arr)} ms (samples: ${arr.length})`
    );
  }

  console.log("\nJupiter Timing:");
  for (const key in stats.jup) {
    const arr = stats.jup[key];
    console.log(
      `  ${key}: avg ${avg(arr)} ms (samples: ${arr.length})`
    );
  }

  console.log("\nDone.\n");
}

function avg(arr) {
  if (!arr.length) return 0;
  return (arr.reduce((a, b) => a + b) / arr.length).toFixed(2);
}

runTest();
