// diagnostic-real-usage.mjs
import fetch from "node-fetch";
import {
  Keypair,
  PublicKey,
  Connection,
  sendAndConfirmTransaction,
  VersionedTransaction
} from "@solana/web3.js";

// ---------------- CONFIG ---------------- //
const NUM_WALLETS = 12;
const MAX_RETRIES = 3;
const RETRY_DELAY_MS = 1500;
const SOLANA_RPC = "https://fluent-fittest-smoke.solana-mainnet.quiknode.pro/b33698f7f6cc05f809d4fb064efb10b0708b9d09/";
const connection = new Connection(SOLANA_RPC);

// Token mint
const TOKEN_MINT = new PublicKey("7XRNzLkfBM5N6FhxsLxtfHZTWGDCLbAazTKHhosmpump");

// ---------------- UTILS ---------------- //
const sleep = (ms) => new Promise((res) => setTimeout(res, ms));

async function getBuyTx(wallet, amount, retries = 0) {
  try {
    const lamports = Math.floor(amount * 1e9);
    const quoteUrl = `https://lite-api.jup.ag/swap/v1/quote?inputMint=So11111111111111111111111111111111111111112&outputMint=${TOKEN_MINT.toBase58()}&amount=${lamports}&slippageBps=100`;

    const quoteResponse = await fetch(quoteUrl, {
      headers: { accept: "application/json", origin: "https://jup.ag" },
    }).then((res) => res.json());

    if (!quoteResponse || quoteResponse.error || !quoteResponse.outAmount) {
      throw new Error("Quote failed");
    }

    const swapResponse = await fetch("https://lite-api.jup.ag/swap/v1/swap", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        accept: "application/json",
        origin: "https://jup.ag",
      },
      body: JSON.stringify({
        userPublicKey: wallet.publicKey.toString(),
        wrapAndUnwrapSol: true,
        quoteResponse: quoteResponse,
        dynamicComputeUnitLimit: true,
        prioritizationFeeLamports: 100_000,
      }),
    }).then((res) => res.json());

    if (!swapResponse?.swapTransaction) throw new Error("No swap transaction returned");

    const txBuf = Buffer.from(swapResponse.swapTransaction, "base64");
    const transaction = VersionedTransaction.deserialize(new Uint8Array(txBuf));
    transaction.sign([wallet]);
    return transaction;
  } catch (err) {
    if (retries < MAX_RETRIES && /429|Rate limit/i.test(err.message)) {
      await sleep(RETRY_DELAY_MS * (retries + 1));
      return getBuyTx(wallet, amount, retries + 1);
    }
    console.log(`Buy tx failed for wallet ${wallet.publicKey.toBase58()}:`, err.message);
    return null;
  }
}

async function getSellTx(wallet, amount, retries = 0) {
  try {
    const quoteUrl = `https://lite-api.jup.ag/swap/v1/quote?inputMint=${TOKEN_MINT.toBase58()}&outputMint=So11111111111111111111111111111111111111112&amount=${amount}&slippageBps=100`;

    const quoteResponse = await fetch(quoteUrl, {
      headers: { accept: "application/json", origin: "https://jup.ag" },
    }).then((res) => res.json());

    if (!quoteResponse || quoteResponse.error || !quoteResponse.outAmount) {
      throw new Error("Sell quote failed");
    }

    const swapResponse = await fetch("https://lite-api.jup.ag/swap/v1/swap", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        accept: "application/json",
        origin: "https://jup.ag",
      },
      body: JSON.stringify({
        userPublicKey: wallet.publicKey.toString(),
        wrapAndUnwrapSol: true,
        useSharedAccounts: false,
        quoteResponse: quoteResponse,
        dynamicComputeUnitLimit: true,
        prioritizationFeeLamports: 100_000,
      }),
    }).then((res) => res.json());

    if (!swapResponse?.swapTransaction) throw new Error("No sell swap transaction returned");

    const txBuf = Buffer.from(swapResponse.swapTransaction, "base64");
    const transaction = VersionedTransaction.deserialize(new Uint8Array(txBuf));
    transaction.sign([wallet]);
    return transaction;
  } catch (err) {
    if (retries < MAX_RETRIES && /429|Rate limit/i.test(err.message)) {
      await sleep(RETRY_DELAY_MS * (retries + 1));
      return getSellTx(wallet, amount, retries + 1);
    }
    console.log(`Sell tx failed for wallet ${wallet.publicKey.toBase58()}:`, err.message);
    return null;
  }
}

async function sendTx(wallet, transaction) {
  if (!transaction) return { success: false };
  try {
    const txSig = await sendAndConfirmTransaction(connection, transaction, [wallet]);
    return { success: true, txSig };
  } catch (err) {
    console.log(`Transaction failed for wallet ${wallet.publicKey.toBase58()}:`, err.message);
    return { success: false, error: err.message };
  }
}

// ---------------- MAIN ---------------- //
async function runDiagnostic() {
  const wallets = Array.from({ length: NUM_WALLETS }, () => Keypair.generate());
  const results = [];

  await Promise.all(
    wallets.map(async (wallet, i) => {
      // Random amount per wallet for demo
      const amount = 0.001 + Math.random() * 0.0015;

      // Buy
      const buyTx = await getBuyTx(wallet, amount);
      const buyResult = await sendTx(wallet, buyTx);
      results.push({ wallet: i + 1, action: "BUY", ...buyResult });

      // Sell
      const sellTx = await getSellTx(wallet, amount * 1e9); // amount in lamports
      const sellResult = await sendTx(wallet, sellTx);
      results.push({ wallet: i + 1, action: "SELL", ...sellResult });
    })
  );

  console.log("\n=== DIAGNOSTIC SUMMARY ===");
  results.forEach((r) => {
    if (r.success) console.log(`Wallet ${r.wallet} ${r.action} ✅ Tx: ${r.txSig}`);
    else console.log(`Wallet ${r.wallet} ${r.action} ❌ ${r.error}`);
  });
}

// ---------------- RUN ---------------- //
runDiagnostic().catch(console.error);
