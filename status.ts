import {
  Keypair,
  Connection,
  PublicKey,
  LAMPORTS_PER_SOL,
} from '@solana/web3.js';
import {
  CHECK_BAL_INTERVAL,
  DISTRIBUTE_WALLET_NUM,
  LOG_LEVEL,
  PRIVATE_KEY,
  RPC_ENDPOINT,
  RPC_WEBSOCKET_ENDPOINT,
  TOKEN_MINT,
} from './constants';
import { deleteConsoleLines, logger, readJson, sleep } from './utils';
import base58 from 'bs58';

export const solanaConnection = new Connection(RPC_ENDPOINT, {
  wsEndpoint: RPC_WEBSOCKET_ENDPOINT,
});

export const mainKp = Keypair.fromSecretKey(base58.decode(PRIVATE_KEY));
const baseMint = new PublicKey(TOKEN_MINT);
const distributionNum = DISTRIBUTE_WALLET_NUM > 20 ? 20 : DISTRIBUTE_WALLET_NUM;

logger.level = LOG_LEVEL;

interface Data {
  privateKey: string;
  pubkey: string;
  solBalance: number | null;
  tokenBuyTx: string | null;
  tokenSellTx: string | null;
}

const data: Data[] = readJson();
const walletPks = data.map(d => d.pubkey);

let bought = 0;
let sold = 0;
let totalSolPut = 0;
let changeAmount = 0;
let buyNum = 0;
let sellNum = 0;

console.log("🚀 Wallets loaded:", walletPks);

interface DexScreenerPair {
  url: string;
  priceNative: string;
  priceUsd: string;
  txns: {
    m5: { buys: number; sells: number };
    h1: { buys: number; sells: number };
    h6: { buys: number; sells: number };
    h24: { buys: number; sells: number };
  };
  volume: { m5: number; h1: number; h6: number; h24: number };
  priceChange: { m5: number; h1: number; h6: number; h24: number };
}

interface DexScreenerResponse {
  pair: DexScreenerPair;
}

const main = async () => {
  const solBalance = (await solanaConnection.getBalance(mainKp.publicKey)) / LAMPORTS_PER_SOL;
  console.log(`Wallet address: ${mainKp.publicKey.toBase58()}`);
  console.log(`Token mint: ${baseMint.toBase58()}`);
  console.log(`Wallet SOL balance: ${solBalance.toFixed(3)} SOL`);
  console.log("Check interval:", CHECK_BAL_INTERVAL, "ms");
  console.log("Monitoring wallets for Jupiter swaps only");

  // Optional: periodically fetch DexScreener stats for the token
  setInterval(async () => {
    try {
      const res = await fetch(`https://api.dexscreener.com/latest/dex/tokens/${baseMint.toBase58()}`);
      const dexData = await res.json() as DexScreenerResponse;
      const { priceNative, priceUsd, volume, txns } = dexData.pair;

      console.log(`\nPrice: ${priceNative} SOL / ${priceUsd} USD`);
      console.log(`Recent txs m5: buys=${txns.m5.buys}, sells=${txns.m5.sells}`);
      console.log(`Volume h1: $${volume.h1}, h24: $${volume.h24}`);
    } catch (err) {
      console.log("Error fetching DexScreener data:", err);
    }
  }, 30000); // every 30s

  trackWallets();
};

// Track wallet transactions for buy/sell activity
async function trackWallets() {
  try {
    solanaConnection.onLogs(
      mainKp.publicKey, // monitor main wallet or all wallets if needed
      async ({ signature }) => {
        try {
          const parsedTx = await solanaConnection.getParsedTransaction(signature, { commitment: "confirmed" });
          const signer = parsedTx?.transaction.message.accountKeys.find(k => k.signer)?.pubkey.toBase58();

          if (!signer || walletPks.includes(signer)) return;

          const preBalance = Number(parsedTx?.meta?.preBalances[0]);
          const postBalance = Number(parsedTx?.meta?.postBalances[0]);

          if (preBalance > postBalance) buyNum++;
          else sellNum++;

          deleteConsoleLines(1);
          console.log(`Other wallets bought ${buyNum - bought} times, sold ${sellNum - sold} times`);
        } catch {}
      },
      "confirmed"
    );
  } catch (err) {
    console.log("Error tracking wallets:", err);
  }
}

main();
