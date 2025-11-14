import {
  NATIVE_MINT,
  getAssociatedTokenAddress,
} from '@solana/spl-token';
import {
  Keypair,
  Connection,
  PublicKey,
  LAMPORTS_PER_SOL,
  VersionedTransaction,
  SystemProgram,
  ComputeBudgetProgram,
  TransactionInstruction,
  TransactionMessage,
  Transaction
} from '@solana/web3.js';
import {
  ADDITIONAL_FEE,
  BUY_AMOUNT,
  BUY_INTERVAL_MAX,
  BUY_INTERVAL_MIN,
  BUY_LOWER_AMOUNT,
  BUY_UPPER_AMOUNT,
  DISTRIBUTE_WALLET_NUM,
  DISTRIBUTION_AMOUNT,
  IS_RANDOM,
  PRIVATE_KEY,
  RPC_ENDPOINT,
  RPC_WEBSOCKET_ENDPOINT,
  TOKEN_MINT,
  SELL_ALL_BY_TIMES,
  SELL_PERCENT,
  SWAP_ROUTING
} from './constants';
import { Data, editJson, readJson, saveDataToFile, sleep } from './utils';
import base58 from 'bs58';
import { getBuyTxWithJupiter, getSellTxWithJupiter } from './utils/swapOnlyAmm';
import { execute } from './executor/legacy';
import { BN } from 'bn.js';

export const solanaConnection = new Connection(RPC_ENDPOINT, {
  wsEndpoint: RPC_WEBSOCKET_ENDPOINT,
});

export const mainKp = Keypair.fromSecretKey(base58.decode(PRIVATE_KEY));
const baseMint = new PublicKey(TOKEN_MINT);
const distributionNum = DISTRIBUTE_WALLET_NUM > 10 ? 10 : DISTRIBUTE_WALLET_NUM;

const main = async () => {
  const solBalance = (await solanaConnection.getBalance(mainKp.publicKey)) / LAMPORTS_PER_SOL;

  console.log(`Volume bot is running`);
  console.log(`Wallet address: ${mainKp.publicKey.toBase58()}`);
  console.log(`Token mint: ${baseMint.toBase58()}`);
  console.log(`Wallet SOL balance: ${solBalance.toFixed(3)}SOL`);
  console.log(`Buying interval max: ${BUY_INTERVAL_MAX}ms`);
  console.log(`Buying interval min: ${BUY_INTERVAL_MIN}ms`);
  console.log(`Buy upper limit amount: ${BUY_UPPER_AMOUNT}SOL`);
  console.log(`Buy lower limit amount: ${BUY_LOWER_AMOUNT}SOL`);
  console.log(`Distributing SOL to ${distributionNum} wallets`);
  console.log(`Using Jupiter swap routing: ${SWAP_ROUTING}`);

  if (solBalance < (BUY_LOWER_AMOUNT + ADDITIONAL_FEE) * distributionNum) {
    console.log("SOL balance is not enough for distribution");
    return;
  }

  const wallets = await distributeSolAndToken(mainKp, distributionNum, baseMint);
  if (!wallets) {
    console.log("Distribution failed");
    return;
  }

  wallets.map(async ({ kp }, i) => {
    await sleep((BUY_INTERVAL_MAX + BUY_INTERVAL_MIN) * i / 2);

    const ata = await getAssociatedTokenAddress(baseMint, kp.publicKey);
    const initBalance = (await solanaConnection.getTokenAccountBalance(ata)).value.uiAmount;
    if (!initBalance || initBalance == 0) {
      console.log("Error, distribution didn't work");
      return;
    }

    let soldIndex = 1;

    while (true) {
      // ---------------- Buy Part ----------------
      const BUY_INTERVAL = Math.round(Math.random() * (BUY_INTERVAL_MAX - BUY_INTERVAL_MIN) + BUY_INTERVAL_MIN);
      const solBal = await solanaConnection.getBalance(kp.publicKey) / LAMPORTS_PER_SOL;

      let buyAmount = IS_RANDOM
        ? Number((Math.random() * (BUY_UPPER_AMOUNT - BUY_LOWER_AMOUNT) + BUY_LOWER_AMOUNT).toFixed(6))
        : BUY_AMOUNT;

      if (solBal < ADDITIONAL_FEE) {
        console.log("Balance too low to buy:", solBal, "SOL");
        return;
      }

      let buyRetry = 0;
      while (true) {
        if (buyRetry > 10) {
          console.log("Buy transaction failed after 10 retries");
          return;
        }
        const result = await buy(kp, baseMint, buyAmount);
        if (result) break;
        buyRetry++;
        console.log("Buy failed, retrying...");
        await sleep(2000);
      }

      await sleep(1000);

      // ---------------- Sell Part ----------------
      let sellRetry = 0;
      while (true) {
        if (sellRetry > 10) {
          console.log("Sell transaction failed after 10 retries");
          return;
        }
        const result = await sell(kp, baseMint, soldIndex, initBalance);
        if (result) {
          soldIndex++;
          break;
        }
        sellRetry++;
        console.log("Sell failed, retrying...");
        await sleep(2000);
      }

      await sleep(5000 + distributionNum * BUY_INTERVAL);
    }
  });
};

// ---------------- Distribution ----------------
const distributeSolAndToken = async (mainKp: Keypair, numWallets: number, baseMint: PublicKey) => {
  const data: Data[] = [];
  const wallets: { kp: Keypair; buyAmount: number }[] = [];

  const mainAta = await getAssociatedTokenAddress(baseMint, mainKp.publicKey);
  let mainTokenBalance: string | null = null;

  try {
    mainTokenBalance = (await solanaConnection.getTokenAccountBalance(mainAta)).value.amount;
  } catch {
    console.log("Error getting token balance of main wallet. Can't continue.");
    return null;
  }

  if (!mainTokenBalance || mainTokenBalance === "0") {
    console.log("Main wallet has no tokens. Can't continue.");
    return null;
  }

  console.log("Main wallet token balance:", mainTokenBalance);

  try {
    const tokenAmountPerWallet = new BN(mainTokenBalance)
      .div(new BN(numWallets))
      .toString();

    const distributionIx: TransactionInstruction[] = [
      ComputeBudgetProgram.setComputeUnitLimit({ units: 800_000 }),
      ComputeBudgetProgram.setComputeUnitPrice({ microLamports: 250_000 })
    ];

    for (let i = 0; i < numWallets; i++) {
      let solAmount = DISTRIBUTION_AMOUNT;
      if (DISTRIBUTION_AMOUNT < ADDITIONAL_FEE + BUY_UPPER_AMOUNT)
        solAmount = ADDITIONAL_FEE + BUY_UPPER_AMOUNT;

      const wallet = Keypair.generate();
      wallets.push({ kp: wallet, buyAmount: solAmount });

      distributionIx.push(
        SystemProgram.transfer({
          fromPubkey: mainKp.publicKey,
          toPubkey: wallet.publicKey,
          lamports: solAmount * LAMPORTS_PER_SOL
        })
      );
    }

    const latestBlockhash = await solanaConnection.getLatestBlockhash();
    const tx = new VersionedTransaction(
      new TransactionMessage({
        payerKey: mainKp.publicKey,
        recentBlockhash: latestBlockhash.blockhash,
        instructions: distributionIx
      }).compileToV0Message()
    );
    tx.sign([mainKp]);
    const txSig = await execute(tx, latestBlockhash);
    console.log("SOL distributed:", txSig ? `https://solscan.io/tx/${txSig}` : 'No tx');

    wallets.map(wallet => {
      data.push({
        privateKey: base58.encode(wallet.kp.secretKey),
        pubkey: wallet.kp.publicKey.toBase58(),
        solBalance: wallet.buyAmount + ADDITIONAL_FEE,
        tokenBuyTx: null,
        tokenSellTx: null
      });
    });

    saveDataToFile(data);
    return wallets;

  } catch (error) {
    console.log("Failed to distribute SOL:", error);
    return null;
  }
};

// ---------------- Buy ----------------
const buy = async (wallet: Keypair, baseMint: PublicKey, amount: number) => {
  try {
    const tx = await getBuyTxWithJupiter(wallet, baseMint, amount);
    if (!tx) return null;

    const latestBlockhash = await solanaConnection.getLatestBlockhash();
    const txSig = await execute(tx, latestBlockhash);
    const tokenBuyTx = txSig ? `https://solscan.io/tx/${txSig}` : '';

    editJson({
      tokenBuyTx,
      pubkey: wallet.publicKey.toBase58(),
    });

    return tokenBuyTx;
  } catch {
    return null;
  }
};

// ---------------- Sell ----------------
const sell = async (wallet: Keypair, baseMint: PublicKey, index: number, initBalance: number) => {
  try {
    const tokenAta = await getAssociatedTokenAddress(baseMint, wallet.publicKey);
    const tokenBalInfo = await solanaConnection.getTokenAccountBalance(tokenAta);
    if (!tokenBalInfo || !tokenBalInfo.value.uiAmount) return null;

    const tokenBalance = tokenBalInfo.value.uiAmount;
    const tokenToSell = Math.max(0, tokenBalance * (SELL_ALL_BY_TIMES - index) / SELL_ALL_BY_TIMES);

    const sellTx = await getSellTxWithJupiter(wallet, baseMint, tokenToSell.toString());
    if (!sellTx) return null;

    const latestBlockhash = await solanaConnection.getLatestBlockhash();
    const txSig = await execute(sellTx, latestBlockhash, false);
    const tokenSellTx = txSig ? `https://solscan.io/tx/${txSig}` : '';

    const solBalance = await solanaConnection.getBalance(wallet.publicKey);
    editJson({
      pubkey: wallet.publicKey.toBase58(),
      tokenSellTx,
      solBalance
    });

    return tokenSellTx;

  } catch {
    return null;
  }
};

main();
