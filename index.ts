// Main index.ts file to manage tg

import TelegramBot from 'node-telegram-bot-api';
import {
  NATIVE_MINT,
  getAssociatedTokenAddress,
} from '@solana/spl-token'
import {
  Keypair,
  Connection,
  PublicKey,
  LAMPORTS_PER_SOL,
  SystemProgram,
  VersionedTransaction,
  TransactionInstruction,
  TransactionMessage,
  ComputeBudgetProgram,
  Transaction
} from '@solana/web3.js'
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
  RPC_ENDPOINT,
  RPC_WEBSOCKET_ENDPOINT,
  SWAP_ROUTING,
  ADMIN_PAYMENT_WALLET,
  TEST_MODE,
  TEST_MODE_TRIGGER_AFTER,
} from './constants'
import { Data, editJson, readJson, saveDataToFile, sleep } from './utils/utils'
import base58 from 'bs58'
import { getBuyTx, getBuyTxWithJupiter, getSellTx, getSellTxWithJupiter } from './utils/swapOnlyAmm'
import { execute } from './executor/legacy'
import { getPoolKeys } from './utils/getPoolInfo'
import * as fs from 'fs'
import * as path from 'path'
import { jupiterQueue } from './utils/requestQueue';
import { quoteCache } from './utils/quoteCache';

const BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN;
let bot: TelegramBot | null = null;

const PAYMENT_AMOUNT = 0.001;

export const solanaConnection = new Connection(RPC_ENDPOINT, {
  wsEndpoint: RPC_WEBSOCKET_ENDPOINT,
})

interface TradingWallet {
  address: string;
  privateKey: string;
}

interface TradingStats {
  totalBuys: number;
  totalSells: number;
  totalVolumeSOL: number;
  successfulTxs: number;
  failedTxs: number;
  startTime: number;
  lastActivity: number;
  lastUpdateSent: number;
}

interface DistributionConfig {
  mode: 'auto' | 'custom'; // auto = distribute all, custom = specific amount
  amountPerWallet?: number; // for custom mode
}

interface UserSession {
  userId: number;
  chatId: number;
  walletKeypair?: string;
  tokenAddress?: string;
  tokenName?: string;
  tokenSymbol?: string;
  status: 'idle' | 'payment_pending' | 'payment_confirmed' | 'wallet_created' | 'token_set' | 'wallet_selection' | 'trading' | 'stopped' | 'awaiting_withdraw_address' | 'awaiting_distribution_amount' | 'awaiting_buy_amount';
  depositAddress?: string;
  requiredDeposit: number;
  isMonitoring: boolean;
  botRunning: boolean;
  tradingWallets: TradingWallet[];
  tradingWalletsHistory: TradingWallet[][];
  createdAt: number;
  tradingStats: TradingStats;
  hasPaid: boolean;
  paymentWallet?: string;
  paymentWalletPrivateKey?: string;
  paymentAmount: number;
  paymentConfirmed: boolean;
  userWalletPrivateKey?: string;
  selectedWalletCount: number;
  lastExportTime?: number;
  distributionConfig: DistributionConfig;
  buyAmountConfig: {
    mode: 'default' | 'custom'; // default uses constants, custom uses user value
    customAmount?: number; // user's preferred buy amount
  };
}

interface CachedTokenAccount {
  address: string;
  mint: string;
  owner: string;
  lastUpdated: number;
}

interface WalletCache {
  tokenAccounts: Map<string, CachedTokenAccount>;
  lastBalanceCheck: number;
  balance: number;
}

// Sell queue for cooldown recovery
interface WalletWithTokens {
  wallet: Keypair;
  walletNumber: number;
  shortWallet: string;
  tokenAmount: number;
  tokenBalance: string;
}

let sellQueue: WalletWithTokens[] = [];
let isSellQueueProcessing = false;

// Adaptive rate limiting
let consecutiveSuccesses = 0;
let recentFailureRate = 0;
let lastAdaptiveCheck = Date.now();
const ADAPTIVE_CHECK_INTERVAL = 60000; // Check every 60 seconds
// Rate limit prediction
let apiCallsSinceLastReset = 0;
let lastResetTime = Date.now();
const MAX_CALLS_BEFORE_LIMIT = 150; // Conservative (test showed 186)

// Test mode tracking
let testModeSuccessfulTrades = 0;

const userSessions = new Map<number, UserSession>();
const SESSION_FILE = './user_sessions.json';
const SESSIONS_DIR = './user_sessions';
const activeTraders = new Set<number>();
// Cooldown system state
let COOLDOWN_MODE = false;
let cooldownStartTime = 0;
let rateLimitFailureCount = 0;
let lastFailureTime = 0;
let consecutiveCooldowns = 0;
const MAX_CONSECUTIVE_COOLDOWNS = 3; // Stop bot if cooldown fails 3 times in a row
const RATE_LIMIT_THRESHOLD = 2;
const FAILURE_WINDOW = 10000; // Within 10 seconds
const COOLDOWN_DURATION = 60000;
const COOLDOWN_TEST_DURATION = 30000; // 30 seconds after sells before testing
const walletCacheMap = new Map<string, WalletCache>();
const TOKEN_ACCOUNT_CACHE_TTL = 60000; // 1 minute cache
const BALANCE_CACHE_TTL = 15000; // 15 seconds cache

// ==================== COOLDOWN SYSTEM FUNCTIONS ====================

function recordRateLimitFailure() {
  const now = Date.now();

  // Reset counter if last failure was too long ago
  if (now - lastFailureTime > FAILURE_WINDOW) {
    rateLimitFailureCount = 0;
  }

  rateLimitFailureCount++;
  lastFailureTime = now;

  console.log(`⚠️ Rate limit failure ${rateLimitFailureCount}/${RATE_LIMIT_THRESHOLD}`);

  if (rateLimitFailureCount >= RATE_LIMIT_THRESHOLD && !COOLDOWN_MODE) {
    triggerCooldown();
  }
}

function recordSuccess() {
  consecutiveSuccesses++;

  // Every 10 successes, check if we can speed up
  if (consecutiveSuccesses >= 10 && Date.now() - lastAdaptiveCheck > ADAPTIVE_CHECK_INTERVAL) {
    const currentRate = 3; // Default rate

    if (recentFailureRate < 0.05) { // Less than 5% failure rate
      // Speed up slightly
      jupiterQueue.setRequestsPerSecond(Math.min(currentRate + 0.5, 5));
      console.log('📈 Adaptive: Increasing request rate (low failure rate)');
    }

    consecutiveSuccesses = 0;
    lastAdaptiveCheck = Date.now();
  }
}

function recordFailure() {
  consecutiveSuccesses = 0;

  // Slow down on failures
  if (Date.now() - lastAdaptiveCheck > ADAPTIVE_CHECK_INTERVAL) {
    const currentRate = 3; // default
    jupiterQueue.setRequestsPerSecond(Math.max(currentRate - 0.5, 1));
    console.log('📉 Adaptive: Decreasing request rate (failures detected)');

    lastAdaptiveCheck = Date.now();
  }
}

function triggerCooldown() {
  COOLDOWN_MODE = true;
  cooldownStartTime = Date.now();
  rateLimitFailureCount = 0;
  consecutiveCooldowns++;

  // If we've entered cooldown too many times consecutively, stop the bot
  if (consecutiveCooldowns >= MAX_CONSECUTIVE_COOLDOWNS) {
    console.log('🛑 CRITICAL: Entered cooldown mode 3 times consecutively!');
    console.log('   This indicates a persistent rate limit issue.');
    console.log('   Stopping bot to prevent further issues.');

    activeTraders.forEach(userId => {
      const session = userSessions.get(userId);
      if (session && bot) {
        session.botRunning = false;
        userSessions.set(userId, session);
        saveSessions();

        safeSendMessage(session.chatId,
          `🛑 BOT AUTO-STOPPED\n\n` +
          `Reason: Repeated rate limit issues\n` +
          `Consecutive cooldowns: ${consecutiveCooldowns}\n\n` +
          `Possible causes:\n` +
          `• Too many wallets trading\n` +
          `• Network congestion\n` +
          `• Jupiter API issues\n\n` +
          `Recommendation:\n` +
          `1. Wait 5 minutes\n` +
          `2. Reduce number of wallets\n` +
          `3. Restart bot\n\n` +
          `Use "Withdraw SOL" to gather funds if needed.`
        );
      }
    });
    return;
  }
}

async function testResumptionWithOneWallet(
  tradingWallets: { kp: Keypair; address: string; privateKey: string }[],
  baseMint: PublicKey,
  poolId: PublicKey | undefined,
  session: UserSession
): Promise<boolean> {
  if (tradingWallets.length === 0) return false;

  const testWallet = tradingWallets[0].kp;
  const walletAddress = testWallet.publicKey.toBase58();
  const shortWallet = walletAddress.substring(0, 6) + '...' + walletAddress.substring(walletAddress.length - 4);

  console.log(`Testing with wallet: ${shortWallet}`);

  try {
    // Try a small buy
    // Try a small buy
    const testAmount = BUY_LOWER_AMOUNT; // Use minimum amount

    // Add extra delay before test to ensure cooldown really complete
    await sleep(5000);

    const buyResult = await performBuy(testWallet, baseMint, poolId, session, 1, shortWallet, true); // ADD true

    if (!buyResult) {
      console.log('⚠️ Test buy failed - extending cooldown');
      return false;
    }

    console.log('✅ Test buy successful - waiting before sell test');
    await sleep(5000); // Extra safety delay

    console.log('✅ Test buy successful');
    await sleep(3000);

    // Try to sell what we just bought
    const sellResult = await performSell(testWallet, baseMint, poolId, session, 1, shortWallet);

    if (!sellResult) {
      console.log('⚠️ Test sell failed - but buy worked, continuing anyway');
      return true; // Buy working is enough
    }

    console.log('✅ Test sell successful');
    return true;

  } catch (error: any) {
    console.error('Test resumption error:', error?.message);
    return false;
  }
}

async function checkAndExitCooldown(
  tradingWallets: { kp: Keypair; address: string; privateKey: string }[],
  baseMint: PublicKey,
  poolId: PublicKey | undefined,
  session: UserSession
): Promise<boolean> {
  if (!COOLDOWN_MODE) return true;

  const cooldownElapsed = Date.now() - cooldownStartTime;

  if (cooldownElapsed < COOLDOWN_DURATION) {
    const remaining = Math.ceil((COOLDOWN_DURATION - cooldownElapsed) / 1000);
    console.log(`⏳ Still in cooldown: ${remaining}s remaining`);
    return false;
  }

  console.log('✅ Cooldown period complete, verifying rate limit has cooled...');

  // Wait an additional test period
  await sleep(5000);

  // Test if rate limit is actually cooled down
  const rateLimitTestPassed = await testRateLimitRecovery();

  if (!rateLimitTestPassed) {
    console.log('❌ Rate limit still active, extending cooldown by 60s');
    cooldownStartTime = Date.now(); // Reset cooldown timer
    return false;
  }

  console.log('✅ Rate limit cooled - starting sell queue process');

  // Build sell queue
  await buildSellQueue(tradingWallets, baseMint, session);

  // Process all sells
  if (sellQueue.length > 0) {
    await processSellQueue(baseMint, poolId, session);
  }

  // Final wait before resuming
  console.log('⏳ Waiting 30s before resuming normal trading...');
  await sleep(30000);

  // Smart resumption test - try 1 wallet first
  console.log('🧪 Testing resumption with 1 wallet...');
  const resumptionTestPassed = await testResumptionWithOneWallet(tradingWallets, baseMint, poolId, session);

  if (!resumptionTestPassed) {
    console.log('❌ Resumption test failed - extending cooldown');
    cooldownStartTime = Date.now();
    return false;
  }

  console.log('✅ Resumption test passed - safe to resume all wallets');

  // IMPORTANT: Slow down queue temporarily after cooldown
  console.log('🐌 Slowing down request rate temporarily after cooldown recovery');
  jupiterQueue.setRequestsPerSecond(2); // Slow to 2 req/sec after cooldown

  // Reset to normal speed after 2 minutes
  setTimeout(() => {
    jupiterQueue.setRequestsPerSecond(4);
    console.log('⚡ Request rate restored to normal (4 req/sec)');
  }, 120000); // 2 minutes

  // Exit cooldown mode
  COOLDOWN_MODE = false;
  rateLimitFailureCount = 0;
  apiCallsSinceLastReset = 0;
  lastResetTime = Date.now();
  consecutiveCooldowns = 0; // Reset consecutive cooldown counter on success

  // Reset test mode counter
  if (TEST_MODE) {
    testModeSuccessfulTrades = 0;
    console.log('🧪 TEST MODE: Trade counter reset after cooldown recovery');
  }

  console.log('✅ COOLDOWN MODE DEACTIVATED - Resuming normal trading');

  activeTraders.forEach(userId => {
    const userSession = userSessions.get(userId);
    if (userSession && bot) {
      safeSendMessage(userSession.chatId,
        `✅ COOLDOWN COMPLETE\n\n` +
        `All tokens sold successfully.\n` +
        `Rate limit has cooled down.\n` +
        `Resuming normal trading operations.`
      );
    }
  });

  return true;
}

async function testRateLimitRecovery(): Promise<boolean> {
  console.log('🧪 Testing rate limit recovery with dummy quote request...');

  try {
    // Make a simple test call to Jupiter
    const testUrl = `https://quote-api.jup.ag/v6/quote?inputMint=So11111111111111111111111111111111111111112&outputMint=EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v&amount=1000000&slippageBps=50`;

    const response = await fetch(testUrl, {
      headers: { 'Accept': 'application/json' }
    });

    if (response.ok) {
      console.log('✅ Test quote successful - rate limit has cooled');
      return true;
    } else if (response.status === 429) {
      console.log('❌ Test quote failed - still rate limited');
      return false;
    } else {
      console.log(`⚠️ Test quote returned ${response.status} - assuming cooled`);
      return true;
    }
  } catch (error: any) {
    const isRateLimit = error?.message?.includes('Rate limit') ||
      error?.message?.includes('429');

    if (isRateLimit) {
      console.log('❌ Test failed - still rate limited');
      return false;
    }

    console.log('⚠️ Test error (assuming network issue, proceeding):', error?.message);
    return true;
  }
}

function resetCooldownCounters() {
  rateLimitFailureCount = 0;
  lastFailureTime = 0;
}

// ==================== SELL QUEUE FUNCTIONS ====================

async function buildSellQueue(
  tradingWallets: { kp: Keypair; address: string; privateKey: string }[],
  baseMint: PublicKey,
  session: UserSession
): Promise<void> {
  console.log('📋 Building sell queue - checking all wallets for tokens...');
  sellQueue = [];

  for (let i = 0; i < tradingWallets.length; i++) {
    try {
      const wallet = tradingWallets[i].kp;
      const walletAddress = wallet.publicKey.toBase58();
      const shortWallet = walletAddress.substring(0, 6) + '...' + walletAddress.substring(walletAddress.length - 4);

      const tokenAccount = await getCachedTokenAccount(wallet.publicKey, baseMint);

      if (!tokenAccount) {
        console.log(`Wallet ${i + 1}: No token account`);
        continue;
      }

      const tokenBalInfo = await solanaConnection.getTokenAccountBalance(tokenAccount, 'confirmed');
      const tokenAmount = tokenBalInfo.value.uiAmount || 0;
      const tokenBalance = tokenBalInfo.value.amount;

      if (tokenAmount > 0) {
        console.log(`Wallet ${i + 1}: Has ${tokenAmount.toFixed(6)} tokens - adding to sell queue`);
        sellQueue.push({
          wallet,
          walletNumber: i + 1,
          shortWallet,
          tokenAmount,
          tokenBalance
        });
      } else {
        console.log(`Wallet ${i + 1}: No tokens`);
      }

      await sleep(500); // Small delay between checks
    } catch (error: any) {
      console.log(`Wallet ${i + 1}: Error checking tokens - ${error?.message}`);
    }
  }

  console.log(`📋 Sell queue built: ${sellQueue.length} wallets with tokens`);

  if (sellQueue.length > 0) {
    safeSendMessage(session.chatId,
      `📋 Cooldown Inventory Complete\n\n` +
      `Wallets with tokens: ${sellQueue.length}\n` +
      `Total wallets checked: ${tradingWallets.length}\n\n` +
      `Preparing to sell all tokens...`
    );
  }
}

async function processSellQueue(
  baseMint: PublicKey,
  poolId: PublicKey | undefined,
  session: UserSession
): Promise<void> {
  if (sellQueue.length === 0) {
    console.log('✅ No tokens to sell - queue empty');
    return;
  }

  isSellQueueProcessing = true;
  console.log(`🔄 Processing sell queue: ${sellQueue.length} wallets`);

  let successfulSells = 0;
  let failedSells = 0;

  for (let i = 0; i < sellQueue.length; i++) {
    const item = sellQueue[i];

    console.log(`\n[${i + 1}/${sellQueue.length}] Selling from wallet ${item.walletNumber}...`);
    console.log(`Amount: ${item.tokenAmount.toFixed(6)} tokens`);

    try {
      // Use high priority for cooldown sells
      let sellTx;
      if (SWAP_ROUTING) {
        sellTx = await getSellTxWithJupiter(item.wallet, baseMint, item.tokenBalance, true); // true = high priority
      } else if (poolId) {
        sellTx = await getSellTx(solanaConnection, item.wallet, baseMint, NATIVE_MINT, item.tokenBalance, poolId.toBase58());
      }

      if (!sellTx) {
        console.log(`❌ Failed to create sell tx for wallet ${item.walletNumber}`);
        failedSells++;
        await sleep(3000);
        continue;
      }

      const latestBlockhash = await solanaConnection.getLatestBlockhash('confirmed');
      const txSig = await execute(sellTx, latestBlockhash, true); // CHANGE false to true (skipPreflight)

      if (txSig) {
        console.log(`✅ Sell successful - Wallet ${item.walletNumber}: ${txSig}`);
        successfulSells++;

        session.tradingStats.totalSells++;
        session.tradingStats.successfulTxs++;
        saveSessions();
      } else {
        console.log(`❌ Sell execution failed - Wallet ${item.walletNumber}`);
        failedSells++;
      }

    } catch (error: any) {
      const errorMsg = error?.message || '';

      // Check if it's a "no route" error (token amount too small)
      if (errorMsg.includes('COULD_NOT_FIND_ANY_ROUTE') || errorMsg.includes('Could not find any route')) {
        console.log(`⚠️ Wallet ${item.walletNumber}: Token amount too small to sell (${item.tokenAmount.toFixed(6)} tokens)`);
        console.log(`   Skipping this wallet - dust amount not worth selling`);
        // Don't count as failure - this is expected for dust amounts
        continue;
      }

      console.error(`❌ Sell error - Wallet ${item.walletNumber}:`, errorMsg);
      failedSells++;
    }

    // Wait between sells to avoid rate limit
    await sleep(5000);
  }

  console.log(`\n✅ Sell queue processing complete!`);
  console.log(`Successful: ${successfulSells}, Failed: ${failedSells}`);

  safeSendMessage(session.chatId,
    `✅ Cooldown Sell Complete\n\n` +
    `Successful sells: ${successfulSells}\n` +
    `Failed sells: ${failedSells}\n` +
    `Total processed: ${sellQueue.length}\n\n` +
    `${failedSells > 0 ? '⚠️ Some sells failed - use Export Session to recover manually\n\n' : ''}` +
    `Preparing to resume trading...`
  );

  sellQueue = [];
  isSellQueueProcessing = false;
}

// ==================== END SELL QUEUE FUNCTIONS ====================

// ==================== END COOLDOWN FUNCTIONS ====================

// Volume calculation helper
function calculateVolumeEstimate(totalSOL: number, walletCount: number) {
  // Account for fees and minimum requirements
  const totalFees = (ADDITIONAL_FEE * walletCount) + 0.01; // Buffer
  const availableForTrading = Math.max(0, totalSOL - totalFees);

  // Calculate per wallet distribution
  const perWallet = availableForTrading / walletCount;

  // Estimate cycles (buy + sell = 1 cycle)
  const avgTrade = (BUY_UPPER_AMOUNT + BUY_LOWER_AMOUNT) / 2;
  const cyclesPerWallet = Math.floor(perWallet / (avgTrade + ADDITIONAL_FEE));

  // Volume calculation
  const minVolume = cyclesPerWallet * walletCount * BUY_LOWER_AMOUNT;
  const maxVolume = cyclesPerWallet * walletCount * BUY_UPPER_AMOUNT;
  const estimatedVolume = cyclesPerWallet * walletCount * avgTrade;

  return {
    minVolume: Math.max(0, minVolume),
    maxVolume: Math.max(0, maxVolume),
    estimatedVolume: Math.max(0, estimatedVolume),
    cyclesPerWallet,
    perWalletAmount: perWallet,
    totalFees,
    availableForTrading
  };
}

// Optimized balance check with caching
async function getCachedBalance(publicKey: PublicKey, forceRefresh = false): Promise<number> {
  const key = publicKey.toBase58();
  const cached = walletCacheMap.get(key);
  const now = Date.now();

  if (!forceRefresh && cached && (now - cached.lastBalanceCheck) < BALANCE_CACHE_TTL) {
    return cached.balance;
  }

  try {
    const balance = await solanaConnection.getBalance(publicKey, 'confirmed');
    const solBalance = balance / LAMPORTS_PER_SOL;

    if (!cached) {
      walletCacheMap.set(key, {
        tokenAccounts: new Map(),
        lastBalanceCheck: now,
        balance: solBalance
      });
    } else {
      cached.balance = solBalance;
      cached.lastBalanceCheck = now;
    }

    return solBalance;
  } catch (error) {
    console.error('Balance check error:', error);
    return cached?.balance || 0;
  }
}

// Batch balance checker for multiple wallets
async function batchCheckBalances(wallets: Keypair[]): Promise<Map<string, number>> {
  const balances = new Map<string, number>();
  const walletsToFetch: Keypair[] = [];
  const now = Date.now();

  for (const wallet of wallets) {
    const key = wallet.publicKey.toBase58();
    const cached = walletCacheMap.get(key);

    if (cached && (now - cached.lastBalanceCheck) < BALANCE_CACHE_TTL) {
      balances.set(key, cached.balance);
    } else {
      walletsToFetch.push(wallet);
    }
  }

  if (walletsToFetch.length > 0) {
    try {
      const publicKeys = walletsToFetch.map(w => w.publicKey);
      const accountInfos = await solanaConnection.getMultipleAccountsInfo(publicKeys, 'confirmed');

      accountInfos.forEach((info, index) => {
        const wallet = walletsToFetch[index];
        const key = wallet.publicKey.toBase58();
        const balance = info ? info.lamports / LAMPORTS_PER_SOL : 0;

        balances.set(key, balance);

        const cached = walletCacheMap.get(key);
        if (cached) {
          cached.balance = balance;
          cached.lastBalanceCheck = now;
        } else {
          walletCacheMap.set(key, {
            tokenAccounts: new Map(),
            lastBalanceCheck: now,
            balance
          });
        }
      });
    } catch (error) {
      console.error('Batch balance check error:', error);
      for (const wallet of walletsToFetch) {
        try {
          const balance = await getCachedBalance(wallet.publicKey, true);
          balances.set(wallet.publicKey.toBase58(), balance);
        } catch (err) {
          balances.set(wallet.publicKey.toBase58(), 0);
        }
      }
    }
  }

  return balances;
}

// Optimized token account fetching with caching
async function getCachedTokenAccount(
  wallet: PublicKey,
  mint: PublicKey,
  forceRefresh = false
): Promise<PublicKey | null> {
  const walletKey = wallet.toBase58();
  const mintKey = mint.toBase58();
  const cached = walletCacheMap.get(walletKey);
  const now = Date.now();

  if (!forceRefresh && cached) {
    const cachedAccount = cached.tokenAccounts.get(mintKey);
    if (cachedAccount && (now - cachedAccount.lastUpdated) < TOKEN_ACCOUNT_CACHE_TTL) {
      return new PublicKey(cachedAccount.address);
    }
  }

  try {
    const tokenAccount = await getAssociatedTokenAddress(mint, wallet);

    if (!cached) {
      walletCacheMap.set(walletKey, {
        tokenAccounts: new Map([[mintKey, {
          address: tokenAccount.toBase58(),
          mint: mintKey,
          owner: walletKey,
          lastUpdated: now
        }]]),
        lastBalanceCheck: 0,
        balance: 0
      });
    } else {
      cached.tokenAccounts.set(mintKey, {
        address: tokenAccount.toBase58(),
        mint: mintKey,
        owner: walletKey,
        lastUpdated: now
      });
    }

    return tokenAccount;
  } catch (error) {
    console.error('Token account fetch error:', error);
    return null;
  }
}

function ensureSessionsDirectory() {
  if (!fs.existsSync(SESSIONS_DIR)) {
    fs.mkdirSync(SESSIONS_DIR, { recursive: true });
  }
}

function safeSendMessage(chatId: number, message: string, options?: any) {
  if (bot) {
    return bot.sendMessage(chatId, message, options);
  }
  console.log('Bot not initialized, message:', message);
}

function loadSessions() {
  try {
    if (fs.existsSync(SESSION_FILE)) {
      const data = fs.readFileSync(SESSION_FILE, 'utf8');
      const sessions = JSON.parse(data);
      Object.entries(sessions).forEach(([userId, session]: [string, any]) => {
        session.botRunning = false;
        session.status = session.status === 'trading' ? 'stopped' : session.status;
        userSessions.set(parseInt(userId), session);
      });
      console.log(`Loaded ${Object.keys(sessions).length} user sessions`);
    }
  } catch (error) {
    console.error('Error loading sessions:', error);
  }
}

function saveIndividualUserSession(userId: number, session: UserSession) {
  ensureSessionsDirectory();

  const userSessionFile = path.join(SESSIONS_DIR, `user_${userId}.json`);

  const allTradingWallets: TradingWallet[] = [];

  if (session.tradingWallets && session.tradingWallets.length > 0) {
    allTradingWallets.push(...session.tradingWallets);
  }

  if (session.tradingWalletsHistory && session.tradingWalletsHistory.length > 0) {
    session.tradingWalletsHistory.forEach(batch => {
      allTradingWallets.push(...batch);
    });
  }

  const uniqueWallets = Array.from(
    new Map(allTradingWallets.map(w => [w.address, w])).values()
  );

  const exportData = {
    userId: session.userId,
    chatId: session.chatId,
    createdAt: session.createdAt,
    lastUpdated: Date.now(),

    mainWallet: {
      address: session.depositAddress,
      privateKey: session.userWalletPrivateKey,
      instructions: "SAVE THIS PRIVATE KEY SECURELY! You can import this wallet into Phantom, Solflare, or any Solana wallet."
    },

    paymentWallet: {
      address: session.paymentWallet,
      privateKey: session.paymentWalletPrivateKey,
      note: "This was your payment wallet. Usually empty after payment processed."
    },

    currentTradingWallets: session.tradingWallets.map((w, idx) => ({
      number: idx + 1,
      address: w.address,
      privateKey: w.privateKey,
      status: "ACTIVE",
      note: "Currently being used for volume trading"
    })),

    allTradingWallets: uniqueWallets.map((w, idx) => ({
      number: idx + 1,
      address: w.address,
      privateKey: w.privateKey,
      note: "Import this to recover any remaining SOL"
    })),

    tradingWalletSummary: {
      currentActive: session.tradingWallets.length,
      totalEverCreated: uniqueWallets.length,
      historicalBatches: session.tradingWalletsHistory?.length || 0
    },

    token: {
      address: session.tokenAddress,
      name: session.tokenName,
      symbol: session.tokenSymbol
    },

    statistics: session.tradingStats,

    status: {
      botRunning: session.botRunning,
      currentStatus: session.status,
      hasPaid: session.hasPaid,
      paymentConfirmed: session.paymentConfirmed
    },

    warnings: [
      "NEVER share your private keys with anyone!",
      "The bot creator/admin NEVER needs your private keys",
      "Keep this file secure and backed up",
      "You can use these keys to recover your wallets in any Solana wallet app",
      `IMPORTANT: You have ${uniqueWallets.length} total trading wallets created. Check ALL of them for remaining SOL!`
    ],

    recoveryInstructions: {
      mainWallet: "Import the mainWallet privateKey into Phantom/Solflare to access your main funds",
      tradingWallets: `You have ${uniqueWallets.length} trading wallets. Import each privateKey from 'allTradingWallets' to recover any remaining SOL`,
      gathering: "Use the 'Withdraw SOL' function in the bot to automatically gather all funds from ALL wallets (current + old), or import each wallet manually",
      checkingBalances: "Import each wallet private key into a Solana wallet to check if there's any SOL remaining"
    }
  };

  try {
    fs.writeFileSync(userSessionFile, JSON.stringify(exportData, null, 2));
    console.log(`Saved individual session for user ${userId} with ${uniqueWallets.length} total trading wallets`);
    return userSessionFile;
  } catch (error) {
    console.error(`Error saving individual session for user ${userId}:`, error);
    return null;
  }
}

function saveSessions() {
  try {
    const sessionsObj: Record<string, UserSession> = {};
    userSessions.forEach((session, userId) => {
      sessionsObj[userId.toString()] = session;

      if (session.walletKeypair) {
        saveIndividualUserSession(userId, session);
      }
    });

    fs.writeFileSync(SESSION_FILE, JSON.stringify(sessionsObj, null, 2));
  } catch (error) {
    console.error('Error saving sessions:', error);
  }
}

function saveAfterCriticalOperation(session: UserSession) {
  userSessions.set(session.userId, session);
  saveSessions();
}

function validateAndFixSession(session: UserSession): boolean {
  let needsFix = false;

  if (session.walletKeypair && !session.userWalletPrivateKey) {
    console.log(`User ${session.userId}: walletKeypair exists but userWalletPrivateKey is missing`);

    try {
      const keypair = Keypair.fromSecretKey(base58.decode(session.walletKeypair));
      session.userWalletPrivateKey = base58.encode(keypair.secretKey);
      session.depositAddress = keypair.publicKey.toBase58();
      needsFix = true;
      console.log(`User ${session.userId}: Recovered private key from walletKeypair`);
    } catch (error) {
      console.error(`User ${session.userId}: Failed to recover private key:`, error);
      session.walletKeypair = undefined;
      session.depositAddress = undefined;
      session.userWalletPrivateKey = undefined;
      needsFix = true;
    }
  }

  if (session.depositAddress && !session.walletKeypair) {
    console.log(`User ${session.userId}: depositAddress exists but walletKeypair is missing - wallet is LOST`);
    session.depositAddress = undefined;
    needsFix = true;
  }

  if (needsFix) {
    userSessions.set(session.userId, session);
    saveSessions();
  }

  return needsFix;
}

function getUserSession(userId: number, chatId: number): UserSession {
  let session = userSessions.get(userId);

  if (!session) {
    const paymentWallet = Keypair.generate();

    session = {
      userId,
      chatId,
      status: 'idle',
      requiredDeposit: 0.02,
      isMonitoring: false,
      botRunning: false,
      tradingWallets: [],
      tradingWalletsHistory: [],
      createdAt: Date.now(),
      tradingStats: {
        totalBuys: 0,
        totalSells: 0,
        totalVolumeSOL: 0,
        successfulTxs: 0,
        failedTxs: 0,
        startTime: 0,
        lastActivity: 0,
        lastUpdateSent: 0
      },
      hasPaid: true,
      paymentWallet: paymentWallet.publicKey.toBase58(),
      paymentWalletPrivateKey: base58.encode(paymentWallet.secretKey),
      paymentAmount: PAYMENT_AMOUNT,
      paymentConfirmed: true,
      selectedWalletCount: 1,
      distributionConfig: {
        mode: 'auto'
      },
      buyAmountConfig: {          // ADD THIS
        mode: 'default'            // ADD THIS
      }                            // ADD THIS
    };

    userSessions.set(userId, session);
    saveSessions();

    console.log(`Created new session for user ${userId}`);
  } else {
    if (!session.tradingWalletsHistory) {
      session.tradingWalletsHistory = [];

      if (session.tradingWallets && session.tradingWallets.length > 0) {
        session.tradingWalletsHistory.push([...session.tradingWallets]);
      }

      userSessions.set(userId, session);
      saveSessions();
    }

    // Initialize distribution config if missing
    if (!session.distributionConfig) {
      session.distributionConfig = { mode: 'auto' };
      userSessions.set(userId, session);
      saveSessions();
    }

    // Initialize buy amount config if missing
    if (!session.buyAmountConfig) {
      session.buyAmountConfig = { mode: 'default' };
      userSessions.set(userId, session);
      saveSessions();
    }

    validateAndFixSession(session);
  }

  if (!session.paymentWallet) {
    const paymentWallet = Keypair.generate();
    session.paymentWallet = paymentWallet.publicKey.toBase58();
    session.paymentWalletPrivateKey = base58.encode(paymentWallet.secretKey);
    userSessions.set(userId, session);
    saveSessions();
  }

  return session;
}

function getWalletSelectionKeyboard() {
  return {
    reply_markup: {
      inline_keyboard: [
        [
          { text: '1 Wallet', callback_data: 'select_wallets_1' },
          { text: '6 Wallets', callback_data: 'select_wallets_6' }
        ],
        [
          { text: '8 Wallets', callback_data: 'select_wallets_8' },
          { text: '10 Wallets', callback_data: 'select_wallets_10' }
        ],
        [
          { text: '12 Wallets (Max)', callback_data: 'select_wallets_12' }
        ],
        [
          { text: 'Back to Menu', callback_data: 'back_to_menu' }
        ]
      ]
    }
  };
}

function getDistributionModeKeyboard() {
  return {
    reply_markup: {
      inline_keyboard: [
        [
          { text: 'Auto (Distribute All)', callback_data: 'dist_mode_auto' }
        ],
        [
          { text: 'Custom Amount', callback_data: 'dist_mode_custom' }
        ],
        [
          { text: 'Back to Menu', callback_data: 'back_to_menu' }
        ]
      ]
    }
  };
}

function getBuyAmountKeyboard() {
  return {
    reply_markup: {
      inline_keyboard: [
        [
          { text: 'Default (Random Range)', callback_data: 'buy_amount_default' }
        ],
        [
          { text: 'Set Custom Amount', callback_data: 'buy_amount_custom' }
        ],
        [
          { text: 'Back to Menu', callback_data: 'back_to_menu' }
        ]
      ]
    }
  };
}

async function checkPaymentStatus(session: UserSession): Promise<boolean> {
  if (!session.paymentWalletPrivateKey) return false;

  try {
    const paymentKeypair = Keypair.fromSecretKey(base58.decode(session.paymentWalletPrivateKey));
    const balance = await solanaConnection.getBalance(paymentKeypair.publicKey);
    const solBalance = balance / LAMPORTS_PER_SOL;

    return solBalance >= PAYMENT_AMOUNT;
  } catch (error) {
    console.error('Error checking payment status:', error);
    return false;
  }
}

async function monitorPayment(userId: number) {
  const session = userSessions.get(userId);
  if (!session || !session.paymentWalletPrivateKey) return;

  let lastBalance = 0;
  let checkCount = 0;
  const maxChecks = 240;

  const checkPayment = setInterval(async () => {
    try {
      checkCount++;
      const currentSession = userSessions.get(userId);
      if (!currentSession || currentSession.paymentConfirmed || checkCount > maxChecks) {
        clearInterval(checkPayment);
        return;
      }

      const paymentKeypair = Keypair.fromSecretKey(base58.decode(currentSession.paymentWalletPrivateKey!));
      const balance = await solanaConnection.getBalance(paymentKeypair.publicKey);
      const solBalance = balance / LAMPORTS_PER_SOL;

      if (solBalance >= PAYMENT_AMOUNT && solBalance > lastBalance) {
        currentSession.hasPaid = true;
        currentSession.paymentConfirmed = true;
        if (currentSession.status === 'payment_pending') {
          currentSession.status = 'payment_confirmed';
        }
        userSessions.set(userId, currentSession);
        saveSessions();

        const message =
          `Payment Confirmed!\n\n` +
          `Received: ${solBalance.toFixed(6)} SOL\n` +
          `Thank you for your payment!\n\n` +
          `You now have full access to the Volume Bot!\n` +
          `Use the menu below to get started:`;

        safeSendMessage(currentSession.chatId, message, getMainMenuKeyboard(true));

        // Auto-transfer payment to admin wallet
        console.log(`Transferring payment to admin for user ${userId}`);
        await sleep(3000); // Wait for confirmation
        const transferred = await transferPaymentToAdmin(currentSession);

        if (transferred) {
          console.log(`Payment successfully transferred to admin from user ${userId}`);
        } else {
          console.log(`Payment transfer to admin failed for user ${userId} - manual collection needed`);
        }

        clearInterval(checkPayment);
      }

      lastBalance = solBalance;

    } catch (error) {
      console.error('Payment monitoring error:', error);
    }
  }, 15000);
}

async function transferPaymentToAdmin(session: UserSession): Promise<boolean> {
  if (!session.paymentWalletPrivateKey || !ADMIN_PAYMENT_WALLET) {
    console.log('Missing payment wallet or admin wallet configuration');
    return false;
  }

  try {
    const paymentKeypair = Keypair.fromSecretKey(base58.decode(session.paymentWalletPrivateKey));
    const adminKeypair = Keypair.fromSecretKey(base58.decode(ADMIN_PAYMENT_WALLET));

    const balance = await solanaConnection.getBalance(paymentKeypair.publicKey);

    if (balance === 0) {
      console.log('Payment wallet is empty, nothing to transfer');
      return false;
    }

    // Keep minimum for rent, transfer rest to admin
    const rent = await solanaConnection.getMinimumBalanceForRentExemption(0);
    const txFee = 10000;
    const transferAmount = balance - rent - txFee;

    if (transferAmount <= 0) {
      console.log('Insufficient balance after fees');
      return false;
    }

    const transaction = new Transaction().add(
      ComputeBudgetProgram.setComputeUnitPrice({ microLamports: 100_000 }),
      ComputeBudgetProgram.setComputeUnitLimit({ units: 40_000 }),
      SystemProgram.transfer({
        fromPubkey: paymentKeypair.publicKey,
        toPubkey: adminKeypair.publicKey,
        lamports: transferAmount
      })
    );

    const latestBlockhash = await solanaConnection.getLatestBlockhash();
    transaction.recentBlockhash = latestBlockhash.blockhash;
    transaction.feePayer = paymentKeypair.publicKey;

    const messageV0 = new TransactionMessage({
      payerKey: paymentKeypair.publicKey,
      recentBlockhash: latestBlockhash.blockhash,
      instructions: transaction.instructions,
    }).compileToV0Message();

    const versionedTx = new VersionedTransaction(messageV0);
    versionedTx.sign([paymentKeypair]);

    const sig = await execute(versionedTx, latestBlockhash);

    if (sig) {
      console.log(`Payment transferred to admin: ${(transferAmount / LAMPORTS_PER_SOL).toFixed(6)} SOL, TX: ${sig}`);
      return true;
    }

    return false;

  } catch (error: any) {
    console.error('Payment transfer error:', error);
    return false;
  }
}

async function fetchTokenInfo(tokenAddress: string): Promise<{ name: string, symbol: string }> {
  const defaultInfo = { name: 'Unknown Token', symbol: 'UNKNOWN' };

  try {
    try {
      const dexResponse = await fetch(`https://api.dexscreener.com/latest/dex/tokens/${tokenAddress}`);
      if (dexResponse.ok) {
        const dexData: any = await dexResponse.json();
        if (dexData.pairs && dexData.pairs.length > 0) {
          for (const pair of dexData.pairs) {
            if (pair.baseToken && pair.baseToken.address === tokenAddress) {
              if (pair.baseToken.name && pair.baseToken.symbol) {
                return {
                  name: pair.baseToken.name,
                  symbol: pair.baseToken.symbol
                };
              }
            }
          }
        }
      }
    } catch (e) {
      // Continue to next method
    }

    try {
      const response = await fetch('https://tokens.jup.ag/all');
      if (response.ok) {
        const tokens: any = await response.json();
        const token = tokens.find((t: any) => t.address === tokenAddress);
        if (token && token.name && token.symbol) {
          return { name: token.name, symbol: token.symbol };
        }
      }
    } catch (e) {
      // Continue
    }

    return defaultInfo;

  } catch (error: any) {
    return defaultInfo;
  }
}

function getMainMenuKeyboard(isPaid: boolean = false) {
  // Payment parameter kept for compatibility but not used anymore
  return {
    reply_markup: {
      inline_keyboard: [
        [
          { text: 'Create Wallet', callback_data: 'create_wallet' },
          { text: 'Check Balance', callback_data: 'check_balance' }
        ],
        [
          { text: 'Add Token', callback_data: 'add_token' },
          { text: 'Select Wallets', callback_data: 'select_wallet_count' }
        ],
        [
          { text: 'Distribution Mode', callback_data: 'distribution_mode' },
          { text: 'Set Buy Amount', callback_data: 'buy_amount_settings' }
        ],
        [
          { text: 'Start Volume', callback_data: 'start_volume' },
          { text: 'Stop Volume', callback_data: 'stop_volume' }
        ],
        [
          { text: 'Status Report', callback_data: 'status_report' },
          { text: 'Withdraw SOL', callback_data: 'withdraw_sol' }
        ],
        [
          { text: 'Show Private Key', callback_data: 'show_main_key' },
          { text: 'Export Session', callback_data: 'export_session' }
        ]
      ]
    }
  };
}

function requirePayment(session: UserSession, chatId: number, action: string): boolean {
  // Free access - always return true
  return true;
}

function getTradingControlsKeyboard(isTrading: boolean) {
  if (isTrading) {
    return {
      reply_markup: {
        inline_keyboard: [
          [
            { text: 'Stop Trading', callback_data: 'stop_volume' },
            { text: 'Live Stats', callback_data: 'live_stats' }
          ],
          [
            { text: 'Back to Menu', callback_data: 'back_to_menu' }
          ]
        ]
      }
    };
  } else {
    return {
      reply_markup: {
        inline_keyboard: [
          [
            { text: 'Start Trading', callback_data: 'start_volume' },
            { text: 'Status Report', callback_data: 'status_report' }
          ],
          [
            { text: 'Back to Menu', callback_data: 'back_to_menu' }
          ]
        ]
      }
    };
  }
}

function sendTradingNotification(session: UserSession, type: 'buy' | 'sell' | 'error', data: any) {
  try {
    let message = '';

    if (type === 'buy' && data.success) {
      message = `BUY EXECUTED\n\n` +
        `Amount: ${data.amount} SOL\n` +
        `Wallet: ${data.wallet}\n` +
        `Token: ${session.tokenSymbol}\n` +
        `TX: https://solscan.io/tx/${data.signature}\n` +
        `Total Buys: ${session.tradingStats.totalBuys}`;
    } else if (type === 'sell' && data.success) {
      message = `SELL EXECUTED\n\n` +
        `Tokens: ${data.tokenAmount} ${session.tokenSymbol}\n` +
        `Wallet: ${data.wallet}\n` +
        `TX: https://solscan.io/tx/${data.signature}\n` +
        `Total Sells: ${session.tradingStats.totalSells}`;
    } else if (type === 'error') {
      message = `TRANSACTION FAILED\n\n` +
        `Type: ${data.type}\n` +
        `Wallet: ${data.wallet}\n` +
        `Error: ${data.error}\n` +
        `Failed TXs: ${session.tradingStats.failedTxs}`;
    }

    if (message) {
      safeSendMessage(session.chatId, message, {
        disable_web_page_preview: true,
        ...getTradingControlsKeyboard(session.botRunning)
      });
    }
  } catch (error) {
    console.error('Error sending notification:', error);
  }
}

function sendPeriodicUpdate(session: UserSession) {
  const stats = session.tradingStats;
  const runtime = (Date.now() - stats.startTime) / 1000 / 60;

  const message =
    `TRADING UPDATE - ${session.tokenSymbol}\n\n` +
    `Successful Buys: ${stats.totalBuys}\n` +
    `Successful Sells: ${stats.totalSells}\n` +
    `Failed TXs: ${stats.failedTxs}\n` +
    `Volume Generated: ${stats.totalVolumeSOL.toFixed(4)} SOL\n` +
    `Runtime: ${runtime.toFixed(1)} minutes\n` +
    `Success Rate: ${stats.successfulTxs > 0 ? ((stats.successfulTxs / (stats.successfulTxs + stats.failedTxs)) * 100).toFixed(1) : 0}%\n\n` +
    `Status: ACTIVE`;

  safeSendMessage(session.chatId, message, getTradingControlsKeyboard(true));

  stats.lastUpdateSent = Date.now();
  saveSessions();
}

// Continue in next message due to length...

// Continuation of index.ts - Handler functions

async function handleVolumeCalculator(userId: number, chatId: number, messageId?: number) {
  try {
    const session = getUserSession(userId, chatId);

    if (!session.walletKeypair) {
      const message = 'No wallet found. Create one first!';
      if (messageId && bot) {
        bot.editMessageText(message, {
          chat_id: chatId,
          message_id: messageId,
          reply_markup: {
            inline_keyboard: [
              [{ text: 'Create Wallet', callback_data: 'create_wallet' }],
              [{ text: 'Back to Menu', callback_data: 'back_to_menu' }]
            ]
          }
        });
      }
      return;
    }

    const keypair = Keypair.fromSecretKey(base58.decode(session.walletKeypair));
    const balance = await solanaConnection.getBalance(keypair.publicKey);
    const solBalance = balance / LAMPORTS_PER_SOL;

    const walletCount = session.selectedWalletCount;
    const calc = calculateVolumeEstimate(solBalance, walletCount);

    const distMode = session.distributionConfig.mode;
    const customAmount = session.distributionConfig.amountPerWallet;

    let distributionInfo = '';
    if (distMode === 'custom' && customAmount) {
      const totalToDistribute = customAmount * walletCount;
      const customCalc = calculateVolumeEstimate(totalToDistribute, walletCount);
      distributionInfo =
        `\nDistribution Mode: CUSTOM\n` +
        `Amount per wallet: ${customAmount.toFixed(4)} SOL\n` +
        `Total to distribute: ${totalToDistribute.toFixed(4)} SOL\n` +
        `Remaining in main: ${(solBalance - totalToDistribute).toFixed(4)} SOL\n\n` +
        `ESTIMATED VOLUME (Custom):\n` +
        `Min: ${customCalc.minVolume.toFixed(4)} SOL\n` +
        `Est: ${customCalc.estimatedVolume.toFixed(4)} SOL\n` +
        `Max: ${customCalc.maxVolume.toFixed(4)} SOL\n` +
        `Cycles per wallet: ~${customCalc.cyclesPerWallet}`;
    } else {
      distributionInfo =
        `\nDistribution Mode: AUTO (All balance)\n` +
        `Total balance: ${solBalance.toFixed(4)} SOL\n` +
        `Per wallet: ${calc.perWalletAmount.toFixed(4)} SOL\n\n` +
        `ESTIMATED VOLUME (Auto):\n` +
        `Min: ${calc.minVolume.toFixed(4)} SOL\n` +
        `Est: ${calc.estimatedVolume.toFixed(4)} SOL\n` +
        `Max: ${calc.maxVolume.toFixed(4)} SOL\n` +
        `Cycles per wallet: ~${calc.cyclesPerWallet}`;
    }

    const message =
      `VOLUME CALCULATOR\n\n` +
      `Main Wallet Balance: ${solBalance.toFixed(4)} SOL\n` +
      `Selected Wallets: ${walletCount}\n` +
      distributionInfo + `\n\n` +
      `Fees: ${calc.totalFees.toFixed(4)} SOL\n` +
      `Trade Range: ${BUY_LOWER_AMOUNT}-${BUY_UPPER_AMOUNT} SOL\n\n` +
      `Note: Actual volume may vary based on\n` +
      `market conditions and trade success rate.`;

    if (messageId && bot) {
      bot.editMessageText(message, {
        chat_id: chatId,
        message_id: messageId,
        reply_markup: {
          inline_keyboard: [
            [
              { text: 'Change Mode', callback_data: 'distribution_mode' },
              { text: 'Select Wallets', callback_data: 'select_wallet_count' }
            ],
            [
              { text: 'Refresh', callback_data: 'volume_calculator' }
            ],
            [
              { text: 'Back to Menu', callback_data: 'back_to_menu' }
            ]
          ]
        }
      });
    }
  } catch (error: any) {
    console.error('Volume calculator error:', error);
    safeSendMessage(chatId, `Error calculating volume: ${error?.message || 'Unknown error'}`);
  }
}

async function handleDistributionMode(userId: number, chatId: number, messageId?: number) {
  try {
    const session = getUserSession(userId, chatId);

    const currentMode = session.distributionConfig.mode;
    const customAmount = session.distributionConfig.amountPerWallet;

    const message =
      `DISTRIBUTION MODE\n\n` +
      `Current Mode: ${currentMode.toUpperCase()}\n` +
      (customAmount ? `Custom Amount: ${customAmount.toFixed(4)} SOL per wallet\n\n` : '\n') +
      `AUTO MODE:\n` +
      `Distributes ALL your wallet balance\n` +
      `equally across selected wallets.\n\n` +
      `CUSTOM MODE:\n` +
      `You specify exact amount per wallet.\n` +
      `Remaining SOL stays in main wallet.\n\n` +
      `Select distribution mode:`;

    if (messageId && bot) {
      bot.editMessageText(message, {
        chat_id: chatId,
        message_id: messageId,
        ...getDistributionModeKeyboard()
      });
    }
  } catch (error: any) {
    console.error('Distribution mode error:', error);
    safeSendMessage(chatId, 'Error displaying distribution mode.');
  }
}

async function handleBuyAmountSettings(userId: number, chatId: number, messageId?: number) {
  try {
    const session = getUserSession(userId, chatId);

    const currentMode = session.buyAmountConfig.mode;
    const customAmount = session.buyAmountConfig.customAmount;

    let modeDescription = '';
    if (currentMode === 'default') {
      modeDescription = `DEFAULT MODE (Active)\n` +
        `Random: ${BUY_LOWER_AMOUNT} - ${BUY_UPPER_AMOUNT} SOL per buy\n` +
        `Uses constants from config\n\n`;
    } else {
      modeDescription = `CUSTOM MODE (Active)\n` +
        `Target: ${customAmount?.toFixed(4)} SOL per buy\n` +
        `Actual: ${(customAmount! * 0.7).toFixed(4)} - ${(customAmount! * 1.3).toFixed(4)} SOL\n` +
        `(±30% randomization)\n\n`;
    }

    const message =
      `🎯 BUY AMOUNT SETTINGS\n\n` +
      modeDescription +
      `DEFAULT MODE:\n` +
      `• Uses config values (${BUY_LOWER_AMOUNT}-${BUY_UPPER_AMOUNT} SOL)\n` +
      `• Standard randomization\n` +
      `• Good for testing\n\n` +
      `CUSTOM MODE:\n` +
      `• Set your preferred buy amount\n` +
      `• Bot randomizes ±30% around your value\n` +
      `• Example: 0.5 SOL → buys 0.35-0.65 SOL\n` +
      `• More realistic trading patterns\n\n` +
      `Select mode:`;

    if (messageId && bot) {
      bot.editMessageText(message, {
        chat_id: chatId,
        message_id: messageId,
        ...getBuyAmountKeyboard()
      });
    }
  } catch (error: any) {
    console.error('Buy amount settings error:', error);
    safeSendMessage(chatId, 'Error displaying buy amount settings.');
  }
}

async function
  handleExportSession(userId: number, chatId: number, messageId?: number) {
  try {
    const session = getUserSession(userId, chatId);

    if (!session.walletKeypair) {
      const message = 'No wallet found. Create a wallet first!';
      if (messageId && bot) {
        bot.editMessageText(message, {
          chat_id: chatId,
          message_id: messageId,
          reply_markup: {
            inline_keyboard: [
              [{ text: 'Create Wallet', callback_data: 'create_wallet' }],
              [{ text: 'Back to Menu', callback_data: 'back_to_menu' }]
            ]
          }
        });
      }
      return;
    }

    const sessionFile = saveIndividualUserSession(userId, session);

    if (!sessionFile) {
      safeSendMessage(chatId, 'Error creating session file. Please try again.');
      return;
    }

    const message =
      `Your Session Export\n\n` +
      `CRITICAL SECURITY WARNING:\n` +
      `This file contains ALL your private keys\n` +
      `NEVER share this file with anyone\n` +
      `Store it securely offline\n` +
      `You can use it to recover your wallets\n\n` +
      `Downloading your session file...\n\n` +
      `The file contains:\n` +
      `Main wallet private key\n` +
      `All trading wallet keys (${session.tradingWallets.length} active)\n` +
      `Token information\n` +
      `Trading statistics\n` +
      `Recovery instructions`;

    session.lastExportTime = Date.now();
    userSessions.set(userId, session);
    saveSessions();

    if (bot) {
      await bot.sendDocument(chatId, sessionFile, {
        caption: message,
        reply_markup: {
          inline_keyboard: [
            [{ text: 'Back to Menu', callback_data: 'back_to_menu' }]
          ]
        }
      });

      setTimeout(() => {
        safeSendMessage(chatId,
          `Security Reminder:\n\n` +
          `Delete the downloaded file from Telegram after you've saved it securely.\n\n` +
          `To delete: Long press the file → Delete`
        );
      }, 2000);
    }

  } catch (error: any) {
    console.error('Export session error:', error);
    safeSendMessage(chatId, `Error exporting session: ${error?.message || 'Unknown error'}`);
  }
}

async function handleShowMainKey(userId: number, chatId: number, messageId?: number) {
  try {
    const session = getUserSession(userId, chatId);

    if (!session.walletKeypair || !session.userWalletPrivateKey) {
      const message = 'No wallet found. Create a wallet first!';
      if (messageId && bot) {
        bot.editMessageText(message, {
          chat_id: chatId,
          message_id: messageId,
          reply_markup: {
            inline_keyboard: [
              [{ text: 'Create Wallet', callback_data: 'create_wallet' }],
              [{ text: 'Back to Menu', callback_data: 'back_to_menu' }]
            ]
          }
        });
      }
      return;
    }

    const message =
      `Your Main Wallet Private Key\n\n` +
      `Address:\n\`${session.depositAddress}\`\n\n` +
      `Private Key:\n\`${session.userWalletPrivateKey}\`\n\n` +
      `NEVER share this key with anyone!\n` +
      `You can import this into any Solana wallet\n\n` +
      `To import into Phantom:\n` +
      `1. Open Phantom wallet\n` +
      `2. Settings → Add/Connect Wallet\n` +
      `3. Import Private Key\n` +
      `4. Paste your private key\n\n` +
      `This message will be deleted in 60 seconds for security.`;

    if (messageId && bot) {
      await bot.editMessageText(message, {
        chat_id: chatId,
        message_id: messageId,
        parse_mode: 'Markdown',
        reply_markup: {
          inline_keyboard: [
            [
              { text: 'Export Full Session', callback_data: 'export_session' },
              { text: 'Check Balance', callback_data: 'check_balance' }
            ],
            [
              { text: 'Back to Menu', callback_data: 'back_to_menu' }
            ]
          ]
        }
      });

      setTimeout(() => {
        if (bot && messageId) {
          bot.deleteMessage(chatId, messageId).catch(() => { });

          safeSendMessage(chatId,
            'Private key message deleted for security.\n\n' +
            'Use "Export Session" if you need to save it permanently.',
            getMainMenuKeyboard(true)
          );
        }
      }, 60000);
    }

  } catch (error: any) {
    console.error('Show main key error:', error);
    safeSendMessage(chatId, `Error displaying key: ${error?.message || 'Unknown error'}`);
  }
}

async function handleCreateWallet(userId: number, chatId: number, messageId?: number) {
  try {
    const session = getUserSession(userId, chatId);

    if (!session.walletKeypair) {
      const newKeypair = Keypair.generate();
      const privateKey = base58.encode(newKeypair.secretKey);
      const address = newKeypair.publicKey.toBase58();

      session.walletKeypair = privateKey;
      session.userWalletPrivateKey = privateKey;
      session.depositAddress = address;
      session.status = 'wallet_created';

      userSessions.set(userId, session);
      saveSessions();

      const verifySession = userSessions.get(userId);
      if (!verifySession?.userWalletPrivateKey) {
        throw new Error('Failed to save private key - please try again');
      }

      console.log(`Created wallet for user ${userId}: ${address}`);

      const message =
        `✅ Trading Wallet Created!\n\n` +
        `🔹 Your Trading Address:\n` +
        `\`${address}\`\n\n` +
        `🔑 Your Private Key:\n` +
        `\`${privateKey}\`\n\n` +
        `⚠️ IMPORTANT: Your wallet keys are *temporarily saved for this session only* and are *not stored permanently* on our servers.\n\n` +
        `Please back up your private key safely — losing it means permanent loss of access to your funds.\n\n` +
        `If your session remains active, you can use the "Export Session" button to download your wallet details.\n\n` +
        `You fully own this wallet and can import it anywhere.\n\n` +
        `💰 Minimum Deposit: ${session.requiredDeposit} SOL\n` +
        `💡 Recommended: 0.5–1.0 SOL\n\n` +
        `This message will auto-delete in 120 seconds for your security.`;

      if (messageId && bot) {
        await bot.editMessageText(message, {
          chat_id: chatId,
          message_id: messageId,
          parse_mode: 'Markdown',
          reply_markup: {
            inline_keyboard: [
              [
                { text: 'Check Balance', callback_data: 'check_balance' },
                { text: 'Add Token', callback_data: 'add_token' }
              ],
              [
                { text: 'Export Session', callback_data: 'export_session' },
                { text: 'Show Key Again', callback_data: 'show_main_key' }
              ],
              [
                { text: 'Back to Menu', callback_data: 'back_to_menu' }
              ]
            ]
          }
        });

        setTimeout(() => {
          if (bot) {
            bot.deleteMessage(chatId, messageId).catch(() => { });
            safeSendMessage(chatId,
              '⚠️ Wallet creation message deleted for security.\n\n' +
              'Your private key is *not stored* on our servers. You are solely responsible for keeping it safe.\n\n' +
              'Please back up your key immediately — once lost, it cannot be recovered.\n\n' +
              'If your session is still active, you can use "Export Session" to retrieve your wallet details.',
              getMainMenuKeyboard(true)
            );
          }
        }, 120000);
      }

      if (!session.isMonitoring) {
        session.isMonitoring = true;
        monitorDeposits(userId);
      }
    } else {
      validateAndFixSession(session);

      if (!session.userWalletPrivateKey) {
        const message =
          `Wallet Recovery Issue Detected\n\n` +
          `Your wallet address exists but the private key was not saved properly.\n\n` +
          `Address: \`${session.depositAddress}\`\n` +
          `Private Key: NOT RECOVERABLE\n\n` +
          `This wallet cannot be accessed anymore.\n` +
          `If there are funds in it, they are lost.\n\n` +
          `Would you like to create a NEW wallet?`;

        if (messageId && bot) {
          bot.editMessageText(message, {
            chat_id: chatId,
            message_id: messageId,
            parse_mode: 'Markdown',
            reply_markup: {
              inline_keyboard: [
                [
                  { text: 'Create New Wallet', callback_data: 'force_new_wallet' }
                ],
                [
                  { text: 'Back to Menu', callback_data: 'back_to_menu' }
                ]
              ]
            }
          });
        }
      } else {
        const message =
          `You already have a trading wallet!\n\n` +
          `Address: \`${session.depositAddress}\`\n\n` +
          `Use the buttons below to view your private key or export full session:`;

        if (messageId && bot) {
          bot.editMessageText(message, {
            chat_id: chatId,
            message_id: messageId,
            parse_mode: 'Markdown',
            reply_markup: {
              inline_keyboard: [
                [
                  { text: 'Show Private Key', callback_data: 'show_main_key' },
                  { text: 'Export Session', callback_data: 'export_session' }
                ],
                [
                  { text: 'Check Balance', callback_data: 'check_balance' },
                  { text: 'Add Token', callback_data: 'add_token' }
                ],
                [
                  { text: 'Back to Menu', callback_data: 'back_to_menu' }
                ]
              ]
            }
          });
        }
      }
    }
  } catch (error: any) {
    console.error('Wallet creation error:', error);
    safeSendMessage(chatId, `Error creating wallet: ${error.message}\n\nPlease try again or contact support.`);
  }
}

async function handleCheckBalance(userId: number, chatId: number, messageId?: number) {
  try {
    const session = getUserSession(userId, chatId);

    if (!session.walletKeypair) {
      const message = 'No wallet found. Create one first!';
      if (messageId && bot) {
        bot.editMessageText(message, {
          chat_id: chatId,
          message_id: messageId,
          reply_markup: {
            inline_keyboard: [
              [{ text: 'Create Wallet', callback_data: 'create_wallet' }],
              [{ text: 'Back to Menu', callback_data: 'back_to_menu' }]
            ]
          }
        });
      }
      return;
    }

    const keypair = Keypair.fromSecretKey(base58.decode(session.walletKeypair));

    // OPTIMIZED: Use batch check if there are trading wallets
    let solBalance = 0;
    let totalDistributed = 0;

    if (session.tradingWallets.length > 0) {
      const allWallets = [keypair, ...session.tradingWallets.map(w =>
        Keypair.fromSecretKey(base58.decode(w.privateKey))
      )];

      // OPTIMIZED: Single batch call instead of N individual calls
      const balances = await batchCheckBalances(allWallets);
      solBalance = balances.get(keypair.publicKey.toBase58()) || 0;

      session.tradingWallets.forEach(wallet => {
        totalDistributed += balances.get(wallet.address) || 0;
      });
    } else {
      // Single wallet, use cached balance
      solBalance = await getCachedBalance(keypair.publicKey);
    }

    const totalBalance = solBalance + totalDistributed;

    const message =
      `Wallet Balance Report\n\n` +
      `Main Wallet: ${solBalance.toFixed(6)} SOL\n` +
      `Distributed: ${totalDistributed.toFixed(6)} SOL (${session.tradingWallets.length} wallets)\n` +
      `Total Balance: ${totalBalance.toFixed(6)} SOL\n` +
      `Address: \`${session.depositAddress}\`\n\n` +
      `Status: ${solBalance >= session.requiredDeposit ? 'Ready for trading' : `Need ${(session.requiredDeposit - solBalance).toFixed(4)} more SOL`}\n` +
      `Token: ${session.tokenName || 'Not set'}\n` +
      `Trading: ${session.botRunning ? 'Active' : 'Inactive'}`;

    if (messageId && bot) {
      bot.editMessageText(message, {
        chat_id: chatId,
        message_id: messageId,
        parse_mode: 'Markdown',
        reply_markup: {
          inline_keyboard: [
            [
              { text: 'Refresh', callback_data: 'check_balance' },
              { text: 'Withdraw', callback_data: 'withdraw_sol' }
            ],
            [{ text: 'Add Token', callback_data: 'add_token' }],
            [{ text: 'Back to Menu', callback_data: 'back_to_menu' }]
          ]
        }
      });
    }
  } catch (error: any) {
    console.error('Balance check error:', error);
    safeSendMessage(chatId, 'Error checking balance. Try again.');
  }
}

// Continue to Part 3 for remaining handlers...

// Part 3: Start Volume, Distribution, and Bot Initialization

async function handleStartVolume(userId: number, chatId: number, messageId?: number) {
  try {
    const session = getUserSession(userId, chatId);

    if (!session.walletKeypair) {
      const message = 'No wallet found. Create one first!';
      if (messageId && bot) {
        bot.editMessageText(message, {
          chat_id: chatId,
          message_id: messageId,
          reply_markup: {
            inline_keyboard: [
              [{ text: 'Create Wallet', callback_data: 'create_wallet' }],
              [{ text: 'Back to Menu', callback_data: 'back_to_menu' }]
            ]
          }
        });
      }
      return;
    }

    if (!session.tokenAddress) {
      const message = 'No token set. Add a token first!';
      if (messageId && bot) {
        bot.editMessageText(message, {
          chat_id: chatId,
          message_id: messageId,
          reply_markup: {
            inline_keyboard: [
              [{ text: 'Add Token', callback_data: 'add_token' }],
              [{ text: 'Back to Menu', callback_data: 'back_to_menu' }]
            ]
          }
        });
      }
      return;
    }

    if (session.botRunning) {
      const message = 'Volume bot already running!';
      if (messageId && bot) {
        bot.editMessageText(message, {
          chat_id: chatId,
          message_id: messageId,
          reply_markup: {
            inline_keyboard: [
              [{ text: 'Stop Volume', callback_data: 'stop_volume' }],
              [{ text: 'Live Stats', callback_data: 'live_stats' }],
              [{ text: 'Back to Menu', callback_data: 'back_to_menu' }]
            ]
          }
        });
      }
      return;
    }

    const keypair = Keypair.fromSecretKey(base58.decode(session.walletKeypair));
    const balance = await solanaConnection.getBalance(keypair.publicKey);
    const solBalance = balance / LAMPORTS_PER_SOL;

    const walletCount = session.selectedWalletCount;
    const distConfig = session.distributionConfig;

    // Calculate required SOL based on distribution mode
    let requiredSol: number;
    let amountPerWallet: number;

    if (distConfig.mode === 'custom' && distConfig.amountPerWallet) {
      amountPerWallet = distConfig.amountPerWallet;
      requiredSol = (amountPerWallet * walletCount) + 0.01; // Add buffer
    } else {
      // Auto mode - use all balance
      const minPerWallet = ADDITIONAL_FEE + BUY_LOWER_AMOUNT + 0.001;
      requiredSol = minPerWallet * walletCount;
      amountPerWallet = Math.max(minPerWallet, (solBalance - 0.01) / walletCount);
    }

    if (solBalance < requiredSol) {
      const message =
        `Insufficient balance!\n\n` +
        `Current: ${solBalance.toFixed(4)} SOL\n` +
        `Required: ${requiredSol.toFixed(4)} SOL\n` +
        `For ${walletCount} wallets\n` +
        `Send to: \`${session.depositAddress}\``;

      if (messageId && bot) {
        bot.editMessageText(message, {
          chat_id: chatId,
          message_id: messageId,
          parse_mode: 'Markdown',
          reply_markup: {
            inline_keyboard: [
              [
                { text: 'Check Balance', callback_data: 'check_balance' },
                { text: 'Reduce Wallets', callback_data: 'select_wallet_count' }
              ],
              [
                { text: 'Back to Menu', callback_data: 'back_to_menu' }
              ]
            ]
          }
        });
      }
      return;
    }

    session.botRunning = true;
    session.status = 'trading';
    session.tradingStats = {
      totalBuys: 0,
      totalSells: 0,
      totalVolumeSOL: 0,
      successfulTxs: 0,
      failedTxs: 0,
      startTime: Date.now(),
      lastActivity: Date.now(),
      lastUpdateSent: Date.now()
    };
    userSessions.set(userId, session);
    activeTraders.add(userId);
    saveSessions();

    const totalToDistribute = amountPerWallet * walletCount;
    const remaining = solBalance - totalToDistribute;

    // Show current buy amount mode
    const buyModeInfo = session.buyAmountConfig.mode === 'custom' && session.buyAmountConfig.customAmount
      ? `Buy Amount: ${session.buyAmountConfig.customAmount.toFixed(4)} SOL (±30%)\n`
      : `Buy Amount: ${BUY_LOWER_AMOUNT}-${BUY_UPPER_AMOUNT} SOL (Random)\n`;

    const message =
      `Volume Generation Started!\n\n` +
      `Token: ${session.tokenName}\n` +
      `Symbol: ${session.tokenSymbol}\n` +
      `Address: \`${session.tokenAddress}\`\n\n` +
      `Distribution Mode: ${distConfig.mode.toUpperCase()}\n` +
      `Per Wallet: ${amountPerWallet.toFixed(4)} SOL\n` +
      `Total Distributed: ${totalToDistribute.toFixed(4)} SOL\n` +
      `Remaining in Main: ${remaining.toFixed(4)} SOL\n` +
      `Trading Wallets: ${walletCount}\n` +
      buyModeInfo +                                                    // ADD THIS LINE
      `\nDistributing SOL to ${walletCount} wallets...\n` +
      `You'll receive live trading updates!\n\n` +
      `Status: ACTIVE`;

    if (messageId && bot) {
      bot.editMessageText(message, {
        chat_id: chatId,
        message_id: messageId,
        parse_mode: 'Markdown',
        reply_markup: {
          inline_keyboard: [
            [
              { text: 'Stop Volume', callback_data: 'stop_volume' },
              { text: 'Live Stats', callback_data: 'live_stats' }
            ],
            [
              { text: 'Back to Menu', callback_data: 'back_to_menu' }
            ]
          ]
        }
      });
    }

    startVolumeBot(session, amountPerWallet);

  } catch (error: any) {
    console.error('Start volume error:', error);
    safeSendMessage(chatId, `Error starting volume: ${error?.message || 'Unknown error'}`);
  }
}

function handleStopVolume(userId: number, chatId: number, messageId?: number) {
  try {
    const session = getUserSession(userId, chatId);

    if (!session.botRunning) {
      const message = 'No volume bot is currently running.';
      if (messageId && bot) {
        bot.editMessageText(message, {
          chat_id: chatId,
          message_id: messageId,
          ...getTradingControlsKeyboard(false)
        });
      }
      return;
    }

    session.botRunning = false;
    session.status = 'stopped';
    activeTraders.delete(userId);

    userSessions.set(userId, session);
    saveSessions();

    const stats = session.tradingStats;
    const runtime = (Date.now() - stats.startTime) / 1000 / 60;

    const message =
      `Volume Bot Stopped!\n\n` +
      `Final Statistics:\n` +
      `Total Buys: ${stats.totalBuys}\n` +
      `Total Sells: ${stats.totalSells}\n` +
      `Volume Generated: ${stats.totalVolumeSOL.toFixed(4)} SOL\n` +
      `Runtime: ${runtime.toFixed(1)} minutes\n` +
      `Success Rate: ${stats.successfulTxs > 0 ? ((stats.successfulTxs / (stats.successfulTxs + stats.failedTxs)) * 100).toFixed(1) : 0}%\n\n` +
      `Trading wallets: ${session.tradingWallets.length} active\n` +
      `Wallet keys preserved - use "Withdraw SOL" to gather funds\n\n` +
      `Status: STOPPED`;

    if (messageId && bot) {
      bot.editMessageText(message, {
        chat_id: chatId,
        message_id: messageId,
        ...getTradingControlsKeyboard(false)
      });
    }
  } catch (error: any) {
    console.error('Stop error:', error);
    safeSendMessage(chatId, 'Error stopping bot.');
  }
}

async function distributeSol(
  mainKp: Keypair,
  distributionNum: number,
  amountPerWallet: number,
  session: UserSession
): Promise<{ kp: Keypair, address: string, privateKey: string }[]> {
  try {
    const wallets: { kp: Keypair, address: string, privateKey: string }[] = [];
    const sendSolTx: TransactionInstruction[] = [];

    sendSolTx.push(
      ComputeBudgetProgram.setComputeUnitLimit({ units: 200_000 }),
      ComputeBudgetProgram.setComputeUnitPrice({ microLamports: 300_000 })
    );

    for (let i = 0; i < distributionNum; i++) {
      const wallet = Keypair.generate();
      const walletData = {
        kp: wallet,
        address: wallet.publicKey.toBase58(),
        privateKey: base58.encode(wallet.secretKey)
      };
      wallets.push(walletData);

      sendSolTx.push(
        SystemProgram.transfer({
          fromPubkey: mainKp.publicKey,
          toPubkey: wallet.publicKey,
          lamports: Math.floor(amountPerWallet * LAMPORTS_PER_SOL)
        })
      );
    }

    const siTx = new Transaction().add(...sendSolTx);
    const latestBlockhash = await solanaConnection.getLatestBlockhash('confirmed'); // OPTIMIZED: Use 'confirmed'
    siTx.feePayer = mainKp.publicKey;
    siTx.recentBlockhash = latestBlockhash.blockhash;

    const messageV0 = new TransactionMessage({
      payerKey: mainKp.publicKey,
      recentBlockhash: latestBlockhash.blockhash,
      instructions: sendSolTx,
    }).compileToV0Message();

    const transaction = new VersionedTransaction(messageV0);
    transaction.sign([mainKp]);

    const txSig = await execute(transaction, latestBlockhash);

    if (txSig) {
      console.log("SOL distributed successfully:", txSig);

      // Archive old trading wallets
      if (session.tradingWallets && session.tradingWallets.length > 0) {
        if (!session.tradingWalletsHistory) {
          session.tradingWalletsHistory = [];
        }
        session.tradingWalletsHistory.push([...session.tradingWallets]);
      }

      session.tradingWallets = wallets.map(w => ({
        address: w.address,
        privateKey: w.privateKey
      }));

      saveAfterCriticalOperation(session);

      // OPTIMIZED: Reduced wait time from 5s to 3s
      console.log("Waiting for distribution confirmation...");
      await sleep(3000);

      // OPTIMIZED: Use batch check instead of individual wallet checks
      const balances = await batchCheckBalances(wallets.map(w => w.kp));
      let allWalletsVerified = true;

      wallets.forEach((wallet, i) => {
        const balance = balances.get(wallet.address) || 0;
        if (balance >= amountPerWallet * 0.95) {
          console.log(`Wallet ${i + 1} verified: ${balance.toFixed(6)} SOL`);
        } else {
          console.log(`Wallet ${i + 1} not verified: ${balance.toFixed(6)} SOL`);
          allWalletsVerified = false;
        }
      });

      let walletList = '';
      wallets.forEach((wallet, i) => {
        walletList += `${i + 1}. ${wallet.address.substring(0, 8)}...${wallet.address.substring(wallet.address.length - 4)}\n`;
      });

      const message =
        `SOL Distribution Complete!\n\n` +
        `Trading Wallets Created:\n${walletList}\n` +
        `Amount per wallet: ${amountPerWallet.toFixed(4)} SOL\n` +
        `Total distributed: ${(amountPerWallet * distributionNum).toFixed(4)} SOL\n` +
        `Transaction: https://solscan.io/tx/${txSig}\n\n` +
        `${allWalletsVerified ? '✅ All wallets verified!' : '⚠️ Some wallets still confirming...'}\n` +
        `Starting volume generation...`;

      safeSendMessage(session.chatId, message);

      return wallets;
    } else {
      throw new Error('Distribution transaction failed');
    }

  } catch (error: any) {
    console.error("Failed to distribute SOL:", error);
    safeSendMessage(session.chatId, `Failed to distribute SOL: ${error?.message || 'Unknown error'}`, getMainMenuKeyboard(true));
    return [];
  }
}

async function startVolumeBot(session: UserSession, amountPerWallet: number) {
  if (!session.tokenAddress || !session.walletKeypair) {
    safeSendMessage(session.chatId, 'Missing requirements for trading', getMainMenuKeyboard(session.hasPaid));
    return;
  }

  try {
    const baseMint = new PublicKey(session.tokenAddress);
    const mainKp = Keypair.fromSecretKey(base58.decode(session.walletKeypair));
    const distributionNum = session.selectedWalletCount;

    let poolKeys = null;
    let poolId: PublicKey | undefined;

    if (!SWAP_ROUTING) {
      poolKeys = await getPoolKeys(solanaConnection, baseMint);
      if (!poolKeys) {
        safeSendMessage(session.chatId, 'Pool not found for token', getMainMenuKeyboard(session.hasPaid));
        session.botRunning = false;
        saveSessions();
        return;
      }
      poolId = new PublicKey(poolKeys.id);
    }

    const tradingWallets = await distributeSol(mainKp, distributionNum, amountPerWallet, session);
    if (!tradingWallets || tradingWallets.length === 0) {
      safeSendMessage(session.chatId, 'Failed to distribute SOL', getMainMenuKeyboard(session.hasPaid));
      session.botRunning = false;
      saveSessions();
      return;
    }

    const updateInterval = setInterval(() => {
      const currentSession = userSessions.get(session.userId);
      if (!currentSession?.botRunning) {
        clearInterval(updateInterval);
        return;
      }

      if (Date.now() - currentSession.tradingStats.lastUpdateSent > 180000) {
        sendPeriodicUpdate(currentSession);
      }
    }, 60000);

    tradingWallets.forEach(async ({ kp }, walletIndex) => {
      await sleep((BUY_INTERVAL_MAX + BUY_INTERVAL_MIN) * walletIndex); // Removed /2 for better spacing

      while (true) {
        try {
          const currentSession = userSessions.get(session.userId);
          if (!currentSession?.botRunning) {
            console.log(`Trading stopped for user ${session.userId}, wallet ${walletIndex + 1}`);
            break;
          }

          // CHECK AND HANDLE COOLDOWN
          if (COOLDOWN_MODE) {
            console.log(`Wallet ${walletIndex + 1}: In cooldown mode, waiting...`);

            // Only the first wallet handles cooldown recovery
            if (walletIndex === 0 && !isSellQueueProcessing) {
              const canProceed = await checkAndExitCooldown(tradingWallets, baseMint, poolId, currentSession);

              if (canProceed) {
                console.log(`Cooldown complete, resuming wallet ${walletIndex + 1}`);
                // Continue to normal trading below
              } else {
                await sleep(10000);
                continue;
              }
            } else {
              // Other wallets just wait
              await sleep(10000);
              continue;
            }
          }

          // NORMAL TRADING (not in cooldown)
          const BUY_INTERVAL = Math.round(Math.random() * (BUY_INTERVAL_MAX - BUY_INTERVAL_MIN) + BUY_INTERVAL_MIN);
          const walletAddress = kp.publicKey.toBase58();
          const shortWallet = walletAddress.substring(0, 6) + '...' + walletAddress.substring(walletAddress.length - 4);

          // ---------------- Buy Part ----------------
          const buyResult = await performBuy(kp, baseMint, poolId, currentSession, walletIndex + 1, shortWallet);
          if (!buyResult || !currentSession.botRunning) {
            console.log(`Buy failed or session stopped for user ${session.userId}`);

            // If buy failed due to rate limit, cooldown is triggered, loop will handle it
            if (COOLDOWN_MODE) {
              await sleep(5000);
              continue;
            }

            // Other failure, wait and retry
            await sleep(10000);
            continue;
          }

          await sleep(3000 + Math.random() * 2000);

          // ---------------- Sell Part ----------------
          const sellResult = await performSell(kp, baseMint, poolId, currentSession, walletIndex + 1, shortWallet);
          if (!sellResult || !currentSession.botRunning) {
            console.log(`Sell failed or session stopped for user ${session.userId}`);

            // If sell failed due to rate limit, cooldown is triggered
            if (COOLDOWN_MODE) {
              await sleep(5000);
              continue;
            }

            // Other failure, wait and retry
            await sleep(10000);
            continue;
          }

          await sleep(BUY_INTERVAL + Math.random() * 3000);

        } catch (error: any) {
          console.error(`Trading loop error for user ${session.userId}, wallet ${walletIndex + 1}:`, error);

          const currentSession = userSessions.get(session.userId);
          if (currentSession) {
            currentSession.tradingStats.failedTxs++;
            saveSessions();
          }

          await sleep(10000);
        }
      }
    });

  } catch (error: any) {
    console.error('Volume bot error:', error);
    const errorMessage = error?.message || 'Unknown error occurred';
    safeSendMessage(session.chatId, `Trading error: ${errorMessage}`, getMainMenuKeyboard(true));
    session.botRunning = false;
    activeTraders.delete(session.userId);
    saveSessions();
  }
}

async function refuelWalletFromMain(
  mainKp: Keypair,
  targetWallet: Keypair,
  session: UserSession,
  walletNumber: number,
  shortWallet: string
): Promise<boolean> {
  try {
    const mainBalance = await solanaConnection.getBalance(mainKp.publicKey, 'confirmed');
    const mainSolBalance = mainBalance / LAMPORTS_PER_SOL;

    // Amount needed for swap fees (enough for several swaps)
    const refuelAmount = ADDITIONAL_FEE * 3; // 3x the additional fee for safety

    if (mainSolBalance < refuelAmount + 0.001) {
      console.log(`Main wallet has insufficient balance to refuel. Main: ${mainSolBalance.toFixed(6)} SOL`);

      safeSendMessage(session.chatId,
        `⚠️ REFUEL FAILED\n\n` +
        `Main wallet has insufficient SOL\n` +
        `Main Balance: ${mainSolBalance.toFixed(6)} SOL\n` +
        `Needed: ${refuelAmount.toFixed(6)} SOL\n\n` +
        `Please deposit more SOL to main wallet:\n` +
        `\`${mainKp.publicKey.toBase58()}\`\n\n` +
        `Trading will pause until refueled.`,
        {
          parse_mode: 'Markdown',
          reply_markup: {
            inline_keyboard: [
              [{ text: 'Check Balance', callback_data: 'check_balance' }],
              [{ text: 'Stop Volume', callback_data: 'stop_volume' }]
            ]
          }
        }
      );
      return false;
    }

    console.log(`Refueling wallet ${walletNumber} with ${refuelAmount.toFixed(6)} SOL`);

    const transaction = new Transaction().add(
      ComputeBudgetProgram.setComputeUnitPrice({ microLamports: 100_000 }),
      ComputeBudgetProgram.setComputeUnitLimit({ units: 40_000 }),
      SystemProgram.transfer({
        fromPubkey: mainKp.publicKey,
        toPubkey: targetWallet.publicKey,
        lamports: Math.floor(refuelAmount * LAMPORTS_PER_SOL)
      })
    );

    const latestBlockhash = await solanaConnection.getLatestBlockhash('confirmed');
    transaction.recentBlockhash = latestBlockhash.blockhash;
    transaction.feePayer = mainKp.publicKey;

    const messageV0 = new TransactionMessage({
      payerKey: mainKp.publicKey,
      recentBlockhash: latestBlockhash.blockhash,
      instructions: transaction.instructions,
    }).compileToV0Message();

    const versionedTx = new VersionedTransaction(messageV0);
    versionedTx.sign([mainKp]);

    const sig = await execute(versionedTx, latestBlockhash);

    if (sig) {
      console.log(`Successfully refueled wallet ${walletNumber}: ${sig}`);

      safeSendMessage(session.chatId,
        `⛽ WALLET REFUELED\n\n` +
        `Wallet: ${shortWallet}\n` +
        `Amount: +${refuelAmount.toFixed(6)} SOL\n` +
        `TX: https://solscan.io/tx/${sig}\n\n` +
        `Trading continues...`,
        { disable_web_page_preview: true }
      );

      // Wait a moment for the transaction to be confirmed
      await sleep(2000);
      return true;
    }

    return false;

  } catch (error: any) {
    console.error(`Refuel error for wallet ${walletNumber}:`, error);

    safeSendMessage(session.chatId,
      `⚠️ REFUEL ERROR\n\n` +
      `Wallet: ${shortWallet}\n` +
      `Error: ${error?.message || 'Unknown error'}\n\n` +
      `Trading may be affected.`
    );

    return false;
  }
}

async function performBuy(
  wallet: Keypair,
  baseMint: PublicKey,
  poolId: PublicKey | undefined,
  session: UserSession,
  walletNumber: number,
  shortWallet: string,
  bypassCooldownCheck: boolean = false  // ← ADD THIS LINE
): Promise<boolean> {
  try {
    // CHECK COOLDOWN MODE (allow bypass for testing)
    if (COOLDOWN_MODE && !bypassCooldownCheck) {
      console.log(`Wallet ${walletNumber}: Skipping buy - in cooldown mode`);
      return false;
    }

    // OPTIMIZED: Use cached balance instead of fresh RPC call
    const solBalance = await getCachedBalance(wallet.publicKey);

    // Calculate buy amount based on user's config
    let buyAmount: number;

    if (session.buyAmountConfig.mode === 'custom' && session.buyAmountConfig.customAmount) {
      // Custom mode: randomize ±30% around user's preferred amount
      const baseAmount = session.buyAmountConfig.customAmount;
      const minMultiplier = 0.7; // 30% below
      const maxMultiplier = 1.3; // 30% above
      const randomMultiplier = minMultiplier + Math.random() * (maxMultiplier - minMultiplier);
      buyAmount = Number((baseAmount * randomMultiplier).toFixed(6));

      console.log(`Custom buy mode: Base ${baseAmount}, Multiplier ${randomMultiplier.toFixed(2)}, Final ${buyAmount}`);
    } else {
      // Default mode: use constants
      buyAmount = IS_RANDOM
        ? Number((Math.random() * (BUY_UPPER_AMOUNT - BUY_LOWER_AMOUNT) + BUY_LOWER_AMOUNT).toFixed(6))
        : BUY_AMOUNT;
    }

    const MINIMUM_BUY_AMOUNT = 0.001; // At least 0.001 SOL (~$0.20 at $200/SOL)
    if (buyAmount < MINIMUM_BUY_AMOUNT) {
      buyAmount = MINIMUM_BUY_AMOUNT;
      console.log(`⚠️ Buy amount too small, adjusted to minimum: ${MINIMUM_BUY_AMOUNT} SOL`);
    }

    const minimumRequired = buyAmount + ADDITIONAL_FEE;

    if (solBalance < minimumRequired) {
      console.log(`Wallet ${walletNumber} insufficient balance: ${solBalance.toFixed(6)} SOL`);

      // OPTIMIZED: Only notify every 10 failures to reduce spam
      if (session.tradingStats.failedTxs % 10 === 0) {
        safeSendMessage(session.chatId,
          `⚠️ Wallet ${walletNumber} low on SOL: ${solBalance.toFixed(6)} SOL\n` +
          `Consider stopping and withdrawing.`,
          {
            reply_markup: {
              inline_keyboard: [
                [{ text: 'Stop Volume', callback_data: 'stop_volume' }]
              ]
            }
          }
        );
      }
      return false;
    }

    console.log(`Attempting buy: Wallet ${walletNumber}, Amount: ${buyAmount.toFixed(6)} SOL, Balance: ${solBalance.toFixed(6)} SOL`);

    let tx;
    let attempts = 0;
    const maxAttempts = 3;

    while (attempts < maxAttempts) {
      try {
        if (SWAP_ROUTING) {
          tx = await getBuyTxWithJupiter(wallet, baseMint, buyAmount);
        } else if (poolId) {
          tx = await getBuyTx(solanaConnection, wallet, baseMint, NATIVE_MINT, buyAmount, poolId.toBase58());
        } else {
          throw new Error('No pool ID available');
        }

        if (tx) {
          console.log(`Transaction created successfully on attempt ${attempts + 1}`);
          break;
        }

        attempts++;
        if (attempts < maxAttempts) await sleep(2000);

      } catch (txError: any) {
        attempts++;
        const errorMsg = txError?.message || '';
        console.error(`Failed to get buy transaction: ${errorMsg}`);

        // DETECT RATE LIMIT
        if (errorMsg.includes('Rate limit') || errorMsg.includes('429') || errorMsg.includes('Too Many Requests')) {
          console.log('🚨 Rate limit detected in buy');
          recordRateLimitFailure();
          recordFailure();
          return false; // Exit immediately
        }

        if (attempts < maxAttempts) {
          console.log(`Buy failed, retrying...`);
          await sleep(2000);
        } else {
          throw txError;
        }
      }
    }

    if (!tx) {
      console.log(`Failed to create buy transaction after ${maxAttempts} attempts`);
      session.tradingStats.failedTxs++;
      saveSessions();
      recordFailure();
      return false;
    }

    const latestBlockhash = await solanaConnection.getLatestBlockhash('confirmed');
    const txSig = await execute(tx, latestBlockhash);

    if (txSig) {
      session.tradingStats.totalBuys++;
      session.tradingStats.totalVolumeSOL += buyAmount;
      session.tradingStats.successfulTxs++;
      session.tradingStats.lastActivity = Date.now();

      // Reset failure counter on success
      resetCooldownCounters();
      recordSuccess();

      // Test mode: trigger cooldown after X successful trades
      if (TEST_MODE) {
        testModeSuccessfulTrades++;

        if (testModeSuccessfulTrades >= TEST_MODE_TRIGGER_AFTER) {
          console.log(`\n🧪 TEST MODE: Triggering cooldown after ${testModeSuccessfulTrades} successful trades`);
          console.log(`   (In production, this would happen after ~75-90 trades)\n`);

          // Force rate limit
          recordRateLimitFailure();
          recordRateLimitFailure();
          recordRateLimitFailure();

          testModeSuccessfulTrades = 0;
        }
      }

      // OPTIMIZED: Invalidate balance cache after successful buy
      const walletKey = wallet.publicKey.toBase58();
      const cached = walletCacheMap.get(walletKey);
      if (cached) {
        cached.lastBalanceCheck = 0; // Force refresh on next check
      }

      userSessions.set(session.userId, session);
      saveSessions();

      console.log(`Buy successful - User: ${session.userId}, Wallet: ${walletNumber}, Amount: ${buyAmount.toFixed(6)} SOL, TX: ${txSig}`);

      // OPTIMIZED: Only send notification every 3rd buy to reduce Telegram spam
      if (session.tradingStats.totalBuys % 3 === 0) {
        sendTradingNotification(session, 'buy', {
          success: true,
          amount: buyAmount.toFixed(6),
          wallet: shortWallet,
          signature: txSig
        });
      }

      return true;
    } else {
      session.tradingStats.failedTxs++;
      saveSessions();
      recordFailure();
      return false;
    }

  } catch (error: any) {
    console.error(`Buy error wallet ${walletNumber}:`, error);

    // Check if it's a rate limit error
    const errorMsg = error?.message || '';
    if (errorMsg.includes('Rate limit') || errorMsg.includes('429') || errorMsg.includes('Too Many Requests')) {
      console.log('🚨 Rate limit detected in buy (catch block)');
      recordRateLimitFailure();
      recordFailure();
    } else {
      recordFailure();
    }

    session.tradingStats.failedTxs++;
    saveSessions();
    return false;
  }
}

async function performSell(
  wallet: Keypair,
  baseMint: PublicKey,
  poolId: PublicKey | undefined,
  session: UserSession,
  walletNumber: number,
  shortWallet: string
): Promise<boolean> {
  try {
    // Note: We DON'T skip sells during cooldown - we want to sell stuck tokens

    // OPTIMIZED: Use cached balance check
    const solBalance = await getCachedBalance(wallet.publicKey);

    if (solBalance < ADDITIONAL_FEE) {
      console.log(`Wallet ${walletNumber} needs refuel: ${solBalance.toFixed(6)} SOL`);

      if (!session.walletKeypair) return false;

      const mainKp = Keypair.fromSecretKey(base58.decode(session.walletKeypair));
      const refueled = await refuelWalletFromMain(mainKp, wallet, session, walletNumber, shortWallet);

      if (!refueled) return false;

      // Force balance refresh after refuel
      await getCachedBalance(wallet.publicKey, true);
    }

    // OPTIMIZED: Use cached token account lookup instead of fresh fetch
    const tokenAccount = await getCachedTokenAccount(wallet.publicKey, baseMint);

    if (!tokenAccount) {
      console.log(`No token account found for wallet ${walletNumber}`);
      return false;
    }

    // OPTIMIZED: Reduced retry attempts from 10 to 5
    let tokenBalance = '0';
    let tokenAmount = 0;
    let attempts = 0;
    const maxAttempts = 10;

    while (attempts < maxAttempts) {
      try {
        const tokenBalInfo = await solanaConnection.getTokenAccountBalance(tokenAccount, 'confirmed');
        tokenBalance = tokenBalInfo.value.amount;
        tokenAmount = tokenBalInfo.value.uiAmount || 0;

        if (tokenAmount > 0) {
          console.log(`Wallet ${walletNumber} token balance: ${tokenAmount.toFixed(6)}`);
          break;
        }
      } catch (error) {
        // Account not ready yet
      }

      console.log(`Waiting for token balance... wallet ${walletNumber} (${attempts + 1}/${maxAttempts})`);
      await sleep(2000);
      attempts++;
    }

    if (tokenAmount === 0 || tokenBalance === '0') {
      console.log(`No tokens to sell for wallet ${walletNumber}`);
      return false;
    }

    console.log(`Attempting sell: Wallet ${walletNumber}, Amount: ${tokenAmount.toFixed(6)} tokens`);

    let sellTx;
    let txAttempts = 0;
    const maxTxAttempts = 3;

    while (txAttempts < maxTxAttempts) {
      try {
        if (SWAP_ROUTING) {
          sellTx = await getSellTxWithJupiter(wallet, baseMint, tokenBalance, false);
        } else if (poolId) {
          sellTx = await getSellTx(solanaConnection, wallet, baseMint, NATIVE_MINT, tokenBalance, poolId.toBase58());
        } else {
          throw new Error('No pool ID available');
        }

        if (sellTx) {
          console.log(`Sell transaction created successfully on attempt ${txAttempts + 1}`);
          break;
        }

        txAttempts++;
        if (txAttempts < maxTxAttempts) await sleep(2000);

      } catch (txError: any) {
        txAttempts++;
        const errorMsg = txError?.message || '';
        console.error(`Failed to get sell transaction: ${errorMsg}`);

        // DETECT RATE LIMIT
        if (errorMsg.includes('Rate limit') || errorMsg.includes('429') || errorMsg.includes('Too Many Requests')) {
          console.log('🚨 Rate limit detected in sell');
          recordRateLimitFailure();
          recordFailure();

          // If in cooldown mode, we still want to try selling after cooldown completes
          // So we return false but don't mark as permanent failure
          if (COOLDOWN_MODE) {
            console.log(`Wallet ${walletNumber}: Will retry sell after cooldown`);
          }
          return false;
        }

        if (txAttempts < maxTxAttempts) {
          console.log(`Sell failed, retrying...`);
          await sleep(2000);
        } else {
          throw txError;
        }
      }
    }

    if (!sellTx) {
      console.log(`Failed to create sell transaction after ${maxTxAttempts} attempts`);
      session.tradingStats.failedTxs++;
      saveSessions();
      console.log(`Sell failed or session stopped for user ${session.userId}`);
      recordFailure();
      return false;
    }

    const latestBlockhash = await solanaConnection.getLatestBlockhash('confirmed');
    const txSig = await execute(sellTx, latestBlockhash, false);

    if (txSig) {
      session.tradingStats.totalSells++;
      session.tradingStats.successfulTxs++;
      session.tradingStats.lastActivity = Date.now();

      // Reset failure counter on success
      resetCooldownCounters();
      recordSuccess();

      // OPTIMIZED: Invalidate caches after successful sell
      const walletKey = wallet.publicKey.toBase58();
      const cached = walletCacheMap.get(walletKey);
      if (cached) {
        cached.lastBalanceCheck = 0;
        cached.tokenAccounts.delete(baseMint.toBase58());
      }

      userSessions.set(session.userId, session);
      saveSessions();

      console.log(`Sell successful - User: ${session.userId}, Wallet: ${walletNumber}, Amount: ${tokenAmount.toFixed(6)} UNKNOWN, TX: ${txSig}`);

      // OPTIMIZED: Only send notification every 3rd sell
      if (session.tradingStats.totalSells % 3 === 0) {
        sendTradingNotification(session, 'sell', {
          success: true,
          tokenAmount: tokenAmount.toFixed(6),
          wallet: shortWallet,
          signature: txSig
        });
      }

      return true;
    } else {
      session.tradingStats.failedTxs++;
      saveSessions();
      recordFailure();
      return false;
    }

  } catch (error: any) {
    console.error(`Sell error wallet ${walletNumber}:`, error);

    const errorMsg = error?.message || '';

    // Check for "no route" error - token amount too small
    if (errorMsg.includes('COULD_NOT_FIND_ANY_ROUTE') || errorMsg.includes('Could not find any route')) {
      console.log(`⚠️ Wallet ${walletNumber}: Token amount too small to sell, skipping...`);
      // Don't mark as failure - this is dust, not a real error
      return false; // Return false but don't increment failed transactions
    }

    // Check if it's a rate limit error
    if (errorMsg.includes('Rate limit') || errorMsg.includes('429') || errorMsg.includes('Too Many Requests')) {
      console.log('🚨 Rate limit detected in sell (catch block)');
      recordRateLimitFailure();
      recordFailure();
    } else {
      recordFailure();
    }

    // Only increment failed transactions if it's not a "no route" error
    if (!errorMsg.includes('COULD_NOT_FIND_ANY_ROUTE') && !errorMsg.includes('Could not find any route')) {
      session.tradingStats.failedTxs++;
    }
    saveSessions();
    return false;
  }
}

async function monitorDeposits(userId: number) {
  const session = userSessions.get(userId);
  if (!session || !session.walletKeypair) return;

  let lastBalance = 0;

  const checkDeposits = setInterval(async () => {
    try {
      const currentSession = userSessions.get(userId);
      if (!currentSession || !currentSession.walletKeypair) {
        clearInterval(checkDeposits);
        return;
      }

      const keypair = Keypair.fromSecretKey(base58.decode(currentSession.walletKeypair));
      const balance = await solanaConnection.getBalance(keypair.publicKey);
      const solBalance = balance / LAMPORTS_PER_SOL;

      if (solBalance > lastBalance + 0.001) {
        const depositAmount = solBalance - lastBalance;
        const walletCount = currentSession.selectedWalletCount;
        const calc = calculateVolumeEstimate(solBalance, walletCount);

        const message =
          `Deposit Detected!\n\n` +
          `+${depositAmount.toFixed(6)} SOL received\n` +
          `New Balance: ${solBalance.toFixed(6)} SOL\n\n` +
          `VOLUME ESTIMATE (${walletCount} wallets):\n` +
          `Min: ${calc.minVolume.toFixed(4)} SOL\n` +
          `Est: ${calc.estimatedVolume.toFixed(4)} SOL\n` +
          `Max: ${calc.maxVolume.toFixed(4)} SOL\n\n` +
          `${solBalance >= currentSession.requiredDeposit ? 'Ready for trading!' : `Need ${(currentSession.requiredDeposit - solBalance).toFixed(4)} more SOL`}`;

        safeSendMessage(currentSession.chatId, message, {
          reply_markup: {
            inline_keyboard: [
              [
                { text: 'Add Token', callback_data: 'add_token' },
                { text: 'Check Balance', callback_data: 'check_balance' }
              ],
              [
                { text: 'Set Buy Amount', callback_data: 'buy_amount_settings' }
              ],
              [
                { text: 'Back to Menu', callback_data: 'back_to_menu' }
              ]
            ]
          }
        });
      }

      lastBalance = solBalance;

      if (Date.now() - currentSession.createdAt > 7200000 ||
        (solBalance >= currentSession.requiredDeposit && !currentSession.botRunning)) {
        clearInterval(checkDeposits);
        currentSession.isMonitoring = false;
        saveSessions();
      }

    } catch (error: any) {
      console.error('Deposit monitoring error:', error);
    }
  }, 15000);
}

// Export for next part
export { handleStartVolume, handleStopVolume, performBuy, performSell, monitorDeposits };

// Part 4: Bot Initialization and Callback Handlers

if (BOT_TOKEN) {
  bot = new TelegramBot(BOT_TOKEN, {
    polling: {
      interval: 300,
      autoStart: true,
      params: { timeout: 10 }
    }
  });

  bot.on('polling_error', (error) => {
    console.log('Polling error:', error.message.substring(0, 50) + '...');
  });

  ensureSessionsDirectory();
  loadSessions();

  console.log('Smart Distribution Volume Bot is running...');

  bot.onText(/\/start/, async (msg) => {
    const chatId = msg.chat.id;
    const userId = msg.from?.id!;
    const firstName = msg.from?.first_name || 'User';

    const session = getUserSession(userId, chatId);

    const welcomeMessage =
      `Welcome ${firstName} to Volume Bot 2.0!\n\n` +
      `Professional Volume Generation Tool\n` +
      `Advanced Multi-Wallet Trading System\n\n` +
      `✨ Features:\n` +
      `• Intelligent volume generation\n` +
      `• Real-time trading analytics\n` +
      `• Multi-wallet distribution\n` +
      `• Live trading notifications\n` +
      `• Auto-refuel system\n\n` +
      `Choose an option below to get started:`;

    safeSendMessage(chatId, welcomeMessage, getMainMenuKeyboard(true));
  });

  bot.on('callback_query', async (callbackQuery) => {
    const msg = callbackQuery.message;
    const data = callbackQuery.data;
    const chatId = msg?.chat.id!;
    const userId = callbackQuery.from.id;

    if (!bot) return;

    bot.answerCallbackQuery(callbackQuery.id);
    const session = getUserSession(userId, chatId);

    switch (data) {
      case 'make_payment':
      case 'check_payment':
      case 'payment_info':
      case 'payment_help':
        bot.editMessageText(
          `🎉 Life time access to the bot!\n\n` +
          `No payment required.\n` +
          `All features are available to everyone.\n\n` +
          `Start using the bot now:`,
          {
            chat_id: chatId,
            message_id: msg?.message_id,
            ...getMainMenuKeyboard(true)
          }
        );
        break;

      case 'select_wallet_count':
        bot.editMessageText(
          `Select Trading Wallets\n\nChoose how many wallets for volume:\n\nMore wallets = More realistic\nFewer wallets = Faster\n\nCurrent: ${session.selectedWalletCount} wallets`,
          { chat_id: chatId, message_id: msg?.message_id, ...getWalletSelectionKeyboard() }
        );
        break;

      case 'select_wallets_1':
      case 'select_wallets_6':
      case 'select_wallets_8':
      case 'select_wallets_10':
      case 'select_wallets_12':
        if (!requirePayment(session, chatId, 'Wallet Selection')) return;

        const walletCount = parseInt(data.split('_')[2]);
        session.selectedWalletCount = walletCount;
        userSessions.set(userId, session);
        saveSessions();

        bot.editMessageText(
          `Wallet Selection Updated!\n\nSelected: ${walletCount} trading wallets\n\nYou can change this anytime.`,
          {
            chat_id: chatId,
            message_id: msg?.message_id,
            reply_markup: {
              inline_keyboard: [
                [{ text: 'Set Buy Amount', callback_data: 'buy_amount_settings' }],
                [{ text: 'Change Selection', callback_data: 'select_wallet_count' }],
                [{ text: 'Back to Menu', callback_data: 'back_to_menu' }]
              ]
            }
          }
        );
        break;

      case 'distribution_mode':
        handleDistributionMode(userId, chatId, msg?.message_id);
        break;

      case 'dist_mode_auto':
        session.distributionConfig = { mode: 'auto' };
        userSessions.set(userId, session);
        saveSessions();

        bot.editMessageText(
          `Distribution Mode: AUTO\n\nALL your wallet balance will be\ndistributed equally across wallets.\n\nThis maximizes volume generation.`,
          {
            chat_id: chatId,
            message_id: msg?.message_id,
            reply_markup: {
              inline_keyboard: [
                [{ text: 'Set Buy Amount', callback_data: 'buy_amount_settings' }],
                [{ text: 'Back to Menu', callback_data: 'back_to_menu' }]
              ]
            }
          }
        );
        break;

      case 'withdraw_sol':

        if (!session.walletKeypair) {
          bot.editMessageText('No wallet found. Create one first!', {
            chat_id: chatId,
            message_id: msg?.message_id,
            reply_markup: {
              inline_keyboard: [
                [{ text: 'Create Wallet', callback_data: 'create_wallet' }],
                [{ text: 'Back to Menu', callback_data: 'back_to_menu' }]
              ]
            }
          });
          return;
        }

        session.status = 'awaiting_withdraw_address';
        userSessions.set(userId, session);
        saveSessions();

        bot.editMessageText(
          `Withdraw All SOL\n\n` +
          `This will gather SOL from ALL wallets:\n` +
          `- Main wallet\n` +
          `- All current trading wallets (${session.tradingWallets.length})\n` +
          `- All historical trading wallets\n\n` +
          `Enter the Solana address to send to:\n` +
          `(Send as a message)`,
          {
            chat_id: chatId,
            message_id: msg?.message_id,
            reply_markup: {
              inline_keyboard: [
                [{ text: 'Cancel', callback_data: 'back_to_menu' }]
              ]
            }
          }
        );
        break;

      case 'status_report':

        const stats = session.tradingStats;
        const runtime = stats.startTime > 0 ? (Date.now() - stats.startTime) / 1000 / 60 : 0;
        const successRate = stats.successfulTxs > 0
          ? ((stats.successfulTxs / (stats.successfulTxs + stats.failedTxs)) * 100).toFixed(1)
          : '0';

        const queueLength = jupiterQueue.getQueueLength();
        const activeRequests = jupiterQueue.getActiveRequests();
        const cacheStats = quoteCache.getStats();

        const statusMessage =
          `STATUS REPORT\n\n` +
          `Bot Status: ${session.botRunning ? 'ACTIVE' : 'STOPPED'}\n` +
          `Cooldown: ${COOLDOWN_MODE ? 'YES' : 'NO'}\n` +
          `Queue: ${queueLength} pending, ${activeRequests} active\n` +
          `Cache: ${cacheStats.hitRate}% hit rate (${cacheStats.hits} hits, ${cacheStats.misses} misses)\n` +
          `Token: ${session.tokenName || 'Not set'} (${session.tokenSymbol || 'N/A'})\n` +
          `Trading Wallets: ${session.tradingWallets.length}\n\n` +
          `STATISTICS:\n` +
          `Total Buys: ${stats.totalBuys}\n` +
          `Total Sells: ${stats.totalSells}\n` +
          `Volume Generated: ${stats.totalVolumeSOL.toFixed(4)} SOL\n` +
          `Successful TXs: ${stats.successfulTxs}\n` +
          `Failed TXs: ${stats.failedTxs}\n` +
          `Success Rate: ${successRate}%\n` +
          `Runtime: ${runtime.toFixed(1)} minutes`;

        bot.editMessageText(statusMessage, {
          chat_id: chatId,
          message_id: msg?.message_id,
          ...getTradingControlsKeyboard(session.botRunning)
        });
        break;

      case 'live_stats':

        if (!session.botRunning) {
          bot.editMessageText('No active trading session.', {
            chat_id: chatId,
            message_id: msg?.message_id,
            ...getTradingControlsKeyboard(false)
          });
          return;
        }

        sendPeriodicUpdate(session);
        break;

      case 'dist_mode_custom':
        session.status = 'awaiting_distribution_amount';
        userSessions.set(userId, session);
        saveSessions();

        bot.editMessageText(
          `Distribution Mode: CUSTOM\n\nEnter amount per wallet in SOL\n\nExample: 0.1\n\nSend the amount as a message.`,
          {
            chat_id: chatId,
            message_id: msg?.message_id,
            reply_markup: {
              inline_keyboard: [
                [{ text: 'Cancel', callback_data: 'distribution_mode' }]
              ]
            }
          }
        );
        break;

      case 'buy_amount_settings':
        handleBuyAmountSettings(userId, chatId, msg?.message_id);
        break;

      case 'buy_amount_default':
        session.buyAmountConfig = { mode: 'default' };
        userSessions.set(userId, session);
        saveSessions();

        bot.editMessageText(
          `✅ Buy Amount Mode: DEFAULT\n\n` +
          `Using config values:\n` +
          `Range: ${BUY_LOWER_AMOUNT} - ${BUY_UPPER_AMOUNT} SOL\n\n` +
          `Each buy will randomly select an amount\n` +
          `within this range.`,
          {
            chat_id: chatId,
            message_id: msg?.message_id,
            reply_markup: {
              inline_keyboard: [
                [{ text: 'Change to Custom', callback_data: 'buy_amount_custom' }],
                [{ text: 'Back to Menu', callback_data: 'back_to_menu' }]
              ]
            }
          }
        );
        break;

      case 'buy_amount_custom':
        session.status = 'awaiting_buy_amount';
        userSessions.set(userId, session);
        saveSessions();

        bot.editMessageText(
          `🎯 Set Custom Buy Amount\n\n` +
          `Enter your preferred buy amount in SOL.\n\n` +
          `Examples:\n` +
          `• 0.1 → Bot buys 0.07-0.13 SOL\n` +
          `• 0.5 → Bot buys 0.35-0.65 SOL\n` +
          `• 1.0 → Bot buys 0.70-1.30 SOL\n\n` +
          `The bot will randomize ±30% around\n` +
          `your value for realistic patterns.\n\n` +
          `Send the amount as a message:`,
          {
            chat_id: chatId,
            message_id: msg?.message_id,
            reply_markup: {
              inline_keyboard: [
                [{ text: 'Cancel', callback_data: 'buy_amount_settings' }]
              ]
            }
          }
        );
        break;

      case 'create_wallet':
        handleCreateWallet(userId, chatId, msg?.message_id);
        break;

      case 'check_balance':
        handleCheckBalance(userId, chatId, msg?.message_id);
        break;

      case 'add_token':
        bot.editMessageText(
          `Add Token\n\nEnter the token address:\n\nSend the token contract address as a message\nBot will validate automatically\n\nAccepts any Solana token with liquidity`,
          {
            chat_id: chatId,
            message_id: msg?.message_id,
            reply_markup: {
              inline_keyboard: [[{ text: 'Back to Menu', callback_data: 'back_to_menu' }]]
            }
          }
        );
        break;

      case 'start_volume':
        handleStartVolume(userId, chatId, msg?.message_id);
        break;

      case 'stop_volume':
        handleStopVolume(userId, chatId, msg?.message_id);
        break;

      case 'export_session':
        handleExportSession(userId, chatId, msg?.message_id);
        break;

      case 'show_main_key':
        handleShowMainKey(userId, chatId, msg?.message_id);
        break;

      case 'back_to_menu':
        const menuMsg = session.hasPaid ? 'Main Menu\n\nChoose what you\'d like to do:' : 'Volume Bot 2.0\n\nPayment required.';
        bot.editMessageText(menuMsg, {
          chat_id: chatId,
          message_id: msg?.message_id,
          ...getMainMenuKeyboard(session.hasPaid)
        });
        break;

      case 'back_to_start':
        const startMsg = session.hasPaid ? 'Main Menu\n\nWelcome back!' : 'Volume Bot 2.0\n\nMake payment to unlock.';
        bot.editMessageText(startMsg, {
          chat_id: chatId,
          message_id: msg?.message_id,
          ...getMainMenuKeyboard(session.hasPaid)
        });
        break;

      case 'force_new_wallet':

        session.walletKeypair = undefined;
        session.userWalletPrivateKey = undefined;
        session.depositAddress = undefined;
        session.tradingWallets = [];
        session.status = 'idle';
        userSessions.set(userId, session);
        saveSessions();

        handleCreateWallet(userId, chatId, msg?.message_id);
        break;

      default:
        if (data && data.startsWith('change_token_')) {

          const newTokenAddress = data.replace('change_token_', '');

          if (session.botRunning) {
            bot.editMessageText(
              'Please stop the volume bot before changing tokens.',
              {
                chat_id: chatId,
                message_id: msg?.message_id,
                reply_markup: {
                  inline_keyboard: [
                    [{ text: 'Stop Volume', callback_data: 'stop_volume' }],
                    [{ text: 'Back to Menu', callback_data: 'back_to_menu' }]
                  ]
                }
              }
            );
            return;
          }

          session.tokenAddress = undefined;
          session.tokenName = undefined;
          session.tokenSymbol = undefined;
          userSessions.set(userId, session);
          saveSessions();

          await handleTokenInput(userId, chatId, newTokenAddress);
        }
        break;
    }
  });

  // Handle text messages
  bot.on('message', async (msg) => {
    const text = msg.text;
    const chatId = msg.chat.id;
    const userId = msg.from?.id!;

    if (!text || text.startsWith('/')) return;

    const session = userSessions.get(userId);
    if (!session || !session.hasPaid || !session.paymentConfirmed) return;

    const trimmedText = text.trim();

    if (session.status === 'awaiting_withdraw_address') {
      try {
        new PublicKey(trimmedText);

        session.status = 'idle';
        userSessions.set(userId, session);
        saveSessions();

        safeSendMessage(chatId, 'Processing withdrawal...\nThis may take a few minutes.');

        await performWithdrawal(session, trimmedText, chatId);
      } catch (error) {
        safeSendMessage(chatId, 'Invalid Solana address. Please try again or cancel.', {
          reply_markup: {
            inline_keyboard: [
              [{ text: 'Cancel', callback_data: 'back_to_menu' }]
            ]
          }
        });
      }
      return;
    }

    // Handle custom distribution amount
    if (session.status === 'awaiting_distribution_amount') {
      const amount = parseFloat(trimmedText);

      if (isNaN(amount) || amount <= 0) {
        safeSendMessage(chatId, 'Invalid amount. Please enter a valid number (e.g., 0.1)');
        return;
      }

      const minAmount = ADDITIONAL_FEE + BUY_LOWER_AMOUNT + 0.001;
      if (amount < minAmount) {
        safeSendMessage(chatId, `Amount too low. Minimum: ${minAmount.toFixed(4)} SOL per wallet`);
        return;
      }

      session.distributionConfig = {
        mode: 'custom',
        amountPerWallet: amount
      };
      session.status = 'idle';
      userSessions.set(userId, session);
      saveSessions();

      safeSendMessage(chatId,
        `Distribution Mode Updated!\n\n` +
        `Mode: CUSTOM\n` +
        `Amount per wallet: ${amount.toFixed(4)} SOL\n` +
        `Selected wallets: ${session.selectedWalletCount}\n` +
        `Total to distribute: ${(amount * session.selectedWalletCount).toFixed(4)} SOL\n\n`,
        getMainMenuKeyboard(true)
      );
      return;
    }

    // Handle custom buy amount input
    if (session.status === 'awaiting_buy_amount') {
      const amount = parseFloat(trimmedText);

      if (isNaN(amount) || amount <= 0) {
        safeSendMessage(chatId, 'Invalid amount. Please enter a valid number (e.g., 0.5)');
        return;
      }

      const minAmount = 0.001; // Minimum 0.001 SOL
      const maxAmount = 10; // Maximum 10 SOL per buy

      if (amount < minAmount) {
        safeSendMessage(chatId, `Amount too low. Minimum: ${minAmount} SOL`);
        return;
      }

      if (amount > maxAmount) {
        safeSendMessage(chatId, `Amount too high. Maximum: ${maxAmount} SOL per buy`);
        return;
      }

      session.buyAmountConfig = {
        mode: 'custom',
        customAmount: amount
      };
      session.status = 'idle';
      userSessions.set(userId, session);
      saveSessions();

      const minBuy = (amount * 0.7).toFixed(4);
      const maxBuy = (amount * 1.3).toFixed(4);

      safeSendMessage(chatId,
        `✅ Custom Buy Amount Set!\n\n` +
        `Target Amount: ${amount.toFixed(4)} SOL\n` +
        `Actual Range: ${minBuy}-${maxBuy} SOL\n` +
        `Randomization: ±30%\n\n` +
        `Each buy will vary within this range\n` +
        `for more realistic trading patterns.\n\n` +
        `You can change this anytime!`,
        getMainMenuKeyboard(true)
      );
      return;
    }

    // Handle token address input
    const isSolanaAddress = trimmedText.length >= 32 &&
      trimmedText.length <= 44 &&
      /^[1-9A-HJ-NP-Za-km-z]+$/.test(trimmedText);

    if (!isSolanaAddress) return;

    let isValidPubkey = false;
    try {
      new PublicKey(trimmedText);
      isValidPubkey = true;
    } catch {
      safeSendMessage(chatId, 'Invalid Solana address format.');
      return;
    }

    if (session.walletKeypair && !session.tokenAddress) {
      await handleTokenInput(userId, chatId, trimmedText);
    } else if (session.walletKeypair && session.tokenAddress) {
      safeSendMessage(chatId,
        `You already have a token set:\n\n` +
        `Current: ${session.tokenName} (${session.tokenSymbol})\n` +
        `${session.tokenAddress}\n\n` +
        `Do you want to change to a new token?\n` +
        `New: ${trimmedText}`,
        {
          reply_markup: {
            inline_keyboard: [
              [
                { text: 'Yes, Change Token', callback_data: `change_token_${trimmedText}` },
                { text: 'Cancel', callback_data: 'back_to_menu' }
              ]
            ]
          }
        }
      );
    } else if (!session.walletKeypair) {
      safeSendMessage(chatId, 'Please create a wallet first before adding a token!', {
        reply_markup: {
          inline_keyboard: [
            [{ text: 'Create Wallet', callback_data: 'create_wallet' }],
            [{ text: 'Back to Menu', callback_data: 'back_to_menu' }]
          ]
        }
      });
    }
  });

  async function handleTokenInput(userId: number, chatId: number, tokenAddress: string) {
    try {
      const session = getUserSession(userId, chatId);

      if (!session.walletKeypair) {
        safeSendMessage(chatId, 'No wallet found. Create one first!', getMainMenuKeyboard(true));
        return;
      }

      let tokenPubkey: PublicKey;
      try {
        tokenPubkey = new PublicKey(tokenAddress);
      } catch {
        safeSendMessage(chatId, 'Invalid token address format.');
        return;
      }

      const validationMsg = await safeSendMessage(chatId, 'Validating token... Please wait...');

      const { name, symbol } = await fetchTokenInfo(tokenAddress);

      if (!SWAP_ROUTING) {
        try {
          const poolKeys = await getPoolKeys(solanaConnection, tokenPubkey);
          if (!poolKeys) {
            if (validationMsg && bot) {
              bot.editMessageText(
                `No Raydium pool found for ${name} (${symbol})\n\n` +
                `This token may not have liquidity on Raydium.\n\n` +
                `Try a different token with established liquidity.`,
                {
                  chat_id: chatId,
                  message_id: validationMsg.message_id,
                  ...getMainMenuKeyboard(true)
                }
              );
            }
            return;
          }
        } catch (poolError: any) {
          if (validationMsg && bot) {
            bot.editMessageText(
              `Error validating pool: ${poolError?.message || 'Unknown error'}\n\n` +
              `Please try again or use a different token.`,
              {
                chat_id: chatId,
                message_id: validationMsg.message_id,
                ...getMainMenuKeyboard(true)
              }
            );
          }
          return;
        }
      }

      session.tokenAddress = tokenAddress;
      session.tokenName = name;
      session.tokenSymbol = symbol;
      session.status = 'token_set';
      userSessions.set(userId, session);
      saveSessions();

      const message =
        `Token Added Successfully!\n\n` +
        `Name: ${name}\n` +
        `Symbol: ${symbol}\n` +
        `Address: \`${tokenAddress}\`\n\n` +
        `${!SWAP_ROUTING ? 'Raydium pool validated\n' : 'Jupiter routing enabled\n'}` +
        `Ready for trading!\n\n` +
        `Choose your next action:`;

      if (validationMsg && bot) {
        bot.editMessageText(message, {
          chat_id: chatId,
          message_id: validationMsg.message_id,
          parse_mode: 'Markdown',
          reply_markup: {
            inline_keyboard: [
              [
                { text: 'Set Buy Amount', callback_data: 'buy_amount_settings' },
                { text: 'Start Volume', callback_data: 'start_volume' }
              ],
              [
                { text: 'Back to Menu', callback_data: 'back_to_menu' }
              ]
            ]
          }
        });
      }
    } catch (error: any) {
      console.error('Add token error:', error);
      safeSendMessage(chatId, `Error adding token: ${error?.message || 'Unknown error'}`, getMainMenuKeyboard(true));
    }
  }
}

// Withdrawal function (same as before - keeps keys in history)
async function performWithdrawal(session: UserSession, withdrawAddress: string, chatId: number) {
  try {
    if (!session.walletKeypair) {
      throw new Error('No wallet found');
    }

    const mainKp = Keypair.fromSecretKey(base58.decode(session.walletKeypair));

    // Track results
    let totalGathered = 0;
    let successfulGathers = 0;
    let failedWallets: { address: string; reason: string; balance: number }[] = [];
    let walletsWithFunds: { address: string; balance: number }[] = [];
    let walletsWithTokens: { address: string; tokenAmount: number; walletNumber: number }[] = [];

    // ============================================================
    // STEP 1: CHECK FOR TOKENS IN ALL WALLETS
    // ============================================================

    safeSendMessage(chatId, `🔍 Step 1: Checking all wallets for tokens...\n\nThis may take a moment...`);

    const allWallets = [...session.tradingWallets];

    // Add historical wallets if they exist
    if (session.tradingWalletsHistory && session.tradingWalletsHistory.length > 0) {
      session.tradingWalletsHistory.forEach(batch => {
        allWallets.push(...batch);
      });
    }

    // Remove duplicates
    const uniqueWallets = Array.from(
      new Map(allWallets.map(w => [w.address, w])).values()
    );

    console.log(`Checking ${uniqueWallets.length} wallets for tokens...`);

    if (!session.tokenAddress) {
      console.log('⚠️ No token set - skipping token check');
      safeSendMessage(chatId, `⚠️ No token configured - skipping token check\n\nProceeding to SOL withdrawal...`);
    } else {
      const baseMint = new PublicKey(session.tokenAddress);

      for (let i = 0; i < uniqueWallets.length; i++) {
        try {
          const wallet = Keypair.fromSecretKey(base58.decode(uniqueWallets[i].privateKey));

          // Check for token account
          const tokenAccount = await getCachedTokenAccount(wallet.publicKey, baseMint);

          if (!tokenAccount) {
            console.log(`Wallet ${i + 1}/${uniqueWallets.length}: No token account`);
            continue;
          }

          const tokenBalInfo = await solanaConnection.getTokenAccountBalance(tokenAccount, 'confirmed');
          const tokenAmount = tokenBalInfo.value.uiAmount || 0;

          if (tokenAmount > 0) {
            console.log(`Wallet ${i + 1}/${uniqueWallets.length}: Has ${tokenAmount.toFixed(6)} ${session.tokenSymbol || 'tokens'}`);
            walletsWithTokens.push({
              address: wallet.publicKey.toBase58(),
              tokenAmount,
              walletNumber: i + 1
            });
          } else {
            console.log(`Wallet ${i + 1}/${uniqueWallets.length}: No tokens`);
          }

          await sleep(500); // Rate limit protection

        } catch (error: any) {
          const errorMsg = error?.message || '';
          if (errorMsg.includes('could not find account')) {
            console.log(`Wallet ${i + 1}/${uniqueWallets.length}: No token account (never traded)`);
          } else {
            console.log(`Wallet ${i + 1}/${uniqueWallets.length}: Error - ${errorMsg}`);
          }
        }
      }

      // ============================================================
      // STEP 2: IF TOKENS FOUND, SELL THEM ALL
      // ============================================================

      if (walletsWithTokens.length > 0) {
        const totalTokens = walletsWithTokens.reduce((sum, w) => sum + w.tokenAmount, 0);

        safeSendMessage(chatId,
          `⚠️ TOKENS DETECTED!\n\n` +
          `Found tokens in ${walletsWithTokens.length} wallets\n` +
          `Total: ${totalTokens.toFixed(6)} ${session.tokenSymbol || 'tokens'}\n\n` +
          `Selling all tokens before withdrawal...\n` +
          `This will take ${Math.ceil(walletsWithTokens.length * 6)} seconds.`
        );

        console.log(`\n${'═'.repeat(80)}`);
        console.log(`SELLING TOKENS FROM ${walletsWithTokens.length} WALLETS`);
        console.log('═'.repeat(80));

        let tokensSold = 0;
        let tokenSellFailed = 0;

        for (let i = 0; i < walletsWithTokens.length; i++) {
          const item = walletsWithTokens[i];

          try {
            console.log(`\n[${i + 1}/${walletsWithTokens.length}] Selling ${item.tokenAmount.toFixed(6)} tokens from wallet ${item.walletNumber}...`);

            const wallet = uniqueWallets.find(w => w.address === item.address);
            if (!wallet) {
              console.log(`❌ Wallet not found in list`);
              tokenSellFailed++;
              continue;
            }

            const walletKp = Keypair.fromSecretKey(base58.decode(wallet.privateKey));
            const tokenAccount = await getAssociatedTokenAddress(baseMint, walletKp.publicKey);
            const tokenBalInfo = await solanaConnection.getTokenAccountBalance(tokenAccount);
            const tokenBalance = tokenBalInfo.value.amount;

            // Try to sell
            let sellTx;
            if (SWAP_ROUTING) {
              sellTx = await getSellTxWithJupiter(walletKp, baseMint, tokenBalance, true); // High priority
            } else {
              const poolKeys = await getPoolKeys(solanaConnection, baseMint);
              if (poolKeys) {
                sellTx = await getSellTx(solanaConnection, walletKp, baseMint, NATIVE_MINT, tokenBalance, poolKeys.id);
              }
            }

            if (!sellTx) {
              console.log(`❌ Failed to create sell transaction`);
              tokenSellFailed++;
              await sleep(3000);
              continue;
            }

            const latestBlockhash = await solanaConnection.getLatestBlockhash('confirmed');
            const txSig = await execute(sellTx, latestBlockhash, false);

            if (txSig) {
              console.log(`✅ Sold successfully: ${txSig}`);
              tokensSold++;

              session.tradingStats.totalSells++;
              session.tradingStats.successfulTxs++;
              saveSessions();
            } else {
              console.log(`❌ Sell transaction failed to execute`);
              tokenSellFailed++;
            }

          } catch (error: any) {
            const errorMsg = error?.message || '';

            // Check if it's a "no route" error (amount too small)
            if (errorMsg.includes('COULD_NOT_FIND_ANY_ROUTE') || errorMsg.includes('Could not find any route')) {
              console.log(`⚠️ Token amount too small to sell (dust) - skipping`);
              // Don't count as failure - this is expected
              continue;
            }

            console.error(`❌ Sell error:`, errorMsg);
            tokenSellFailed++;
          }

          // Wait between sells to avoid rate limit
          await sleep(6000);
        }

        console.log(`\n${'═'.repeat(80)}`);
        console.log(`TOKEN SELL COMPLETE`);
        console.log(`Successful: ${tokensSold} | Failed: ${tokenSellFailed}`);
        console.log('═'.repeat(80) + '\n');

        safeSendMessage(chatId,
          `✅ Token Sell Complete\n\n` +
          `Sold: ${tokensSold}/${walletsWithTokens.length}\n` +
          `Failed: ${tokenSellFailed}\n\n` +
          `${tokenSellFailed > 0 ? '⚠️ Some tokens could not be sold (likely dust amounts)\n\n' : ''}` +
          `Proceeding to SOL withdrawal...`
        );

        // Wait a bit for sells to settle
        await sleep(5000);
      } else {
        console.log('✅ No tokens found in any wallet');
        safeSendMessage(chatId, `✅ No tokens detected\n\nProceeding to SOL withdrawal...`);
      }
    }

    // ============================================================
    // STEP 3: GATHER SOL FROM CURRENT TRADING WALLETS
    // ============================================================

    safeSendMessage(chatId, `💰 Step 2: Gathering SOL from trading wallets...`);

    if (session.tradingWallets && session.tradingWallets.length > 0) {
      console.log(`\nGathering from ${session.tradingWallets.length} active trading wallets...`);

      for (let i = 0; i < session.tradingWallets.length; i++) {
        try {
          const wallet = Keypair.fromSecretKey(base58.decode(session.tradingWallets[i].privateKey));
          const balance = await solanaConnection.getBalance(wallet.publicKey);
          const solBalance = balance / LAMPORTS_PER_SOL;

          if (balance === 0) {
            console.log(`Active wallet ${i + 1} has 0 balance, skipping`);
            continue;
          }

          walletsWithFunds.push({
            address: wallet.publicKey.toBase58(),
            balance: solBalance
          });

          const rent = await solanaConnection.getMinimumBalanceForRentExemption(0);
          const txFee = 20000;
          const transferAmount = balance - rent - txFee;

          if (transferAmount <= 0) {
            console.log(`Active wallet ${i + 1} has insufficient funds after rent: ${solBalance.toFixed(6)} SOL`);
            failedWallets.push({
              address: wallet.publicKey.toBase58(),
              reason: 'Insufficient after rent',
              balance: solBalance
            });
            continue;
          }

          const transaction = new Transaction().add(
            ComputeBudgetProgram.setComputeUnitPrice({ microLamports: 200_000 }),
            ComputeBudgetProgram.setComputeUnitLimit({ units: 40_000 }),
            SystemProgram.transfer({
              fromPubkey: wallet.publicKey,
              toPubkey: mainKp.publicKey,
              lamports: transferAmount
            })
          );

          const latestBlockhash = await solanaConnection.getLatestBlockhash('confirmed');
          transaction.recentBlockhash = latestBlockhash.blockhash;
          transaction.feePayer = wallet.publicKey;

          const messageV0 = new TransactionMessage({
            payerKey: wallet.publicKey,
            recentBlockhash: latestBlockhash.blockhash,
            instructions: transaction.instructions,
          }).compileToV0Message();

          const versionedTx = new VersionedTransaction(messageV0);
          versionedTx.sign([wallet]);

          const sig = await execute(versionedTx, latestBlockhash);

          if (sig) {
            const gatheredAmount = transferAmount / LAMPORTS_PER_SOL;
            totalGathered += gatheredAmount;
            successfulGathers++;
            console.log(`Gathered ${gatheredAmount.toFixed(6)} SOL from active wallet ${i + 1}`);
          } else {
            failedWallets.push({
              address: wallet.publicKey.toBase58(),
              reason: 'Transaction failed',
              balance: solBalance
            });
          }

          await sleep(1500);

        } catch (error: any) {
          const errorMsg = error?.message || 'Unknown error';
          console.error(`Failed to gather from active wallet ${i + 1}:`, errorMsg);

          try {
            const balance = await solanaConnection.getBalance(
              Keypair.fromSecretKey(base58.decode(session.tradingWallets[i].privateKey)).publicKey
            );
            failedWallets.push({
              address: session.tradingWallets[i].address,
              reason: errorMsg,
              balance: balance / LAMPORTS_PER_SOL
            });
          } catch { }

          continue;
        }
      }

      if (successfulGathers > 0) {
        safeSendMessage(chatId,
          `✅ Gathered ${totalGathered.toFixed(6)} SOL from ${successfulGathers}/${session.tradingWallets.length} active wallets!`
        );
        await sleep(3000);
      }
    }

    // ============================================================
    // STEP 4: GATHER SOL FROM HISTORICAL WALLETS
    // ============================================================

    let historicalWallets: TradingWallet[] = [];
    if (session.tradingWalletsHistory && session.tradingWalletsHistory.length > 0) {
      session.tradingWalletsHistory.forEach(batch => {
        historicalWallets.push(...batch);
      });

      const currentAddresses = new Set(session.tradingWallets.map(w => w.address));
      historicalWallets = Array.from(
        new Map(historicalWallets.map(w => [w.address, w])).values()
      ).filter(w => !currentAddresses.has(w.address));

      if (historicalWallets.length > 0) {
        safeSendMessage(chatId, `🔍 Step 3: Checking ${historicalWallets.length} historical wallets...`);

        let historicalGathered = 0;
        let historicalSuccess = 0;

        for (let i = 0; i < historicalWallets.length; i++) {
          try {
            const wallet = Keypair.fromSecretKey(base58.decode(historicalWallets[i].privateKey));
            const balance = await solanaConnection.getBalance(wallet.publicKey);
            const solBalance = balance / LAMPORTS_PER_SOL;

            if (balance === 0) continue;

            walletsWithFunds.push({
              address: wallet.publicKey.toBase58(),
              balance: solBalance
            });

            const rent = await solanaConnection.getMinimumBalanceForRentExemption(0);
            const txFee = 20000;
            const transferAmount = balance - rent - txFee;

            if (transferAmount <= 0) {
              failedWallets.push({
                address: wallet.publicKey.toBase58(),
                reason: 'Insufficient after rent',
                balance: solBalance
              });
              continue;
            }

            const transaction = new Transaction().add(
              ComputeBudgetProgram.setComputeUnitPrice({ microLamports: 200_000 }),
              ComputeBudgetProgram.setComputeUnitLimit({ units: 40_000 }),
              SystemProgram.transfer({
                fromPubkey: wallet.publicKey,
                toPubkey: mainKp.publicKey,
                lamports: transferAmount
              })
            );

            const latestBlockhash = await solanaConnection.getLatestBlockhash('confirmed');
            transaction.recentBlockhash = latestBlockhash.blockhash;
            transaction.feePayer = wallet.publicKey;

            const messageV0 = new TransactionMessage({
              payerKey: wallet.publicKey,
              recentBlockhash: latestBlockhash.blockhash,
              instructions: transaction.instructions,
            }).compileToV0Message();

            const versionedTx = new VersionedTransaction(messageV0);
            versionedTx.sign([wallet]);

            const sig = await execute(versionedTx, latestBlockhash);

            if (sig) {
              const gatheredAmount = transferAmount / LAMPORTS_PER_SOL;
              historicalGathered += gatheredAmount;
              historicalSuccess++;
            } else {
              failedWallets.push({
                address: wallet.publicKey.toBase58(),
                reason: 'Transaction failed',
                balance: solBalance
              });
            }

            await sleep(1500);

          } catch (error: any) {
            const errorMsg = error?.message || 'Unknown error';
            try {
              const balance = await solanaConnection.getBalance(
                Keypair.fromSecretKey(base58.decode(historicalWallets[i].privateKey)).publicKey
              );
              failedWallets.push({
                address: historicalWallets[i].address,
                reason: errorMsg,
                balance: balance / LAMPORTS_PER_SOL
              });
            } catch { }
            continue;
          }
        }

        if (historicalSuccess > 0) {
          totalGathered += historicalGathered;
          successfulGathers += historicalSuccess;
          safeSendMessage(chatId,
            `✅ Gathered ${historicalGathered.toFixed(6)} SOL from ${historicalSuccess} historical wallets!`
          );
          await sleep(3000);
        }
      }
    }

    // ============================================================
    // STEP 5: WITHDRAW FROM MAIN WALLET TO USER ADDRESS
    // ============================================================

    safeSendMessage(chatId, `💸 Step 4: Withdrawing from main wallet to your address...`);

    const finalBalance = await solanaConnection.getBalance(mainKp.publicKey);
    if (finalBalance === 0) {
      let statusMsg = `No SOL to withdraw from main wallet.\n\n`;

      if (walletsWithFunds.length === 0 && failedWallets.length === 0) {
        statusMsg += `All wallets are empty.`;
      } else if (failedWallets.length > 0) {
        statusMsg += `⚠️ ${failedWallets.length} wallets have SOL but couldn't be gathered:\n\n`;
        failedWallets.slice(0, 5).forEach(w => {
          statusMsg += `${w.address.substring(0, 8)}...${w.address.substring(w.address.length - 4)}\n`;
          statusMsg += `Balance: ${w.balance.toFixed(6)} SOL\n`;
          statusMsg += `Reason: ${w.reason}\n\n`;
        });
        if (failedWallets.length > 5) {
          statusMsg += `...and ${failedWallets.length - 5} more\n\n`;
        }
        statusMsg += `Use "Export Session" to get private keys and manually withdraw.`;
      }

      safeSendMessage(chatId, statusMsg, getMainMenuKeyboard(true));
      return;
    }

    const solBalance = finalBalance / LAMPORTS_PER_SOL;
    const rentExemption = await solanaConnection.getMinimumBalanceForRentExemption(0);
    const txFee = 15000;
    const withdrawableAmount = finalBalance - rentExemption - txFee;

    if (withdrawableAmount <= 5000) {
      safeSendMessage(chatId,
        `Insufficient balance in main wallet.\n\n` +
        `Main Balance: ${solBalance.toFixed(6)} SOL\n` +
        `After fees: Too low to withdraw\n\n` +
        `Total gathered from trading wallets: ${totalGathered.toFixed(6)} SOL`,
        getMainMenuKeyboard(true)
      );
      return;
    }

    const withdrawableSol = withdrawableAmount / LAMPORTS_PER_SOL;

    const withdrawTx = new Transaction().add(
      ComputeBudgetProgram.setComputeUnitLimit({ units: 40_000 }),
      ComputeBudgetProgram.setComputeUnitPrice({ microLamports: 100_000 }),
      SystemProgram.transfer({
        fromPubkey: mainKp.publicKey,
        toPubkey: new PublicKey(withdrawAddress),
        lamports: withdrawableAmount
      })
    );

    const latestBlockhash = await solanaConnection.getLatestBlockhash('confirmed');
    withdrawTx.feePayer = mainKp.publicKey;
    withdrawTx.recentBlockhash = latestBlockhash.blockhash;

    const messageV0 = new TransactionMessage({
      payerKey: mainKp.publicKey,
      recentBlockhash: latestBlockhash.blockhash,
      instructions: withdrawTx.instructions,
    }).compileToV0Message();

    const transaction = new VersionedTransaction(messageV0);
    transaction.sign([mainKp]);

    const txSig = await execute(transaction, latestBlockhash);

    if (txSig) {
      if (session.tradingWallets && session.tradingWallets.length > 0) {
        if (!session.tradingWalletsHistory) {
          session.tradingWalletsHistory = [];
        }
        session.tradingWalletsHistory.push([...session.tradingWallets]);
      }

      session.tradingWallets = [];
      userSessions.set(session.userId, session);
      saveAfterCriticalOperation(session);

      let successMsg =
        `✅ WITHDRAWAL SUCCESSFUL!\n\n` +
        `📊 SUMMARY:\n` +
        `${walletsWithTokens.length > 0 ? `Tokens Sold: ${walletsWithTokens.length} wallets\n` : ''}` +
        `SOL Gathered: ${totalGathered.toFixed(6)} SOL (${successfulGathers} wallets)\n` +
        `Main Wallet: ${withdrawableSol.toFixed(6)} SOL\n` +
        `━━━━━━━━━━━━━━━━━━━━━━\n` +
        `Grand Total: ${(totalGathered + withdrawableSol).toFixed(6)} SOL\n\n` +
        `💸 Sent to:\n${withdrawAddress.substring(0, 8)}...${withdrawAddress.substring(withdrawAddress.length - 4)}\n\n` +
        `🔗 Transaction:\nhttps://solscan.io/tx/${txSig}`;

      if (failedWallets.length > 0) {
        successMsg +=
          `\n\n⚠️ ${failedWallets.length} wallets couldn't be gathered:\n\n`;
        failedWallets.slice(0, 3).forEach(w => {
          successMsg += `${w.address.substring(0, 6)}...${w.address.substring(w.address.length - 4)}: `;
          successMsg += `${w.balance.toFixed(6)} SOL (${w.reason})\n`;
        });
        if (failedWallets.length > 3) {
          successMsg += `...and ${failedWallets.length - 3} more\n`;
        }
        successMsg += `\nUse "Export Session" to manually recover these.`;
      }

      safeSendMessage(chatId, successMsg, getMainMenuKeyboard(true));
    } else {
      throw new Error('Withdrawal transaction failed');
    }

  } catch (error: any) {
    console.error('Withdrawal error:', error);
    safeSendMessage(chatId,
      `Withdrawal failed: ${error?.message || 'Unknown error'}\n\n` +
      `Try again or use "Export Session" to manually withdraw.`,
      getMainMenuKeyboard(true)
    );
  }
}

process.on('SIGINT', () => {
  console.log('Shutting down bot gracefully...');
  activeTraders.forEach(userId => {
    const session = userSessions.get(userId);
    if (session && bot) {
      session.botRunning = false;
      safeSendMessage(session.chatId, 'Bot is shutting down. All trading stopped.');
    }
  });
  saveSessions();
  console.log('All sessions saved. Goodbye!');
  process.exit(0);
});

process.on('SIGTERM', () => {
  saveSessions();
  process.exit(0);
});

// Test mode startup warning
if (TEST_MODE) {
  console.log('\n' + '═'.repeat(80));
  console.log('🧪 TEST MODE ENABLED - FOR TESTING ONLY');
  console.log('═'.repeat(80));
  console.log(`Will trigger cooldown after ${TEST_MODE_TRIGGER_AFTER} successful trades`);
  console.log(`(In production: ~75-90 trades before real rate limit)`);
  console.log(`Set TEST_MODE=false in .env to disable`);
  console.log('═'.repeat(80) + '\n');
}

if (!BOT_TOKEN) {
  console.log('No Telegram token found. Add TELEGRAM_BOT_TOKEN to .env file.');
  process.exit(1);
} else {
  console.log('Smart Distribution Volume Bot initialized!');
  console.log(`Sessions directory: ${SESSIONS_DIR}`);
  console.log(`Using ${SWAP_ROUTING ? 'Jupiter' : 'Raydium'} for swaps`);
  console.log(`Buy range: ${BUY_LOWER_AMOUNT} - ${BUY_UPPER_AMOUNT} SOL`);
}