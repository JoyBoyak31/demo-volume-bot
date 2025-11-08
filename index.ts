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
} from './constants'
import { Data, editJson, readJson, saveDataToFile, sleep } from './utils'
import base58 from 'bs58'
import { getBuyTx, getBuyTxWithJupiter, getSellTx, getSellTxWithJupiter } from './utils/swapOnlyAmm'
import { execute } from './executor/legacy'
import { getPoolKeys } from './utils/getPoolInfo'
import * as fs from 'fs'
import * as path from 'path'

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
  status: 'idle' | 'payment_pending' | 'payment_confirmed' | 'wallet_created' | 'token_set' | 'wallet_selection' | 'trading' | 'stopped' | 'awaiting_withdraw_address' | 'awaiting_distribution_amount';
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
}

const userSessions = new Map<number, UserSession>();
const SESSION_FILE = './user_sessions.json';
const SESSIONS_DIR = './user_sessions';
const activeTraders = new Set<number>();

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
      hasPaid: true,  // CHANGED: Set to true for free access
      paymentWallet: paymentWallet.publicKey.toBase58(),
      paymentWalletPrivateKey: base58.encode(paymentWallet.secretKey),
      paymentAmount: PAYMENT_AMOUNT,
      paymentConfirmed: true,  // CHANGED: Set to true for free access
      selectedWalletCount: 1,
      distributionConfig: {
        mode: 'auto'
      }
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
          { text: '2 Wallet', callback_data: 'select_wallets_2' },
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
          { text: 'Volume Calculator', callback_data: 'volume_calculator' }
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
        `Trading Wallet Created!\n\n` +
        `Your Trading Address:\n` +
        `\`${address}\`\n\n` +
        `Your Private Key:\n` +
        `\`${privateKey}\`\n\n` +
        `IMPORTANT: Your keys are auto-saved!\n` +
        `Use "Export Session" button anytime to download\n` +
        `You own this wallet and can import it anywhere\n` +
        `Use this for deposits and trading\n\n` +
        `Minimum: ${session.requiredDeposit} SOL\n` +
        `Recommended: 0.5-1.0 SOL\n\n` +
        `This message will auto-delete in 120 seconds for security.`;

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
              'Wallet creation message deleted for security.\n\n' +
              'Your wallet keys are safely stored. Use "Export Session" or "Show Private Key" to view them again.',
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
              [
                { text: 'Create Wallet', callback_data: 'create_wallet' }
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

    const keypair = Keypair.fromSecretKey(base58.decode(session.walletKeypair));
    const balance = await solanaConnection.getBalance(keypair.publicKey);
    const solBalance = balance / LAMPORTS_PER_SOL;

    let totalDistributed = 0;
    if (session.tradingWallets.length > 0) {
      for (const wallet of session.tradingWallets) {
        try {
          const walletBalance = await solanaConnection.getBalance(new PublicKey(wallet.address));
          totalDistributed += walletBalance / LAMPORTS_PER_SOL;
        } catch (e) {
          console.log(`Error fetching balance for wallet ${wallet.address}`);
        }
      }
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
            [
              { text: 'Add Token', callback_data: 'add_token' }
            ],
            [
              { text: 'Back to Menu', callback_data: 'back_to_menu' }
            ]
          ]
        }
      });
    }
  } catch (error: any) {
    console.error('Balance check error:', error);
    safeSendMessage(chatId, 'Error checking balance. Please try again.');
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

    const message =
      `Volume Generation Started!\n\n` +
      `Token: ${session.tokenName}\n` +
      `Symbol: ${session.tokenSymbol}\n` +
      `Address: \`${session.tokenAddress}\`\n\n` +
      `Distribution Mode: ${distConfig.mode.toUpperCase()}\n` +
      `Per Wallet: ${amountPerWallet.toFixed(4)} SOL\n` +
      `Total Distributed: ${totalToDistribute.toFixed(4)} SOL\n` +
      `Remaining in Main: ${remaining.toFixed(4)} SOL\n` +
      `Trading Wallets: ${walletCount}\n\n` +
      `Distributing SOL to ${walletCount} wallets...\n` +
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

async function distributeSol(mainKp: Keypair, distributionNum: number, amountPerWallet: number, session: UserSession): Promise<{ kp: Keypair, address: string, privateKey: string }[]> {
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
    const latestBlockhash = await solanaConnection.getLatestBlockhash();
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

      // Archive old trading wallets to history
      if (session.tradingWallets && session.tradingWallets.length > 0) {
        if (!session.tradingWalletsHistory) {
          session.tradingWalletsHistory = [];
        }
        session.tradingWalletsHistory.push([...session.tradingWallets]);
        console.log(`Archived ${session.tradingWallets.length} old trading wallets to history for user ${session.userId}`);
      }

      // Set new trading wallets as current
      session.tradingWallets = wallets.map(w => ({
        address: w.address,
        privateKey: w.privateKey
      }));

      saveAfterCriticalOperation(session);

      // CRITICAL: Wait for balances to be confirmed on-chain
      console.log("Waiting for distribution to be confirmed on-chain...");
      await sleep(5000); // Wait 5 seconds for blockchain confirmation

      // Verify each wallet received the funds
      let allWalletsVerified = true;
      for (let i = 0; i < wallets.length; i++) {
        const wallet = wallets[i];
        let attempts = 0;
        let verified = false;

        while (attempts < 10 && !verified) {
          try {
            const balance = await solanaConnection.getBalance(new PublicKey(wallet.address));
            const solBalance = balance / LAMPORTS_PER_SOL;

            if (solBalance >= amountPerWallet * 0.95) { // Allow 5% margin for fees
              console.log(`Wallet ${i + 1} verified: ${solBalance.toFixed(6)} SOL`);
              verified = true;
              break;
            }

            console.log(`Wallet ${i + 1} balance not ready yet: ${solBalance.toFixed(6)} SOL, waiting...`);
            await sleep(2000);
            attempts++;
          } catch (error) {
            console.log(`Error checking wallet ${i + 1} balance, attempt ${attempts + 1}/10`);
            await sleep(2000);
            attempts++;
          }
        }

        if (!verified) {
          console.log(`Warning: Wallet ${i + 1} balance could not be verified after 10 attempts`);
          allWalletsVerified = false;
        }
      }

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
        `Starting volume generation...\n` +
        `Live trading alerts incoming!`;

      safeSendMessage(session.chatId, message);

      return wallets;
    } else {
      throw new Error('Distribution transaction failed');
    }

  } catch (error: any) {
    console.error("Failed to distribute SOL:", error);
    const errorMessage = error?.message || 'Unknown error';
    safeSendMessage(session.chatId, `Failed to distribute SOL: ${errorMessage}`, getMainMenuKeyboard(true));
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
      await sleep((BUY_INTERVAL_MAX + BUY_INTERVAL_MIN) * walletIndex / 2);

      while (true) {
        try {
          const currentSession = userSessions.get(session.userId);
          if (!currentSession?.botRunning) {
            console.log(`Trading stopped for user ${session.userId}, wallet ${walletIndex + 1}`);
            break;
          }

          const BUY_INTERVAL = Math.round(Math.random() * (BUY_INTERVAL_MAX - BUY_INTERVAL_MIN) + BUY_INTERVAL_MIN);
          const walletAddress = kp.publicKey.toBase58();
          const shortWallet = walletAddress.substring(0, 6) + '...' + walletAddress.substring(walletAddress.length - 4);

          // Pre-check wallet balance before attempting buy
          let walletBalance = 0;
          let balanceCheckAttempts = 0;

          while (balanceCheckAttempts < 5) {
            try {
              const balance = await solanaConnection.getBalance(kp.publicKey);
              walletBalance = balance / LAMPORTS_PER_SOL;

              if (walletBalance > 0) {
                console.log(`Wallet ${walletIndex + 1} balance confirmed: ${walletBalance.toFixed(6)} SOL`);
                break;
              }

              console.log(`Wallet ${walletIndex + 1} balance still 0, waiting... (${balanceCheckAttempts + 1}/5)`);
              await sleep(3000);
              balanceCheckAttempts++;
            } catch (error) {
              console.log(`Error checking wallet ${walletIndex + 1} balance`);
              await sleep(3000);
              balanceCheckAttempts++;
            }
          }

          if (walletBalance === 0) {
            console.log(`Wallet ${walletIndex + 1} has no balance after 5 checks, skipping this cycle`);
            await sleep(10000); // Wait longer before trying again
            continue;
          }

          const buyResult = await performBuy(kp, baseMint, poolId, currentSession, walletIndex + 1, shortWallet);
          if (!buyResult || !currentSession.botRunning) {
            console.log(`Buy failed or session stopped for user ${session.userId}`);
            break;
          }

          await sleep(3000 + Math.random() * 2000);

          const sellResult = await performSell(kp, baseMint, poolId, currentSession, walletIndex + 1, shortWallet);
          if (!sellResult || !currentSession.botRunning) {
            console.log(`Sell failed or session stopped for user ${session.userId}`);
            break;
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
    const mainBalance = await solanaConnection.getBalance(mainKp.publicKey);
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

    const latestBlockhash = await solanaConnection.getLatestBlockhash();
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
  shortWallet: string
): Promise<boolean> {
  try {
    const walletBalance = await solanaConnection.getBalance(wallet.publicKey);
    const solBalance = walletBalance / LAMPORTS_PER_SOL;

    const buyAmount = IS_RANDOM
      ? Number((Math.random() * (BUY_UPPER_AMOUNT - BUY_LOWER_AMOUNT) + BUY_LOWER_AMOUNT).toFixed(6))
      : BUY_AMOUNT;

    const minimumRequired = buyAmount + ADDITIONAL_FEE;

    // Check if insufficient SOL for buying
    if (solBalance < minimumRequired) {
      console.log(`Wallet ${walletNumber} insufficient balance: ${solBalance.toFixed(6)} SOL, needs ${minimumRequired.toFixed(6)} SOL`);

      safeSendMessage(session.chatId,
        `⚠️ INSUFFICIENT SOL FOR TRADING\n\n` +
        `Wallet ${walletNumber} (${shortWallet})\n` +
        `Current: ${solBalance.toFixed(6)} SOL\n` +
        `Needed: ${minimumRequired.toFixed(6)} SOL\n\n` +
        `This wallet has run out of SOL for trading.\n` +
        `Consider stopping and withdrawing remaining funds.`,
        {
          reply_markup: {
            inline_keyboard: [
              [{ text: 'Stop Volume', callback_data: 'stop_volume' }],
              [{ text: 'Withdraw All', callback_data: 'withdraw_sol' }]
            ]
          }
        }
      );

      return false;
    }

    console.log(`Attempting buy: Wallet ${walletNumber}, Amount: ${buyAmount} SOL, Balance: ${solBalance} SOL`);

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
        if (attempts < maxAttempts) {
          await sleep(2000);
        }
      } catch (txError: any) {
        attempts++;
        console.error(`Transaction creation attempt ${attempts} failed:`, txError?.message);

        if (attempts < maxAttempts) {
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

      sendTradingNotification(session, 'error', {
        type: 'BUY',
        wallet: shortWallet,
        error: 'Transaction creation failed'
      });

      return false;
    }

    const latestBlockhash = await solanaConnection.getLatestBlockhash();
    const txSig = await execute(tx, latestBlockhash);

    if (txSig) {
      session.tradingStats.totalBuys++;
      session.tradingStats.totalVolumeSOL += buyAmount;
      session.tradingStats.successfulTxs++;
      session.tradingStats.lastActivity = Date.now();
      userSessions.set(session.userId, session);
      saveSessions();

      console.log(`Buy successful - User: ${session.userId}, Wallet: ${walletNumber}, Amount: ${buyAmount} SOL, TX: ${txSig}`);

      sendTradingNotification(session, 'buy', {
        success: true,
        amount: buyAmount.toFixed(6),
        wallet: shortWallet,
        signature: txSig
      });

      return true;
    } else {
      session.tradingStats.failedTxs++;
      saveSessions();

      sendTradingNotification(session, 'error', {
        type: 'BUY',
        wallet: shortWallet,
        error: 'Transaction execution failed'
      });

      return false;
    }

  } catch (error: any) {
    console.error(`Buy error for user ${session.userId}, wallet ${walletNumber}:`, error);
    session.tradingStats.failedTxs++;
    saveSessions();

    const errorMessage = error?.message || 'Unknown error';
    const shortError = errorMessage.length > 50 ? errorMessage.substring(0, 50) + '...' : errorMessage;

    sendTradingNotification(session, 'error', {
      type: 'BUY',
      wallet: shortWallet,
      error: shortError
    });

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
    // Check SOL balance before attempting sell
    const walletBalance = await solanaConnection.getBalance(wallet.publicKey);
    const solBalance = walletBalance / LAMPORTS_PER_SOL;

    // Need at least ADDITIONAL_FEE for swap fees
    if (solBalance < ADDITIONAL_FEE) {
      console.log(`Wallet ${walletNumber} needs refuel for swap. Balance: ${solBalance.toFixed(6)} SOL`);

      // Get main wallet keypair
      if (!session.walletKeypair) {
        console.log(`Cannot refuel - main wallet not found`);
        return false;
      }

      const mainKp = Keypair.fromSecretKey(base58.decode(session.walletKeypair));

      // Auto-refuel from main wallet
      const refueled = await refuelWalletFromMain(mainKp, wallet, session, walletNumber, shortWallet);

      if (!refueled) {
        console.log(`Failed to refuel wallet ${walletNumber}`);
        return false;
      }

      // Recheck balance after refuel
      const newBalance = await solanaConnection.getBalance(wallet.publicKey);
      const newSolBalance = newBalance / LAMPORTS_PER_SOL;

      if (newSolBalance < ADDITIONAL_FEE) {
        console.log(`Refuel completed but still insufficient: ${newSolBalance.toFixed(6)} SOL`);
        return false;
      }

      console.log(`Wallet ${walletNumber} refueled successfully. New balance: ${newSolBalance.toFixed(6)} SOL`);
    }

    // Find actual token account that holds this token
    const accounts = await solanaConnection.getTokenAccountsByOwner(wallet.publicKey, { mint: baseMint });

    if (accounts.value.length === 0) {
      console.log(`No token account found for wallet ${walletNumber}`);
      return false;
    }

    // Use the first token account found
    const tokenAccount = accounts.value[0].pubkey;

    // Wait for balance to appear
    let tokenBalance = '0';
    let tokenAmount = 0;
    let attempts = 0;

    while (attempts < 10) {
      try {
        const tokenBalInfo = await solanaConnection.getTokenAccountBalance(tokenAccount);
        tokenBalance = tokenBalInfo.value.amount;
        tokenAmount = tokenBalInfo.value.uiAmount || 0;

        if (tokenAmount > 0) {
          console.log(`Wallet ${walletNumber} token balance: ${tokenAmount}`);
          break;
        }
      } catch (error) {
        // Account might not be ready yet
      }

      console.log(`Waiting for token balance... wallet ${walletNumber} (${attempts + 1}/10)`);
      await sleep(2000);
      attempts++;
    }

    if (tokenAmount === 0 || tokenBalance === '0') {
      console.log(`No tokens to sell for wallet ${walletNumber}`);
      return false;
    }

    console.log(`Attempting sell: Wallet ${walletNumber}, Amount: ${tokenAmount} tokens`);

    let sellTx;
    let txAttempts = 0;
    const maxAttempts = 3;

    while (txAttempts < maxAttempts) {
      try {
        if (SWAP_ROUTING) {
          sellTx = await getSellTxWithJupiter(wallet, baseMint, tokenBalance);
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
        if (txAttempts < maxAttempts) {
          await sleep(2000);
        }
      } catch (txError: any) {
        txAttempts++;
        console.error(`Sell transaction creation attempt ${txAttempts} failed:`, txError?.message);

        if (txAttempts < maxAttempts) {
          await sleep(2000);
        } else {
          throw txError;
        }
      }
    }

    if (!sellTx) {
      console.log(`Failed to create sell transaction after ${maxAttempts} attempts`);
      session.tradingStats.failedTxs++;
      saveSessions();

      sendTradingNotification(session, 'error', {
        type: 'SELL',
        wallet: shortWallet,
        error: 'Transaction creation failed'
      });

      return false;
    }

    const latestBlockhash = await solanaConnection.getLatestBlockhash();
    const txSig = await execute(sellTx, latestBlockhash, false);

    if (txSig) {
      session.tradingStats.totalSells++;
      session.tradingStats.successfulTxs++;
      session.tradingStats.lastActivity = Date.now();
      userSessions.set(session.userId, session);
      saveSessions();

      console.log(`Sell successful - User: ${session.userId}, Wallet: ${walletNumber}, Amount: ${tokenAmount} ${session.tokenSymbol}, TX: ${txSig}`);

      sendTradingNotification(session, 'sell', {
        success: true,
        tokenAmount: tokenAmount.toFixed(6),
        wallet: shortWallet,
        signature: txSig
      });

      return true;
    } else {
      session.tradingStats.failedTxs++;
      saveSessions();

      sendTradingNotification(session, 'error', {
        type: 'SELL',
        wallet: shortWallet,
        error: 'Transaction execution failed'
      });

      return false;
    }

  } catch (error: any) {
    console.error(`Sell error for user ${session.userId}, wallet ${walletNumber}:`, error);
    session.tradingStats.failedTxs++;
    saveSessions();

    const errorMessage = error?.message || 'Unknown error';
    const shortError = errorMessage.length > 50 ? errorMessage.substring(0, 50) + '...' : errorMessage;

    sendTradingNotification(session, 'error', {
      type: 'SELL',
      wallet: shortWallet,
      error: shortError
    });

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
                { text: 'Volume Calculator', callback_data: 'volume_calculator' }
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
      `• Smart volume calculator\n` +
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

      case 'select_wallets_2':
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
                [{ text: 'Volume Calculator', callback_data: 'volume_calculator' }],
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
                [{ text: 'Volume Calculator', callback_data: 'volume_calculator' }],
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

        const statusMessage =
          `STATUS REPORT\n\n` +
          `Bot Status: ${session.botRunning ? 'ACTIVE' : 'STOPPED'}\n` +
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

      case 'volume_calculator':
        handleVolumeCalculator(userId, chatId, msg?.message_id);
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
        `Total to distribute: ${(amount * session.selectedWalletCount).toFixed(4)} SOL\n\n` +
        `Use Volume Calculator to see estimates.`,
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
                { text: 'Volume Calculator', callback_data: 'volume_calculator' },
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

    // PRIORITY 1: Gather from CURRENT trading wallets first
    if (session.tradingWallets && session.tradingWallets.length > 0) {
      safeSendMessage(chatId, `Gathering from ${session.tradingWallets.length} active trading wallets...`);

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

          // Calculate how much we can safely withdraw
          const rent = await solanaConnection.getMinimumBalanceForRentExemption(0);
          const txFee = 20000; // Conservative fee estimate
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

          await sleep(1500); // Rate limiting

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

    // PRIORITY 2: Gather from historical wallets (if any)
    let historicalWallets: TradingWallet[] = [];
    if (session.tradingWalletsHistory && session.tradingWalletsHistory.length > 0) {
      session.tradingWalletsHistory.forEach(batch => {
        historicalWallets.push(...batch);
      });

      // Remove duplicates and current wallets
      const currentAddresses = new Set(session.tradingWallets.map(w => w.address));
      historicalWallets = Array.from(
        new Map(historicalWallets.map(w => [w.address, w])).values()
      ).filter(w => !currentAddresses.has(w.address));

      if (historicalWallets.length > 0) {
        safeSendMessage(chatId, `Checking ${historicalWallets.length} historical wallets...`);

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

    // Now withdraw from main wallet
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
        `✅ Withdrawal Successful!\n\n` +
        `Total Gathered: ${totalGathered.toFixed(6)} SOL (${successfulGathers} wallets)\n` +
        `Main Wallet: ${withdrawableSol.toFixed(6)} SOL\n` +
        `Grand Total: ${(totalGathered + withdrawableSol).toFixed(6)} SOL\n` +
        `To: ${withdrawAddress.substring(0, 8)}...${withdrawAddress.substring(withdrawAddress.length - 4)}\n` +
        `TX: https://solscan.io/tx/${txSig}\n\n`;

      if (failedWallets.length > 0) {
        successMsg +=
          `\n⚠️ ${failedWallets.length} wallets couldn't be gathered:\n\n`;
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

if (!BOT_TOKEN) {
  console.log('No Telegram token found. Add TELEGRAM_BOT_TOKEN to .env file.');
  process.exit(1);
} else {
  console.log('Smart Distribution Volume Bot initialized!');
  console.log(`Sessions directory: ${SESSIONS_DIR}`);
  console.log(`Using ${SWAP_ROUTING ? 'Jupiter' : 'Raydium'} for swaps`);
  console.log(`Buy range: ${BUY_LOWER_AMOUNT} - ${BUY_UPPER_AMOUNT} SOL`);
}