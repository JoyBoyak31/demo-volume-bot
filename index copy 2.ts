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
} from './constants'
import { Data, editJson, readJson, saveDataToFile, sleep } from './utils'
import base58 from 'bs58'
import { getBuyTx, getBuyTxWithJupiter, getSellTx, getSellTxWithJupiter } from './utils/swapOnlyAmm'
import { execute } from './executor/legacy'
import { getPoolKeys } from './utils/getPoolInfo'
import * as fs from 'fs'

const BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN;
let bot: TelegramBot | null = null;

const PAYMENT_AMOUNT = 0.00001 ; // SOL required to access bot (fixed from 0.0001)

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

interface UserSession {
  userId: number;
  chatId: number;
  walletKeypair?: string;
  tokenAddress?: string;
  tokenName?: string;
  tokenSymbol?: string;
  status: 'idle' | 'payment_pending' | 'payment_confirmed' | 'wallet_created' | 'token_set' | 'wallet_selection' | 'trading' | 'stopped' | 'awaiting_withdraw_address';
  depositAddress?: string;
  requiredDeposit: number;
  isMonitoring: boolean;
  botRunning: boolean;
  tradingWallets: TradingWallet[];
  createdAt: number;
  tradingStats: TradingStats;
  hasPaid: boolean;
  paymentWallet?: string;
  paymentWalletPrivateKey?: string;
  paymentAmount: number;
  paymentConfirmed: boolean;
  userWalletPrivateKey?: string;
  selectedWalletCount: number;
}

const userSessions = new Map<number, UserSession>();
const SESSION_FILE = './user_sessions.json';
const activeTraders = new Set<number>();

function safeSendMessage(chatId: number, message: string, options?: any) {
  if (bot) {
    return bot.sendMessage(chatId, message, options);
  }
  console.log('Bot not initialized, message:', message);
}

function safeSendPhoto(chatId: number, photo: string, options?: any) {
  if (bot) {
    return bot.sendPhoto(chatId, photo, options);
  }
  console.log('Bot not initialized, photo message');
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

function saveSessions() {
  try {
    const sessionsObj: Record<string, UserSession> = {};
    userSessions.forEach((session, userId) => {
      sessionsObj[userId.toString()] = session;
    });
    fs.writeFileSync(SESSION_FILE, JSON.stringify(sessionsObj, null, 2));
  } catch (error) {
    console.error('Error saving sessions:', error);
  }
}

function getWalletSelectionKeyboard() {
  return {
    reply_markup: {
      inline_keyboard: [
        [
          { text: '1 Wallets', callback_data: 'select_wallets_1' },
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
          { text: '🔙 Back to Menu', callback_data: 'back_to_menu' }
        ]
      ]
    }
  };
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
      hasPaid: false,
      paymentWallet: paymentWallet.publicKey.toBase58(),
      paymentWalletPrivateKey: base58.encode(paymentWallet.secretKey),
      paymentAmount: PAYMENT_AMOUNT,
      paymentConfirmed: false,
      selectedWalletCount: 1
    };

    userSessions.set(userId, session);
    saveSessions();

    console.log(`Created new session for user ${userId} with payment wallet: ${session.paymentWallet}`);
  }

  if (!session.paymentWallet) {
    console.log(`Fixing missing payment wallet for user ${userId}`);
    const paymentWallet = Keypair.generate();
    session.paymentWallet = paymentWallet.publicKey.toBase58();
    session.paymentWalletPrivateKey = base58.encode(paymentWallet.secretKey);
    userSessions.set(userId, session);
    saveSessions();
  }

  return session;
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
        // Keep status as payment_confirmed, don't reset to idle
        if (currentSession.status === 'payment_pending') {
          currentSession.status = 'payment_confirmed';
        }
        userSessions.set(userId, currentSession);
        saveSessions();

        const message =
          `✅ Payment Confirmed!\n\n` +
          `💰 Received: ${solBalance.toFixed(6)} SOL\n` +
          `🎉 Thank you for your payment!\n\n` +
          `🚀 You now have full access to the Volume Bot!\n` +
          `Use the menu below to get started:`;

        safeSendMessage(currentSession.chatId, message, getMainMenuKeyboard(true));
        clearInterval(checkPayment);
      }

      lastBalance = solBalance;

    } catch (error) {
      console.error('Payment monitoring error:', error);
    }
  }, 15000);
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
                console.log(`Found token via DexScreener: ${pair.baseToken.name} (${pair.baseToken.symbol})`);
                return {
                  name: pair.baseToken.name,
                  symbol: pair.baseToken.symbol
                };
              }
            }
          }
        }
      }
    } catch (e: any) {
      console.log('DexScreener API failed:', e?.message || 'Unknown error');
    }

    try {
      const response = await fetch('https://tokens.jup.ag/all');
      if (response.ok) {
        const tokens: any = await response.json();
        const token = tokens.find((t: any) => t.address === tokenAddress);
        if (token && token.name && token.symbol) {
          console.log(`Found token via Jupiter list: ${token.name} (${token.symbol})`);
          return { name: token.name, symbol: token.symbol };
        }
      }
    } catch (e: any) {
      console.log('Jupiter token list failed:', e?.message || 'Unknown error');
    }

    console.log(`Could not fetch metadata for token: ${tokenAddress}`);
    return defaultInfo;

  } catch (error: any) {
    console.error('Token info fetch error:', error?.message || 'Unknown error');
    return defaultInfo;
  }
}

function getMainMenuKeyboard(isPaid: boolean = false) {
  if (!isPaid) {
    return {
      reply_markup: {
        inline_keyboard: [
          [
            { text: '💳 Make Payment (0.5 SOL)', callback_data: 'make_payment' }
          ],
          [
            { text: '❓ Why Payment Required?', callback_data: 'payment_info' },
            { text: '🔄 Check Payment', callback_data: 'check_payment' }
          ]
        ]
      }
    };
  }

  return {
    reply_markup: {
      inline_keyboard: [
        [
          { text: '💳 Create Wallet', callback_data: 'create_wallet' },
          { text: '💎 Check Balance', callback_data: 'check_balance' }
        ],
        [
          { text: '🪙 Add Token', callback_data: 'add_token' },
          { text: '🎛️ Select Wallets', callback_data: 'select_wallet_count' }
        ],
        [
          { text: '🚀 Start Volume', callback_data: 'start_volume' },
          { text: '🛑 Stop Volume', callback_data: 'stop_volume' }
        ],
        [
          { text: '📊 Status Report', callback_data: 'status_report' },
          { text: '💸 Withdraw SOL', callback_data: 'withdraw_sol' }
        ]
      ]
    }
  };
}

function requirePayment(session: UserSession, chatId: number, action: string): boolean {
  if (!session.hasPaid || !session.paymentConfirmed) {
    const message =
      `🔒 Payment Required\n\n` +
      `To use ${action}, you need to make a payment first.\n\n` +
      `💰 Payment: 0.5 SOL\n` +
      `🎯 Get full access to Volume Bot features\n\n` +
      `Click below to make payment:`;

    safeSendMessage(chatId, message, getMainMenuKeyboard(false));
    return false;
  }
  return true;
}

function getTradingControlsKeyboard(isTrading: boolean) {
  if (isTrading) {
    return {
      reply_markup: {
        inline_keyboard: [
          [
            { text: '🛑 Stop Trading', callback_data: 'stop_volume' },
            { text: '📊 Live Stats', callback_data: 'live_stats' }
          ],
          [
            { text: '🔙 Back to Menu', callback_data: 'back_to_menu' }
          ]
        ]
      }
    };
  } else {
    return {
      reply_markup: {
        inline_keyboard: [
          [
            { text: '🚀 Start Trading', callback_data: 'start_volume' },
            { text: '📊 Status Report', callback_data: 'status_report' }
          ],
          [
            { text: '🔙 Back to Menu', callback_data: 'back_to_menu' }
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
      message = `🟢 BUY EXECUTED\n\n` +
        `Amount: ${data.amount} SOL\n` +
        `Wallet: ${data.wallet}\n` +
        `Token: ${session.tokenSymbol}\n` +
        `TX: https://solscan.io/tx/${data.signature}\n` +
        `Total Buys: ${session.tradingStats.totalBuys}`;
    } else if (type === 'sell' && data.success) {
      message = `🔴 SELL EXECUTED\n\n` +
        `Tokens: ${data.tokenAmount} ${session.tokenSymbol}\n` +
        `Wallet: ${data.wallet}\n` +
        `TX: https://solscan.io/tx/${data.signature}\n` +
        `Total Sells: ${session.tradingStats.totalSells}`;
    } else if (type === 'error') {
      message = `❌ TRANSACTION FAILED\n\n` +
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
    `📊 TRADING UPDATE - ${session.tokenSymbol}\n\n` +
    `✅ Successful Buys: ${stats.totalBuys}\n` +
    `✅ Successful Sells: ${stats.totalSells}\n` +
    `❌ Failed TXs: ${stats.failedTxs}\n` +
    `📈 Volume Generated: ${stats.totalVolumeSOL.toFixed(4)} SOL\n` +
    `⏱️ Runtime: ${runtime.toFixed(1)} minutes\n` +
    `🎯 Success Rate: ${stats.successfulTxs > 0 ? ((stats.successfulTxs / (stats.successfulTxs + stats.failedTxs)) * 100).toFixed(1) : 0}%\n\n` +
    `Status: ACTIVE`;

  safeSendMessage(session.chatId, message, getTradingControlsKeyboard(true));

  stats.lastUpdateSent = Date.now();
  saveSessions();
}

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

  loadSessions();
  console.log('Enhanced Telegram Volume Bot is running...');

  bot.onText(/\/start/, async (msg) => {
    const chatId = msg.chat.id;
    const userId = msg.from?.id!;
    const firstName = msg.from?.first_name || 'User';

    const session = getUserSession(userId, chatId);

    console.log(`User ${userId} started bot. Payment wallet: ${session.paymentWallet}`);

    if (session.hasPaid && session.paymentConfirmed) {
      const welcomeMessage =
        `🚀 Welcome back ${firstName}!\n\n` +
        `✅ Payment Status: Confirmed\n` +
        `🎯 Volume Bot - Ready to use\n\n` +
        `Choose an option below:`;

      safeSendMessage(chatId, welcomeMessage, getMainMenuKeyboard(true));
    } else {
      const welcomeMessage =
        `🚀 Welcome ${firstName} to Volume Bot 2.0!\n\n` +
        `🎯 Professional Volume Generation Tool\n` +
        `💰 Advanced Multi-Wallet Trading System\n\n` +
        `✨ Features:\n` +
        `• Intelligent volume generation\n` +
        `• Real-time trading analytics\n` +
        `• Multi-wallet distribution\n` +
        `• Live trading notifications\n\n` +
        `💳 One-time payment: 0.5 SOL\n` +
        `🔓 Unlock full access to all features\n\n` +
        `Click below to get started:`;

      safeSendMessage(chatId, welcomeMessage, getMainMenuKeyboard(false));
    }
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
        if (!session.paymentWallet) {
          console.log(`Generating missing payment wallet for user ${userId}`);
          const paymentWallet = Keypair.generate();
          session.paymentWallet = paymentWallet.publicKey.toBase58();
          session.paymentWalletPrivateKey = base58.encode(paymentWallet.secretKey);
          userSessions.set(userId, session);
          saveSessions();
        }

        const paymentMessage =
          `💳 Payment Instructions\n\n` +
          `💰 Amount: ${PAYMENT_AMOUNT} SOL\n` +
          `📍 Send to: \`${session.paymentWallet}\`\n\n` +
          `⏰ Payment will be auto-detected\n` +
          `🔔 You'll receive confirmation when payment is received\n\n` +
          `⚠️ Send exactly ${PAYMENT_AMOUNT} SOL or more\n` +
          `⚠️ Payment timeout: 1 hour`;

        bot.editMessageText(paymentMessage, {
          chat_id: chatId,
          message_id: msg?.message_id,
          parse_mode: 'Markdown',
          reply_markup: {
            inline_keyboard: [
              [
                { text: '🔄 Check Payment', callback_data: 'check_payment' },
                { text: '❓ Need Help?', callback_data: 'payment_help' }
              ],
              [
                { text: '🔙 Back', callback_data: 'back_to_start' }
              ]
            ]
          }
        });

        if (!session.isMonitoring) {
          session.isMonitoring = true;
          session.status = 'payment_pending';
          saveSessions();
          monitorPayment(userId);
        }
        break;

      case 'check_payment':
        const paymentStatus = await checkPaymentStatus(session);
        if (paymentStatus) {
          session.hasPaid = true;
          session.paymentConfirmed = true;
          session.status = 'payment_confirmed';
          userSessions.set(userId, session);
          saveSessions();

          const confirmMessage =
            `✅ Payment Confirmed!\n\n` +
            `🎉 Welcome to Volume Bot!\n` +
            `🚀 You now have full access to all features\n\n` +
            `Get started:`;

          bot.editMessageText(confirmMessage, {
            chat_id: chatId,
            message_id: msg?.message_id,
            ...getMainMenuKeyboard(true)
          });
        } else {
          const pendingMessage =
            `⏳ Payment Pending\n\n` +
            `💰 Required: ${PAYMENT_AMOUNT} SOL\n` +
            `📍 Send to: \`${session.paymentWallet}\`\n\n` +
            `🔍 We're monitoring for your payment...\n` +
            `⏰ Please allow a few minutes for confirmation`;

          bot.editMessageText(pendingMessage, {
            chat_id: chatId,
            message_id: msg?.message_id,
            parse_mode: 'Markdown',
            reply_markup: {
              inline_keyboard: [
                [
                  { text: '🔄 Check Again', callback_data: 'check_payment' }
                ],
                [
                  { text: '🔙 Back', callback_data: 'back_to_start' }
                ]
              ]
            }
          });
        }
        break;

      case 'payment_info':
        const infoMessage =
          `💡 Why Payment Required?\n\n` +
          `🛡️ Quality Service: Ensures dedicated server resources\n` +
          `⚡ Premium Features: Access to advanced trading tools\n` +
          `🔧 Maintenance: Covers development and server costs\n` +
          `💪 Support: 24/7 customer support included\n\n` +
          `💰 One-time payment: 0.5 SOL\n` +
          `🎯 Lifetime access to Volume Bot\n` +
          `📈 Professional volume generation tools`;

        bot.editMessageText(infoMessage, {
          chat_id: chatId,
          message_id: msg?.message_id,
          reply_markup: {
            inline_keyboard: [
              [
                { text: '💳 Make Payment', callback_data: 'make_payment' }
              ],
              [
                { text: '🔙 Back', callback_data: 'back_to_start' }
              ]
            ]
          }
        });
        break;

      case 'payment_help':
        const helpMessage =
          `❓ Payment Help\n\n` +
          `1️⃣ Copy the wallet address\n` +
          `2️⃣ Send exactly 0.5 SOL (or more)\n` +
          `3️⃣ Wait for confirmation (1-2 minutes)\n` +
          `4️⃣ Access will be automatically granted\n\n` +
          `⚠️ Common Issues:\n` +
          `• Sending less than 0.5 SOL\n` +
          `• Wrong wallet address\n` +
          `• Network delays\n\n` +
          `📞 Need more help? Contact support`;

        bot.editMessageText(helpMessage, {
          chat_id: chatId,
          message_id: msg?.message_id,
          reply_markup: {
            inline_keyboard: [
              [
                { text: '🔄 Check Payment', callback_data: 'check_payment' }
              ],
              [
                { text: '🔙 Back', callback_data: 'make_payment' }
              ]
            ]
          }
        });
        break;

      case 'back_to_start':
        // Don't reset payment status when going back
        const startMessage = session.hasPaid ?
          `🏠 Main Menu\n\nWelcome back! Choose what you'd like to do:` :
          `🚀 Volume Bot 2.0\n\nProfessional trading tool requires payment to access.\n\nMake payment to unlock all features:`;

        bot.editMessageText(startMessage, {
          chat_id: chatId,
          message_id: msg?.message_id,
          ...getMainMenuKeyboard(session.hasPaid)
        });
        break;

      case 'select_wallet_count':
        if (!requirePayment(session, chatId, 'Wallet Selection')) return;

        const selectionMessage =
          `🎛️ Select Trading Wallets\n\n` +
          `Choose how many wallets to use for volume generation:\n\n` +
          `💡 More wallets = More realistic trading patterns\n` +
          `⚡ Fewer wallets = Faster execution\n\n` +
          `Current selection: ${session.selectedWalletCount} wallets\n` +
          `Range: 1-12 wallets`;

        bot.editMessageText(selectionMessage, {
          chat_id: chatId,
          message_id: msg?.message_id,
          ...getWalletSelectionKeyboard()
        });
        break;

      case 'select_wallets_1':
      case 'select_wallets_6':
      case 'select_wallets_8':
      case 'select_wallets_10':
      case 'select_wallets_12':
        if (!requirePayment(session, chatId, 'Wallet Selection')) return;

        const walletCount = parseInt(data.split('_')[2]);
        session.selectedWalletCount = walletCount;
        session.status = 'wallet_selection';
        userSessions.set(userId, session);
        saveSessions();

        const confirmMessage =
          `✅ Wallet Selection Updated!\n\n` +
          `📊 Selected: ${walletCount} trading wallets\n\n` +
          `Benefits of ${walletCount} wallets:\n` +
          `${getWalletBenefits(walletCount)}\n\n` +
          `You can change this selection anytime before starting volume generation.`;

        bot.editMessageText(confirmMessage, {
          chat_id: chatId,
          message_id: msg?.message_id,
          reply_markup: {
            inline_keyboard: [
              [
                { text: '🚀 Start Volume', callback_data: 'start_volume' },
                { text: '🔄 Change Selection', callback_data: 'select_wallet_count' }
              ],
              [
                { text: '🔙 Back to Menu', callback_data: 'back_to_menu' }
              ]
            ]
          }
        });
        break;

      case 'create_wallet':
        if (!requirePayment(session, chatId, 'Wallet Creation')) return;
        handleCreateWallet(userId, chatId, msg?.message_id);
        break;

      case 'check_balance':
        if (!requirePayment(session, chatId, 'Balance Check')) return;
        handleCheckBalance(userId, chatId, msg?.message_id);
        break;

      case 'add_token':
        if (!requirePayment(session, chatId, 'Add Token')) return;
        const tokenMessage =
          `🪙 Add Token\n\n` +
          `Enter the token address you want to trade:\n\n` +
          `📍 Send the token contract address as a message\n` +
          `🔍 Bot will validate the token automatically\n` +
          `✅ Pool verification included\n\n` +
          `⚠️ Accepts any Solana token with liquidity\n` +
          `Example: 74Rq6Bmckiq8qvARhdqxPfQtkQsxsqVKCbDQL5PKpump`;

        bot.editMessageText(tokenMessage, {
          chat_id: chatId,
          message_id: msg?.message_id,
          reply_markup: {
            inline_keyboard: [
              [{ text: '🔙 Back to Menu', callback_data: 'back_to_menu' }]
            ]
          }
        });
        break;

      case 'start_volume':
        if (!requirePayment(session, chatId, 'Volume Generation')) return;
        handleStartVolume(userId, chatId, msg?.message_id);
        break;

      case 'stop_volume':
        if (!requirePayment(session, chatId, 'Stop Volume')) return;
        handleStopVolume(userId, chatId, msg?.message_id);
        break;

      case 'back_to_menu':
        // Preserve payment status when going back to menu
        if (!session.hasPaid) {
          const message = `🚀 Volume Bot 2.0\n\nPayment required to access features.`;
          bot.editMessageText(message, {
            chat_id: chatId,
            message_id: msg?.message_id,
            ...getMainMenuKeyboard(false)
          });
        } else {
          const menuMessage = `🏠 Main Menu\n\nChoose what you'd like to do:`;
          bot.editMessageText(menuMessage, {
            chat_id: chatId,
            message_id: msg?.message_id,
            ...getMainMenuKeyboard(true)
          });
        }
        break;

      default:
        // Handle dynamic callbacks like change_token_
        if (data && data.startsWith('change_token_')) {
          if (!requirePayment(session, chatId, 'Change Token')) return;

          const newTokenAddress = data.replace('change_token_', '');

          // Stop bot if running
          if (session.botRunning) {
            bot.editMessageText(
              '❌ Please stop the volume bot before changing tokens.',
              {
                chat_id: chatId,
                message_id: msg?.message_id,
                reply_markup: {
                  inline_keyboard: [
                    [{ text: '🛑 Stop Volume', callback_data: 'stop_volume' }],
                    [{ text: '🔙 Back to Menu', callback_data: 'back_to_menu' }]
                  ]
                }
              }
            );
            return;
          }

          // Clear old token and trading wallets
          session.tokenAddress = undefined;
          session.tokenName = undefined;
          session.tokenSymbol = undefined;
          session.tradingWallets = [];
          userSessions.set(userId, session);
          saveSessions();

          // Process new token
          await handleTokenInput(userId, chatId, newTokenAddress);
        }
        break;

      case 'status_report':
        if (!requirePayment(session, chatId, 'Status Report')) return;
        handleStatusReport(userId, chatId, msg?.message_id);
        break;

      case 'live_stats':
        if (!requirePayment(session, chatId, 'Live Stats')) return;
        if (session.botRunning) {
          sendPeriodicUpdate(session);
        } else {
          bot.editMessageText('❌ Trading is not currently active.', {
            chat_id: chatId,
            message_id: msg?.message_id,
            ...getTradingControlsKeyboard(false)
          });
        }
        break;

      case 'withdraw_sol':
        if (!requirePayment(session, chatId, 'Withdraw SOL')) return;
        handleWithdrawCommand(userId, chatId, msg?.message_id);
        break;
    }
  });

  function getWalletBenefits(count: number): string {
    switch (count) {
      case 4:
        return `• Fast execution\n• Lower SOL requirements\n• Quick setup`;
      case 6:
        return `• Balanced speed & distribution\n• Good for medium tokens\n• Moderate SOL needs`;
      case 8:
        return `• Enhanced distribution\n• Better volume spread\n• Professional pattern`;
      case 10:
        return `• High-quality patterns\n• Excellent distribution\n• Institutional-like trading`;
      case 12:
        return `• Maximum distribution\n• Most realistic patterns\n• Premium volume generation`;
      default:
        return `• Customized trading pattern`;
    }
  }

  async function handleCreateWallet(userId: number, chatId: number, messageId?: number) {
    try {
      const session = getUserSession(userId, chatId);

      if (!session.walletKeypair) {
        const newKeypair = Keypair.generate();
        session.walletKeypair = base58.encode(newKeypair.secretKey);
        session.depositAddress = newKeypair.publicKey.toBase58();
        session.userWalletPrivateKey = base58.encode(newKeypair.secretKey);
        session.status = 'wallet_created';
        userSessions.set(userId, session);
        saveSessions();

        const message =
          `✅ Trading Wallet Created!\n\n` +
          `💳 Your Trading Address:\n` +
          `\`${session.depositAddress}\`\n\n` +
          `🔑 Your Private Key:\n` +
          `\`${session.userWalletPrivateKey}\`\n\n` +
          `⚠️ IMPORTANT: Save your private key securely!\n` +
          `🔒 You own this wallet and can import it anywhere\n` +
          `💰 Use this for deposits and trading\n\n` +
          `💰 Minimum: ${session.requiredDeposit} SOL\n` +
          `💰 Recommended: 0.5-1.0 SOL`;

        if (messageId && bot) {
          bot.editMessageText(message, {
            chat_id: chatId,
            message_id: messageId,
            parse_mode: 'Markdown',
            reply_markup: {
              inline_keyboard: [
                [
                  { text: '💎 Check Balance', callback_data: 'check_balance' },
                  { text: '🪙 Add Token', callback_data: 'add_token' }
                ],
                [
                  { text: '🔙 Back to Menu', callback_data: 'back_to_menu' }
                ]
              ]
            }
          });
        }

        if (!session.isMonitoring) {
          session.isMonitoring = true;
          monitorDeposits(userId);
        }
      } else {
        const message =
          `✅ You already have a trading wallet!\n\n` +
          `💳 Address: \`${session.depositAddress}\`\n` +
          `🔑 Private Key: \`${session.userWalletPrivateKey}\`\n\n` +
          `Use the options below to continue:`;

        if (messageId && bot) {
          bot.editMessageText(message, {
            chat_id: chatId,
            message_id: messageId,
            parse_mode: 'Markdown',
            reply_markup: {
              inline_keyboard: [
                [
                  { text: '💎 Check Balance', callback_data: 'check_balance' },
                  { text: '🪙 Add Token', callback_data: 'add_token' }
                ],
                [
                  { text: '🔙 Back to Menu', callback_data: 'back_to_menu' }
                ]
              ]
            }
          });
        }
      }
    } catch (error: any) {
      console.error('Wallet creation error:', error);
      safeSendMessage(chatId, 'Error creating wallet. Please try again.');
    }
  }

  async function handleCheckBalance(userId: number, chatId: number, messageId?: number) {
    try {
      const session = getUserSession(userId, chatId);

      if (!session.walletKeypair) {
        const message = '❌ No wallet found. Create one first!';

        if (messageId && bot) {
          bot.editMessageText(message, {
            chat_id: chatId,
            message_id: messageId,
            reply_markup: {
              inline_keyboard: [
                [
                  { text: '💳 Create Wallet', callback_data: 'create_wallet' }
                ],
                [
                  { text: '🔙 Back to Menu', callback_data: 'back_to_menu' }
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

      // Calculate total balance including distributed wallets
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
        `💰 Wallet Balance Report\n\n` +
        `💎 Main Wallet: ${solBalance.toFixed(6)} SOL\n` +
        `📊 Distributed: ${totalDistributed.toFixed(6)} SOL (${session.tradingWallets.length} wallets)\n` +
        `💰 Total Balance: ${totalBalance.toFixed(6)} SOL\n` +
        `📍 Address: \`${session.depositAddress}\`\n\n` +
        `Status: ${solBalance >= session.requiredDeposit ? '✅ Ready for trading' : `❌ Need ${(session.requiredDeposit - solBalance).toFixed(4)} more SOL`}\n` +
        `Token: ${session.tokenName || 'Not set'}\n` +
        `Trading: ${session.botRunning ? '🟢 Active' : '🔴 Inactive'}`;

      if (messageId && bot) {
        bot.editMessageText(message, {
          chat_id: chatId,
          message_id: messageId,
          parse_mode: 'Markdown',
          reply_markup: {
            inline_keyboard: [
              [
                { text: '🔄 Refresh', callback_data: 'check_balance' },
                { text: '💸 Withdraw', callback_data: 'withdraw_sol' }
              ],
              [
                { text: '🔙 Back to Menu', callback_data: 'back_to_menu' }
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

  async function handleStartVolume(userId: number, chatId: number, messageId?: number) {
    try {
      const session = getUserSession(userId, chatId);

      if (!session.walletKeypair) {
        const message = '❌ No wallet found. Create one first!';
        if (messageId && bot) {
          bot.editMessageText(message, {
            chat_id: chatId,
            message_id: messageId,
            reply_markup: {
              inline_keyboard: [
                [{ text: '💳 Create Wallet', callback_data: 'create_wallet' }],
                [{ text: '🔙 Back to Menu', callback_data: 'back_to_menu' }]
              ]
            }
          });
        }
        return;
      }

      if (!session.tokenAddress) {
        const message = '❌ No token set. Add a token first!';
        if (messageId && bot) {
          bot.editMessageText(message, {
            chat_id: chatId,
            message_id: messageId,
            reply_markup: {
              inline_keyboard: [
                [{ text: '🪙 Add Token', callback_data: 'add_token' }],
                [{ text: '🔙 Back to Menu', callback_data: 'back_to_menu' }]
              ]
            }
          });
        }
        return;
      }

      if (session.botRunning) {
        const message = '⚠️ Volume bot already running!';
        if (messageId && bot) {
          bot.editMessageText(message, {
            chat_id: chatId,
            message_id: messageId,
            reply_markup: {
              inline_keyboard: [
                [{ text: '🛑 Stop Volume', callback_data: 'stop_volume' }],
                [{ text: '📊 Live Stats', callback_data: 'live_stats' }],
                [{ text: '🔙 Back to Menu', callback_data: 'back_to_menu' }]
              ]
            }
          });
        }
        return;
      }

      const keypair = Keypair.fromSecretKey(base58.decode(session.walletKeypair));
      const balance = await solanaConnection.getBalance(keypair.publicKey);
      const solBalance = balance / LAMPORTS_PER_SOL;

      const requiredSol = (DISTRIBUTION_AMOUNT * session.selectedWalletCount) + 0.001;

      if (solBalance < requiredSol) {
        const message =
          `❌ Insufficient balance!\n\n` +
          `💰 Current: ${solBalance.toFixed(4)} SOL\n` +
          `💰 Required: ${requiredSol.toFixed(4)} SOL\n` +
          `📊 For ${session.selectedWalletCount} wallets\n` +
          `💳 Send to: \`${session.depositAddress}\``;

        if (messageId && bot) {
          bot.editMessageText(message, {
            chat_id: chatId,
            message_id: messageId,
            parse_mode: 'Markdown',
            reply_markup: {
              inline_keyboard: [
                [
                  { text: '💎 Check Balance', callback_data: 'check_balance' },
                  { text: '🎛️ Reduce Wallets', callback_data: 'select_wallet_count' }
                ],
                [
                  { text: '🔙 Back to Menu', callback_data: 'back_to_menu' }
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

      const message =
        `🚀 Volume Generation Started!\n\n` +
        `🔥 Token: ${session.tokenName}\n` +
        `🏷️ Symbol: ${session.tokenSymbol}\n` +
        `📍 Address: \`${session.tokenAddress}\`\n` +
        `💰 Balance: ${solBalance.toFixed(4)} SOL\n` +
        `📊 Trading Wallets: ${session.selectedWalletCount}\n\n` +
        `⏳ Distributing SOL to ${session.selectedWalletCount} wallets...\n` +
        `🔔 You'll receive live trading updates!\n\n` +
        `Status: 🟢 ACTIVE`;

      if (messageId && bot) {
        bot.editMessageText(message, {
          chat_id: chatId,
          message_id: messageId,
          parse_mode: 'Markdown',
          reply_markup: {
            inline_keyboard: [
              [
                { text: '🛑 Stop Volume', callback_data: 'stop_volume' },
                { text: '📊 Live Stats', callback_data: 'live_stats' }
              ],
              [
                { text: '🔙 Back to Menu', callback_data: 'back_to_menu' }
              ]
            ]
          }
        });
      }

      startVolumeBot(session);

    } catch (error: any) {
      console.error('Start volume error:', error);
      safeSendMessage(chatId, `❌ Error starting volume: ${error?.message || 'Unknown error'}`);
    }
  }

  function handleStopVolume(userId: number, chatId: number, messageId?: number) {
    try {
      const session = getUserSession(userId, chatId);

      if (!session.botRunning) {
        const message = '❌ No volume bot is currently running.';
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
        `🛑 Volume Bot Stopped!\n\n` +
        `📊 Final Statistics:\n` +
        `✅ Total Buys: ${stats.totalBuys}\n` +
        `✅ Total Sells: ${stats.totalSells}\n` +
        `📈 Volume Generated: ${stats.totalVolumeSOL.toFixed(4)} SOL\n` +
        `⏱️ Runtime: ${runtime.toFixed(1)} minutes\n` +
        `🎯 Success Rate: ${stats.successfulTxs > 0 ? ((stats.successfulTxs / (stats.successfulTxs + stats.failedTxs)) * 100).toFixed(1) : 0}%\n\n` +
        `Status: 🔴 STOPPED`;

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

  async function handleStatusReport(userId: number, chatId: number, messageId?: number) {
    try {
      const session = getUserSession(userId, chatId);

      let message = `📊 DETAILED BOT STATUS REPORT\n\n`;

      message += `🔹 Status: ${session.status.toUpperCase()}\n`;
      message += `🔹 Created: ${new Date(session.createdAt).toLocaleString()}\n\n`;

      if (session.walletKeypair) {
        const keypair = Keypair.fromSecretKey(base58.decode(session.walletKeypair));
        try {
          const balance = await solanaConnection.getBalance(keypair.publicKey);
          const solBalance = balance / LAMPORTS_PER_SOL;
          message += `💳 MAIN WALLET:\n`;
          message += `   💰 Balance: ${solBalance.toFixed(6)} SOL\n`;
          message += `   📍 Address: \`${session.depositAddress}\`\n\n`;
        } catch {
          message += `💳 MAIN WALLET: Error fetching balance\n\n`;
        }
      } else {
        message += `💳 MAIN WALLET: Not created\n\n`;
      }

      if (session.tokenAddress) {
        message += `🪙 TOKEN INFO:\n`;
        message += `   🔥 Name: ${session.tokenName}\n`;
        message += `   🏷️ Symbol: ${session.tokenSymbol}\n`;
        message += `   📍 Address: \`${session.tokenAddress}\`\n\n`;
      } else {
        message += `🪙 TOKEN: Not set\n\n`;
      }

      if (session.tradingWallets.length > 0) {
        message += `🔄 TRADING WALLETS (${session.tradingWallets.length}):\n`;
        let totalDistributed = 0;
        for (let i = 0; i < Math.min(session.tradingWallets.length, 3); i++) {
          const wallet = session.tradingWallets[i];
          try {
            const balance = await solanaConnection.getBalance(new PublicKey(wallet.address));
            const solBalance = balance / LAMPORTS_PER_SOL;
            totalDistributed += solBalance;
            message += `   ${i + 1}. ${wallet.address.substring(0, 8)}...${wallet.address.substring(wallet.address.length - 4)} (${solBalance.toFixed(4)} SOL)\n`;
          } catch {
            message += `   ${i + 1}. ${wallet.address.substring(0, 8)}...${wallet.address.substring(wallet.address.length - 4)} (Error)\n`;
          }
        }
        if (session.tradingWallets.length > 3) {
          message += `   ... and ${session.tradingWallets.length - 3} more\n`;
        }
        message += `   💰 Total Distributed: ${totalDistributed.toFixed(4)} SOL\n`;
        message += `\n`;
      }

      const stats = session.tradingStats;
      if (stats && stats.startTime > 0) {
        const runtime = (Date.now() - stats.startTime) / 1000 / 60;
        message += `📈 TRADING STATISTICS:\n`;
        message += `   ✅ Successful Buys: ${stats.totalBuys}\n`;
        message += `   ✅ Successful Sells: ${stats.totalSells}\n`;
        message += `   ❌ Failed TXs: ${stats.failedTxs}\n`;
        message += `   📈 Volume Generated: ${stats.totalVolumeSOL.toFixed(6)} SOL\n`;
        message += `   🎯 Success Rate: ${stats.successfulTxs > 0 ? ((stats.successfulTxs / (stats.successfulTxs + stats.failedTxs)) * 100).toFixed(1) : 0}%\n`;
        message += `   ⏱️ Runtime: ${runtime.toFixed(1)} minutes\n`;
        if (runtime > 0) {
          message += `   📊 Avg Volume/Min: ${(stats.totalVolumeSOL / runtime).toFixed(4)} SOL\n`;
        }
        message += `   🕐 Last Activity: ${new Date(stats.lastActivity).toLocaleTimeString()}\n\n`;
      }

      message += `🤖 BOT STATUS:\n`;
      message += `   🔄 Trading: ${session.botRunning ? '🟢 ACTIVE' : '🔴 STOPPED'}\n`;
      message += `   👁️ Monitoring: ${session.isMonitoring ? '🟢 YES' : '🔴 NO'}\n\n`;

      message += `📝 NEXT ACTIONS:\n`;
      if (!session.walletKeypair) {
        message += `   • Create wallet first\n`;
      } else if (!session.tokenAddress) {
        message += `   • Add token to trade\n`;
      } else if (!session.botRunning) {
        message += `   • Ready to start trading!\n`;
      } else {
        message += `   • Bot is actively trading!\n   • Monitor live stats\n`;
      }

      if (messageId && bot) {
        bot.editMessageText(message, {
          chat_id: chatId,
          message_id: messageId,
          parse_mode: 'Markdown',
          reply_markup: {
            inline_keyboard: [
              [
                { text: '🔄 Refresh', callback_data: 'status_report' },
                { text: '📊 Live Stats', callback_data: 'live_stats' }
              ],
              [
                { text: '🔙 Back to Menu', callback_data: 'back_to_menu' }
              ]
            ]
          }
        });
      }
    } catch (error: any) {
      console.error('Status error:', error);
      safeSendMessage(chatId, 'Error getting status.');
    }
  }

  function handleWithdrawCommand(userId: number, chatId: number, messageId?: number) {
    try {
      const session = getUserSession(userId, chatId);

      if (!session.walletKeypair) {
        const message = '❌ No wallet found. Create one first!';
        if (messageId && bot) {
          bot.editMessageText(message, {
            chat_id: chatId,
            message_id: messageId,
            reply_markup: {
              inline_keyboard: [
                [{ text: '💳 Create Wallet', callback_data: 'create_wallet' }],
                [{ text: '🔙 Back to Menu', callback_data: 'back_to_menu' }]
              ]
            }
          });
        }
        return;
      }

      if (session.botRunning) {
        const message = '❌ Please stop the bot first before withdrawing.';
        if (messageId && bot) {
          bot.editMessageText(message, {
            chat_id: chatId,
            message_id: messageId,
            reply_markup: {
              inline_keyboard: [
                [{ text: '🛑 Stop Volume', callback_data: 'stop_volume' }],
                [{ text: '🔙 Back to Menu', callback_data: 'back_to_menu' }]
              ]
            }
          });
        }
        return;
      }

      // Set status to awaiting withdraw address
      session.status = 'awaiting_withdraw_address';
      userSessions.set(userId, session);
      saveSessions();

      const message =
        `💸 Withdraw SOL\n\n` +
        `📤 Enter the Solana address to withdraw to:\n\n` +
        `📍 Send the destination address as a message\n` +
        `⚠️ Make sure the address is correct!\n` +
        `💰 All SOL from main wallet and distributed wallets will be gathered\n\n` +
        `Example: 9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM`;

      if (messageId && bot) {
        bot.editMessageText(message, {
          chat_id: chatId,
          message_id: messageId,
          reply_markup: {
            inline_keyboard: [
              [{ text: '🔙 Back to Menu', callback_data: 'back_to_menu' }]
            ]
          }
        });
      }

    } catch (error: any) {
      console.error('Withdraw command error:', error);
      safeSendMessage(chatId, 'Error initiating withdrawal.');
    }
  }

  // Handle text messages for token input AND withdrawal address
  // Handle text messages for token input AND withdrawal address
  bot.on('message', async (msg) => {
    const text = msg.text;
    const chatId = msg.chat.id;
    const userId = msg.from?.id!;

    // Skip if no text or it's a command
    if (!text || text.startsWith('/')) return;

    const session = userSessions.get(userId);
    if (!session) return;

    // Skip if user hasn't paid
    if (!session.hasPaid || !session.paymentConfirmed) {
      return;
    }

    const trimmedText = text.trim();

    // Check if it's a valid Solana address (32-44 characters, base58)
    const isSolanaAddress = trimmedText.length >= 32 && 
                           trimmedText.length <= 44 && 
                           /^[1-9A-HJ-NP-Za-km-z]+$/.test(trimmedText);

    if (!isSolanaAddress) {
      // Not a valid Solana address format
      return;
    }

    // Validate it's actually a valid PublicKey
    let isValidPubkey = false;
    try {
      new PublicKey(trimmedText);
      isValidPubkey = true;
    } catch {
      safeSendMessage(chatId, '❌ Invalid Solana address format.');
      return;
    }

    // Check if user is awaiting withdrawal address
    if (session.status === 'awaiting_withdraw_address') {
      // Reset status immediately to prevent duplicate processing
      session.status = 'stopped';
      userSessions.set(userId, session);
      saveSessions();
      
      await performWithdrawal(session, trimmedText, chatId);
      return;
    }

    // Otherwise, treat it as a token address
    if (session.walletKeypair && !session.tokenAddress) {
      // Only process if user has wallet but no token set
      await handleTokenInput(userId, chatId, trimmedText);
    } else if (session.walletKeypair && session.tokenAddress) {
      // User already has a token, confirm if they want to change it
      safeSendMessage(chatId, 
        `⚠️ You already have a token set:\n\n` +
        `Current: ${session.tokenName} (${session.tokenSymbol})\n` +
        `${session.tokenAddress}\n\n` +
        `Do you want to change to a new token?\n` +
        `New: ${trimmedText}`, 
        {
          reply_markup: {
            inline_keyboard: [
              [
                { text: '✅ Yes, Change Token', callback_data: `change_token_${trimmedText}` },
                { text: '❌ Cancel', callback_data: 'back_to_menu' }
              ]
            ]
          }
        }
      );
    } else if (!session.walletKeypair) {
      safeSendMessage(chatId, '❌ Please create a wallet first before adding a token!', {
        reply_markup: {
          inline_keyboard: [
            [{ text: '💳 Create Wallet', callback_data: 'create_wallet' }],
            [{ text: '🔙 Back to Menu', callback_data: 'back_to_menu' }]
          ]
        }
      });
    }
  });

  async function handleTokenInput(userId: number, chatId: number, tokenAddress: string) {
    try {
      const session = getUserSession(userId, chatId);

      if (!session.walletKeypair) {
        safeSendMessage(chatId, '❌ No wallet found. Create one first!', {
          reply_markup: {
            inline_keyboard: [
              [{ text: '💳 Create Wallet', callback_data: 'create_wallet' }],
              [{ text: '🔙 Back to Menu', callback_data: 'back_to_menu' }]
            ]
          }
        });
        return;
      }

      let tokenPubkey: PublicKey;
      try {
        tokenPubkey = new PublicKey(tokenAddress);
      } catch {
        safeSendMessage(chatId, '❌ Invalid token address format.');
        return;
      }

      // Send a single validation message
      const validationMsg = await safeSendMessage(chatId, '🔍 Validating token... Please wait...');

      const { name, symbol } = await fetchTokenInfo(tokenAddress);

      if (!SWAP_ROUTING) {
        try {
          const poolKeys = await getPoolKeys(solanaConnection, tokenPubkey);
          if (!poolKeys) {
            if (validationMsg && bot) {
              bot.editMessageText(
                `❌ No Raydium trading pool found for ${name} (${symbol})\n\n` +
                `This token may:\n` +
                `• Not have liquidity on Raydium\n` +
                `• Be a new token without a pool\n` +
                `• Have insufficient liquidity\n\n` +
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
          console.log('Pool validation successful:', poolKeys.id);
        } catch (poolError: any) {
          if (validationMsg && bot) {
            bot.editMessageText(
              `❌ Error validating pool: ${poolError?.message || 'Unknown error'}\n\n` +
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
        `✅ Token Added Successfully!\n\n` +
        `🔥 Name: ${name}\n` +
        `🏷️ Symbol: ${symbol}\n` +
        `📍 Address: \`${tokenAddress}\`\n\n` +
        `${!SWAP_ROUTING ? '✅ Raydium pool validated\n' : '✅ Jupiter routing enabled\n'}` +
        `🚀 Ready for trading!\n\n` +
        `Choose your next action:`;

      if (validationMsg && bot) {
        bot.editMessageText(message, {
          chat_id: chatId,
          message_id: validationMsg.message_id,
          parse_mode: 'Markdown',
          reply_markup: {
            inline_keyboard: [
              [
                { text: '🚀 Start Volume', callback_data: 'start_volume' },
                { text: '📊 Check Status', callback_data: 'status_report' }
              ],
              [
                { text: '🔙 Back to Menu', callback_data: 'back_to_menu' }
              ]
            ]
          }
        });
      }
    } catch (error: any) {
      console.error('Add token error:', error);
      safeSendMessage(chatId, `❌ Error adding token: ${error?.message || 'Unknown error'}\n\nPlease try again.`, getMainMenuKeyboard(true));
    }
  }
}

// Withdrawal function with gathering from all wallets
async function performWithdrawal(session: UserSession, withdrawAddress: string, chatId: number) {
  try {
    if (!session.walletKeypair) {
      throw new Error('No wallet found');
    }

    const mainKp = Keypair.fromSecretKey(base58.decode(session.walletKeypair));

    // First, gather SOL from all distributed wallets back to main wallet
    if (session.tradingWallets.length > 0) {
      safeSendMessage(chatId, `⏳ Gathering SOL from ${session.tradingWallets.length} trading wallets...`);

      let totalGathered = 0;
      let successfulGathers = 0;

      for (let i = 0; i < session.tradingWallets.length; i++) {
        try {
          const wallet = Keypair.fromSecretKey(base58.decode(session.tradingWallets[i].privateKey));
          const balance = await solanaConnection.getBalance(wallet.publicKey);

          if (balance === 0) {
            console.log(`Wallet ${i + 1} has 0 balance, skipping...`);
            continue;
          }

          // More conservative rent calculation
          const rent = await solanaConnection.getMinimumBalanceForRentExemption(0);
          // Leave more buffer: rent + 15000 lamports for transaction fee
          const feeBuffer = 15000;
          const transferAmount = balance - rent - feeBuffer;

          if (transferAmount <= 5000) { // Skip if less than 5000 lamports (~0.000005 SOL)
            console.log(`Wallet ${i + 1} has insufficient balance to transfer: ${balance / LAMPORTS_PER_SOL} SOL`);
            continue;
          }

          const transaction = new Transaction().add(
            ComputeBudgetProgram.setComputeUnitPrice({ microLamports: 500_000 }),
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
            console.log(`✅ Gathered from wallet ${i + 1}: ${gatheredAmount.toFixed(6)} SOL`);
          }

          // Delay between transactions to avoid rate limiting
          await sleep(1500);

        } catch (error: any) {
          console.error(`Failed to gather from wallet ${i + 1}:`, error);
          // Continue with other wallets even if one fails
          continue;
        }
      }

      if (successfulGathers > 0) {
        safeSendMessage(chatId,
          `✅ Gathered ${totalGathered.toFixed(6)} SOL from ${successfulGathers}/${session.tradingWallets.length} wallets!\n` +
          `⏳ Now withdrawing total balance...`
        );

        // Wait for gathering transactions to finalize
        await sleep(5000);
      } else {
        safeSendMessage(chatId,
          `⚠️ Could not gather from distributed wallets (insufficient balance)\n` +
          `⏳ Withdrawing from main wallet only...`
        );
      }
    }

    // Now withdraw from main wallet
    const finalBalance = await solanaConnection.getBalance(mainKp.publicKey);

    if (finalBalance === 0) {
      safeSendMessage(chatId, '💰 No SOL balance to withdraw.', getMainMenuKeyboard(true));
      return;
    }

    const solBalance = finalBalance / LAMPORTS_PER_SOL;
    const rentExemption = await solanaConnection.getMinimumBalanceForRentExemption(0);
    const txFee = 15000; // More conservative fee estimate

    const withdrawableAmount = finalBalance - rentExemption - txFee;

    if (withdrawableAmount <= 5000) {
      safeSendMessage(chatId,
        `❌ Insufficient balance for withdrawal.\n\n` +
        `Current: ${solBalance.toFixed(6)} SOL\n` +
        `Rent + Fee: ${((rentExemption + txFee) / LAMPORTS_PER_SOL).toFixed(6)} SOL\n\n` +
        `Cannot withdraw - balance too low.`,
        getMainMenuKeyboard(true)
      );
      return;
    }

    const withdrawableSol = withdrawableAmount / LAMPORTS_PER_SOL;

    safeSendMessage(chatId,
      `💸 Withdrawal Details:\n\n` +
      `Current Balance: ${solBalance.toFixed(6)} SOL\n` +
      `Withdrawable: ${withdrawableSol.toFixed(6)} SOL\n` +
      `To: ${withdrawAddress.substring(0, 8)}...${withdrawAddress.substring(withdrawAddress.length - 4)}\n\n` +
      `⏳ Processing withdrawal...`
    );

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
      // Clear trading wallets after successful withdrawal
      session.tradingWallets = [];
      userSessions.set(session.userId, session);
      saveSessions();

      safeSendMessage(chatId,
        `✅ Withdrawal Successful!\n\n` +
        `Amount: ${withdrawableSol.toFixed(6)} SOL\n` +
        `To: ${withdrawAddress}\n` +
        `Transaction: https://solscan.io/tx/${txSig}\n\n` +
        `💡 Your main wallet still exists for future use!\n` +
        `Trading wallets have been cleared.\n\n` +
        `You can continue using the bot anytime.`,
        getMainMenuKeyboard(true)
      );

      console.log(`Withdrawal successful for user ${session.userId}: ${withdrawableSol} SOL to ${withdrawAddress}`);
    } else {
      throw new Error('Transaction failed to execute');
    }

  } catch (error: any) {
    console.error('Withdrawal error:', error);
    const errorMessage = error?.message || 'Unknown error occurred';
    safeSendMessage(chatId, `❌ Withdrawal failed: ${errorMessage}`, getMainMenuKeyboard(true));
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
        const message =
          `💎 Deposit Detected!\n\n` +
          `📈 +${depositAmount.toFixed(6)} SOL received\n` +
          `💰 New Balance: ${solBalance.toFixed(6)} SOL\n\n` +
          `${solBalance >= currentSession.requiredDeposit ? '✅ Ready for trading!' : `❌ Need ${(currentSession.requiredDeposit - solBalance).toFixed(4)} more SOL`}`;

        safeSendMessage(currentSession.chatId, message, {
          reply_markup: {
            inline_keyboard: [
              [
                { text: '🪙 Add Token', callback_data: 'add_token' },
                { text: '💎 Check Balance', callback_data: 'check_balance' }
              ],
              [
                { text: '🔙 Back to Menu', callback_data: 'back_to_menu' }
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

async function startVolumeBot(session: UserSession) {
  if (!session.tokenAddress || !session.walletKeypair) {
    safeSendMessage(session.chatId, '❌ Missing requirements for trading', getMainMenuKeyboard(session.hasPaid));
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
        safeSendMessage(session.chatId, '❌ Pool not found for token', getMainMenuKeyboard(session.hasPaid));
        session.botRunning = false;
        saveSessions();
        return;
      }
      poolId = new PublicKey(poolKeys.id);
    }

    const tradingWallets = await distributeSol(mainKp, distributionNum, session);
    if (!tradingWallets || tradingWallets.length === 0) {
      safeSendMessage(session.chatId, '❌ Failed to distribute SOL', getMainMenuKeyboard(session.hasPaid));
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

async function distributeSol(mainKp: Keypair, distributionNum: number, session: UserSession): Promise<{ kp: Keypair, address: string, privateKey: string }[]> {
  try {
    const wallets: { kp: Keypair, address: string, privateKey: string }[] = [];
    const sendSolTx: TransactionInstruction[] = [];

    sendSolTx.push(
      ComputeBudgetProgram.setComputeUnitLimit({ units: 200_000 }),
      ComputeBudgetProgram.setComputeUnitPrice({ microLamports: 300_000 })
    );

    for (let i = 0; i < distributionNum; i++) {
      let solAmount = DISTRIBUTION_AMOUNT;
      if (DISTRIBUTION_AMOUNT < ADDITIONAL_FEE + BUY_UPPER_AMOUNT) {
        solAmount = ADDITIONAL_FEE + BUY_UPPER_AMOUNT;
      }

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
          lamports: Math.floor(solAmount * LAMPORTS_PER_SOL)
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

      session.tradingWallets = wallets.map(w => ({
        address: w.address,
        privateKey: w.privateKey
      }));
      saveSessions();

      let walletList = '';
      wallets.forEach((wallet, i) => {
        walletList += `${i + 1}. ${wallet.address.substring(0, 8)}...${wallet.address.substring(wallet.address.length - 4)}\n`;
      });

      const message =
        `✅ SOL Distribution Complete!\n\n` +
        `📊 Trading Wallets Created:\n${walletList}\n` +
        `🔗 Transaction: https://solscan.io/tx/${txSig}\n\n` +
        `🚀 Starting volume generation...\n` +
        `🔔 Live trading alerts incoming!`;

      safeSendMessage(session.chatId, message);

      return wallets;
    } else {
      throw new Error('Distribution transaction failed');
    }

  } catch (error: any) {
    console.error("Failed to distribute SOL:", error);
    const errorMessage = error?.message || 'Unknown error';
    safeSendMessage(session.chatId, `❌ Failed to distribute SOL: ${errorMessage}`, getMainMenuKeyboard(true));
    return [];
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
    if (solBalance < minimumRequired) {
      console.log(`Wallet ${walletNumber} insufficient balance: ${solBalance} SOL, needs ${minimumRequired} SOL`);
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

      console.log(`✅ Buy successful - User: ${session.userId}, Wallet: ${walletNumber}, Amount: ${buyAmount} SOL, TX: ${txSig}`);

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
    console.error(`❌ Buy error for user ${session.userId}, wallet ${walletNumber}:`, error);
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
    const tokenAta = await getAssociatedTokenAddress(baseMint, wallet.publicKey);

    let tokenBalInfo;
    try {
      tokenBalInfo = await solanaConnection.getTokenAccountBalance(tokenAta);
    } catch (error) {
      console.log(`Token account not found for wallet ${walletNumber}, might not have bought yet`);
      await sleep(5000);

      try {
        tokenBalInfo = await solanaConnection.getTokenAccountBalance(tokenAta);
      } catch (error2) {
        console.log(`Still no token account for wallet ${walletNumber}, skipping sell`);
        return false;
      }
    }

    if (!tokenBalInfo || !tokenBalInfo.value.amount || tokenBalInfo.value.amount === '0') {
      console.log(`No tokens to sell for wallet ${walletNumber}`);
      return false;
    }

    const tokenBalance = tokenBalInfo.value.amount;
    const tokenAmount = tokenBalInfo.value.uiAmount || 0;

    if (tokenAmount < 0.000001) {
      console.log(`Token amount too small to sell: ${tokenAmount}`);
      return false;
    }

    console.log(`Attempting sell: Wallet ${walletNumber}, Amount: ${tokenAmount} tokens`);

    let sellTx;
    let attempts = 0;
    const maxAttempts = 3;

    while (attempts < maxAttempts) {
      try {
        if (SWAP_ROUTING) {
          sellTx = await getSellTxWithJupiter(wallet, baseMint, tokenBalance);
        } else if (poolId) {
          sellTx = await getSellTx(solanaConnection, wallet, baseMint, NATIVE_MINT, tokenBalance, poolId.toBase58());
        } else {
          throw new Error('No pool ID available');
        }

        if (sellTx) {
          console.log(`Sell transaction created successfully on attempt ${attempts + 1}`);
          break;
        }

        attempts++;
        if (attempts < maxAttempts) {
          await sleep(2000);
        }
      } catch (txError: any) {
        attempts++;
        console.error(`Sell transaction creation attempt ${attempts} failed:`, txError?.message);

        if (attempts < maxAttempts) {
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

      console.log(`✅ Sell successful - User: ${session.userId}, Wallet: ${walletNumber}, Amount: ${tokenAmount} ${session.tokenSymbol}, TX: ${txSig}`);

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
    console.error(`❌ Sell error for user ${session.userId}, wallet ${walletNumber}:`, error);
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
  console.log('Received SIGTERM, shutting down gracefully...');
  saveSessions();
  process.exit(0);
});

process.on('unhandledRejection', (reason, promise) => {
  console.error('Unhandled Rejection at:', promise, 'reason:', reason);
});

process.on('uncaughtException', (error) => {
  console.error('Uncaught Exception:', error);
  saveSessions();
  process.exit(1);
});

if (!BOT_TOKEN) {
  console.log('No Telegram token found. Add TELEGRAM_BOT_TOKEN to .env file.');
  process.exit(1);
} else {
  console.log('Enhanced Volume Bot initialization complete!');
  console.log(`Using ${SWAP_ROUTING ? 'Jupiter' : 'Raydium'} for swaps`);
  console.log(`Distribution: ${DISTRIBUTE_WALLET_NUM} wallets, ${DISTRIBUTION_AMOUNT} SOL each`);
  console.log(`Buy range: ${BUY_LOWER_AMOUNT} - ${BUY_UPPER_AMOUNT} SOL`);
  console.log(`Interval: ${BUY_INTERVAL_MIN} - ${BUY_INTERVAL_MAX} ms`);
}