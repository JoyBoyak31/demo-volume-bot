import assert from 'assert';
import {
  jsonInfo2PoolKeys,
  Liquidity,
  LiquidityPoolKeys,
  Percent,
  Token,
  TokenAmount,
  ApiPoolInfoV4,
  LIQUIDITY_STATE_LAYOUT_V4,
  MARKET_STATE_LAYOUT_V3,
  Market,
  SPL_MINT_LAYOUT,
  SPL_ACCOUNT_LAYOUT,
  TokenAccount,
  TxVersion,
  buildSimpleTransaction,
  LOOKUP_TABLE_CACHE,
} from '@raydium-io/raydium-sdk';

import {
  PublicKey,
  Keypair,
  Connection,
  VersionedTransaction
} from '@solana/web3.js';

import { jupiterQueue } from './requestQueue';
import { quoteCache } from './quoteCache';

import { TOKEN_PROGRAM_ID, getAssociatedTokenAddress, getMint } from '@solana/spl-token';
import { logger } from '.';
import { TOKEN_MINT, TX_FEE } from '../constants';
import base58 from 'bs58';
import { BN } from 'bn.js';

// -------------------- CACHES --------------------
const walletTokenAccountsCache = new Map<string, TokenAccount[]>()
const poolInfoCache = new Map<string, ApiPoolInfoV4>()

type WalletTokenAccounts = Awaited<ReturnType<typeof getWalletTokenAccount>>
type TestTxInputInfo = {
  outputToken: Token
  targetPool: string
  inputTokenAmount: TokenAmount
  slippage: Percent
  walletTokenAccounts: WalletTokenAccounts
  wallet: Keypair
}

// -------------------- WALLET ACCOUNT --------------------
async function getWalletTokenAccount(connection: Connection, wallet: PublicKey): Promise<TokenAccount[]> {
  const walletTokenAccount = await connection.getTokenAccountsByOwner(wallet, {
    programId: TOKEN_PROGRAM_ID,
  });
  return walletTokenAccount.value.map((i) => ({
    pubkey: i.pubkey,
    programId: i.account.owner,
    accountInfo: SPL_ACCOUNT_LAYOUT.decode(i.account.data),
  }));
}

async function getCachedWalletTokenAccounts(connection: Connection, wallet: PublicKey) {
  const key = wallet.toBase58()
  if (walletTokenAccountsCache.has(key)) return walletTokenAccountsCache.get(key)!
  const accounts = await getWalletTokenAccount(connection, wallet)
  walletTokenAccountsCache.set(key, accounts)
  return accounts
}

// -------------------- POOL INFO --------------------
async function getCachedPoolInfo(connection: Connection, poolId: string) {
  if (poolInfoCache.has(poolId)) return poolInfoCache.get(poolId)!
  const info = await formatAmmKeysById(connection, poolId)
  poolInfoCache.set(poolId, info)
  return info
}

// -------------------- SWAP INSTRUCTIONS --------------------
async function swapOnlyAmm(connection: Connection, input: TestTxInputInfo) {
  // Get pool info from cache
  const targetPoolInfo = await getCachedPoolInfo(connection, input.targetPool)
  assert(targetPoolInfo, 'cannot find the target pool')
  const poolKeys = jsonInfo2PoolKeys(targetPoolInfo) as LiquidityPoolKeys

  // Compute amount out
  const { amountOut, minAmountOut } = Liquidity.computeAmountOut({
    poolKeys: poolKeys,
    poolInfo: await Liquidity.fetchInfo({ connection, poolKeys }),
    amountIn: input.inputTokenAmount,
    currencyOut: input.outputToken,
    slippage: input.slippage,
  })

  // Get wallet token accounts from cache
  const walletTokenAccounts = await getCachedWalletTokenAccounts(connection, input.wallet.publicKey)

  // Build swap instructions
  const { innerTransactions } = await Liquidity.makeSwapInstructionSimple({
    connection,
    poolKeys,
    userKeys: {
      tokenAccounts: walletTokenAccounts,
      owner: input.wallet.publicKey,
    },
    amountIn: input.inputTokenAmount,
    amountOut: minAmountOut,
    fixedSide: 'in',
    makeTxVersion: TxVersion.V0,
    computeBudgetConfig: {
      microLamports: 12_000 * TX_FEE,
      units: 100_000
    }
  })
  return innerTransactions
}

// -------------------- POOL INFO FORMAT --------------------
export async function formatAmmKeysById(connection: Connection, id: string): Promise<ApiPoolInfoV4> {
  const account = await connection.getAccountInfo(new PublicKey(id))
  if (!account) throw Error(' get id info error ')
  const info = LIQUIDITY_STATE_LAYOUT_V4.decode(account.data)

  const marketId = info.marketId
  const marketAccount = await connection.getAccountInfo(marketId)
  if (!marketAccount) throw Error(' get market info error')
  const marketInfo = MARKET_STATE_LAYOUT_V3.decode(marketAccount.data)

  const lpMint = info.lpMint
  const lpMintAccount = await connection.getAccountInfo(lpMint)
  if (!lpMintAccount) throw Error(' get lp mint info error')
  const lpMintInfo = SPL_MINT_LAYOUT.decode(lpMintAccount.data)

  return {
    id,
    baseMint: info.baseMint.toString(),
    quoteMint: info.quoteMint.toString(),
    lpMint: info.lpMint.toString(),
    baseDecimals: info.baseDecimal.toNumber(),
    quoteDecimals: info.quoteDecimal.toNumber(),
    lpDecimals: lpMintInfo.decimals,
    version: 4,
    programId: account.owner.toString(),
    authority: Liquidity.getAssociatedAuthority({ programId: account.owner }).publicKey.toString(),
    openOrders: info.openOrders.toString(),
    targetOrders: info.targetOrders.toString(),
    baseVault: info.baseVault.toString(),
    quoteVault: info.quoteVault.toString(),
    withdrawQueue: info.withdrawQueue.toString(),
    lpVault: info.lpVault.toString(),
    marketVersion: 3,
    marketProgramId: info.marketProgramId.toString(),
    marketId: info.marketId.toString(),
    marketAuthority: Market.getAssociatedAuthority({ programId: info.marketProgramId, marketId: info.marketId }).publicKey.toString(),
    marketBaseVault: marketInfo.baseVault.toString(),
    marketQuoteVault: marketInfo.quoteVault.toString(),
    marketBids: marketInfo.bids.toString(),
    marketAsks: marketInfo.asks.toString(),
    marketEventQueue: marketInfo.eventQueue.toString(),
    lookupTableAccount: PublicKey.default.toString()
  }
}

// -------------------- BUY / SELL TX --------------------
export async function getBuyTx(
  solanaConnection: Connection,
  wallet: Keypair,
  baseMint: PublicKey,
  quoteMint: PublicKey,
  amount: number,
  targetPool: string
) {
  const baseInfo = await getMint(solanaConnection, baseMint)
  if (!baseInfo) return null

  const baseToken = new Token(TOKEN_PROGRAM_ID, baseMint, baseInfo.decimals)
  const quoteToken = new Token(TOKEN_PROGRAM_ID, quoteMint, 9)
  const quoteTokenAmount = new TokenAmount(quoteToken, Math.floor(amount * 10 ** 9))
  const slippage = new Percent(100, 100)

  const instructions = await swapOnlyAmm(solanaConnection, {
    outputToken: baseToken,
    targetPool,
    inputTokenAmount: quoteTokenAmount,
    slippage,
    walletTokenAccounts: await getCachedWalletTokenAccounts(solanaConnection, wallet.publicKey),
    wallet
  })

  const willSendTx = (await buildSimpleTransaction({
    connection: solanaConnection,
    makeTxVersion: TxVersion.V0,
    payer: wallet.publicKey,
    innerTransactions: instructions,
    addLookupTableInfo: LOOKUP_TABLE_CACHE
  }))[0]

  if (willSendTx instanceof VersionedTransaction) {
    willSendTx.sign([wallet])
    return willSendTx
  }
  return null
}

export async function getSellTx(
  solanaConnection: Connection,
  wallet: Keypair,
  baseMint: PublicKey,
  quoteMint: PublicKey,
  amount: string,
  targetPool: string
) {
  try {
    const tokenAta = await getAssociatedTokenAddress(baseMint, wallet.publicKey)
    const tokenBal = await solanaConnection.getTokenAccountBalance(tokenAta)
    if (!tokenBal || tokenBal.value.uiAmount == 0) return null

    const baseToken = new Token(TOKEN_PROGRAM_ID, baseMint, tokenBal.value.decimals)
    const quoteToken = new Token(TOKEN_PROGRAM_ID, quoteMint, 9)
    const baseTokenAmount = new TokenAmount(baseToken, amount)
    const slippage = new Percent(99, 100)

    const instructions = await swapOnlyAmm(solanaConnection, {
      outputToken: quoteToken,
      targetPool,
      inputTokenAmount: baseTokenAmount,
      slippage,
      walletTokenAccounts: await getCachedWalletTokenAccounts(solanaConnection, wallet.publicKey),
      wallet
    })

    const willSendTx = (await buildSimpleTransaction({
      connection: solanaConnection,
      makeTxVersion: TxVersion.V0,
      payer: wallet.publicKey,
      innerTransactions: instructions,
      addLookupTableInfo: LOOKUP_TABLE_CACHE
    }))[0]

    if (willSendTx instanceof VersionedTransaction) {
      willSendTx.sign([wallet])
      return willSendTx
    }
    return null
  } catch (error) {
    console.log("Error in selling token")
    return null
  }
}

// -------------------- JUPITER ULTRA API --------------------
const PRIORITY_FEE = 500000; // 0.0005 SOL for faster confirmation
const SLIPPAGE_BPS = 100; // 1%

interface UltraOrderResponse {
  error?: string;
  outAmount?: string;
  inAmount?: string;
  priceImpactPct?: string;
  priceImpact?: number;
  transaction?: string;
  prioritizationFeeLamports?: number;
  [key: string]: any;
}

// -------------------- BUY WITH ULTRA API --------------------
export const getBuyTxWithJupiter = async (wallet: Keypair, baseMint: PublicKey, amount: number) => {
  return jupiterQueue.add(async () => {
    try {
      const lamports = Math.floor(amount * 1e9);
      const inputMint = 'So11111111111111111111111111111111111111112'; // SOL
      const outputMint = baseMint.toBase58();

      // Check cache first
      let orderData = quoteCache.get(inputMint, outputMint, lamports);

      if (!orderData) {
        // Fetch from Ultra API
        const orderUrl = `https://ultra-api.jup.ag/order?inputMint=${inputMint}&outputMint=${outputMint}&amount=${lamports}&slippageBps=${SLIPPAGE_BPS}&taker=${wallet.publicKey.toString()}&swapMode=ExactIn&prioritizationFeeLamports=${PRIORITY_FEE}`;

        console.log(`[Ultra] Fetching buy order for ${amount.toFixed(6)} SOL (CACHE MISS)`);

        const orderResponse = await fetch(orderUrl, { 
          headers: { 
            'accept': 'application/json',
          } 
        });

        if (!orderResponse.ok) {
          const errorText = await orderResponse.text();
          throw new Error(`Ultra API order failed: ${orderResponse.status} - ${errorText}`);
        }

        orderData = await orderResponse.json() as UltraOrderResponse;
        
        if (orderData.error || !orderData.outAmount) {
          throw new Error(orderData.error || 'No order response');
        }

        // Cache the order
        quoteCache.set(inputMint, outputMint, lamports, orderData);
        
        console.log(`[Ultra] Buy order received - Expected: ${(Number(orderData.outAmount) / 1e9).toFixed(2)} tokens, Price Impact: ${orderData.priceImpactPct || orderData.priceImpact}%`);
      } else {
        console.log(`[Ultra] Using cached buy order for ${amount.toFixed(6)} SOL`);
      }

      // Ultra API returns the transaction directly in the response
      if (!orderData.transaction) {
        throw new Error('No transaction in Ultra API response');
      }

      // Deserialize and sign the transaction
      const txBuf = Buffer.from(orderData.transaction, 'base64');
      const transaction = VersionedTransaction.deserialize(new Uint8Array(txBuf));
      transaction.sign([wallet]);
      
      console.log(`[Ultra] Buy transaction created and signed successfully`);
      return transaction;

    } catch (error: any) {
      console.error(`[Ultra] Buy transaction error:`, error?.message || error);
      throw error;
    }
  }, 'normal');
};

// -------------------- SELL WITH ULTRA API --------------------
export const getSellTxWithJupiter = async (wallet: Keypair, baseMint: PublicKey, amount: string, isHighPriority: boolean = false) => {
  return jupiterQueue.add(async () => {
    try {
      const inputMint = baseMint.toBase58();
      const outputMint = 'So11111111111111111111111111111111111111112'; // SOL
      const tokenAmount = parseInt(amount);

      // Check cache first
      let orderData = quoteCache.get(inputMint, outputMint, tokenAmount);

      if (!orderData) {
        // Fetch from Ultra API
        const orderUrl = `https://ultra-api.jup.ag/order?inputMint=${inputMint}&outputMint=${outputMint}&amount=${amount}&slippageBps=${SLIPPAGE_BPS}&taker=${wallet.publicKey.toString()}&swapMode=ExactIn&prioritizationFeeLamports=${PRIORITY_FEE}`;

        console.log(`[Ultra] Fetching sell order for ${amount} tokens (CACHE MISS)`);

        const orderResponse = await fetch(orderUrl, { 
          headers: { 
            'accept': 'application/json',
          } 
        });

        if (!orderResponse.ok) {
          const errorText = await orderResponse.text();
          throw new Error(`Ultra API sell order failed: ${orderResponse.status} - ${errorText}`);
        }

        orderData = await orderResponse.json() as UltraOrderResponse;
        
        if (orderData.error || !orderData.outAmount) {
          throw new Error(orderData.error || 'No sell order response');
        }

        // Cache the order
        quoteCache.set(inputMint, outputMint, tokenAmount, orderData);
        
        console.log(`[Ultra] Sell order received - Expected: ${(Number(orderData.outAmount) / 1e9).toFixed(6)} SOL, Price Impact: ${orderData.priceImpactPct || orderData.priceImpact}%`);
      } else {
        console.log(`[Ultra] Using cached sell order for ${amount} tokens`);
      }

      // Ultra API returns the transaction directly in the response
      if (!orderData.transaction) {
        throw new Error('No transaction in Ultra API sell response');
      }

      // Deserialize and sign the transaction
      const txBuf = Buffer.from(orderData.transaction, 'base64');
      const transaction = VersionedTransaction.deserialize(new Uint8Array(txBuf));
      transaction.sign([wallet]);
      
      console.log(`[Ultra] Sell transaction created and signed successfully`);
      return transaction;

    } catch (error: any) {
      console.error(`[Ultra] Sell transaction error:`, error?.message || error);
      throw error;
    }
  }, isHighPriority ? 'high' : 'normal');
};