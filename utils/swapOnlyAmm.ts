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

// -------------------- JUPITER TX --------------------
interface JupiterQuoteResponse {
  error?: string;
  outAmount?: string;
  [key: string]: any;
}

interface JupiterSwapResponse {
  swapTransaction?: string;
  error?: string;
  [key: string]: any;
}

export const getBuyTxWithJupiter = async (wallet: Keypair, baseMint: PublicKey, amount: number) => {
  return jupiterQueue.add(async () => {
    try {
      const lamports = Math.floor(amount * 1e9);
      const inputMint = 'So11111111111111111111111111111111111111112'; // SOL
      const outputMint = baseMint.toBase58();

      // Try to get cached quote first
      let quoteData = quoteCache.get(inputMint, outputMint, lamports);

      if (!quoteData) {
        // Cache miss - fetch new quote
        const quoteUrl = `https://lite-api.jup.ag/swap/v1/quote?inputMint=${inputMint}&outputMint=${outputMint}&amount=${lamports}&slippageBps=100`;

        console.log(`[Queue] Fetching buy quote for ${amount.toFixed(6)} SOL (CACHE MISS)`);

        const quoteResponse = await fetch(quoteUrl, { 
          headers: { 
            'accept': 'application/json',
            'origin': 'https://jup.ag'
          } 
        });

        if (!quoteResponse.ok) {
          const errorText = await quoteResponse.text();
          throw new Error(`Quote failed: ${quoteResponse.status} - ${errorText}`);
        }

        quoteData = await quoteResponse.json() as JupiterQuoteResponse;
        
        if (quoteData.error || !quoteData.outAmount) {
          throw new Error(quoteData.error || 'No quote received');
        }

        // Cache the quote for reuse
        quoteCache.set(inputMint, outputMint, lamports, quoteData);
      } else {
        console.log(`[Queue] Using cached buy quote for ${amount.toFixed(6)} SOL`);
      }

      console.log(`[Queue] Fetching buy swap transaction`);

      const swapResponse = await fetch('https://lite-api.jup.ag/swap/v1/swap', {
        method: 'POST',
        headers: { 
          'Content-Type': 'application/json',
          'accept': 'application/json',
          'origin': 'https://jup.ag'
        },
        body: JSON.stringify({ 
          userPublicKey: wallet.publicKey.toString(), 
          wrapAndUnwrapSol: true, 
          quoteResponse: quoteData,
          dynamicComputeUnitLimit: true, 
          prioritizationFeeLamports: 100000
        })
      });

      if (!swapResponse.ok) {
        const errorText = await swapResponse.text();
        throw new Error(`Swap failed: ${swapResponse.status} - ${errorText}`);
      }

      const swapData = await swapResponse.json() as JupiterSwapResponse;
      
      if (!swapData.swapTransaction) {
        throw new Error('No swap transaction returned');
      }

      const swapTransactionBuf = Buffer.from(swapData.swapTransaction, 'base64');
      const transaction = VersionedTransaction.deserialize(new Uint8Array(swapTransactionBuf));
      transaction.sign([wallet]);
      
      console.log(`[Queue] Buy transaction created successfully`);
      return transaction;

    } catch (error: any) {
      console.error(`[Queue] Buy transaction error:`, error?.message || error);
      throw error;
    }
  }, 'normal');
};

export const getSellTxWithJupiter = async (wallet: Keypair, baseMint: PublicKey, amount: string, isHighPriority: boolean = false) => {
  return jupiterQueue.add(async () => {
    try {
      const inputMint = baseMint.toBase58();
      const outputMint = 'So11111111111111111111111111111111111111112'; // SOL
      const tokenAmount = parseInt(amount);

      // Try to get cached quote first
      let quoteData = quoteCache.get(inputMint, outputMint, tokenAmount);

      if (!quoteData) {
        // Cache miss - fetch new quote
        const quoteUrl = `https://lite-api.jup.ag/swap/v1/quote?inputMint=${inputMint}&outputMint=${outputMint}&amount=${amount}&slippageBps=100`;

        console.log(`[Queue] Fetching sell quote for ${amount} tokens (CACHE MISS)`);

        const quoteResponse = await fetch(quoteUrl, { 
          headers: { 
            'accept': 'application/json',
            'origin': 'https://jup.ag'
          } 
        });

        if (!quoteResponse.ok) {
          const errorText = await quoteResponse.text();
          throw new Error(`Sell quote failed: ${quoteResponse.status} - ${errorText}`);
        }

        quoteData = await quoteResponse.json() as JupiterQuoteResponse;
        
        if (quoteData.error || !quoteData.outAmount) {
          throw new Error(quoteData.error || 'No sell quote');
        }

        // Cache the quote for reuse
        quoteCache.set(inputMint, outputMint, tokenAmount, quoteData);
      } else {
        console.log(`[Queue] Using cached sell quote for ${amount} tokens`);
      }

      console.log(`[Queue] Fetching sell swap transaction`);

      const swapResponse = await fetch('https://lite-api.jup.ag/swap/v1/swap', {
        method: 'POST',
        headers: { 
          'Content-Type': 'application/json',
          'accept': 'application/json',
          'origin': 'https://jup.ag'
        },
        body: JSON.stringify({ 
          userPublicKey: wallet.publicKey.toString(), 
          wrapAndUnwrapSol: true, 
          useSharedAccounts: false,
          quoteResponse: quoteData,
          dynamicComputeUnitLimit: true, 
          prioritizationFeeLamports: 100000
        })
      });

      if (!swapResponse.ok) {
        const errorText = await swapResponse.text();
        throw new Error(`Sell swap failed: ${swapResponse.status} - ${errorText}`);
      }

      const swapData = await swapResponse.json() as JupiterSwapResponse;
      
      if (!swapData.swapTransaction) {
        throw new Error('No sell swap transaction');
      }

      const swapTransactionBuf = Buffer.from(swapData.swapTransaction, 'base64');
      const transaction = VersionedTransaction.deserialize(new Uint8Array(swapTransactionBuf));
      transaction.sign([wallet]);
      
      console.log(`[Queue] Sell transaction created successfully`);
      return transaction;

    } catch (error: any) {
      console.error(`[Queue] Sell transaction error:`, error?.message || error);
      throw error;
    }
  }, isHighPriority ? 'high' : 'normal');
};