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
  try {
    const lamports = Math.floor(amount * 1e9)
    const quoteUrl = `https://lite-api.jup.ag/swap/v1/quote?inputMint=So11111111111111111111111111111111111111112&outputMint=${baseMint.toBase58()}&amount=${lamports}&slippageBps=100`

    const quoteResponse = await fetch(quoteUrl, { headers: { accept: 'application/json', origin: 'https://jup.ag' } })
      .then(res => res.json()) as JupiterQuoteResponse
    if (quoteResponse.error || !quoteResponse.outAmount) {
      console.log('Quote failed:', JSON.stringify(quoteResponse))
      return null
    }

    const swapResponse = await fetch('https://lite-api.jup.ag/swap/v1/swap', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', accept: 'application/json', origin: 'https://jup.ag' },
      body: JSON.stringify({ userPublicKey: wallet.publicKey.toString(), wrapAndUnwrapSol: true, quoteResponse, dynamicComputeUnitLimit: true, prioritizationFeeLamports: 100000 })
    }).then(res => res.json()) as JupiterSwapResponse
    if (!swapResponse.swapTransaction) {
      console.log('No swap transaction returned')
      return null
    }

    const swapTransactionBuf = Buffer.from(swapResponse.swapTransaction, 'base64')
    const transaction = VersionedTransaction.deserialize(new Uint8Array(swapTransactionBuf))
    transaction.sign([wallet])
    return transaction
  } catch (error) {
    console.log("Failed to get buy transaction:", error)
    return null
  }
}

export const getSellTxWithJupiter = async (wallet: Keypair, baseMint: PublicKey, amount: string) => {
  try {
    const quoteUrl = `https://lite-api.jup.ag/swap/v1/quote?inputMint=${baseMint.toBase58()}&outputMint=So11111111111111111111111111111111111111112&amount=${amount}&slippageBps=100`

    const quoteResponse = await fetch(quoteUrl, { headers: { accept: 'application/json', origin: 'https://jup.ag' } })
      .then(res => res.json()) as JupiterQuoteResponse
    if (quoteResponse.error || !quoteResponse.outAmount) {
      console.log('Sell quote failed:', JSON.stringify(quoteResponse))
      return null
    }

    const swapResponse = await fetch('https://lite-api.jup.ag/swap/v1/swap', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', accept: 'application/json', origin: 'https://jup.ag' },
      body: JSON.stringify({ userPublicKey: wallet.publicKey.toString(), wrapAndUnwrapSol: true, useSharedAccounts: false, quoteResponse, dynamicComputeUnitLimit: true, prioritizationFeeLamports: 100000 })
    }).then(res => res.json()) as JupiterSwapResponse
    if (!swapResponse.swapTransaction) {
      console.log('No sell swap transaction returned')
      return null
    }

    const swapTransactionBuf = Buffer.from(swapResponse.swapTransaction, 'base64')
    const transaction = VersionedTransaction.deserialize(new Uint8Array(swapTransactionBuf))
    transaction.sign([wallet])
    return transaction
  } catch (error) {
    console.log("Failed to get sell transaction:", error)
    return null
  }
}
