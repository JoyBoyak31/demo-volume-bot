import {
  Keypair,
  Connection,
  Transaction,
  SystemProgram,
  sendAndConfirmTransaction,
  ComputeBudgetProgram,
  PublicKey,
  LAMPORTS_PER_SOL,
} from '@solana/web3.js'
import {
  getAssociatedTokenAddress,
  NATIVE_MINT,
} from '@solana/spl-token'
import {
  PRIVATE_KEY,
  RPC_ENDPOINT,
  RPC_WEBSOCKET_ENDPOINT,
  TOKEN_MINT,
  ADDITIONAL_FEE,
  SWAP_ROUTING,
  POOL_ID,
} from './constants'
import { Data, readJson, sleep } from './utils'
import base58 from 'bs58'
import { getSellTx, getSellTxWithJupiter } from './utils/swapOnlyAmm'
import { execute } from './executor/legacy'
import { getPoolKeys } from './utils/getPoolInfo'

export const solanaConnection = new Connection(RPC_ENDPOINT, {
  wsEndpoint: RPC_WEBSOCKET_ENDPOINT,
})
const mainKp = Keypair.fromSecretKey(base58.decode(PRIVATE_KEY))
const baseMint = new PublicKey(TOKEN_MINT)

interface WalletStatus {
  pubkey: string;
  privateKey: string;
  solBalance: number;
  tokenBalance: number;
  tokenBalanceRaw: string;
  hasTokens: boolean;
}

// Check if wallet has unsold tokens
async function checkWallet(wallet: Keypair, privateKey: string): Promise<WalletStatus> {
  try {
    const solBalance = await solanaConnection.getBalance(wallet.publicKey)
    const tokenAta = await getAssociatedTokenAddress(baseMint, wallet.publicKey)
    
    let tokenBalance = 0
    let tokenBalanceRaw = "0"
    
    try {
      const tokenBalInfo = await solanaConnection.getTokenAccountBalance(tokenAta)
      if (tokenBalInfo && tokenBalInfo.value.uiAmount) {
        tokenBalance = tokenBalInfo.value.uiAmount
        tokenBalanceRaw = tokenBalInfo.value.amount
      }
    } catch (error) {
      // No token account or empty
    }

    return {
      pubkey: wallet.publicKey.toBase58(),
      privateKey,
      solBalance: solBalance / LAMPORTS_PER_SOL,
      tokenBalance,
      tokenBalanceRaw,
      hasTokens: tokenBalance > 0,
    }
  } catch (error) {
    return {
      pubkey: wallet.publicKey.toBase58(),
      privateKey,
      solBalance: 0,
      tokenBalance: 0,
      tokenBalanceRaw: "0",
      hasTokens: false,
    }
  }
}

// Try to sell tokens from a wallet
async function sellTokens(wallet: Keypair, tokenBalanceRaw: string, poolId: string): Promise<boolean> {
  try {
    let sellTx
    if (SWAP_ROUTING) {
      sellTx = await getSellTxWithJupiter(wallet, baseMint, tokenBalanceRaw)
    } else {
      sellTx = await getSellTx(solanaConnection, wallet, baseMint, NATIVE_MINT, tokenBalanceRaw, poolId)
    }

    if (sellTx == null) {
      return false
    }

    const latestBlockhash = await solanaConnection.getLatestBlockhash()
    const txSig = await execute(sellTx, latestBlockhash, false)
    
    if (txSig) {
      console.log(`   ✅ Sold tokens: https://solscan.io/tx/${txSig}`)
      return true
    }
    return false
  } catch (error) {
    return false
  }
}

// Transfer SOL between wallets
async function transferSol(from: Keypair, to: PublicKey, amount: number): Promise<boolean> {
  try {
    const transaction = new Transaction().add(
      ComputeBudgetProgram.setComputeUnitPrice({ microLamports: 600_000 }),
      ComputeBudgetProgram.setComputeUnitLimit({ units: 20_000 }),
      SystemProgram.transfer({
        fromPubkey: from.publicKey,
        toPubkey: to,
        lamports: Math.floor(amount * LAMPORTS_PER_SOL),
      })
    )

    transaction.recentBlockhash = (await solanaConnection.getLatestBlockhash()).blockhash
    transaction.feePayer = from.publicKey

    const sig = await sendAndConfirmTransaction(solanaConnection, transaction, [from], { skipPreflight: true })
    console.log(`   💸 SOL sent: https://solscan.io/tx/${sig}`)
    return true
  } catch (error) {
    return false
  }
}

const gather = async () => {
  console.log("\n" + "=".repeat(80))
  console.log("🚀 STARTING WITHDRAWAL PROCESS")
  console.log("=".repeat(80) + "\n")

  const data: Data[] = readJson()
  
  if (data.length === 0) {
    console.log("❌ No wallets found to gather from\n")
    return
  }

  console.log(`📊 Checking ${data.length} wallets...\n`)

  // Step 1: Check all wallets
  const walletStatuses: WalletStatus[] = []
  let walletsWithTokens: WalletStatus[] = []
  let walletsReady: WalletStatus[] = []

  for (let i = 0; i < data.length; i++) {
    const wallet = Keypair.fromSecretKey(base58.decode(data[i].privateKey))
    const status = await checkWallet(wallet, data[i].privateKey)
    walletStatuses.push(status)

    if (status.hasTokens) {
      walletsWithTokens.push(status)
    } else if (status.solBalance > 0.001) {
      walletsReady.push(status)
    }
  }

  // Step 2: Show status
  console.log("=".repeat(80))
  console.log("📋 STATUS REPORT")
  console.log("=".repeat(80))
  console.log(`Total Wallets: ${data.length}`)
  console.log(`Wallets with Unsold Tokens: ${walletsWithTokens.length}`)
  console.log(`Wallets Ready to Gather: ${walletsReady.length}`)
  console.log(`Empty Wallets: ${data.length - walletsWithTokens.length - walletsReady.length}`)

  if (walletsWithTokens.length > 0) {
    console.log("\n⚠️  UNSOLD TOKENS DETECTED:")
    walletsWithTokens.forEach((w, i) => {
      console.log(`   ${i + 1}. ${w.pubkey.slice(0, 8)}... - ${w.tokenBalance.toFixed(4)} tokens, ${w.solBalance.toFixed(6)} SOL`)
    })
  }
  console.log("=".repeat(80) + "\n")

  // Step 3: Get pool info if needed
  let poolId = POOL_ID
  if (walletsWithTokens.length > 0 && !SWAP_ROUTING) {
    try {
      const poolKeys = await getPoolKeys(solanaConnection, baseMint)
      if (poolKeys) {
        poolId = poolKeys.id
        console.log(`✅ Pool found: ${poolId}\n`)
      }
    } catch (error) {
      console.log("⚠️  Could not fetch pool info\n")
    }
  }

  // Step 4: Gather from wallets without tokens
  console.log("💰 GATHERING SOL FROM CLEAN WALLETS\n")
  let totalGathered = 0
  let successCount = 0

  for (const walletStatus of walletsReady) {
    try {
      const wallet = Keypair.fromSecretKey(base58.decode(walletStatus.privateKey))
      const balance = await solanaConnection.getBalance(wallet.publicKey)
      
      if (balance === 0) continue

      const rent = await solanaConnection.getMinimumBalanceForRentExemption(32)
      const amountToGather = balance - 13_000 - rent

      if (amountToGather <= 0) continue

      const transaction = new Transaction().add(
        ComputeBudgetProgram.setComputeUnitPrice({ microLamports: 600_000 }),
        ComputeBudgetProgram.setComputeUnitLimit({ units: 20_000 }),
        SystemProgram.transfer({
          fromPubkey: wallet.publicKey,
          toPubkey: mainKp.publicKey,
          lamports: amountToGather,
        })
      )

      transaction.recentBlockhash = (await solanaConnection.getLatestBlockhash()).blockhash
      transaction.feePayer = wallet.publicKey

      const sig = await sendAndConfirmTransaction(solanaConnection, transaction, [wallet], { skipPreflight: true })

      const gathered = amountToGather / LAMPORTS_PER_SOL
      totalGathered += gathered
      successCount++

      console.log(`✅ Gathered ${gathered.toFixed(6)} SOL - TX: https://solscan.io/tx/${sig}`)
    } catch (error) {
      console.log(`❌ Failed to gather from ${walletStatus.pubkey.slice(0, 8)}...`)
    }
  }

  console.log(`\n💰 Total Gathered: ${totalGathered.toFixed(6)} SOL from ${successCount} wallets\n`)

  // Step 5: Handle wallets with tokens
  if (walletsWithTokens.length === 0) {
    console.log("✅ ALL DONE! No tokens to recover.\n")
    const finalBalance = (await solanaConnection.getBalance(mainKp.publicKey)) / LAMPORTS_PER_SOL
    console.log(`💼 Main Wallet Balance: ${finalBalance.toFixed(6)} SOL\n`)
    return
  }

  console.log("=".repeat(80))
  console.log("🔧 RECOVERING WALLETS WITH UNSOLD TOKENS")
  console.log("=".repeat(80) + "\n")

  const unrecoverableWallets: WalletStatus[] = []
  let recoveredCount = 0

  for (let i = 0; i < walletsWithTokens.length; i++) {
    const walletStatus = walletsWithTokens[i]
    console.log(`\n[${i + 1}/${walletsWithTokens.length}] Processing ${walletStatus.pubkey.slice(0, 8)}...`)
    console.log(`   Tokens: ${walletStatus.tokenBalance.toFixed(4)}, SOL: ${walletStatus.solBalance.toFixed(6)}`)

    const wallet = Keypair.fromSecretKey(base58.decode(walletStatus.privateKey))

    // Try to sell with existing balance first
    if (walletStatus.solBalance >= 0.01) {
      console.log(`   🔄 Trying to sell with existing balance...`)
      const sold = await sellTokens(wallet, walletStatus.tokenBalanceRaw, poolId)
      
      if (sold) {
        console.log(`   ✅ Success! Tokens sold`)
        recoveredCount++
        
        // Gather SOL back
        await sleep(2000)
        try {
          const balance = await solanaConnection.getBalance(wallet.publicKey)
          const rent = await solanaConnection.getMinimumBalanceForRentExemption(32)
          if (balance > rent + 13_000) {
            await transferSol(wallet, mainKp.publicKey, (balance - rent - 13_000) / LAMPORTS_PER_SOL)
          }
        } catch (error) {
          // Ignore gather errors
        }
        continue
      }
    }

    // Need to send SOL for selling
    const mainBalance = await solanaConnection.getBalance(mainKp.publicKey)
    const solNeeded = ADDITIONAL_FEE

    if (mainBalance < solNeeded * LAMPORTS_PER_SOL) {
      console.log(`   ❌ Insufficient SOL in main wallet for recovery`)
      unrecoverableWallets.push(walletStatus)
      continue
    }

    console.log(`   💸 Sending ${solNeeded} SOL from main wallet...`)
    const sentSol = await transferSol(mainKp, wallet.publicKey, solNeeded)
    
    if (!sentSol) {
      console.log(`   ❌ Failed to send SOL`)
      unrecoverableWallets.push(walletStatus)
      continue
    }

    await sleep(2000)

    // Try to sell again
    console.log(`   🔄 Attempting to sell tokens...`)
    const sold = await sellTokens(wallet, walletStatus.tokenBalanceRaw, poolId)
    
    if (sold) {
      console.log(`   ✅ Recovery successful!`)
      recoveredCount++
      
      // Gather all SOL back
      await sleep(2000)
      try {
        const balance = await solanaConnection.getBalance(wallet.publicKey)
        const rent = await solanaConnection.getMinimumBalanceForRentExemption(32)
        if (balance > rent + 13_000) {
          console.log(`   🔄 Gathering SOL back...`)
          await transferSol(wallet, mainKp.publicKey, (balance - rent - 13_000) / LAMPORTS_PER_SOL)
        }
      } catch (error) {
        // Ignore gather errors
      }
    } else {
      console.log(`   ❌ Recovery failed`)
      unrecoverableWallets.push(walletStatus)
    }
  }

  // Final Report
  console.log("\n" + "=".repeat(80))
  console.log("📊 FINAL REPORT")
  console.log("=".repeat(80))
  console.log(`Total Wallets: ${data.length}`)
  console.log(`SOL Gathered: ${totalGathered.toFixed(6)} SOL`)
  console.log(`Wallets with Tokens: ${walletsWithTokens.length}`)
  console.log(`Recovered: ${recoveredCount}`)
  console.log(`Failed: ${unrecoverableWallets.length}`)
  console.log("=".repeat(80))

  if (unrecoverableWallets.length > 0) {
    console.log("\n⚠️  UNRECOVERABLE WALLETS - MANUAL ACTION REQUIRED\n")
    
    const fs = require('fs')
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-')
    const filename = `unrecoverable-wallets-${timestamp}.json`
    
    const recoveryData = unrecoverableWallets.map(w => ({
      wallet: w.pubkey,
      privateKey: w.privateKey,
      tokenBalance: w.tokenBalance,
      solBalance: w.solBalance
    }))

    fs.writeFileSync(filename, JSON.stringify(recoveryData, null, 2))

    unrecoverableWallets.forEach((w, i) => {
      console.log(`${i + 1}. ${w.pubkey}`)
      console.log(`   Private Key: ${w.privateKey}`)
      console.log(`   Tokens: ${w.tokenBalance.toFixed(4)}`)
      console.log(`   SOL: ${w.solBalance.toFixed(6)}\n`)
    })

    console.log(`💾 Details saved to: ${filename}`)
    console.log("⚠️  Import these private keys to Phantom/Solflare to sell manually\n")
  } else {
    console.log("\n✅ ALL TOKENS RECOVERED SUCCESSFULLY!\n")
  }

  const finalBalance = (await solanaConnection.getBalance(mainKp.publicKey)) / LAMPORTS_PER_SOL
  console.log(`💼 Main Wallet Final Balance: ${finalBalance.toFixed(6)} SOL`)
  console.log(`   Address: ${mainKp.publicKey.toBase58()}\n`)
}

gather()
  .then(() => process.exit(0))
  .catch((error) => {
    console.error("❌ Fatal error:", error)
    process.exit(1)
  })