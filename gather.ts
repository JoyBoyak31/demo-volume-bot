import {
  Keypair,
  Connection,
  Transaction,
  SystemProgram,
  sendAndConfirmTransaction,
  ComputeBudgetProgram,
  LAMPORTS_PER_SOL
} from '@solana/web3.js';
import {
  PRIVATE_KEY,
  RPC_ENDPOINT,
  RPC_WEBSOCKET_ENDPOINT,
} from './constants';
import { Data, readJson } from './utils';
import base58 from 'bs58';

export const solanaConnection = new Connection(RPC_ENDPOINT, {
  wsEndpoint: RPC_WEBSOCKET_ENDPOINT,
});

const mainKp = Keypair.fromSecretKey(base58.decode(PRIVATE_KEY));
const TX_FEE_BUFFER = 0.013 * LAMPORTS_PER_SOL; // ~0.013 SOL buffer

const gather = async () => {
  const data: Data[] = readJson();
  if (data.length === 0) {
    console.log("No wallets to gather");
    return;
  }

  let totalGathered = 0;

  for (const walletData of data) {
    try {
      const wallet = Keypair.fromSecretKey(base58.decode(walletData.privateKey));
      const balance = await solanaConnection.getBalance(wallet.publicKey);
      if (balance <= 0) {
        console.log(`Wallet ${wallet.publicKey.toBase58()} has 0 SOL, skipping`);
        continue;
      }

      const rent = await solanaConnection.getMinimumBalanceForRentExemption(32);
      const lamportsToSend = balance - rent - TX_FEE_BUFFER;
      if (lamportsToSend <= 0) {
        console.log(`Wallet ${wallet.publicKey.toBase58()} has insufficient balance after rent & fee`);
        continue;
      }

      const transaction = new Transaction().add(
        ComputeBudgetProgram.setComputeUnitPrice({ microLamports: 600_000 }),
        ComputeBudgetProgram.setComputeUnitLimit({ units: 20_000 }),
        SystemProgram.transfer({
          fromPubkey: wallet.publicKey,
          toPubkey: mainKp.publicKey,
          lamports: lamportsToSend,
        })
      );

      transaction.recentBlockhash = (await solanaConnection.getLatestBlockhash()).blockhash;
      transaction.feePayer = wallet.publicKey;

      console.log(`Simulating transaction from ${wallet.publicKey.toBase58()}...`);
      console.log(await solanaConnection.simulateTransaction(transaction));

      const sig = await sendAndConfirmTransaction(solanaConnection, transaction, [wallet], { skipPreflight: true });
      console.log(`Collected ${lamportsToSend / LAMPORTS_PER_SOL} SOL from ${wallet.publicKey.toBase58()} => tx: ${sig}`);
      totalGathered += lamportsToSend;

    } catch (err) {
      console.log(`Failed to gather SOL from wallet ${walletData.pubkey}:`, err);
    }
  }

  console.log(`✅ Total SOL gathered: ${totalGathered / LAMPORTS_PER_SOL} SOL`);
};

gather();
