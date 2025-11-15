const fetch = require('node-fetch');

// Configuration
const TEST_TOKEN = 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v'; // USDC
const SOL_MINT = 'So11111111111111111111111111111111111111112';
const AMOUNT = '1000000'; // 0.001 SOL
let DELAY_BETWEEN_REQUESTS = 200; // Start safer (5 req/sec)

// Stats tracking
let totalRequests = 0;
let successfulRequests = 0;
let failedRequests = 0;
let rateLimitHits = 0;

let isRateLimited = false;
let firstRateLimitTime = null;
let cooldownMs = 0; // cooldown timer

async function testJupiterRateLimit() {
  console.log('🧪 Jupiter Rate Limit Test Starting...\n');
  console.log(`Testing endpoint: https://lite-api.jup.ag/swap/v1/quote`);
  console.log(`Initial delay: ${DELAY_BETWEEN_REQUESTS}ms (≈ ${(
    1000 / DELAY_BETWEEN_REQUESTS
  ).toFixed(1)} req/sec)`);
  console.log(`Cooldown will be applied when rate limited.\n`);
  console.log('Press Ctrl+C to stop\n');
  console.log('═'.repeat(80));

  while (true) {
    const startTime = Date.now();
    const quoteUrl = `https://lite-api.jup.ag/swap/v1/quote?inputMint=${SOL_MINT}&outputMint=${TEST_TOKEN}&amount=${AMOUNT}&slippageBps=50`;

    try {
      totalRequests++;

      const response = await fetch(quoteUrl, {
        headers: {
          accept: 'application/json',
          origin: 'https://jup.ag',
        },
      });

      const responseTime = Date.now() - startTime;

      // -----------------------------------------
      // SUCCESS CASE
      // -----------------------------------------
      if (response.ok) {
        successfulRequests++;

        // If previously rate limited → we recovered
        if (isRateLimited) {
          const resetTime = ((Date.now() - firstRateLimitTime) / 1000).toFixed(2);
          console.log(`\n\n✅ RATE LIMIT RECOVERED after ${resetTime}s!`);
          console.log('Requests resuming...\n');
          isRateLimited = false;
          cooldownMs = 0;
          DELAY_BETWEEN_REQUESTS = 200; // reset back to safe 5 req/sec
        }

        process.stdout.write(
          `\r✅ Request #${totalRequests} SUCCESS (${responseTime}ms) | Success: ${successfulRequests} | Failed: ${failedRequests} | RL Hits: ${rateLimitHits}`
        );
      }

      // -----------------------------------------
      // RATE LIMITED
      // -----------------------------------------
      else if (response.status === 429) {
        rateLimitHits++;
        failedRequests++;

        // First time hitting RL
        if (!isRateLimited) {
          isRateLimited = true;
          firstRateLimitTime = Date.now();
          cooldownMs = 60000; // 1 minute first cooldown

          console.log('\n\n' + '═'.repeat(80));
          console.log(`🚨 RATE LIMIT HIT! After ${successfulRequests} successful requests`);
          console.log(`Cooldown: ${cooldownMs / 1000}s`);
          console.log('═'.repeat(80));
        } else {
          // Increase cooldown each time
          cooldownMs = Math.min(cooldownMs * 2, 10 * 60 * 1000); // max 10 mins
          console.log(`\n⏳ Still rate limited. Increasing cooldown to ${cooldownMs / 1000}s`);
        }

        process.stdout.write(
          `\r⏳ Cooling down for ${cooldownMs / 1000}s (RL Hits: ${rateLimitHits})`
        );

        await sleep(cooldownMs);
        continue;
      }

      // -----------------------------------------
      // NON-429 ERROR
      // -----------------------------------------
      else {
        failedRequests++;
        console.log(`\n⚠️ Unexpected response ${response.status}`);
      }
    } catch (error) {
      failedRequests++;
      console.log(`\n❌ Error: ${error.message}`);
    }

    await sleep(DELAY_BETWEEN_REQUESTS);
  }
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// Graceful shutdown
process.on('SIGINT', () => {
  console.log('\n\n🛑 Test stopped manually\n');
  console.log('📊 FINAL SUMMARY');
  console.log('─'.repeat(80));
  console.log(`Total Requests:      ${totalRequests}`);
  console.log(`Successful Requests: ${successfulRequests}`);
  console.log(`Failed Requests:     ${failedRequests}`);
  console.log(`Rate Limit Hits:     ${rateLimitHits}`);
  console.log('─'.repeat(80));
  process.exit(0);
});

// Run
testJupiterRateLimit();
