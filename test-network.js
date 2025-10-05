// test-network.js - Run this to test your network connectivity
const fetch = require('cross-fetch');

async function testConnectivity() {
  console.log('🧪 Testing network connectivity...\n');
  
  const tests = [
    {
      name: 'Google DNS',
      url: 'https://dns.google/resolve?name=quote-api.jup.ag&type=A',
      critical: false
    },
    {
      name: 'Jupiter API',
      url: 'https://quote-api.jup.ag/v6/quote?inputMint=So11111111111111111111111111111111111111112&outputMint=EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v&amount=1000000',
      critical: false
    },
    {
      name: 'DexScreener API',
      url: 'https://api.dexscreener.com/latest/dex/tokens/So11111111111111111111111111111111111111112',
      critical: false
    },
    {
      name: 'Solana RPC',
      url: 'https://api.mainnet-beta.solana.com',
      method: 'POST',
      body: JSON.stringify({
        jsonrpc: '2.0',
        id: 1,
        method: 'getVersion'
      }),
      headers: {
        'Content-Type': 'application/json'
      },
      critical: true
    },
    {
      name: 'Alternative RPC (Ankr)',
      url: 'https://rpc.ankr.com/solana',
      method: 'POST',
      body: JSON.stringify({
        jsonrpc: '2.0',
        id: 1,
        method: 'getVersion'
      }),
      headers: {
        'Content-Type': 'application/json'
      },
      critical: true
    }
  ];
  
  const results = {};
  
  for (const test of tests) {
    try {
      console.log(`Testing ${test.name}...`);
      
      const options = {
        method: test.method || 'GET',
        headers: test.headers || {
          'Accept': 'application/json',
          'User-Agent': 'NetworkTest/1.0'
        },
        timeout: 10000
      };
      
      if (test.body) {
        options.body = test.body;
      }
      
      const response = await fetch(test.url, options);
      
      if (response.ok) {
        console.log(`✅ ${test.name}: SUCCESS (${response.status})`);
        results[test.name] = { success: true, status: response.status };
        
        // Show some response data for RPC tests
        if (test.name.includes('RPC')) {
          const data = await response.json();
          if (data.result && data.result['solana-core']) {
            console.log(`   Solana version: ${data.result['solana-core']}`);
          }
        }
      } else {
        console.log(`❌ ${test.name}: FAILED (${response.status} - ${response.statusText})`);
        results[test.name] = { success: false, status: response.status, error: response.statusText };
      }
    } catch (error) {
      console.log(`❌ ${test.name}: ERROR - ${error.message}`);
      results[test.name] = { success: false, error: error.message };
      
      if (test.critical) {
        console.log(`⚠️  Critical service ${test.name} is down!`);
      }
    }
    
    console.log(''); // Empty line for readability
  }
  
  // Summary
  console.log('📊 SUMMARY:');
  console.log('='.repeat(50));
  
  const successCount = Object.values(results).filter(r => r.success).length;
  const totalCount = Object.keys(results).length;
  
  console.log(`Overall: ${successCount}/${totalCount} services accessible`);
  
  // Recommendations
  console.log('\n💡 RECOMMENDATIONS:');
  console.log('='.repeat(50));
  
  if (!results['Jupiter API']?.success) {
    console.log('❌ Jupiter API inaccessible - Set JUPITER_DISABLED=true in .env');
  }
  
  if (!results['DexScreener API']?.success) {
    console.log('❌ DexScreener API inaccessible - Token info fetching will fail');
  }
  
  if (results['Solana RPC']?.success) {
    console.log('✅ Main Solana RPC working - Bot can function');
  } else if (results['Alternative RPC (Ankr)']?.success) {
    console.log('✅ Alternative RPC working - Update RPC_ENDPOINT in .env to https://rpc.ankr.com/solana');
  } else {
    console.log('❌ All RPC endpoints failed - Check your internet connection');
  }
  
  // Configuration suggestions
  console.log('\n🔧 SUGGESTED .env CONFIGURATION:');
  console.log('='.repeat(50));
  
  if (!results['Jupiter API']?.success) {
    console.log('JUPITER_DISABLED=true');
    console.log('FORCE_RAYDIUM_ONLY=true');
    console.log('SWAP_ROUTING=false');
  }
  
  if (results['Alternative RPC (Ankr)']?.success && !results['Solana RPC']?.success) {
    console.log('RPC_ENDPOINT=https://rpc.ankr.com/solana');
  }
}

// Run the test
testConnectivity().catch(console.error);