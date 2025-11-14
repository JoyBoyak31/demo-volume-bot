import fetch from "node-fetch";

const TOKEN_IN = "So11111111111111111111111111111111111111112"; // SOL
const TOKEN_OUT = "Es9vMFrzaCERzJuVD7iSkwaq5vqRv4r4qtq5R9Xh8C2"; // USDT (any token you want)
const AMOUNT = 0.01 * 1e9; // 0.01 SOL

async function testEndpoint(name, url) {
    console.log(`\n==============================`);
    console.log(`🔍 Testing: ${name}`);
    console.log(`==============================`);

    const params = new URLSearchParams({
        inputMint: TOKEN_IN,
        outputMint: TOKEN_OUT,
        amount: AMOUNT.toString(),
    });

    const fullUrl = `${url}?${params.toString()}`;
    const start = Date.now();

    try {
        const response = await fetch(fullUrl);

        const elapsed = Date.now() - start;

        if (response.status === 429) {
            console.log(`❌ 429 RATE LIMIT — ${name} (${elapsed}ms)`);
            const txt = await response.text();
            console.log("Response:", txt);
            return;
        }

        if (!response.ok) {
            console.log(`⚠️ ERROR ${response.status} — ${name}`);
            console.log(await response.text());
            return;
        }

        const data = await response.json();
        console.log(`✅ OK — ${name} (${elapsed}ms)`);
        console.log("Best Route:", data?.routePlan?.length ? "Found" : "No Route");

    } catch (err) {
        console.log(`❌ ERROR — ${name}`);
        console.log(err.message);
    }
}

async function runTests() {
    console.log("\n🚀 Starting Route / RPC Inspection...\n");

    await testEndpoint(
        "Jupiter Ultra (api.jup.ag/ultra/quote)",
        "https://api.jup.ag/ultra/quote"
    );

    await testEndpoint(
        "Jupiter Lite (lite-api.jup.ag)",
        "https://lite-api.jup.ag/v1/quote"
    );

    console.log("\n🏁 Inspection Completed.\n");
}

runTests();
