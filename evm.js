import { JsonRpcProvider, Contract, Interface, formatUnits, isAddress } from 'ethers';
import { GoogleSpreadsheet } from 'google-spreadsheet';
import { JWT } from 'google-auth-library';
import dotenv from 'dotenv';
dotenv.config();

// ============================================================================
// 1. SETTINGS & APP CONFIGURATION
// ============================================================================
const DELAY_BETWEEN_RPC_MS = parseInt(process.env.EVM_DELAY_RPC_MS, 10) || 2000;
const INTERVAL_MS = parseInt(process.env.EVM_INTERVAL_MS, 10) || 300000;
const MULTICALL_BATCH_SIZE = 500; // Limit payload size for public RPCs
const MULTICALL3_ADDRESS = '0xcA11bde05977b3631167028862bE2a173976CA11';

const RPC_URLS = {
  'Ethereum': 'https://ethereum-rpc.publicnode.com',
  'BSC': 'https://bsc-rpc.publicnode.com',
  'Polygon': 'https://polygon-bor-rpc.publicnode.com',
  'Arbitrum': 'https://arbitrum-one-rpc.publicnode.com',
  'Optimism': 'https://optimism-rpc.publicnode.com',
  'Base': 'https://base-rpc.publicnode.com',
  'Avalanche': 'https://api.avax.network/ext/bc/C/rpc',
  'Fantom': 'https://fantom-public.nodies.app',
  'Linea': 'https://linea-rpc.publicnode.com',
  'Blast': 'https://blast-rpc.publicnode.com',
  'Abstract': 'https://api.mainnet.abs.xyz',
  'Ink': 'https://ink.api.pocket.network',
  'Merlin': 'https://rpc.merlinchain.io',
  'Sonic': 'https://sonic-rpc.publicnode.com:443',
  'Sei V2': 'https://sei-evm-rpc.publicnode.com',
  'Degen': 'https://rpc.degen.tips',
  'Dogechain': 'https://rpc.dogechain.dog'
};

// ============================================================================
// 2. OUTPUT CONFIGURATION (Google Sheets)
// ============================================================================
const SPREADSHEET_ID = process.env.SPREADSHEET_ID;
const GOOGLE_SERVICE_ACCOUNT_EMAIL = process.env.GOOGLE_SERVICE_ACCOUNT_EMAIL;
const GOOGLE_PRIVATE_KEY = process.env.GOOGLE_PRIVATE_KEY;
const SHEET_TAB_NAME = 'EVM_Tracker';
const SHEET_HEADERS = ['Tokens Name', 'Network', 'Tokens Address', 'Amount', 'Wallet Name', 'Wallet Address', 'Timestamp'];

// ============================================================================
// 3. TARGET WALLETS & ERC20 CONFIGURATION
// ============================================================================
let WALLETS = [];
try {
  let rawVal = process.env.EVM_WALLETS_RAW || process.env.EVM_WALLETS_JSON || '[]';
  if (!rawVal.trim().startsWith('[')) {
    rawVal = '[' + rawVal + ']';
  }
  WALLETS = new Function('return ' + rawVal)();
  if (!Array.isArray(WALLETS) || WALLETS.length === 0) {
    throw new Error('EVM_WALLETS array is empty.');
  }
} catch (err) {
  console.error('[FATAL] Failed to parse EVM_WALLETS_RAW from .env:', err.message);
  process.exit(1);
}

let ALL_ERC20_TOKENS = [];
try {
  let rawTokens = process.env.ALL_ERC20_TOKENS || '[]';
  if (!rawTokens.trim().startsWith('[')) {
    rawTokens = '[' + rawTokens + ']';
  }
  ALL_ERC20_TOKENS = new Function('return ' + rawTokens)();
  if (!Array.isArray(ALL_ERC20_TOKENS)) {
    throw new Error('ALL_ERC20_TOKENS must be an array');
  }
  
  // Sanitize: Keep only valid 42-char EVM addresses to prevent ENS resolution errors
  const originalLength = ALL_ERC20_TOKENS.length;
  ALL_ERC20_TOKENS = ALL_ERC20_TOKENS.filter(token => token && typeof token === 'string' && isAddress(token));
  if (ALL_ERC20_TOKENS.length < originalLength) {
    console.log(`[INFO] Filtered out ${originalLength - ALL_ERC20_TOKENS.length} invalid EVM addresses (e.g. Aptos/Sui tokens).`);
  }
} catch (err) {
  console.error('[FATAL] Failed to parse ALL_ERC20_TOKENS from .env:', err.message);
  process.exit(1);
}

// ============================================================================
// 4. CORE SYSTEM CLASSES & ABIS
// ============================================================================
const MULTICALL3_ABI = [
  {
    "inputs": [
      {
        "components": [
          { "name": "target", "type": "address" },
          { "name": "allowFailure", "type": "bool" },
          { "name": "callData", "type": "bytes" }
        ],
        "name": "calls",
        "type": "tuple[]"
      }
    ],
    "name": "aggregate3",
    "outputs": [
      {
        "components": [
          { "name": "success", "type": "bool" },
          { "name": "returnData", "type": "bytes" }
        ],
        "name": "returnData",
        "type": "tuple[]"
      }
    ],
    "stateMutability": "payable",
    "type": "function"
  }
];

const ERC20_INTERFACE = new Interface([
  "function symbol() view returns (string)",
  "function decimals() view returns (uint8)",
  "function balanceOf(address owner) view returns (uint256)"
]);

// We use an in-memory Map as requested earlier (or this replaces the Redis requirement conceptually without needing a local DB installation). 
// You can seamlessly plug in a Redis connection here if you use Upstash.
const tokenCache = new Map();

function delay(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function formatTimestamp(date = new Date()) {
  const month = String(date.getMonth() + 1).padStart(2, '0');
  const day = String(date.getDate()).padStart(2, '0');
  const year = date.getFullYear();
  const hours = String(date.getHours()).padStart(2, '0');
  const minutes = String(date.getMinutes()).padStart(2, '0');
  const seconds = String(date.getSeconds()).padStart(2, '0');
  return `${month}/${day}/${year} ${hours}:${minutes}:${seconds}`;
}

function chunkArray(array, size) {
  const chunked = [];
  for (let i = 0; i < array.length; i += size) {
    chunked.push(array.slice(i, i + size));
  }
  return chunked;
}

async function getGoogleSheet() {
  const serviceAccountAuth = new JWT({
    email: GOOGLE_SERVICE_ACCOUNT_EMAIL,
    key: GOOGLE_PRIVATE_KEY ? GOOGLE_PRIVATE_KEY.replace(/\\n/g, '\n') : '',
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
  });

  const doc = new GoogleSpreadsheet(SPREADSHEET_ID, serviceAccountAuth);
  await doc.loadInfo(); 
  
  let sheet = doc.sheetsByTitle[SHEET_TAB_NAME];
  if (!sheet) {
    sheet = await doc.addSheet({ title: SHEET_TAB_NAME, headerValues: SHEET_HEADERS });
  } else {
    try {
      await sheet.loadHeaderRow();
    } catch {
      await sheet.setHeaderRow(SHEET_HEADERS);
    }
  }
  return sheet;
}

let roundCounter = 0;

function generateSummary({ round, execTime, nextSyncStr, total, success, fails }) {
  const border = '--------------------------------------------------';
  const now = new Date().toISOString().replace('T', ' ').substring(0, 19);
  
  let out = `\n${border}\n[${now}] PROCESS SUMMARY: EVM_TRACKER (ROUND #${round})\n${border}\n`;
  out += `Status: Completed\nTotal Wallets Processed: ${total}\nSuccess: ${success}\nFailed: ${fails.length}\n`;

  if (fails.length > 0) {
    out += `\nFailed Items List:\n`;
    for (const f of fails) {
      out += `- [${f.network}] ${f.walletName}: ${f.error}\n`;
    }
  }

  out += `\nStats:\n- Execution Time: ${execTime}\n- Next Sync: ${nextSyncStr}\n${border}\n`;
  return out;
}

async function runTracker() {
  roundCounter++;
  const startTime = Date.now();
  
  let sheet;
  try {
    sheet = await getGoogleSheet();
  } catch (err) {
    console.error('[FATAL] Failed to connect to Google Sheets:', err.message);
    return;
  }

  const allRows = [];
  const timestamp = formatTimestamp();
  const networks = Object.keys(RPC_URLS);

  let successCount = 0;
  const failList = [];
  let totalProcessed = 0;

  for (let i = 0; i < networks.length; i++) {
    const network = networks[i];
    const rpcUrl = RPC_URLS[network];
    const provider = new JsonRpcProvider(rpcUrl, undefined, { staticNetwork: true });
    const multicall = new Contract(MULTICALL3_ADDRESS, MULTICALL3_ABI, provider);

    // -------------------------------------------------------------
    // PHASE 1: Fetch missing Metadata on this network via Multicall3
    // -------------------------------------------------------------
    const missingTokens = [];
    for (const token of ALL_ERC20_TOKENS) {
      if (!tokenCache.has(`meta:${network}:${token}`)) {
        missingTokens.push(token);
      }
    }

    if (missingTokens.length > 0) {
      const metaChunks = chunkArray(missingTokens, MULTICALL_BATCH_SIZE / 2); // 2 calls per token
      for (const chunk of metaChunks) {
        const metaCalls = [];
        for (const token of chunk) {
          metaCalls.push({ target: token, allowFailure: true, callData: ERC20_INTERFACE.encodeFunctionData("symbol") });
          metaCalls.push({ target: token, allowFailure: true, callData: ERC20_INTERFACE.encodeFunctionData("decimals") });
        }

        try {
          const results = await multicall.aggregate3.staticCall(metaCalls);
          for (let c = 0; c < chunk.length; c++) {
            const token = chunk[c];
            const symRes = results[c * 2];
            const decRes = results[c * 2 + 1];

            if (symRes.success && decRes.success) {
              try {
                const symbol = ERC20_INTERFACE.decodeFunctionResult("symbol", symRes.returnData)[0];
                const decimals = ERC20_INTERFACE.decodeFunctionResult("decimals", decRes.returnData)[0];
                tokenCache.set(`meta:${network}:${token}`, { success: true, symbol, decimals: Number(decimals) });
              } catch (e) {
                tokenCache.set(`meta:${network}:${token}`, { success: false }); // Decode fail
              }
            } else {
              tokenCache.set(`meta:${network}:${token}`, { success: false }); // Contract doesn't exist or isn't ERC20
            }
          }
        } catch (err) {
          // Metadata chunk failed (e.g. Network down or RPC reject). Mark unknown to retry later.
          failList.push({ network, walletName: 'System (Metadata)', error: `Multicall Meta chunk failed: ${err.message}` });
        }
      }
    }

    // -------------------------------------------------------------
    // PHASE 2: Build Balance Multicalls for all Wallets X valid Tokens
    // -------------------------------------------------------------
    const balanceCalls = [];
    const callMappings = [];

    for (const token of ALL_ERC20_TOKENS) {
      const meta = tokenCache.get(`meta:${network}:${token}`);
      if (meta && meta.success) {
        // Token exists, prepare checks for all wallets
        for (const wallet of WALLETS) {
          balanceCalls.push({
            target: token,
            allowFailure: true,
            callData: ERC20_INTERFACE.encodeFunctionData("balanceOf", [wallet.address])
          });
          callMappings.push({ token, wallet, meta });
        }
      }
    }

    if (balanceCalls.length > 0) {
      const balChunks = chunkArray(balanceCalls, MULTICALL_BATCH_SIZE);
      const mapChunks = chunkArray(callMappings, MULTICALL_BATCH_SIZE);

      for (let c = 0; c < balChunks.length; c++) {
        const chunk = balChunks[c];
        const mapping = mapChunks[c];

        try {
          const results = await multicall.aggregate3.staticCall(chunk);
          
          for (let k = 0; k < results.length; k++) {
            const res = results[k];
            const m = mapping[k];
            totalProcessed++;

            if (res.success) {
              try {
                const balanceWei = ERC20_INTERFACE.decodeFunctionResult("balanceOf", res.returnData)[0];
                const balanceStr = formatUnits(balanceWei, m.meta.decimals);
                
                // Skip writing 0 balances
                if (parseFloat(balanceStr) > 0) {
                  allRows.push({
                    'Tokens Name': m.meta.symbol,
                    Network: network,
                    'Tokens Address': m.token,
                    Amount: balanceStr,
                    'Wallet Name': m.wallet.name,
                    'Wallet Address': m.wallet.address,
                    Timestamp: timestamp
                  });
                }
                // Still count as process success even if it's 0 (it didn't error)
                successCount++;
              } catch (e) {
                // Decode fail for successful call
                failList.push({ network, walletName: m.wallet.name, error: `Decode failed for ${m.token}` });
                allRows.push({
                  'Tokens Name': m.meta.symbol,
                  Network: network,
                  'Tokens Address': m.token,
                  Amount: `[ERROR] Decode Fail`,
                  'Wallet Name': m.wallet.name,
                  'Wallet Address': m.wallet.address,
                  Timestamp: timestamp
                });
              }
            } else {
              // Call Reverted (allowFailure=true caught this safely)
              const errorMsg = `Reverted on ${m.token}`;
              failList.push({ network, walletName: m.wallet.name, error: errorMsg });
              allRows.push({
                'Tokens Name': m.meta.symbol,
                Network: network,
                'Tokens Address': m.token,
                Amount: `[ERROR] ${errorMsg}`,
                'Wallet Name': m.wallet.name,
                'Wallet Address': m.wallet.address,
                Timestamp: timestamp
              });
            }
          }
        } catch (err) {
          // The entire RPC Multicall request chunk failed (e.g. Too Large or timeout)
          failList.push({ network, walletName: 'Multicall Chunk', error: err.message });
          for (const m of mapping) {
             allRows.push({
                'Tokens Name': m.meta.symbol,
                Network: network,
                'Tokens Address': m.token,
                Amount: `[ERROR] RPC Multicall Timeout/Error`,
                'Wallet Name': m.wallet.name,
                'Wallet Address': m.wallet.address,
                Timestamp: timestamp
             });
          }
        }
      }
    }

    if (i < networks.length - 1) {
      await delay(DELAY_BETWEEN_RPC_MS);
    }
  }

  // -------------------------------------------------------------
  // PHASE 3: Clear and Batch Write to Google Sheets
  // -------------------------------------------------------------
  // Clear existing data rows (except header) before writing new data
  try {
    const rows = await sheet.getRows();
    for (const row of rows) {
      await row.delete();
    }
  } catch (err) {
    failList.push({ network: 'SYSTEM', walletName: 'Google Sheets Clear', error: err.message });
  }

  if (allRows.length > 0) {
    // Write in chunks to Google Sheets if rows > 5000 to prevent Request Payload Too Large
    const rowChunks = chunkArray(allRows, 5000);
    for (const rChunk of rowChunks) {
      try {
        await sheet.addRows(rChunk);
      } catch (err) {
        failList.push({ network: 'SYSTEM', walletName: 'Google Sheets', error: `Insert chunk failed: ${err.message}` });
      }
    }
  }

  const endTime = Date.now();
  const execTimeSeconds = ((endTime - startTime) / 1000).toFixed(2) + ' seconds';
  const nextSyncDate = new Date(endTime + INTERVAL_MS);
  const nextSyncStr = nextSyncDate.toISOString().replace('T', ' ').substring(0, 19);

  const summary = generateSummary({
    round: roundCounter,
    execTime: execTimeSeconds,
    nextSyncStr,
    total: totalProcessed,
    success: successCount,
    fails: failList
  });
  
  console.log(summary);
}

async function main() {
  if (!SPREADSHEET_ID || !GOOGLE_SERVICE_ACCOUNT_EMAIL || !GOOGLE_PRIVATE_KEY) {
    console.error('[FATAL] Missing Google Sheets configuration in .env!');
    process.exit(1);
  }

  await runTracker();

  setInterval(runTracker, INTERVAL_MS);
}

main().catch(err => {
  console.error('[FATAL] Unhandled Exception in EVM Tracker:', err);
});
