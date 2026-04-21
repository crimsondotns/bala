import { JsonRpcProvider, Contract, formatUnits } from 'ethers';
import { GoogleSpreadsheet } from 'google-spreadsheet';
import { JWT } from 'google-auth-library';
import dotenv from 'dotenv';
dotenv.config();

// ============================================================================
// 1. SETTINGS & APP CONFIGURATION
// ============================================================================
const DELAY_BETWEEN_RPC_MS = parseInt(process.env.EVM_DELAY_RPC_MS, 10) || 2000;
const DELAY_BETWEEN_WALLET_MS = parseInt(process.env.EVM_DELAY_WALLET_MS, 10) || 500;
const INTERVAL_MS = parseInt(process.env.EVM_INTERVAL_MS, 10) || 300000;

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
} catch (err) {
  console.error('[FATAL] Failed to parse ALL_ERC20_TOKENS from .env:', err.message);
  process.exit(1);
}

const ERC20_ABI = [
  "function symbol() view returns (string)",
  "function decimals() view returns (uint8)",
  "function balanceOf(address owner) view returns (uint256)"
];

// ============================================================================
// 4. CORE SYSTEM CLASSES & INTERNAL CACHE
// ============================================================================
const tokenCache = new Map();

function delay(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
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

async function getTokenMetadata(contractAddress, provider, network) {
  // Check Cache
  const key = `token_meta:${network}:${contractAddress}`;
  if (tokenCache.has(key)) {
    return tokenCache.get(key);
  }

  // Fetch directly from Smart Contract
  const contract = new Contract(contractAddress, ERC20_ABI, provider);
  const symbol = await contract.symbol();
  const decimals = await contract.decimals();

  const meta = { symbol, decimals: Number(decimals) };

  // Cache in memory
  tokenCache.set(key, meta);

  return meta;
}

let roundCounter = 0;

function generateSummary({ round, execTime, nextSyncStr, total, success, fails }) {
  const border = '--------------------------------------------------';
  const now = new Date().toISOString().replace('T', ' ').substring(0, 19);
  
  let out = `\n${border}\n[${now}] PROCESS SUMMARY: EVM_TRACKER (ROUND #${round})\n${border}\n`;
  out += `Status: Completed\nTotal Wallets: ${total}\nSuccess: ${success}\nFailed: ${fails.length}\n`;

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
  const timestamp = new Date().toISOString();
  const networks = Object.keys(RPC_URLS);

  let successCount = 0;
  const failList = [];

  for (let i = 0; i < networks.length; i++) {
    const network = networks[i];
    const rpcUrl = RPC_URLS[network];
    
    // Set staticNetwork: true to gracefully handle unreachable networks
    const provider = new JsonRpcProvider(rpcUrl, undefined, { staticNetwork: true });

    for (let t = 0; t < ALL_ERC20_TOKENS.length; t++) {
      const tokenAddress = ALL_ERC20_TOKENS[t];

      // 1. Cross-chain Code Check
      let code;
      try {
        code = await provider.getCode(tokenAddress);
      } catch (err) {
        // network likely down or timed out. Skip silently to save time.
        continue; 
      }

      if (code === '0x') {
        // Smart contract does not exist on this chain
        continue;
      }

      // 2. Fetch Metadata
      let meta;
      try {
        meta = await getTokenMetadata(tokenAddress, provider, network);
      } catch (err) {
        // Cant fetch metadata, push error and continue
        failList.push({
          network,
          walletName: 'System (Metadata Fetch)',
          error: `Failed metadata for ${tokenAddress}: ${err.message}`
        });
        continue;
      }

      const contract = new Contract(tokenAddress, ERC20_ABI, provider);

      // 3. Process Wallets
      for (let j = 0; j < WALLETS.length; j++) {
        const wallet = WALLETS[j];
        try {
          const tokenBalanceWei = await contract.balanceOf(wallet.address);
          const tokenBalance = formatUnits(tokenBalanceWei, meta.decimals);
          
          allRows.push({
            'Tokens Name': meta.symbol,
            Network: network,
            'Tokens Address': tokenAddress,
            Amount: tokenBalance,
            'Wallet Name': wallet.name,
            'Wallet Address': wallet.address,
            Timestamp: timestamp
          });
          
          successCount++;
        } catch (err) {
          const errorMsg = `[ERROR] Failed to fetch data: ${err.message}`;
          
          failList.push({
            network,
            walletName: wallet.name,
            error: err.message
          });

          allRows.push({
            'Tokens Name': meta.symbol,
            Network: network,
            'Tokens Address': tokenAddress,
            Amount: errorMsg,
            'Wallet Name': wallet.name,
            'Wallet Address': wallet.address,
            Timestamp: timestamp
          });
        }
        
        if (j < WALLETS.length - 1) {
          await delay(DELAY_BETWEEN_WALLET_MS);
        }
      }
    }
    
    if (i < networks.length - 1) {
      await delay(DELAY_BETWEEN_RPC_MS);
    }
  }

  // Batch insert
  if (allRows.length > 0) {
    try {
      await sheet.addRows(allRows);
    } catch (err) {
      failList.push({
        network: 'SYSTEM_ERROR',
        walletName: 'Google Sheets Insert',
        error: err.message
      });
    }
  }

  const endTime = Date.now();
  const execTimeSeconds = ((endTime - startTime) / 1000).toFixed(2) + ' seconds';
  
  const nextSyncDate = new Date(endTime + INTERVAL_MS);
  const nextSyncStr = nextSyncDate.toISOString().replace('T', ' ').substring(0, 19);

  // Reflect exactly what we attempted based on contract existence
  const totalAttempts = successCount + failList.filter(f => f.network !== 'SYSTEM_ERROR' && f.walletName !== 'System (Metadata Fetch)').length;

  const summary = generateSummary({
    round: roundCounter,
    execTime: execTimeSeconds,
    nextSyncStr,
    total: totalAttempts, 
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
