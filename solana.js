import { Connection, PublicKey } from '@solana/web3.js';
import { TOKEN_PROGRAM_ID } from '@solana/spl-token';
import { GoogleSpreadsheet } from 'google-spreadsheet';
import { JWT } from 'google-auth-library';
import dotenv from 'dotenv';
dotenv.config();

// ============================================================================
// 1. SETTINGS & APP CONFIGURATION
// ============================================================================
const RPC_ENDPOINT = process.env.RPC_ENDPOINT || 'https://solana-rpc.publicnode.com';
const INTERVAL_MS = parseInt(process.env.INTERVAL_MS, 10) || 300000; 
const DELAY_BETWEEN_WALLETS_MS = parseInt(process.env.DELAY_BETWEEN_WALLETS_MS, 10) || 1000;

// ============================================================================
// 2. OUTPUT CONFIGURATION (Google Sheets)
// ============================================================================
const SPREADSHEET_ID = process.env.SPREADSHEET_ID;
const GOOGLE_SERVICE_ACCOUNT_EMAIL = process.env.GOOGLE_SERVICE_ACCOUNT_EMAIL;
const GOOGLE_PRIVATE_KEY = process.env.GOOGLE_PRIVATE_KEY;
const SHEET_TAB_NAME = 'Solana_Tracker';
const SHEET_HEADERS = ['Timestamp', 'Wallet Name', 'Wallet Address', 'Token Mint', 'Symbol', 'Amount'];

// ============================================================================
// 3. TARGET WALLETS
// ============================================================================
let WALLETS = [];
try {
  let rawVal = process.env.WALLETS_RAW || process.env.WALLETS_JSON || '[]';
  if (!rawVal.trim().startsWith('[')) {
    rawVal = '[' + rawVal + ']';
  }
  WALLETS = new Function('return ' + rawVal)();
  if (!Array.isArray(WALLETS) || WALLETS.length === 0) {
    throw new Error('Wallets array is empty.');
  }
} catch (err) {
  console.error('[FATAL] Failed to parse WALLETS_RAW from .env:', err.message);
  process.exit(1);
}

// ============================================================================
// 4. CORE SYSTEM CLASSES & INTERNAL CACHE
// ============================================================================
const connection = new Connection(RPC_ENDPOINT, 'confirmed');

// To hold Jupiter Token List for fast symbol lookup
let tokenMap = new Map();

async function fetchTokenList() {
  try {
    const response = await fetch('https://tokens.jup.ag/tokens?tags=verified');
    if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
    const data = await response.json();
    data.forEach(token => {
      tokenMap.set(token.address, token.symbol);
    });
  } catch (error) {
    console.error('Failed to fetch Jupiter token list (Symbols might be missing):', error.message);
  }
}

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

async function fetchWalletTokens(wallet) {
  const pubKey = new PublicKey(wallet.address);
  // Using getParsedTokenAccountsByOwner prevents us from needing to fetch Mint decimals individually
  const response = await connection.getParsedTokenAccountsByOwner(pubKey, {
    programId: TOKEN_PROGRAM_ID,
  });

  const rows = [];
  const timestamp = new Date().toISOString();

  for (const accountInfo of response.value) {
    const parsedInfo = accountInfo.account.data.parsed.info;
    const mint = parsedInfo.mint;
    const tokenAmount = parsedInfo.tokenAmount;
    
    // Check if tokenAmount is 0; optionally ignore to save space, but typically 0 balances are fine or can be filtered
    if (tokenAmount.uiAmount === 0) continue; 

    const symbol = tokenMap.get(mint) || 'Unknown';
    
    rows.push({
      Timestamp: timestamp,
      'Wallet Name': wallet.name,
      'Wallet Address': wallet.address,
      'Token Mint': mint,
      Symbol: symbol,
      Amount: tokenAmount.uiAmountString // Use string to prevent floating point issues
    });
  }
  
  return rows;
}

let roundCounter = 0;

function generateSummary({ round, execTime, nextSyncStr, total, success, fails }) {
  const border = '--------------------------------------------------';
  const now = new Date().toISOString().replace('T', ' ').substring(0, 19);
  
  let out = `\n${border}\n[${now}] PROCESS SUMMARY: SOLANA_TRACKER (ROUND #${round})\n${border}\n`;
  out += `Status: Completed\nTotal Wallets: ${total}\nSuccess: ${success}\nFailed: ${fails.length}\n`;

  if (fails.length > 0) {
    out += `\nFailed Items List:\n`;
    for (const f of fails) {
      out += `- [Solana] ${f.walletName}: ${f.error}\n`;
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

  const allCollectedRows = [];
  let successCount = 0;
  const failList = [];
  const totalCount = WALLETS.length;

  for (let i = 0; i < WALLETS.length; i++) {
    const wallet = WALLETS[i];
    try {
      const rows = await fetchWalletTokens(wallet);
      allCollectedRows.push(...rows);
      successCount++;
    } catch (err) {
      failList.push({
        walletName: wallet.name,
        error: err.message
      });
      // Requirements: 'หากเกิด Error... ห้ามใส่ค่าเป็น 0 ให้แสดงข้อความ Error หรือพ่น Error'
      allCollectedRows.push({
        Timestamp: new Date().toISOString(),
        'Wallet Name': wallet.name,
        'Wallet Address': wallet.address,
        'Token Mint': 'Fetch Error',
        Symbol: '-',
        Amount: err.message
      });
    }
    
    // Throttling to prevent Rate Limits, except after the last item
    if (i < WALLETS.length - 1) {
      await delay(DELAY_BETWEEN_WALLETS_MS);
    }
  }

  // Batch insert to Google Sheets
  if (allCollectedRows.length > 0) {
    try {
      await sheet.addRows(allCollectedRows);
    } catch (err) {
      failList.push({
        walletName: 'Google Sheets Insert',
        error: err.message
      });
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
    total: totalCount,
    success: successCount,
    fails: failList
  });
  
  console.log(summary);
}

async function main() {
  if (!SPREADSHEET_ID || !process.env.GOOGLE_SERVICE_ACCOUNT_EMAIL || !process.env.GOOGLE_PRIVATE_KEY) {
    console.error('[FATAL] Missing Google Sheets configuration in .env!');
    process.exit(1);
  }

  // Initial setup: get token list for symbols
  await fetchTokenList();
  
  await runTracker();

  setInterval(runTracker, INTERVAL_MS);
}

main().catch(err => {
  console.error('[FATAL] Unhandled Exception:', err);
});
