import { Connection, PublicKey } from '@solana/web3.js';
import { TOKEN_PROGRAM_ID } from '@solana/spl-token';
import { GoogleSpreadsheet } from 'google-spreadsheet';
import { JWT } from 'google-auth-library';
import dotenv from 'dotenv';
dotenv.config();

// Configuration
const RPC_ENDPOINT = process.env.RPC_ENDPOINT || 'https://solana-rpc.publicnode.com';
const INTERVAL_MS = parseInt(process.env.INTERVAL_MS, 10) || 300000; // Default: 5 minutes
const DELAY_BETWEEN_WALLETS_MS = parseInt(process.env.DELAY_BETWEEN_WALLETS_MS, 10) || 1000;
const SPREADSHEET_ID = process.env.SPREADSHEET_ID;

const connection = new Connection(RPC_ENDPOINT, 'confirmed');

let wallets = [];
try {
  let rawVal = process.env.WALLETS_RAW || process.env.WALLETS_JSON || '[]';
  if (!rawVal.trim().startsWith('[')) {
    rawVal = '[' + rawVal + ']';
  }
  wallets = new Function('return ' + rawVal)();
  if (!Array.isArray(wallets) || wallets.length === 0) {
    throw new Error('Wallets array is empty.');
  }
} catch (err) {
  console.error('[FATAL] Failed to parse WALLETS_RAW from .env:', err.message);
  process.exit(1);
}

// To hold Jupiter Token List for fast symbol lookup
let tokenMap = new Map();

async function fetchTokenList() {
  try {
    console.log('Fetching Jupiter token list for symbols...');
    const response = await fetch('https://tokens.jup.ag/tokens?tags=verified');
    if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
    const data = await response.json();
    data.forEach(token => {
      tokenMap.set(token.address, token.symbol);
    });
    console.log(`Loaded ${tokenMap.size} verified tokens from Jupiter.`);
  } catch (error) {
    console.error('Failed to fetch Jupiter token list (Symbols might be missing):', error.message);
  }
}

function delay(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function getGoogleSheet() {
  const serviceAccountAuth = new JWT({
    email: process.env.GOOGLE_SERVICE_ACCOUNT_EMAIL,
    key: process.env.GOOGLE_PRIVATE_KEY ? process.env.GOOGLE_PRIVATE_KEY.replace(/\\n/g, '\n') : '',
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
  });

  const doc = new GoogleSpreadsheet(SPREADSHEET_ID, serviceAccountAuth);
  await doc.loadInfo(); // loads document properties and worksheets
  return doc.sheetsByIndex[0]; // assuming we use the first sheet
}

async function ensureHeaders(sheet) {
  try {
    await sheet.loadHeaderRow();
  } catch (e) {
    await sheet.setHeaderRow(['Timestamp', 'Wallet Name', 'Wallet Address', 'Token Mint', 'Symbol', 'Amount']);
  }
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
    await ensureHeaders(sheet);
  } catch (err) {
    console.error('[FATAL] Failed to connect to Google Sheets:', err.message);
    return; 
  }

  const allCollectedRows = [];
  let successCount = 0;
  const failList = [];
  const totalCount = wallets.length;

  for (let i = 0; i < wallets.length; i++) {
    const wallet = wallets[i];
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
    if (i < wallets.length - 1) {
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
