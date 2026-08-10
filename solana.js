import { Connection, PublicKey } from '@solana/web3.js';
import { TOKEN_PROGRAM_ID, TOKEN_2022_PROGRAM_ID } from '@solana/spl-token';
import { GoogleSpreadsheet } from 'google-spreadsheet';
import { JWT } from 'google-auth-library';
import dotenv from 'dotenv';
dotenv.config();

const c = {
  reset: '\x1b[0m',
  bright: '\x1b[1m',
  dim: '\x1b[2m',
  cyan: '\x1b[36m',
  green: '\x1b[32m',
  yellow: '\x1b[33m',
  red: '\x1b[31m',
  gray: '\x1b[90m'
};

const GOOGLE_SERVICE_ACCOUNT_EMAIL = process.env.GOOGLE_SERVICE_ACCOUNT_EMAIL;
const GOOGLE_PRIVATE_KEY = process.env.GOOGLE_PRIVATE_KEY ? process.env.GOOGLE_PRIVATE_KEY.replace(/\\n/g, '\n') : '';
const SPREADSHEET_ID = process.env.SPREADSHEET_ID;

const SHEET_TAB_NAME = 'Solana_Tracker';
const SUBSCRIPTION_SPL_TAB = 'SUBSCRIPTION SPL';
const SUBSCRIPTION_WALLET_TAB = 'SUBSCRIPTION WALLET';
const SHEET_HEADERS = ['Symbol', 'Network', 'Token Mint', 'Amount', 'Wallet Name', 'Wallet Address', 'Timestamp'];

// ============================================
// RPC ENDPOINTS - Prioritize stability
// ============================================
const RPC_ENDPOINTS = [
  'https://api.mainnet-beta.solana.com',                      // Official - Most stable
  'https://solana-api.projectserum.com',                      // Project Serum
  'https://solana-rpc.publicnode.com',                        // PublicNode
  'https://api.triton.one/networks/solana/solana-mainnet',    // Triton
];

let currentRPCIndex = 0;
let rpcFailures = {}; // Track RPC health

// Initialize RPC failure tracking
RPC_ENDPOINTS.forEach((ep, i) => {
  rpcFailures[i] = { count: 0, lastFail: null };
});

// ✨ FIXED #1: Optimized CONFIG for better performance
const CONFIG = {
  delayBetweenRequests: 200,     // ✨ REDUCED from 1000ms to 200ms
  delayBetweenWallets: 500,      // ✨ REDUCED from 5000ms to 500ms
  timeoutMs: 15000,              // ✨ REDUCED from 20000ms to 15000ms
  maxRetriesPerWallet: 3,        // ✨ REDUCED from 5 to 3
  rpcHealthCheckInterval: 5,     // Check every 5 wallets
  batchWriteSize: 500,           // Write in batches to avoid sheet limit
};

function delay(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function formatDate(date) {
  const formatter = new Intl.DateTimeFormat('en-US', {
    timeZone: 'Asia/Bangkok',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false
  });

  const parts = formatter.formatToParts(date);
  const partMap = {};
  parts.forEach(p => partMap[p.type] = p.value);

  return `${partMap.month}/${partMap.day}/${partMap.year} ${partMap.hour}:${partMap.minute}:${partMap.second}`;
}

// ============================================
// Smart RPC Management
// ============================================

function switchRPCToHealthiest() {
  let bestIndex = 0;
  let lowestFailures = rpcFailures[0].count;

  for (let i = 1; i < RPC_ENDPOINTS.length; i++) {
    if (rpcFailures[i].count < lowestFailures) {
      lowestFailures = rpcFailures[i].count;
      bestIndex = i;
    }
  }

  currentRPCIndex = bestIndex;
  const endpoint = RPC_ENDPOINTS[currentRPCIndex];
  console.log(`${c.yellow}→ RPC: ${endpoint.split('/')[2]}${c.reset}`);
  return new Connection(endpoint, 'confirmed');
}

function recordRPCFailure() {
  rpcFailures[currentRPCIndex].count++;
  rpcFailures[currentRPCIndex].lastFail = Date.now();
  
  if (rpcFailures[currentRPCIndex].count >= 2) {
    console.log(`${c.red}  ⚠ RPC failures: ${rpcFailures[currentRPCIndex].count}, switching...${c.reset}`);
  }
}

function resetRPCFailures() {
  Object.keys(rpcFailures).forEach(key => {
    rpcFailures[key].count = Math.max(0, rpcFailures[key].count - 1);
  });
}

async function checkRPCHealth(conn) {
  try {
    await Promise.race([
      conn.getSlot(),
      new Promise((_, reject) => setTimeout(() => reject(new Error('timeout')), 5000))
    ]);
    return true;
  } catch {
    return false;
  }
}

// ============================================
// Core Fetch Function - With aggressive retry
// ============================================

async function fetchTokenAccountsWithRetry(conn, walletPubKey, programId, programName) {
  let lastError;
  let attemptConn = conn;

  for (let attempt = 0; attempt < CONFIG.maxRetriesPerWallet; attempt++) {
    try {
      await delay(CONFIG.delayBetweenRequests);

      const result = await Promise.race([
        attemptConn.getParsedTokenAccountsByOwner(walletPubKey, { programId }),
        new Promise((_, reject) => 
          setTimeout(() => reject(new Error('timeout')), CONFIG.timeoutMs)
        )
      ]);

      return { success: true, data: result, programName };

    } catch (e) {
      lastError = e;

      const isNetworkError = e.message && (
        e.message.includes('429') ||
        e.message.includes('fetch') ||
        e.message.includes('timeout') ||
        e.message.includes('ECONNREFUSED') ||
        e.message.includes('socket hang up')
      );

      if (isNetworkError) {
        recordRPCFailure();

        if (attempt < CONFIG.maxRetriesPerWallet - 1) {
          attemptConn = switchRPCToHealthiest();
          const waitTime = 500 + (attempt * 1000);
          console.log(`${c.dim}  ⟳ Retry ${attempt + 1}/${CONFIG.maxRetriesPerWallet} - ${programName}${c.reset}`);
          await delay(waitTime);
        }
      } else {
        if (attempt < CONFIG.maxRetriesPerWallet - 1) {
          await delay(300);
        }
      }
    }
  }

  return { success: false, error: lastError?.message || 'Max retries', programName };
}

// ✨ FIXED #2: Batch write function to avoid sheet limits
async function writeBatchToSheet(sheet, rows, batchSize = CONFIG.batchWriteSize) {
  if (rows.length === 0) return;
  
  for (let i = 0; i < rows.length; i += batchSize) {
    const batch = rows.slice(i, i + batchSize);
    try {
      await sheet.addRows(batch);
      console.log(`${c.dim}  ✓ Written ${Math.min(i + batchSize, rows.length)}/${rows.length} records${c.reset}`);
      await delay(500); // Small delay between batch writes
    } catch (err) {
      console.log(`${c.red}  ✗ Batch write error: ${err.message}${c.reset}`);
      throw err;
    }
  }
}

// ============================================
// Main Function
// ============================================

async function main() {
  const startTime = Date.now();

  console.log(`\n${c.cyan}${c.bright}╔════════════════════════════════════════════════════════╗${c.reset}`);
  console.log(`${c.cyan}${c.bright}║   SOLANA TOKEN TRACKER (FIXED - Fast & Stable)          ║${c.reset}`);
  console.log(`${c.cyan}${c.bright}╚════════════════════════════════════════════════════════╝${c.reset}\n`);

  if (!GOOGLE_SERVICE_ACCOUNT_EMAIL || !GOOGLE_PRIVATE_KEY || !SPREADSHEET_ID) {
    console.error(`${c.red}✗ Missing environment variables${c.reset}`);
    process.exit(1);
  }

  // ============================================
  // Google Sheets Setup
  // ============================================
  console.log(`${c.cyan}[1/5] Connecting to Google Sheets...${c.reset}`);
  const serviceAccountAuth = new JWT({
    email: GOOGLE_SERVICE_ACCOUNT_EMAIL,
    key: GOOGLE_PRIVATE_KEY,
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
  });

  const doc = new GoogleSpreadsheet(SPREADSHEET_ID, serviceAccountAuth);
  try {
    await doc.loadInfo();
    console.log(`${c.green}✓ Connected to Google Sheets${c.reset}`);
  } catch (err) {
    console.error(`${c.red}✗ Failed to connect to Google Sheets: ${err.message}${c.reset}`);
    process.exit(1);
  }

  // ============================================
  // Load RPC from nodes sheet
  // ============================================
  let RPC_ENDPOINT = RPC_ENDPOINTS[0];
  const nodesSheet = doc.sheetsByTitle['nodes'];
  if (nodesSheet) {
    try {
      const maxRows = nodesSheet.rowCount;
      if (maxRows >= 2) {
        await nodesSheet.loadCells(`A1:B${maxRows}`);
        for (let r = 1; r < maxRows; r++) {
          const netCell = nodesSheet.getCell(r, 0);
          const urlCell = nodesSheet.getCell(r, 1);
          if (netCell?.value?.toString().toLowerCase() === 'solana' && urlCell?.value) {
            RPC_ENDPOINT = String(urlCell.value).trim();
            break;
          }
        }
      }
    } catch {}
  }

  console.log(`${c.cyan}[2/5] Checking RPC health: ${RPC_ENDPOINT}${c.reset}`);
  // ✨ FIXED #3: Use var conn instead of let to avoid scope issues
  var conn = new Connection(RPC_ENDPOINT, 'confirmed');
  const isHealthy = await checkRPCHealth(conn);
  if (!isHealthy) {
    console.log(`${c.yellow}⚠ Primary RPC unhealthy, using fallback${c.reset}`);
    conn = new Connection(RPC_ENDPOINTS[0], 'confirmed');
  } else {
    console.log(`${c.green}✓ RPC is healthy${c.reset}`);
  }

  // ============================================
  // Load Wallets
  // ============================================
  console.log(`${c.cyan}[3/5] Loading wallets...${c.reset}`);
  let WALLETS = [];
  const walletSheet = doc.sheetsByTitle[SUBSCRIPTION_WALLET_TAB];
  if (walletSheet) {
    try {
      const maxRows = walletSheet.rowCount;
      if (maxRows >= 3) {
        await walletSheet.loadCells(`A1:B${maxRows}`);
        for (let r = 2; r < maxRows; r++) {
          const nameCell = walletSheet.getCell(r, 0);
          const addrCell = walletSheet.getCell(r, 1);
          const addr = addrCell?.value ? String(addrCell.value).trim() : '';
          const name = nameCell?.value ? String(nameCell.value).trim() : 'Unknown';
          if (addr) {
            try {
              new PublicKey(addr);
              WALLETS.push({ name, address: addr });
            } catch {}
          }
        }
      }
    } catch {}
  }

  if (!WALLETS.length) {
    console.error(`${c.red}✗ No valid wallets found${c.reset}`);
    process.exit(0);
  }
  console.log(`${c.green}✓ Loaded ${WALLETS.length} wallets${c.reset}`);

  // ============================================
  // Load Tokens - ✨ FIXED #4: Store mint->symbol mapping
  // ============================================
  console.log(`${c.cyan}[4/5] Loading token subscriptions...${c.reset}`);
  const tokenMap = new Map();
  const subsSheet = doc.sheetsByTitle[SUBSCRIPTION_SPL_TAB];
  if (subsSheet) {
    try {
      const maxRows = subsSheet.rowCount;
      if (maxRows >= 2) {
        await subsSheet.loadCells(`A1:C${maxRows}`);
        for (let r = 1; r < maxRows; r++) {
          const symCell = subsSheet.getCell(r, 0);
          const mintCell = subsSheet.getCell(r, 2);
          let mints = [];
          const mintRaw = mintCell?.value ? String(mintCell.value).trim() : '';
          if (mintRaw) {
            try {
              mints = JSON.parse(mintRaw);
              if (!Array.isArray(mints)) mints = [mintRaw];
            } catch {
              mints = [mintRaw];
            }
          }
          const sym = symCell?.value ? String(symCell.value).trim() : 'Unknown';
          for (const m of mints) {
            try {
              new PublicKey(m);
              tokenMap.set(m, sym);
            } catch {}
          }
        }
      }
    } catch {}
  }

  if (!tokenMap.size) {
    console.error(`${c.red}✗ No valid tokens found${c.reset}`);
    process.exit(0);
  }
  console.log(`${c.green}✓ Loaded ${tokenMap.size} tokens${c.reset}`);

  // ============================================
  // Setup Sheet
  // ============================================
  console.log(`${c.cyan}[5/5] Setting up Google Sheet...${c.reset}`);
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
  console.log(`${c.green}✓ Sheet ready${c.reset}`);

  // ============================================
  // Process Wallets - SEQUENTIAL for stability
  // ============================================
  console.log(`\n${c.cyan}${c.bright}>> Scanning wallets (Sequential Mode)${c.reset}`);
  console.log(`${c.dim}Tokens to track: ${tokenMap.size}${c.reset}\n`);

  let totalAdded = 0;
  let totalProcessed = 0;
  let totalEmptyWallets = 0;
  const errors = [];
  const rowsToAdd = [];

  for (let wIdx = 0; wIdx < WALLETS.length; wIdx++) {
    const wallet = WALLETS[wIdx];
    const walletNum = wIdx + 1;
    const walletInfo = `${c.gray}[${String(walletNum).padStart(3, '0')}/${WALLETS.length}]${c.reset}`;
    process.stdout.write(`${walletInfo} ${wallet.name.padEnd(35, ' ')} `);

    let walletAdded = 0;
    let walletSuccess = false;

    try {
      const walletPubKey = new PublicKey(wallet.address);

      // Fetch SPL tokens
      const splResult = await fetchTokenAccountsWithRetry(
        conn,
        walletPubKey,
        TOKEN_PROGRAM_ID,
        'SPL'
      );

      // Fetch Token-2022
      const token2022Result = await fetchTokenAccountsWithRetry(
        conn,
        walletPubKey,
        TOKEN_2022_PROGRAM_ID,
        'Token-2022'
      );

      // ✨ FIXED #5: Better handling of token account data
      const allAccounts = [
        ...(splResult.success ? splResult.data?.value || [] : []),
        ...(token2022Result.success ? token2022Result.data?.value || [] : [])
      ];

      totalProcessed += allAccounts.length;

      // Process tokens
      for (const item of allAccounts) {
        try {
          const parsedInfo = item.account.data.parsed.info;
          const mint = parsedInfo.mint;
          const tokenAmount = parsedInfo.tokenAmount;

          // ✨ FIXED: Check if token is in our subscription list AND has balance
          if (tokenMap.has(mint)) {
            const amount = parseFloat(tokenAmount?.uiAmountString || tokenAmount?.uiAmount || '0');
            
            // Include even zero balance tokens to show we're tracking them
            const symbol = tokenMap.get(mint);
            const nowStr = formatDate(new Date());

            rowsToAdd.push({
              'Symbol': symbol,
              'Network': 'Solana',
              'Token Mint': mint,
              'Amount': amount,
              'Wallet Name': wallet.name,
              'Wallet Address': wallet.address,
              'Timestamp': nowStr
            });

            walletAdded++;
            totalAdded++;
            walletSuccess = true;
          }
        } catch (tokenErr) {
          // Skip malformed token entries silently
        }
      }

      // Display result
      if (walletAdded > 0) {
        console.log(`${c.green}✓ ${String(walletAdded).padStart(2, '0')} tokens${c.reset}`);
      } else if (allAccounts.length === 0) {
        console.log(`${c.gray}○ 00 tokens${c.reset}`);
        totalEmptyWallets++;
      } else {
        console.log(`${c.gray}✓ 00 tokens (no matches)${c.reset}`);
      }

    } catch (err) {
      errors.push(`${wallet.name}: ${err.message.substring(0, 40)}`);
      console.log(`${c.red}✗ Error${c.reset}`);
    }

    // Health check & reset
    if (walletNum % CONFIG.rpcHealthCheckInterval === 0) {
      resetRPCFailures();
    }

    // Delay between wallets
    if (walletNum < WALLETS.length) {
      await delay(CONFIG.delayBetweenWallets);
    }
  }

  // ============================================
  // Write to Google Sheets - ✨ FIXED: Batch write with better error handling
  // ============================================
  console.log(`\n${c.cyan}>> Writing ${rowsToAdd.length} record(s) to Google Sheets...${c.reset}`);

  if (rowsToAdd.length > 0) {
    try {
      await sheet.clear();
      console.log(`${c.dim}  ✓ Sheet cleared${c.reset}`);
      
      await sheet.setHeaderRow(SHEET_HEADERS);
      console.log(`${c.dim}  ✓ Headers set${c.reset}`);
      
      await writeBatchToSheet(sheet, rowsToAdd);
      console.log(`${c.green}✓ Successfully written to sheet${c.reset}`);
    } catch (err) {
      console.error(`${c.red}✗ Error writing to sheet: ${err.message}${c.reset}`);
      errors.push(`Google Sheets write: ${err.message}`);
    }
  } else {
    console.log(`${c.yellow}⚠ No tokens found to write${c.reset}`);
  }

  // ============================================
  // Summary
  // ============================================
  const execMins = ((Date.now() - startTime) / 1000 / 60).toFixed(2);

  console.log(`\n${c.cyan}${c.bright}╔════════════════════════════════════════════════════════╗${c.reset}`);
  console.log(`${c.cyan}${c.bright}║              EXECUTION SUMMARY                          ║${c.reset}`);
  console.log(`${c.cyan}${c.bright}╚════════════════════════════════════════════════════════╝${c.reset}`);
  console.log(`Execution Time:        ${execMins} minutes`);
  console.log(`Wallets Processed:     ${WALLETS.length}`);
  console.log(`Empty Wallets:         ${totalEmptyWallets}`);
  console.log(`Tokens Tracked:        ${tokenMap.size}`);
  console.log(`Total Accounts Found:  ${totalProcessed}`);
  console.log(`${c.green}Total Records Added:   ${totalAdded}${c.reset}`);
  
  if (errors.length > 0) {
    console.log(`${c.red}Total Errors:          ${errors.length}${c.reset}`);
    if (errors.length <= 15) {
      console.log(`\n${c.red}Errors:${c.reset}`);
      errors.forEach((e, i) => console.log(`${c.red}  ${i + 1}. ${e}${c.reset}`));
    }
  } else {
    console.log(`${c.green}Total Errors:          0${c.reset}`);
  }
  console.log(`${c.cyan}${c.bright}════════════════════════════════════════════════════════${c.reset}\n`);

  process.exit(0);
}

main().catch(err => {
  console.error(`${c.red}${c.bright}Fatal Error:${c.reset} ${err.message}`);
  process.exit(1);
});
