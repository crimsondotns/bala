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

const CONFIG = {
  // Stability > Speed
  delayBetweenRequests: 1000,    // 1 second between individual API calls
  delayBetweenWallets: 5000,     // 5 seconds between wallets (large gap)
  timeoutMs: 20000,               // Long timeout to allow RPC to respond
  maxRetriesPerWallet: 5,         // Try hard to get data for each wallet
  rpcHealthCheckInterval: 3,      // Check RPC health every 3 wallets
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
  // Find RPC with least failures
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
  
  if (rpcFailures[currentRPCIndex].count >= 3) {
    console.log(`${c.red}  ⚠ RPC failures: ${rpcFailures[currentRPCIndex].count}, switching...${c.reset}`);
  }
}

function resetRPCFailures() {
  // Every few wallets, reset failure counts to give RPCs a chance
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
      // Wait before request
      await delay(CONFIG.delayBetweenRequests);

      // Make request with timeout
      const result = await Promise.race([
        attemptConn.getParsedTokenAccountsByOwner(walletPubKey, { programId }),
        new Promise((_, reject) => 
          setTimeout(() => reject(new Error('timeout')), CONFIG.timeoutMs)
        )
      ]);

      // Success - return data
      return { success: true, data: result, programName };

    } catch (e) {
      lastError = e;

      // Determine if we should retry with different RPC
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
          // Switch to healthier RPC
          attemptConn = switchRPCToHealthiest();
          // Wait before retry
          const waitTime = 1000 + (attempt * 1500);
          console.log(`${c.dim}  ⟳ Retry ${attempt + 1}/${CONFIG.maxRetriesPerWallet} (wait ${waitTime}ms) - ${programName}${c.reset}`);
          await delay(waitTime);
        }
      } else {
        // Non-network error - brief wait and retry same RPC
        if (attempt < CONFIG.maxRetriesPerWallet - 1) {
          await delay(1000);
        }
      }
    }
  }

  return { success: false, error: lastError?.message || 'Max retries', programName };
}

// ============================================
// Main Function
// ============================================

async function main() {
  const startTime = Date.now();

  console.log(`\n${c.cyan}${c.bright}╔════════════════════════════════════════════════════════╗${c.reset}`);
  console.log(`${c.cyan}${c.bright}║   SOLANA TOKEN TRACKER (Stability-First)                ║${c.reset}`);
  console.log(`${c.cyan}${c.bright}║   Priority: Data Completeness over Speed                ║${c.reset}`);
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
  let conn = new Connection(RPC_ENDPOINT, 'confirmed');
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
  // Load Tokens
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
  console.log(`\n${c.cyan}${c.bright}>> Scanning wallets (Sequential Mode - Data Completeness)${c.reset}`);
  console.log(`${c.cyan}Stability Settings:${c.reset}`);
  console.log(`${c.dim}  • Delay between requests: ${CONFIG.delayBetweenRequests}ms${c.reset}`);
  console.log(`${c.dim}  • Delay between wallets: ${CONFIG.delayBetweenWallets}ms${c.reset}`);
  console.log(`${c.dim}  • Max retries per wallet: ${CONFIG.maxRetriesPerWallet}${c.reset}`);
  console.log(`${c.dim}  • Timeout: ${CONFIG.timeoutMs}ms\n${c.reset}`);

  let totalAdded = 0;
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

      // Fetch SPL tokens (sequential)
      const splResult = await fetchTokenAccountsWithRetry(
        conn,
        walletPubKey,
        TOKEN_PROGRAM_ID,
        'SPL'
      );

      // Fetch Token-2022 (sequential)
      const token2022Result = await fetchTokenAccountsWithRetry(
        conn,
        walletPubKey,
        TOKEN_2022_PROGRAM_ID,
        'Token-2022'
      );

      // Combine results
      const allAccounts = [
        ...(splResult.success ? splResult.data?.value || [] : []),
        ...(token2022Result.success ? token2022Result.data?.value || [] : [])
      ];

      // Process tokens
      for (const item of allAccounts) {
        try {
          const parsedInfo = item.account.data.parsed.info;
          const mint = parsedInfo.mint;
          const tokenAmount = parsedInfo.tokenAmount;

          if (tokenMap.has(mint) && tokenAmount?.uiAmount > 0) {
            const balanceFloat = parseFloat(tokenAmount.uiAmountString || tokenAmount.uiAmount);
            const symbol = tokenMap.get(mint);
            const nowStr = formatDate(new Date());

            rowsToAdd.push({
              'Symbol': symbol,
              'Network': 'Solana',
              'Token Mint': mint,
              'Amount': balanceFloat,
              'Wallet Name': wallet.name,
              'Wallet Address': wallet.address,
              'Timestamp': nowStr
            });

            walletAdded++;
            totalAdded++;
            walletSuccess = true;
          }
        } catch {}
      }

      // Success or partial success
      if (walletSuccess || allAccounts.length > 0) {
        const color = walletAdded > 0 ? c.green : c.gray;
        console.log(`${color}✓ ${String(walletAdded).padStart(2, '0')} tokens${c.reset}`);
      } else if (!splResult.success && !token2022Result.success) {
        errors.push(`${wallet.name}: Both SPL & Token-2022 failed`);
        console.log(`${c.red}✗ No data${c.reset}`);
      } else {
        console.log(`${c.gray}✓ 00 tokens${c.reset}`);
      }

    } catch (err) {
      errors.push(`${wallet.name}: ${err.message.substring(0, 40)}`);
      console.log(`${c.red}✗ Error${c.reset}`);
    }

    // Health check & reset every N wallets
    if (walletNum % CONFIG.rpcHealthCheckInterval === 0) {
      resetRPCFailures();
    }

    // Large delay between wallets
    if (walletNum < WALLETS.length) {
      await delay(CONFIG.delayBetweenWallets);
    }
  }

  // ============================================
  // Write to Google Sheets
  // ============================================
  console.log(`\n${c.cyan}>> Writing ${rowsToAdd.length} record(s) to Google Sheets...${c.reset}`);

  if (rowsToAdd.length > 0) {
    try {
      await sheet.clear();
      await sheet.setHeaderRow(SHEET_HEADERS);
      await sheet.addRows(rowsToAdd);
      console.log(`${c.green}✓ Successfully written to sheet${c.reset}`);
    } catch (err) {
      console.error(`${c.red}✗ Error writing to sheet: ${err.message}${c.reset}`);
    }
  } else {
    console.log(`${c.yellow}⚠ No tokens found${c.reset}`);
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
  console.log(`Tokens Tracked:        ${tokenMap.size}`);
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
