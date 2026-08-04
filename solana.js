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

// ============================================
// 1. CONFIGURATION
// ============================================
const GOOGLE_SERVICE_ACCOUNT_EMAIL = process.env.GOOGLE_SERVICE_ACCOUNT_EMAIL;
const GOOGLE_PRIVATE_KEY = process.env.GOOGLE_PRIVATE_KEY ? process.env.GOOGLE_PRIVATE_KEY.replace(/\\n/g, '\n') : '';
const SPREADSHEET_ID = process.env.SPREADSHEET_ID;

const SHEET_TAB_NAME = 'Solana_Tracker';
const SUBSCRIPTION_SPL_TAB = 'SUBSCRIPTION SPL';
const SUBSCRIPTION_WALLET_TAB = 'SUBSCRIPTION WALLET';
const SHEET_HEADERS = ['Symbol', 'Network', 'Token Mint', 'Amount', 'Wallet Name', 'Wallet Address', 'Timestamp'];

// ============================================
// 2. RPC ENDPOINTS (Improved & Prioritized)
// ============================================
const RPC_ENDPOINTS = [
  'https://api.mainnet-beta.solana.com',           // Official Solana
  'https://solana-api.projectserum.com',           // Project Serum
  'https://solana-rpc.publicnode.com',             // PublicNode
  'https://rpc.solflare.com',                      // Solflare
  'https://api.orca.so',                           // Orca
  'https://rpc.ankr.com/solana',                   // Ankr
];

let currentRPCIndex = 0;
const RPC_CONFIG = {
  maxRetries: 6,
  retryDelayMs: 1000,
  timeoutMs: 20000,
  healthCheckTimeoutMs: 5000
};

// ============================================
// 3. UTILITY FUNCTIONS
// ============================================

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

function switchRPC() {
  currentRPCIndex = (currentRPCIndex + 1) % RPC_ENDPOINTS.length;
  const endpoint = RPC_ENDPOINTS[currentRPCIndex];
  console.log(`${c.yellow}⚠ Switching RPC to: ${endpoint}${c.reset}`);
  return new Connection(endpoint, 'confirmed');
}

// ============================================
// 4. RPC HEALTH CHECK
// ============================================

async function checkRPCHealth(endpoint) {
  try {
    const conn = new Connection(endpoint, 'confirmed');
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), RPC_CONFIG.healthCheckTimeoutMs);
    
    const slot = await conn.getSlot();
    clearTimeout(timeoutId);
    
    return { healthy: true, slot };
  } catch (err) {
    return { healthy: false, error: err.message };
  }
}

// ============================================
// 5. RETRY LOGIC WITH EXPONENTIAL BACKOFF
// ============================================

async function getWalletTokenAccountsWithRetry(connectionState, walletPubKey, programId) {
  let conn = connectionState.conn;
  let lastError;
  const startTime = Date.now();

  for (let attempt = 0; attempt < RPC_CONFIG.maxRetries; attempt++) {
    try {
      // Create timeout promise
      const timeoutPromise = new Promise((_, reject) =>
        setTimeout(() => reject(new Error('RPC timeout')), RPC_CONFIG.timeoutMs)
      );

      // Race between actual call and timeout
      const result = await Promise.race([
        conn.getParsedTokenAccountsByOwner(walletPubKey, { programId }),
        timeoutPromise
      ]);

      return result;

    } catch (e) {
      lastError = e;
      const elapsedSecs = ((Date.now() - startTime) / 1000).toFixed(1);

      // Determine if this is a network/RPC error
      const isNetworkError = e.message && (
        e.message.includes('fetch') ||
        e.message.includes('429') ||
        e.message.includes('403') ||
        e.message.includes('blocked') ||
        e.message.includes('Too Many Requests') ||
        e.message.includes('ECONNREFUSED') ||
        e.message.includes('ETIMEDOUT') ||
        e.message.includes('timeout') ||
        e.message.includes('ENOTFOUND')
      );

      if (isNetworkError && attempt < RPC_CONFIG.maxRetries - 1) {
        // Switch RPC and wait with exponential backoff
        conn = switchRPC();
        connectionState.conn = conn;
        const backoffMs = RPC_CONFIG.retryDelayMs * Math.pow(2, attempt);
        console.log(`   ${c.dim}Retry ${attempt + 1}/${RPC_CONFIG.maxRetries} after ${backoffMs}ms (elapsed: ${elapsedSecs}s)${c.reset}`);
        await delay(backoffMs);
      } else if (attempt < RPC_CONFIG.maxRetries - 1) {
        // Non-network error, wait shorter
        const backoffMs = 500 * (attempt + 1);
        await delay(backoffMs);
      }
    }
  }

  throw lastError || new Error('Max retries exceeded');
}

// ============================================
// 6. MAIN FUNCTION
// ============================================

async function main() {
  const startTime = Date.now();

  console.log(`\n${c.cyan}${c.bright}╔════════════════════════════════════════════════════════╗${c.reset}`);
  console.log(`${c.cyan}${c.bright}║        SOLANA TOKEN TRACKER (Fixed Version)             ║${c.reset}`);
  console.log(`${c.cyan}${c.bright}╚════════════════════════════════════════════════════════╝${c.reset}\n`);

  // Validate environment variables
  if (!GOOGLE_SERVICE_ACCOUNT_EMAIL || !GOOGLE_PRIVATE_KEY || !SPREADSHEET_ID) {
    console.error(`${c.red}✗ Fatal Error: Missing required environment variables.${c.reset}`);
    console.error(`  Required: GOOGLE_SERVICE_ACCOUNT_EMAIL, GOOGLE_PRIVATE_KEY, SPREADSHEET_ID`);
    process.exit(1);
  }

  // ============================================
  // 7. GOOGLE SHEETS INITIALIZATION
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
    console.log(`${c.green}✓ Google Sheets connected${c.reset}`);
  } catch (err) {
    console.error(`${c.red}✗ Fatal Error: Failed to connect to Google Sheets${c.reset}`);
    console.error(`  ${err.message}`);
    process.exit(1);
  }

  // ============================================
  // 8. LOAD RPC ENDPOINT FROM NODES SHEET
  // ============================================
  let RPC_ENDPOINT = '';
  const nodesSheet = doc.sheetsByTitle['nodes'];
  if (nodesSheet) {
    try {
      const maxRows = nodesSheet.rowCount;
      if (maxRows >= 2) {
        await nodesSheet.loadCells(`A1:B${maxRows}`);
        for (let r = 1; r < maxRows; r++) {
          const netCell = nodesSheet.getCell(r, 0);
          const urlCell = nodesSheet.getCell(r, 1);
          if (netCell && netCell.value && String(netCell.value).trim().toLowerCase() === 'solana') {
            RPC_ENDPOINT = urlCell && urlCell.value ? String(urlCell.value).trim() : '';
            break;
          }
        }
      }
    } catch (err) {
      console.log(`${c.dim}Warning: Failed to read from 'nodes' tab: ${err.message}${c.reset}`);
    }
  }

  if (!RPC_ENDPOINT) {
    RPC_ENDPOINT = RPC_ENDPOINTS[0];
  }

  // ============================================
  // 9. RPC HEALTH CHECK
  // ============================================
  console.log(`${c.cyan}[2/5] Checking RPC health: ${RPC_ENDPOINT}${c.reset}`);
  const healthCheck = await checkRPCHealth(RPC_ENDPOINT);
  
  if (!healthCheck.healthy) {
    console.log(`${c.yellow}⚠ Primary RPC unhealthy: ${healthCheck.error}${c.reset}`);
    console.log(`${c.yellow}  Falling back to: ${RPC_ENDPOINTS[0]}${c.reset}`);
    RPC_ENDPOINT = RPC_ENDPOINTS[0];
  } else {
    console.log(`${c.green}✓ RPC healthy (slot: ${healthCheck.slot})${c.reset}`);
  }

  // ============================================
  // 10. LOAD WALLETS
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
          
          const addrVal = (addrCell && addrCell.value && typeof addrCell.value === 'string') ? addrCell.value.trim() : '';
          const nameVal = (nameCell && nameCell.value) ? String(nameCell.value).trim() : 'Unknown Wallet';

          if (addrVal) {
            try {
              new PublicKey(addrVal);
              WALLETS.push({ name: nameVal, address: addrVal });
            } catch (e) {
              // Invalid address, skip silently
            }
          }
        }
      }
    } catch (err) {
      console.log(`${c.red}Warning: Failed to read from ${SUBSCRIPTION_WALLET_TAB}: ${err.message}${c.reset}`);
    }
  }

  if (WALLETS.length === 0) {
    console.error(`${c.red}✗ No valid wallets found. Exiting.${c.reset}`);
    process.exit(0);
  }
  console.log(`${c.green}✓ Loaded ${WALLETS.length} wallet(s)${c.reset}`);

  // ============================================
  // 11. LOAD TOKENS & CREATE MAP
  // ============================================
  console.log(`${c.cyan}[4/5] Loading token subscriptions...${c.reset}`);
  
  const tokenMap = new Map(); // Mint -> Symbol
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
          const mintRaw = mintCell && mintCell.value ? String(mintCell.value).trim() : '';
          if (mintRaw) {
            try {
              mints = JSON.parse(mintRaw);
              if (!Array.isArray(mints)) mints = [mintRaw];
            } catch {
              mints = [mintRaw];
            }
          }
          
          const symVal = symCell && symCell.value ? String(symCell.value).trim() : 'Unknown';
          for (const m of mints) {
            try {
              new PublicKey(m);
              tokenMap.set(m, symVal);
            } catch (e) {
              // Invalid mint, skip silently
            }
          }
        }
      }
    } catch (err) {
      console.log(`${c.red}Warning: Failed to read from ${SUBSCRIPTION_SPL_TAB}: ${err.message}${c.reset}`);
    }
  }

  if (tokenMap.size === 0) {
    console.error(`${c.red}✗ No valid tokens found to track. Exiting.${c.reset}`);
    process.exit(0);
  }
  console.log(`${c.green}✓ Loaded ${tokenMap.size} token(s) to track${c.reset}`);

  // ============================================
  // 12. SETUP TARGET SHEET
  // ============================================
  console.log(`${c.cyan}[5/5] Setting up Google Sheet...${c.reset}`);
  
  let sheet = doc.sheetsByTitle[SHEET_TAB_NAME];
  if (!sheet) {
    sheet = await doc.addSheet({ title: SHEET_TAB_NAME, headerValues: SHEET_HEADERS });
    console.log(`${c.green}✓ Created new sheet: ${SHEET_TAB_NAME}${c.reset}`);
  } else {
    try {
      await sheet.loadHeaderRow();
      console.log(`${c.green}✓ Using existing sheet: ${SHEET_TAB_NAME}${c.reset}`);
    } catch {
      await sheet.setHeaderRow(SHEET_HEADERS);
    }
  }

  // ============================================
  // 13. PROCESS WALLETS
  // ============================================
  console.log(`\n${c.cyan}${c.bright}>> Starting scan: SOLANA (SPL + Token-2022)${c.reset}`);
  console.log(`${c.gray}RPC Endpoint: ${RPC_ENDPOINT}${c.reset}`);
  console.log(`${c.gray}Wallets: ${WALLETS.length} | Tokens: ${tokenMap.size}${c.reset}\n`);
  
  let totalAdded = 0;
  const errors = [];
  const rowsToAdd = [];

  const connectionState = { conn: new Connection(RPC_ENDPOINT, 'confirmed') };

  for (let wIdx = 0; wIdx < WALLETS.length; wIdx++) {
    const wallet = WALLETS[wIdx];
    const walletInfo = `${c.gray}[${String(wIdx + 1).padStart(3, '0')}/${String(WALLETS.length).padStart(3, '0')}]${c.reset}`;
    process.stdout.write(`${walletInfo} ${wallet.name.padEnd(35, ' ')} `);

    let walletAdded = 0;

    try {
      const walletPubKey = new PublicKey(wallet.address);

      // Fetch both SPL and Token-2022 accounts
      const [splAccounts, token2022Accounts] = await Promise.all([
        getWalletTokenAccountsWithRetry(connectionState, walletPubKey, TOKEN_PROGRAM_ID),
        getWalletTokenAccountsWithRetry(connectionState, walletPubKey, TOKEN_2022_PROGRAM_ID)
      ]);

      const allAccounts = [
        ...(splAccounts?.value || []),
        ...(token2022Accounts?.value || [])
      ];

      for (const item of allAccounts) {
        try {
          const parsedInfo = item.account.data.parsed.info;
          const mint = parsedInfo.mint;
          const tokenAmount = parsedInfo.tokenAmount;

          // Check if mint is tracked and has balance > 0
          if (tokenMap.has(mint) && tokenAmount && tokenAmount.uiAmount > 0) {
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
          }
        } catch (e) {
          // Parse error on specific account, skip
        }
      }

      const statusColor = walletAdded > 0 ? c.green : c.gray;
      console.log(`${statusColor}✓ Found ${String(walletAdded).padStart(2, '0')} tokens${c.reset}`);

    } catch (err) {
      const errorMsg = err.message.length > 50 ? err.message.substring(0, 50) + '...' : err.message;
      errors.push(`${wallet.name} (${wallet.address}): ${err.message}`);
      console.log(`${c.red}✗ ${errorMsg}${c.reset}`);
    }

    // Delay between wallets (increased from 150ms to 500ms)
    await delay(500);
  }

  // ============================================
  // 14. WRITE TO GOOGLE SHEETS
  // ============================================
  console.log(`\n${c.cyan}${c.bright}>> Writing results to Google Sheets...${c.reset}`);
  
  if (rowsToAdd.length > 0) {
    try {
      await sheet.clear();
      await sheet.setHeaderRow(SHEET_HEADERS);
      console.log(`${c.gray}Cleared existing data${c.reset}`);
      
      await sheet.addRows(rowsToAdd);
      console.log(`${c.green}✓ Written ${rowsToAdd.length} row(s) to sheet${c.reset}`);
    } catch (err) {
      errors.push(`Failed to write rows to sheet: ${err.message}`);
      console.error(`${c.red}✗ Error writing to Google Sheets: ${err.message}${c.reset}`);
    }
  } else {
    console.log(`${c.yellow}⚠ No matching token balances found to write${c.reset}`);
  }

  // ============================================
  // 15. SUMMARY REPORT
  // ============================================
  const execSecs = ((Date.now() - startTime) / 1000).toFixed(2);

  console.log(`\n${c.cyan}${c.bright}╔════════════════════════════════════════════════════════╗${c.reset}`);
  console.log(`${c.cyan}${c.bright}║              EXECUTION SUMMARY                          ║${c.reset}`);
  console.log(`${c.cyan}${c.bright}╚════════════════════════════════════════════════════════╝${c.reset}`);
  console.log(`${c.gray}Execution Time:        ${execSecs} seconds${c.reset}`);
  console.log(`${c.gray}Wallets Processed:     ${WALLETS.length}${c.reset}`);
  console.log(`${c.gray}Tokens Tracked:        ${tokenMap.size}${c.reset}`);
  console.log(`${c.green}Total Records Added:   ${totalAdded}${c.reset}`);
  
  if (errors.length > 0) {
    console.log(`${c.red}Total Errors:          ${errors.length}${c.reset}`);
    console.log(`\n${c.red}Errors (showing first 10):${c.reset}`);
    errors.slice(0, 10).forEach((e, i) => {
      console.log(`${c.red}  ${i + 1}. ${e}${c.reset}`);
    });
  } else {
    console.log(`${c.green}Total Errors:          0${c.reset}`);
  }
  console.log(`${c.cyan}${c.bright}════════════════════════════════════════════════════════${c.reset}\n`);
  
  process.exit(0);
}

// ============================================
// 16. RUN MAIN
// ============================================
main().catch(err => {
  console.error(`${c.red}${c.bright}Fatal Error:${c.reset} ${err.message}`);
  process.exit(1);
});
