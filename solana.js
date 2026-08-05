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
// 2. RPC ENDPOINTS (Optimized for High Volume)
// ============================================
const RPC_ENDPOINTS = [
  'https://api.mainnet-beta.solana.com',                      // Official Solana
  'https://solana-rpc.publicnode.com',                        // PublicNode
  'https://solana-api.projectserum.com',                      // Project Serum
  'https://api.triton.one/networks/solana/solana-mainnet',    // Triton
];

let currentRPCIndex = 0;
const RPC_CONFIG = {
  maxRetries: 1,              // Disable retry (we handle it better)
  timeoutMs: 15000,
  healthCheckTimeoutMs: 5000,
  delayBetweenRequests: 600,  // ms between individual requests
  delayBetweenWallets: 2500,  // ms between wallets (increased from 1200)
  requestBatchSize: 10        // Max concurrent Solana requests
};

// Rate limiter
class RateLimiter {
  constructor(maxPerSecond) {
    this.maxPerSecond = maxPerSecond;
    this.timestamps = [];
  }

  async wait() {
    const now = Date.now();
    // Remove timestamps older than 1 second
    this.timestamps = this.timestamps.filter(ts => now - ts < 1000);

    if (this.timestamps.length >= this.maxPerSecond) {
      const oldestTimestamp = this.timestamps[0];
      const waitTime = 1000 - (now - oldestTimestamp) + 10; // +10ms buffer
      await delay(waitTime);
      this.timestamps.shift();
    }

    this.timestamps.push(now);
  }
}

const rateLimiter = new RateLimiter(40); // Max 40 requests per second

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
// 5. OPTIMIZED RETRY LOGIC (Sequential + Rate Limited)
// ============================================

async function fetchTokenAccountsWithRetry(conn, walletPubKey, programId, programName) {
  let lastError;
  let currentConn = conn;
  let rpcSwitches = 0;

  for (let attempt = 0; attempt < 3; attempt++) {
    try {
      // Rate limit before request
      await rateLimiter.wait();
      await delay(RPC_CONFIG.delayBetweenRequests);

      // Timeout wrapper
      const timeoutPromise = new Promise((_, reject) =>
        setTimeout(() => reject(new Error('Request timeout')), RPC_CONFIG.timeoutMs)
      );

      const result = await Promise.race([
        currentConn.getParsedTokenAccountsByOwner(walletPubKey, { programId }),
        timeoutPromise
      ]);

      return { success: true, data: result, programName };

    } catch (e) {
      lastError = e;
      const isNetworkError = e.message && (
        e.message.includes('429') ||
        e.message.includes('403') ||
        e.message.includes('timeout') ||
        e.message.includes('fetch') ||
        e.message.includes('ECONNREFUSED')
      );

      if (isNetworkError && attempt < 2 && rpcSwitches < 2) {
        // Switch RPC on network error
        currentConn = switchRPC();
        rpcSwitches++;
        await delay(1000 + (attempt * 500));
      } else if (!isNetworkError && attempt < 2) {
        // Brief delay for non-network errors
        await delay(300);
      }
    }
  }

  return { success: false, error: lastError?.message || 'Max retries exceeded', programName };
}

// ============================================
// 6. MAIN FUNCTION
// ============================================

async function main() {
  const startTime = Date.now();

  console.log(`\n${c.cyan}${c.bright}╔════════════════════════════════════════════════════════╗${c.reset}`);
  console.log(`${c.cyan}${c.bright}║        SOLANA TOKEN TRACKER (Optimized)                 ║${c.reset}`);
  console.log(`${c.cyan}${c.bright}╚════════════════════════════════════════════════════════╝${c.reset}\n`);

  // Validate environment variables
  if (!GOOGLE_SERVICE_ACCOUNT_EMAIL || !GOOGLE_PRIVATE_KEY || !SPREADSHEET_ID) {
    console.error(`${c.red}✗ Fatal Error: Missing required environment variables.${c.reset}`);
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
      console.log(`${c.dim}Warning: Failed to read from 'nodes' tab${c.reset}`);
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
    console.log(`${c.yellow}⚠ Primary RPC unhealthy, using fallback${c.reset}`);
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
              // Invalid address, skip
            }
          }
        }
      }
    } catch (err) {
      console.log(`${c.red}Warning: Failed to read wallets${c.reset}`);
    }
  }

  if (WALLETS.length === 0) {
    console.error(`${c.red}✗ No valid wallets found.${c.reset}`);
    process.exit(0);
  }
  console.log(`${c.green}✓ Loaded ${WALLETS.length} wallet(s)${c.reset}`);

  // ============================================
  // 11. LOAD TOKENS & CREATE MAP
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
              // Invalid mint, skip
            }
          }
        }
      }
    } catch (err) {
      console.log(`${c.red}Warning: Failed to read tokens${c.reset}`);
    }
  }

  if (tokenMap.size === 0) {
    console.error(`${c.red}✗ No valid tokens found.${c.reset}`);
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
  } else {
    try {
      await sheet.loadHeaderRow();
    } catch {
      await sheet.setHeaderRow(SHEET_HEADERS);
    }
  }
  console.log(`${c.green}✓ Sheet ready${c.reset}`);

  // ============================================
  // 13. PROCESS WALLETS
  // ============================================
  console.log(`\n${c.cyan}${c.bright}>> Starting scan: SOLANA (Sequential Fetch Mode)${c.reset}`);
  console.log(`${c.gray}RPC: ${RPC_ENDPOINT.substring(0, 40)}...${c.reset}`);
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

      // Fetch SPL and Token-2022 SEQUENTIALLY (not in parallel)
      const splResult = await fetchTokenAccountsWithRetry(
        connectionState.conn, 
        walletPubKey, 
        TOKEN_PROGRAM_ID, 
        'SPL'
      );

      if (!splResult.success) {
        console.log(`${c.dim}(SPL: ${splResult.error?.substring(0, 20)})${c.reset}`);
      }

      const token2022Result = await fetchTokenAccountsWithRetry(
        connectionState.conn, 
        walletPubKey, 
        TOKEN_2022_PROGRAM_ID, 
        'Token-2022'
      );

      if (!token2022Result.success) {
        console.log(`${c.dim}(Token-2022: ${token2022Result.error?.substring(0, 20)})${c.reset}`);
      }

      const allAccounts = [
        ...(splResult.success ? splResult.data?.value || [] : []),
        ...(token2022Result.success ? token2022Result.data?.value || [] : [])
      ];

      for (const item of allAccounts) {
        try {
          const parsedInfo = item.account.data.parsed.info;
          const mint = parsedInfo.mint;
          const tokenAmount = parsedInfo.tokenAmount;

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
          // Parse error on account, skip
        }
      }

      const statusColor = walletAdded > 0 ? c.green : c.gray;
      console.log(`${statusColor}✓ Found ${String(walletAdded).padStart(2, '0')} tokens${c.reset}`);

    } catch (err) {
      const msg = err.message.substring(0, 40);
      errors.push(`${wallet.name}: ${msg}`);
      console.log(`${c.red}✗ ${msg}${c.reset}`);
    }

    // Delay between wallets (critical for rate limiting)
    await delay(RPC_CONFIG.delayBetweenWallets);
  }

  // ============================================
  // 14. WRITE TO GOOGLE SHEETS
  // ============================================
  console.log(`\n${c.cyan}${c.bright}>> Writing to Google Sheets...${c.reset}`);
  
  if (rowsToAdd.length > 0) {
    try {
      await sheet.clear();
      await sheet.setHeaderRow(SHEET_HEADERS);
      
      await sheet.addRows(rowsToAdd);
      console.log(`${c.green}✓ Written ${rowsToAdd.length} row(s)${c.reset}`);
    } catch (err) {
      errors.push(`Sheet write failed: ${err.message}`);
      console.error(`${c.red}✗ Error: ${err.message}${c.reset}`);
    }
  } else {
    console.log(`${c.yellow}⚠ No tokens found${c.reset}`);
  }

  // ============================================
  // 15. SUMMARY
  // ============================================
  const execSecs = ((Date.now() - startTime) / 1000).toFixed(2);

  console.log(`\n${c.cyan}${c.bright}╔════════════════════════════════════════════════════════╗${c.reset}`);
  console.log(`${c.cyan}${c.bright}║              EXECUTION SUMMARY                          ║${c.reset}`);
  console.log(`${c.cyan}${c.bright}╚════════════════════════════════════════════════════════╝${c.reset}`);
  console.log(`Execution Time:        ${execSecs}s`);
  console.log(`Wallets:               ${WALLETS.length}`);
  console.log(`Tokens Tracked:        ${tokenMap.size}`);
  console.log(`${c.green}Total Records:         ${totalAdded}${c.reset}`);
  
  if (errors.length > 0) {
    console.log(`${c.red}Errors:                ${errors.length}${c.reset}`);
  } else {
    console.log(`${c.green}Errors:                0${c.reset}`);
  }
  console.log(`${c.cyan}${c.bright}════════════════════════════════════════════════════════${c.reset}\n`);
  
  process.exit(0);
}

// ============================================
// 16. RUN
// ============================================
main().catch(err => {
  console.error(`${c.red}Fatal: ${err.message}${c.reset}`);
  process.exit(1);
});
