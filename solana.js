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
  gray: '\x1b[90m',
  magenta: '\x1b[35m'
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
// 2. RPC ENDPOINTS (แยกตาม Token Type)
// ============================================

// 🔹 SPL Tokens - ใช้ได้ทุก node (จากโค้ดเดิม)
const SPL_RPC_ENDPOINTS = [
  'https://api.mainnet-beta.solana.com',
  'https://solana-rpc.publicnode.com',
  'https://solana-api.projectserum.com',
  'https://api.triton.one/networks/solana/solana-mainnet',
];

// 🔹 Token-2022 - ต้องใช้ node ที่รองรับดี
const TOKEN2022_RPC_ENDPOINTS = [
  'https://mainnet.helius-rpc.com/?api-key=YOUR_HELIUS_KEY',
  'https://solana-mainnet.rpc.extrnode.com',
  'https://api.triton.one/networks/solana/solana-mainnet',
  'https://api.mainnet-beta.solana.com',
];

let currentSPLIndex = 0;
let currentToken2022Index = 0;
const RPC_CONFIG = {
  maxRetries: 3,
  retryDelayMs: 800,
  timeoutMs: 12000,
  healthCheckTimeoutMs: 5000,
  // 🆕 Token-2022 specific
  token2022DelayMs: 2000,
  token2022TimeoutMs: 30000,
  token2022MaxRetries: 5,
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
    year: 'numeric', month: '2-digit', day: '2-digit',
    hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false
  });
  const parts = formatter.formatToParts(date);
  const partMap = {};
  parts.forEach(p => partMap[p.type] = p.value);
  return `${partMap.month}/${partMap.day}/${partMap.year} ${partMap.hour}:${partMap.minute}:${partMap.second}`;
}

function switchSPLRPC() {
  currentSPLIndex = (currentSPLIndex + 1) % SPL_RPC_ENDPOINTS.length;
  const endpoint = SPL_RPC_ENDPOINTS[currentSPLIndex];
  console.log(`${c.yellow}⚠ [SPL] Switching to: ${endpoint.split('/')[2]}${c.reset}`);
  return new Connection(endpoint, 'confirmed');
}

function switchToken2022RPC() {
  currentToken2022Index = (currentToken2022Index + 1) % TOKEN2022_RPC_ENDPOINTS.length;
  const endpoint = TOKEN2022_RPC_ENDPOINTS[currentToken2022Index];
  console.log(`${c.yellow}⚠ [Token-2022] Switching to: ${endpoint.split('/')[2]}${c.reset}`);
  return new Connection(endpoint, 'confirmed');
}

// ============================================
// 4. RPC HEALTH CHECK
// ============================================

async function checkRPCHealth(endpoint) {
  try {
    const conn = new Connection(endpoint, 'confirmed');
    const slot = await Promise.race([
      conn.getSlot(),
      new Promise((_, reject) => setTimeout(() => reject(new Error('timeout')), RPC_CONFIG.healthCheckTimeoutMs))
    ]);
    return { healthy: true, slot };
  } catch (err) {
    return { healthy: false, error: err.message };
  }
}

// ============================================
// 5. SPL FETCH (เร็วตามปกติ)
// ============================================

async function fetchSPLAccounts(walletPubKey) {
  let conn = new Connection(SPL_RPC_ENDPOINTS[currentSPLIndex], 'confirmed');
  let lastError;

  for (let attempt = 0; attempt < RPC_CONFIG.maxRetries; attempt++) {
    try {
      const timeoutPromise = new Promise((_, reject) =>
        setTimeout(() => reject(new Error('RPC timeout')), RPC_CONFIG.timeoutMs)
      );

      const result = await Promise.race([
        conn.getParsedTokenAccountsByOwner(walletPubKey, { programId: TOKEN_PROGRAM_ID }),
        timeoutPromise
      ]);

      return { success: true, data: result.value };

    } catch (e) {
      lastError = e;
      const isNetworkError = e.message && (
        e.message.includes('fetch') || e.message.includes('429') ||
        e.message.includes('403') || e.message.includes('timeout') ||
        e.message.includes('ECONNREFUSED') || e.message.includes('ETIMEDOUT')
      );

      if (attempt < RPC_CONFIG.maxRetries - 1 && isNetworkError) {
        conn = switchSPLRPC();
        await delay(RPC_CONFIG.retryDelayMs + (attempt * 500));
      }
    }
  }

  return { success: false, error: lastError?.message, data: [] };
}

// ============================================
// 6. TOKEN-2022 FETCH (ช้าแต่ชัวร์)
// ============================================

async function fetchToken2022Accounts(walletPubKey) {
  console.log(`${c.dim}    [Token-2022] Starting fetch...${c.reset}`);
  
  // ลองทุก RPC ในลิสต์
  for (let rpcIdx = 0; rpcIdx < TOKEN2022_RPC_ENDPOINTS.length; rpcIdx++) {
    const endpoint = TOKEN2022_RPC_ENDPOINTS[rpcIdx];
    const conn = new Connection(endpoint, 'confirmed');
    
    console.log(`${c.dim}    Trying ${endpoint.split('/')[2]}...${c.reset}`);
    
    for (let attempt = 0; attempt < RPC_CONFIG.token2022MaxRetries; attempt++) {
      try {
        await delay(RPC_CONFIG.token2022DelayMs + (attempt * 1000));
        
        const result = await Promise.race([
          conn.getParsedTokenAccountsByOwner(walletPubKey, { programId: TOKEN_2022_PROGRAM_ID }),
          new Promise((_, reject) => setTimeout(() => reject(new Error('timeout')), RPC_CONFIG.token2022TimeoutMs))
        ]);
        
        console.log(`${c.green}    ✓ Token-2022: ${result.value.length} accounts from ${endpoint.split('/')[2]}${c.reset}`);
        return { success: true, data: result.value };

      } catch (e) {
        const isUnsupported = e.message && (
          e.message.includes('not supported') || 
          e.message.includes('Method not found') ||
          e.message.includes('Invalid param') ||
          e.message.includes('programId not supported')
        );
        
        if (isUnsupported) {
          console.log(`${c.yellow}    ⚠ ${endpoint.split('/')[2]} doesn't support Token-2022${c.reset}`);
          break;
        }
        
        const isRateLimit = e.message && (e.message.includes('429') || e.message.includes('Too Many'));
        if (isRateLimit && attempt < RPC_CONFIG.token2022MaxRetries - 1) {
          const waitTime = 3000 * Math.pow(2, attempt);
          console.log(`${c.dim}    ⟳ 429 - wait ${waitTime}ms${c.reset}`);
          await delay(waitTime);
        } else {
          console.log(`${c.dim}    ✗ ${e.message.substring(0, 50)}${c.reset}`);
        }
      }
    }
  }
  
  console.log(`${c.red}    ✗ All Token-2022 RPC exhausted${c.reset}`);
  return { success: false, error: 'All RPC exhausted', data: [] };
}

// ============================================
// 7. MAIN FUNCTION
// ============================================

async function main() {
  const startTime = Date.now();

  console.log(`\n${c.cyan}${c.bright}╔════════════════════════════════════════════════════════╗${c.reset}`);
  console.log(`${c.cyan}${c.bright}║  SOLANA TOKEN TRACKER (SPL + Token-2022 Split)         ║${c.reset}`);
  console.log(`${c.cyan}${c.bright}╚════════════════════════════════════════════════════════╝${c.reset}\n`);

  if (!GOOGLE_SERVICE_ACCOUNT_EMAIL || !GOOGLE_PRIVATE_KEY || !SPREADSHEET_ID) {
    console.error(`${c.red}✗ Missing environment variables${c.reset}`);
    process.exit(1);
  }

  // Google Sheets
  console.log(`${c.cyan}[1/5] Connecting to Google Sheets...${c.reset}`);
  const serviceAccountAuth = new JWT({
    email: GOOGLE_SERVICE_ACCOUNT_EMAIL,
    key: GOOGLE_PRIVATE_KEY,
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
  });

  const doc = new GoogleSpreadsheet(SPREADSHEET_ID, serviceAccountAuth);
  try {
    await doc.loadInfo();
    console.log(`${c.green}✓ Connected${c.reset}`);
  } catch (err) {
    console.error(`${c.red}✗ Failed: ${err.message}${c.reset}`);
    process.exit(1);
  }

  // Load custom RPC from nodes sheet
  const nodesSheet = doc.sheetsByTitle['nodes'];
  if (nodesSheet) {
    try {
      const maxRows = nodesSheet.rowCount;
      if (maxRows >= 2) {
        await nodesSheet.loadCells(`A1:C${maxRows}`);
        for (let r = 1; r < maxRows; r++) {
          const netCell = nodesSheet.getCell(r, 0);
          const urlCell = nodesSheet.getCell(r, 1);
          const typeCell = nodesSheet.getCell(r, 2);
          
          if (netCell?.value?.toString().toLowerCase() === 'solana' && urlCell?.value) {
            const url = String(urlCell.value).trim();
            const type = typeCell?.value ? String(typeCell.value).toLowerCase().trim() : 'spl';
            
            if (type === 'token2022') {
              TOKEN2022_RPC_ENDPOINTS.unshift(url);
              console.log(`${c.dim}  + Custom Token-2022 RPC: ${url.split('/')[2]}${c.reset}`);
            } else {
              SPL_RPC_ENDPOINTS.unshift(url);
              console.log(`${c.dim}  + Custom SPL RPC: ${url.split('/')[2]}${c.reset}`);
            }
          }
        }
      }
    } catch {}
  }

  // Check health
  console.log(`${c.cyan}[2/5] Checking RPC health...${c.reset}`);
  const splHealth = await checkRPCHealth(SPL_RPC_ENDPOINTS[0]);
  const t2022Health = await checkRPCHealth(TOKEN2022_RPC_ENDPOINTS[0]);
  
  console.log(`${splHealth.healthy ? c.green : c.red}  SPL: ${splHealth.healthy ? '✓ ' + SPL_RPC_ENDPOINTS[0].split('/')[2] : '✗ ' + splHealth.error}${c.reset}`);
  console.log(`${t2022Health.healthy ? c.green : c.red}  Token-2022: ${t2022Health.healthy ? '✓ ' + TOKEN2022_RPC_ENDPOINTS[0].split('/')[2] : '✗ Unhealthy'}${c.reset}`);

  // Load Wallets
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
            try { new PublicKey(addr); WALLETS.push({ name, address: addr }); } catch {}
          }
        }
      }
    } catch {}
  }

  if (!WALLETS.length) {
    console.error(`${c.red}✗ No valid wallets${c.reset}`);
    process.exit(0);
  }
  console.log(`${c.green}✓ Loaded ${WALLETS.length} wallet(s)${c.reset}`);

  // Load Tokens
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
            } catch { mints = [mintRaw]; }
          }
          
          const sym = symCell?.value ? String(symCell.value).trim() : 'Unknown';
          for (const m of mints) {
            try { new PublicKey(m); tokenMap.set(m, sym); } catch {}
          }
        }
      }
    } catch {}
  }

  if (!tokenMap.size) {
    console.error(`${c.red}✗ No valid tokens${c.reset}`);
    process.exit(0);
  }
  console.log(`${c.green}✓ Loaded ${tokenMap.size} token(s)${c.reset}`);

  // Setup Sheet
  console.log(`${c.cyan}[5/5] Setting up Google Sheet...${c.reset}`);
  let sheet = doc.sheetsByTitle[SHEET_TAB_NAME];
  if (!sheet) {
    sheet = await doc.addSheet({ title: SHEET_TAB_NAME, headerValues: SHEET_HEADERS });
  } else {
    try { await sheet.loadHeaderRow(); }
    catch { await sheet.setHeaderRow(SHEET_HEADERS); }
  }
  console.log(`${c.green}✓ Sheet ready${c.reset}`);

  // ============================================
  // 8. PROCESS WALLETS (แยก SPL / Token-2022)
  // ============================================
  console.log(`\n${c.cyan}${c.bright}>> Scanning wallets${c.reset}`);
  console.log(`${c.gray}SPL: ${SPL_RPC_ENDPOINTS[0].split('/')[2]} | Token-2022: ${TOKEN2022_RPC_ENDPOINTS[0].split('/')[2]}${c.reset}\n`);

  let totalAdded = 0;
  let totalToken2022Found = 0;
  const errors = [];
  const rowsToAdd = [];

  for (let wIdx = 0; wIdx < WALLETS.length; wIdx++) {
    const wallet = WALLETS[wIdx];
    const walletInfo = `${c.gray}[${String(wIdx + 1).padStart(3, '0')}/${String(WALLETS.length).padStart(3, '0')}]${c.reset}`;
    process.stdout.write(`${walletInfo} ${wallet.name.padEnd(35, ' ')} `);

    let walletAdded = 0;
    let walletT2022 = 0;

    try {
      const walletPubKey = new PublicKey(wallet.address);

      // 1️⃣ Fetch SPL (เร็ว)
      const splResult = await fetchSPLAccounts(walletPubKey);
      let allAccounts = [];
      
      if (splResult.success) {
        allAccounts = splResult.data;
      } else {
        errors.push(`${wallet.name} [SPL]: ${splResult.error}`);
      }

      // 2️⃣ Fetch Token-2022 (ช้าแต่ละเอียด)
      const t2022Result = await fetchToken2022Accounts(walletPubKey);
      
      if (t2022Result.success) {
        allAccounts.push(...t2022Result.data);
        walletT2022 = t2022Result.data.length;
        totalToken2022Found += walletT2022;
      } else {
        errors.push(`${wallet.name} [Token-2022]: ${t2022Result.error}`);
      }

      // Process
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
        } catch (e) {}
      }

      const t2022Tag = walletT2022 > 0 ? `${c.magenta}[T2022:${walletT2022}]${c.reset}` : '';
      const statusColor = walletAdded > 0 ? c.green : c.gray;
      console.log(`${statusColor}✓ ${String(walletAdded).padStart(2, '0')} tokens${c.reset} ${t2022Tag}`);

    } catch (err) {
      errors.push(`${wallet.name}: ${err.message}`);
      console.log(`${c.red}✗ ${err.message.substring(0, 50)}${c.reset}`);
    }

    await delay(1200);
  }

  // Write
  console.log(`\n${c.cyan}${c.bright}>> Writing ${rowsToAdd.length} record(s)...${c.reset}`);
  
  if (rowsToAdd.length > 0) {
    try {
      await sheet.clear();
      await sheet.setHeaderRow(SHEET_HEADERS);
      await sheet.addRows(rowsToAdd);
      console.log(`${c.green}✓ Written ${rowsToAdd.length} row(s)${c.reset}`);
    } catch (err) {
      console.error(`${c.red}✗ Write error: ${err.message}${c.reset}`);
      errors.push(`Sheets: ${err.message}`);
    }
  } else {
    console.log(`${c.yellow}⚠ No data to write${c.reset}`);
  }

  // Summary
  const execSecs = ((Date.now() - startTime) / 1000).toFixed(2);

  console.log(`\n${c.cyan}${c.bright}╔════════════════════════════════════════════════════════╗${c.reset}`);
  console.log(`${c.cyan}${c.bright}║  EXECUTION SUMMARY                                     ║${c.reset}`);
  console.log(`${c.cyan}${c.bright}╚════════════════════════════════════════════════════════╝${c.reset}`);
  console.log(`${c.gray}Time: ${execSecs}s | Wallets: ${WALLETS.length}${c.reset}`);
  console.log(`${c.gray}Token-2022 Accounts Found: ${totalToken2022Found}${c.reset}`);
  console.log(`${c.green}Records Added: ${totalAdded}${c.reset}`);
  
  if (errors.length > 0) {
    console.log(`${c.red}Errors: ${errors.length}${c.reset}`);
    errors.slice(0, 10).forEach((e, i) => console.log(`${c.red}  ${i + 1}. ${e}${c.reset}`));
  }
  console.log(`${c.cyan}${c.bright}════════════════════════════════════════════════════════${c.reset}\n`);

  process.exit(0);
}

main().catch(err => {
  console.error(`${c.red}${c.bright}Fatal Error:${c.reset} ${err.message}`);
  process.exit(1);
});
