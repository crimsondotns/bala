import { Connection, PublicKey } from '@solana/web3.js';
import { TOKEN_PROGRAM_ID, TOKEN_2022_PROGRAM_ID } from '@solana/spl-token';
import { GoogleSpreadsheet } from 'google-spreadsheet';
import { JWT } from 'google-auth-library';
import dotenv from 'dotenv';
dotenv.config();

const c = {
  reset: '\x1b[0m', bright: '\x1b[1m', dim: '\x1b[2m',
  cyan: '\x1b[36m', green: '\x1b[32m', yellow: '\x1b[33m',
  red: '\x1b[31m', gray: '\x1b[90m', magenta: '\x1b[35m'
};

const GOOGLE_SERVICE_ACCOUNT_EMAIL = process.env.GOOGLE_SERVICE_ACCOUNT_EMAIL;
const GOOGLE_PRIVATE_KEY = process.env.GOOGLE_PRIVATE_KEY ? process.env.GOOGLE_PRIVATE_KEY.replace(/\\n/g, '\n') : '';
const SPREADSHEET_ID = process.env.SPREADSHEET_ID;

const SHEET_TAB_NAME = 'Solana_Tracker';
const SUBSCRIPTION_SPL_TAB = 'SUBSCRIPTION SPL';
const SUBSCRIPTION_WALLET_TAB = 'SUBSCRIPTION WALLET';
const SHEET_HEADERS = ['Symbol', 'Network', 'Token Mint', 'Amount', 'Wallet Name', 'Wallet Address', 'Timestamp'];

// ============================================
// RPC ENDPOINTS
// ============================================
const SPL_RPC_ENDPOINTS = [
  'https://api.mainnet-beta.solana.com',
  'https://solana-rpc.publicnode.com',
];

const TOKEN2022_RPC_ENDPOINTS = [
  'https://solana-rpc.publicnode.com',
  'https://api.mainnet-beta.solana.com',
];

let splConn = null;
let token2022Conn = null;

// 🆕 ลด timeout ลง ไม่รอนาน
const CONFIG = {
  splTimeoutMs: 10000,           // SPL 10 วิ
  token2022TimeoutMs: 15000,     // Token-2022 15 วิ (ลดจาก 30 วิ)
  token2022DelayMs: 500,         // ห่าง 0.5 วิ (ลดจาก 2 วิ)
  maxRetries: 2,                 // ลองแค่ 2 ครั้ง
  walletDelayMs: 800,            // ห่างระหว่าง wallet 0.8 วิ
};

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

// ============================================
// INIT CONNECTIONS
// ============================================

async function initConnections() {
  for (const endpoint of SPL_RPC_ENDPOINTS) {
    try {
      const conn = new Connection(endpoint, 'confirmed');
      await Promise.race([
        conn.getSlot(),
        new Promise((_, reject) => setTimeout(() => reject(new Error('timeout')), 5000))
      ]);
      splConn = conn;
      console.log(`${c.green}✓ [SPL] ${endpoint.split('/')[2]}${c.reset}`);
      break;
    } catch {
      console.log(`${c.yellow}⚠ [SPL] Skip ${endpoint.split('/')[2]}${c.reset}`);
    }
  }

  for (const endpoint of TOKEN2022_RPC_ENDPOINTS) {
    try {
      const conn = new Connection(endpoint, 'confirmed');
      await Promise.race([
        conn.getSlot(),
        new Promise((_, reject) => setTimeout(() => reject(new Error('timeout')), 5000))
      ]);
      token2022Conn = conn;
      console.log(`${c.green}✓ [T2022] ${endpoint.split('/')[2]}${c.reset}`);
      break;
    } catch {
      console.log(`${c.yellow}⚠ [T2022] Skip ${endpoint.split('/')[2]}${c.reset}`);
    }
  }

  if (!splConn) {
    console.error(`${c.red}✗ No SPL RPC${c.reset}`);
    process.exit(1);
  }
}

// ============================================
// FETCH SPL - เร็ว
// ============================================

async function fetchSPLAccounts(walletPubKey) {
  if (!splConn) return { success: false, data: [] };

  try {
    await delay(100);
    const result = await Promise.race([
      splConn.getParsedTokenAccountsByOwner(walletPubKey, { programId: TOKEN_PROGRAM_ID }),
      new Promise((_, reject) => setTimeout(() => reject(new Error('timeout')), CONFIG.splTimeoutMs))
    ]);
    return { success: true, data: result.value };
  } catch (e) {
    return { success: false, error: e.message, data: [] };
  }
}

// ============================================
// 🆕 FETCH TOKEN-2022 - เร็ว ไม่รอนาน
// ============================================

async function fetchToken2022Accounts(walletPubKey) {
  if (!token2022Conn) return { success: false, data: [] };

  for (let attempt = 0; attempt < CONFIG.maxRetries; attempt++) {
    try {
      await delay(CONFIG.token2022DelayMs);
      
      const result = await Promise.race([
        token2022Conn.getParsedTokenAccountsByOwner(walletPubKey, { programId: TOKEN_2022_PROGRAM_ID }),
        new Promise((_, reject) => setTimeout(() => reject(new Error('timeout')), CONFIG.token2022TimeoutMs))
      ]);

      return { success: true, data: result.value };

    } catch (e) {
      const isTimeout = e.message?.includes('timeout');
      const is429 = e.message?.includes('429');
      
      if ((isTimeout || is429) && attempt < CONFIG.maxRetries - 1) {
        await delay(1000); // รอ 1 วิแล้วลองใหม่
      } else {
        return { success: false, error: e.message, data: [] };
      }
    }
  }
  
  return { success: false, error: 'Max retries', data: [] };
}

// ============================================
// MAIN
// ============================================

async function main() {
  const startTime = Date.now();

  console.log(`\n${c.cyan}${c.bright}╔════════════════════════════════════════════════════════╗${c.reset}`);
  console.log(`${c.cyan}${c.bright}║  SOLANA TOKEN TRACKER (Fast Token-2022)                ║${c.reset}`);
  console.log(`${c.cyan}${c.bright}╚════════════════════════════════════════════════════════╝${c.reset}\n`);

  if (!GOOGLE_SERVICE_ACCOUNT_EMAIL || !GOOGLE_PRIVATE_KEY || !SPREADSHEET_ID) {
    console.error(`${c.red}✗ Missing env vars${c.reset}`);
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

  // Load custom RPC
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
            
            if (type === 'token2022') TOKEN2022_RPC_ENDPOINTS.unshift(url);
            else SPL_RPC_ENDPOINTS.unshift(url);
          }
        }
      }
    } catch {}
  }

  // Init connections
  console.log(`${c.cyan}[2/5] Initializing RPC...${c.reset}`);
  await initConnections();

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
  console.log(`${c.cyan}[4/5] Loading tokens...${c.reset}`);
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
  console.log(`${c.cyan}[5/5] Setting up sheet...${c.reset}`);
  let sheet = doc.sheetsByTitle[SHEET_TAB_NAME];
  if (!sheet) {
    sheet = await doc.addSheet({ title: SHEET_TAB_NAME, headerValues: SHEET_HEADERS });
  } else {
    try { await sheet.loadHeaderRow(); }
    catch { await sheet.setHeaderRow(SHEET_HEADERS); }
  }
  console.log(`${c.green}✓ Sheet ready${c.reset}`);

  // ============================================
  // 🆕 PROCESS WALLETS - เร็วขึ้น
  // ============================================
  console.log(`\n${c.cyan}${c.bright}>> Scanning wallets${c.reset}`);
  console.log(`${c.gray}SPL: ${splConn.rpcEndpoint.split('/')[2]} | T2022: ${token2022Conn ? token2022Conn.rpcEndpoint.split('/')[2] : 'N/A'}${c.reset}\n`);

  let totalAdded = 0;
  let totalT2022Found = 0;
  const errors = [];
  const rowsToAdd = [];

  for (let wIdx = 0; wIdx < WALLETS.length; wIdx++) {
    const wallet = WALLETS[wIdx];
    const walletNum = wIdx + 1;
    const walletInfo = `${c.gray}[${String(walletNum).padStart(3, '0')}/${WALLETS.length}]${c.reset}`;
    const startWalletTime = Date.now();
    
    process.stdout.write(`${walletInfo} ${wallet.name.padEnd(35, ' ')} `);

    let walletAdded = 0;
    let walletT2022 = 0;

    try {
      const walletPubKey = new PublicKey(wallet.address);

      // 1️⃣ Fetch SPL (เร็ว)
      const splResult = await fetchSPLAccounts(walletPubKey);
      let allAccounts = splResult.success ? splResult.data : [];
      
      if (!splResult.success) {
        errors.push(`${wallet.name} [SPL]: ${splResult.error}`);
      }

      // 2️⃣ 🆕 Fetch Token-2022 (เร็ว ไม่รอนาน)
      const t2022Result = await fetchToken2022Accounts(walletPubKey);
      
      if (t2022Result.success) {
        allAccounts.push(...t2022Result.data);
        walletT2022 = t2022Result.data.length;
        totalT2022Found += walletT2022;
      } else if (t2022Result.error) {
        // 🆕 ไม่ log error ถ้า timeout แค่ข้ามไป
        if (!t2022Result.error.includes('timeout')) {
          errors.push(`${wallet.name} [T2022]: ${t2022Result.error}`);
        }
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

      const walletTime = ((Date.now() - startWalletTime) / 1000).toFixed(1);
      const t2022Tag = walletT2022 > 0 ? `${c.magenta}[T2022:${walletT2022}]${c.reset}` : '';
      const statusColor = walletAdded > 0 ? c.green : c.gray;
      console.log(`${statusColor}✓ ${String(walletAdded).padStart(2, '0')} tokens${c.reset} ${t2022Tag} ${c.dim}(${walletTime}s)${c.reset}`);

    } catch (err) {
      errors.push(`${wallet.name}: ${err.message}`);
      console.log(`${c.red}✗ ${err.message.substring(0, 50)}${c.reset}`);
    }

    // 🆕 Delay สั้นลง
    await delay(CONFIG.walletDelayMs);
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
    console.log(`${c.yellow}⚠ No data${c.reset}`);
  }

  // Summary
  const execSecs = ((Date.now() - startTime) / 1000).toFixed(0);
  const execMins = (execSecs / 60).toFixed(1);

  console.log(`\n${c.cyan}${c.bright}╔════════════════════════════════════════════════════════╗${c.reset}`);
  console.log(`${c.cyan}${c.bright}║  EXECUTION SUMMARY                                     ║${c.reset}`);
  console.log(`${c.cyan}${c.bright}╚════════════════════════════════════════════════════════╝${c.reset}`);
  console.log(`${c.gray}Time: ${execMins} min | Wallets: ${WALLETS.length}${c.reset}`);
  console.log(`${c.gray}Token-2022 Accounts: ${totalT2022Found}${c.reset}`);
  console.log(`${c.green}Records Added: ${totalAdded}${c.reset}`);
  
  if (errors.length > 0) {
    console.log(`${c.red}Errors: ${errors.length}${c.reset}`);
    errors.slice(0, 5).forEach((e, i) => console.log(`${c.red}  ${i + 1}. ${e}${c.reset}`));
  } else {
    console.log(`${c.green}Errors: 0${c.reset}`);
  }
  console.log(`${c.cyan}${c.bright}════════════════════════════════════════════════════════${c.reset}\n`);

  process.exit(0);
}

main().catch(err => {
  console.error(`${c.red}${c.bright}Fatal Error:${c.reset} ${err.message}`);
  process.exit(1);
});
