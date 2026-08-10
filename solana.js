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

// 🔹 SPL / Detect - หลายตัวสลับใช้
const DETECT_RPC_ENDPOINTS = [
  'https://api.mainnet-beta.solana.com',
  'https://solana-rpc.publicnode.com',
  'https://rpc.ankr.com/solana',
  'https://solana-api.projectserum.com',
];

// 🔹 Token-2022 fetch
const TOKEN2022_RPC_ENDPOINTS = [
  'https://mainnet.helius-rpc.com/?api-key=YOUR_HELIUS_KEY',
  'https://solana-mainnet.rpc.extrnode.com',
  'https://api.triton.one/networks/solana/solana-mainnet',
  'https://api.mainnet-beta.solana.com',
];

let currentDetectIndex = 0;
let currentToken2022Index = 0;
let rpcFailures = { detect: {}, token2022: {} };

DETECT_RPC_ENDPOINTS.forEach((_, i) => { rpcFailures.detect[i] = { count: 0, lastFail: null }; });
TOKEN2022_RPC_ENDPOINTS.forEach((_, i) => { rpcFailures.token2022[i] = { count: 0, lastFail: null }; });

const CONFIG = {
  delayBetweenRequests: 200,
  delayBetweenWallets: 500,
  timeoutMs: 15000,
  maxRetriesPerWallet: 3,
  rpcHealthCheckInterval: 5,
  batchWriteSize: 500,
  mintTypeCacheExpiry: 30 * 60 * 1000,
  // 🆕 Detect settings
  detectBatchSize: 3,           // ลดจาก 10 -> 3
  detectDelayMs: 500,           // เพิ่มจาก 200 -> 500
  detectTimeoutMs: 10000,
  detectMaxRetries: 3,
  // 🆕 getMultipleAccounts batch
  multiAccountBatchSize: 100,   // 1 RPC call = 100 accounts
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
// Smart RPC Management
// ============================================

function switchToHealthiest(type) {
  const endpoints = type === 'detect' ? DETECT_RPC_ENDPOINTS : TOKEN2022_RPC_ENDPOINTS;
  const failures = rpcFailures[type];
  let bestIndex = 0;
  let lowestFailures = failures[0].count;

  for (let i = 1; i < endpoints.length; i++) {
    if (failures[i].count < lowestFailures) {
      lowestFailures = failures[i].count;
      bestIndex = i;
    }
  }

  if (type === 'detect') currentDetectIndex = bestIndex;
  else currentToken2022Index = bestIndex;

  const endpoint = endpoints[bestIndex];
  console.log(`${c.yellow}→ [${type.toUpperCase()}] RPC: ${endpoint.split('/')[2]}${c.reset}`);
  return new Connection(endpoint, 'confirmed');
}

function recordFailure(type) {
  const index = type === 'detect' ? currentDetectIndex : currentToken2022Index;
  rpcFailures[type][index].count++;
  rpcFailures[type][index].lastFail = Date.now();
}

function resetFailures(type) {
  Object.keys(rpcFailures[type]).forEach(key => {
    rpcFailures[type][key].count = Math.max(0, rpcFailures[type][key].count - 1);
  });
}

async function checkRPCHealth(conn) {
  try {
    await Promise.race([
      conn.getSlot(),
      new Promise((_, reject) => setTimeout(() => reject(new Error('timeout')), 5000))
    ]);
    return true;
  } catch { return false; }
}

// ============================================
// Fetch Function for wallet scanning
// ============================================

async function fetchTokenAccountsWithRetry(walletPubKey, programId, tokenType) {
  const endpoints = tokenType === 'token2022' ? TOKEN2022_RPC_ENDPOINTS : DETECT_RPC_ENDPOINTS;
  let currentIndex = tokenType === 'token2022' ? currentToken2022Index : currentDetectIndex;
  let attemptConn = new Connection(endpoints[currentIndex], 'confirmed');
  let lastError;

  for (let attempt = 0; attempt < CONFIG.maxRetriesPerWallet; attempt++) {
    try {
      await delay(CONFIG.delayBetweenRequests);

      const result = await Promise.race([
        attemptConn.getParsedTokenAccountsByOwner(walletPubKey, { programId }),
        new Promise((_, reject) => setTimeout(() => reject(new Error('timeout')), CONFIG.timeoutMs))
      ]);

      return { success: true, data: result, tokenType };

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
        recordFailure(tokenType === 'token2022' ? 'token2022' : 'detect');
        if (attempt < CONFIG.maxRetriesPerWallet - 1) {
          attemptConn = switchToHealthiest(tokenType === 'token2022' ? 'token2022' : 'detect');
          const waitTime = 500 + (attempt * 1000);
          console.log(`${c.dim} ⟳ Retry ${attempt + 1}/${CONFIG.maxRetriesPerWallet} - ${tokenType.toUpperCase()}${c.reset}`);
          await delay(waitTime);
        }
      } else {
        if (attempt < CONFIG.maxRetriesPerWallet - 1) await delay(300);
      }
    }
  }

  return { success: false, error: lastError?.message || 'Max retries', tokenType };
}

// ============================================
// 🆕🆕🆕 AUTO-DETECT WITH getMultipleAccountsInfo
// ============================================

class MintTypeCache {
  constructor() {
    this.cache = new Map(); // mint -> { type, timestamp }
  }

  get(mint) {
    const entry = this.cache.get(mint);
    if (!entry) return null;
    if (Date.now() - entry.timestamp > CONFIG.mintTypeCacheExpiry) {
      this.cache.delete(mint);
      return null;
    }
    return entry.type;
  }

  set(mint, type) {
    this.cache.set(mint, { type, timestamp: Date.now() });
  }

  getStats() {
    let spl = 0, token2022 = 0;
    for (const entry of this.cache.values()) {
      if (entry.type === 'spl') spl++;
      else token2022++;
    }
    return { total: this.cache.size, spl, token2022 };
  }
}

const mintCache = new MintTypeCache();

/**
 * 🆕 ใช้ getMultipleAccountsInfo แทน getAccountInfo หลายครั้ง
 * ลดจำนวน RPC calls จาก 599 -> ~6 calls (599/100)
 */
async function batchDetectMintTypes(mints) {
  const results = new Map();
  const toDetect = [];

  // แยกที่มี cache
  for (const mint of mints) {
    const cached = mintCache.get(mint);
    if (cached) results.set(mint, cached);
    else toDetect.push(mint);
  }

  if (toDetect.length === 0) {
    console.log(`${c.dim}  ✓ All ${mints.length} mints cached${c.reset}`);
    return results;
  }

  console.log(`${c.cyan}  🔍 Auto-detecting ${toDetect.length} mint(s) using getMultipleAccountsInfo...${c.reset}`);

  let conn = new Connection(DETECT_RPC_ENDPOINTS[currentDetectIndex], 'confirmed');
  let detected = 0;
  let failed = 0;

  // แบ่งเป็น batch ละ 100 mints (limit ของ getMultipleAccountsInfo)
  for (let i = 0; i < toDetect.length; i += CONFIG.multiAccountBatchSize) {
    const batch = toDetect.slice(i, i + CONFIG.multiAccountBatchSize);
    const pubKeys = batch.map(m => new PublicKey(m));

    let success = false;
    let retries = 0;

    while (!success && retries < CONFIG.detectMaxRetries) {
      try {
        await delay(CONFIG.detectDelayMs);

        const accounts = await Promise.race([
          conn.getMultipleAccountsInfo(pubKeys),
          new Promise((_, reject) => setTimeout(() => reject(new Error('timeout')), CONFIG.detectTimeoutMs))
        ]);

        // ประมวลผลผลลัพธ์
        for (let j = 0; j < batch.length; j++) {
          const mint = batch[j];
          const account = accounts[j];

          if (!account) {
            // Account ไม่มี -> อาจเป็น invalid mint หรือ closed
            mintCache.set(mint, 'spl');
            results.set(mint, 'spl');
            continue;
          }

          const owner = account.owner.toBase58();

          if (owner === TOKEN_2022_PROGRAM_ID.toBase58()) {
            mintCache.set(mint, 'token2022');
            results.set(mint, 'token2022');
            detected++;
          } else if (owner === TOKEN_PROGRAM_ID.toBase58()) {
            mintCache.set(mint, 'spl');
            results.set(mint, 'spl');
            detected++;
          } else {
            // Unknown owner -> fallback SPL
            mintCache.set(mint, 'spl');
            results.set(mint, 'spl');
          }
        }

        success = true;

      } catch (e) {
        retries++;
        const isRateLimit = e.message && e.message.includes('429');
        const isTimeout = e.message && e.message.includes('timeout');

        if (isRateLimit || isTimeout) {
          recordFailure('detect');
          if (retries < CONFIG.detectMaxRetries) {
            conn = switchToHealthiest('detect');
            const waitTime = isRateLimit ? 1000 * Math.pow(2, retries) : 500;
            console.log(`${c.yellow}  ⚠ ${isRateLimit ? '429' : 'Timeout'} on batch ${Math.floor(i / CONFIG.multiAccountBatchSize) + 1}, switch RPC & wait ${waitTime}ms...${c.reset}`);
            await delay(waitTime);
          }
        } else {
          // Other error -> mark all in batch as failed
          console.log(`${c.red}  ✗ Batch error: ${e.message.substring(0, 60)}${c.reset}`);
          for (const mint of batch) {
            mintCache.set(mint, 'spl');
            results.set(mint, 'spl');
            failed++;
          }
          success = true; // ข้ามไป batch ถัดไป
        }
      }
    }

    if (!success) {
      // Max retries exceeded -> fallback all
      for (const mint of batch) {
        mintCache.set(mint, 'spl');
        results.set(mint, 'spl');
        failed++;
      }
    }

    // Progress indicator
    const progress = Math.min(i + CONFIG.multiAccountBatchSize, toDetect.length);
    if (progress % 100 === 0 || progress === toDetect.length) {
      process.stdout.write(`${c.dim}  ${progress}/${toDetect.length} processed...${c.reset}\r`);
    }
  }

  const stats = mintCache.getStats();
  console.log(`\n${c.green}  ✓ Detected: ${stats.spl} SPL, ${stats.token2022} Token-2022${c.reset}`);
  if (failed > 0) console.log(`${c.yellow}  ⚠ Failed (fallback SPL): ${failed}${c.reset}`);

  return results;
}

// ============================================
// Token Classifier
// ============================================

class TokenClassifier {
  constructor() {
    this.splTokens = new Map();
    this.token2022Tokens = new Map();
    this.unknownTokens = new Map();
  }

  addToken(mint, symbol, type = null) {
    if (type === 'token2022') this.token2022Tokens.set(mint, symbol);
    else if (type === 'spl') this.splTokens.set(mint, symbol);
    else this.unknownTokens.set(mint, symbol);
  }

  async classifyFromDetections(detections) {
    for (const [mint, type] of detections) {
      const symbol = this.unknownTokens.get(mint) ||
                     this.splTokens.get(mint) ||
                     this.token2022Tokens.get(mint);
      if (!symbol) continue;

      if (type === 'token2022') {
        this.token2022Tokens.set(mint, symbol);
        this.splTokens.delete(mint);
      } else {
        this.splTokens.set(mint, symbol);
        this.token2022Tokens.delete(mint);
      }
      this.unknownTokens.delete(mint);
    }
  }

  getType(mint) {
    if (this.token2022Tokens.has(mint)) return 'token2022';
    if (this.splTokens.has(mint)) return 'spl';
    return null;
  }

  getSymbol(mint) {
    return this.token2022Tokens.get(mint) || this.splTokens.get(mint) || this.unknownTokens.get(mint);
  }

  get allMints() {
    return new Set([...this.splTokens.keys(), ...this.token2022Tokens.keys(), ...this.unknownTokens.keys()]);
  }

  get trackedMints() {
    return new Set([...this.splTokens.keys(), ...this.token2022Tokens.keys()]);
  }

  get splCount() { return this.splTokens.size; }
  get token2022Count() { return this.token2022Tokens.size; }
  get unknownCount() { return this.unknownTokens.size; }
}

// ============================================
// Batch Write
// ============================================

async function writeBatchToSheet(sheet, rows, batchSize = CONFIG.batchWriteSize) {
  if (rows.length === 0) return;
  for (let i = 0; i < rows.length; i += batchSize) {
    const batch = rows.slice(i, i + batchSize);
    try {
      await sheet.addRows(batch);
      console.log(`${c.dim} ✓ Written ${Math.min(i + batchSize, rows.length)}/${rows.length} records${c.reset}`);
      await delay(500);
    } catch (err) {
      console.log(`${c.red} ✗ Batch write error: ${err.message}${c.reset}`);
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
  console.log(`${c.cyan}${c.bright}║  SOLANA TOKEN TRACKER (Auto-Detect v2 - Optimized)     ║${c.reset}`);
  console.log(`${c.cyan}${c.bright}╚════════════════════════════════════════════════════════╝${c.reset}\n`);

  if (!GOOGLE_SERVICE_ACCOUNT_EMAIL || !GOOGLE_PRIVATE_KEY || !SPREADSHEET_ID) {
    console.error(`${c.red}✗ Missing environment variables${c.reset}`);
    process.exit(1);
  }

  // Google Sheets
  console.log(`${c.cyan}[1/7] Connecting to Google Sheets...${c.reset}`);
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
    console.error(`${c.red}✗ Failed to connect: ${err.message}${c.reset}`);
    process.exit(1);
  }

  // Load custom RPC
  let customDetect = null;
  let customToken2022 = null;
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
            const type = typeCell?.value?.toString().toLowerCase().trim();
            if (type === 'token2022') customToken2022 = String(urlCell.value).trim();
            else customDetect = String(urlCell.value).trim();
          }
        }
      }
    } catch {}
  }

  if (customDetect) DETECT_RPC_ENDPOINTS.unshift(customDetect);
  if (customToken2022) TOKEN2022_RPC_ENDPOINTS.unshift(customToken2022);

  // Check health
  console.log(`${c.cyan}[2/7] Checking RPC health...${c.reset}`);
  const detectConn = new Connection(DETECT_RPC_ENDPOINTS[0], 'confirmed');
  const t2022Conn = new Connection(TOKEN2022_RPC_ENDPOINTS[0], 'confirmed');
  const [detectHealthy, t2022Healthy] = await Promise.all([
    checkRPCHealth(detectConn),
    checkRPCHealth(t2022Conn)
  ]);
  console.log(`${detectHealthy ? c.green : c.red}  Detect RPC: ${detectHealthy ? '✓ Healthy' : '✗ Unhealthy'}${c.reset}`);
  console.log(`${t2022Healthy ? c.green : c.red}  Token-2022 RPC: ${t2022Healthy ? '✓ Healthy' : '✗ Unhealthy'}${c.reset}`);

  // Load Wallets
  console.log(`${c.cyan}[3/7] Loading wallets...${c.reset}`);
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
    console.error(`${c.red}✗ No valid wallets found${c.reset}`);
    process.exit(0);
  }
  console.log(`${c.green}✓ Loaded ${WALLETS.length} wallets${c.reset}`);

  // Load Tokens
  console.log(`${c.cyan}[4/7] Loading token subscriptions...${c.reset}`);
  const classifier = new TokenClassifier();
  const subsSheet = doc.sheetsByTitle[SUBSCRIPTION_SPL_TAB];

  if (subsSheet) {
    try {
      const maxRows = subsSheet.rowCount;
      if (maxRows >= 2) {
        await subsSheet.loadCells(`A1:C${maxRows}`);
        for (let r = 1; r < maxRows; r++) {
          const symCell = subsSheet.getCell(r, 0);
          const mintCell = subsSheet.getCell(r, 2);
          const sym = symCell?.value ? String(symCell.value).trim() : 'Unknown';

          let mints = [];
          const mintRaw = mintCell?.value ? String(mintCell.value).trim() : '';
          if (mintRaw) {
            try {
              mints = JSON.parse(mintRaw);
              if (!Array.isArray(mints)) mints = [mintRaw];
            } catch { mints = [mintRaw]; }
          }

          for (const m of mints) {
            try { new PublicKey(m); classifier.addToken(m, sym, null); } catch {}
          }
        }
      }
    } catch (err) {
      console.log(`${c.yellow}⚠ Error loading tokens: ${err.message}${c.reset}`);
    }
  }

  if (!classifier.allMints.size) {
    console.error(`${c.red}✗ No valid tokens found${c.reset}`);
    process.exit(0);
  }
  console.log(`${c.green}✓ Loaded ${classifier.allMints.size} tokens (pending auto-detect)${c.reset}`);

  // ============================================
  // 🆕 STEP 5: AUTO-DETECT (Optimized)
  // ============================================
  console.log(`${c.cyan}[5/7] Auto-detecting token types (getMultipleAccountsInfo)...${c.reset}`);
  const allMints = Array.from(classifier.allMints);
  const detections = await batchDetectMintTypes(allMints);
  await classifier.classifyFromDetections(detections);

  console.log(`${c.green}✓ Classification complete:${c.reset}`);
  console.log(`  ${c.cyan}• SPL Tokens: ${classifier.splCount}${c.reset}`);
  console.log(`  ${c.magenta}• Token-2022: ${classifier.token2022Count}${c.reset}`);

  // Setup Sheet
  console.log(`${c.cyan}[6/7] Setting up Google Sheet...${c.reset}`);
  let sheet = doc.sheetsByTitle[SHEET_TAB_NAME];
  if (!sheet) {
    sheet = await doc.addSheet({ title: SHEET_TAB_NAME, headerValues: SHEET_HEADERS });
  } else {
    try { await sheet.loadHeaderRow(); }
    catch { await sheet.setHeaderRow(SHEET_HEADERS); }
  }
  console.log(`${c.green}✓ Sheet ready${c.reset}`);

  // Process Wallets
  console.log(`\n${c.cyan}${c.bright}>> Scanning wallets${c.reset}`);
  console.log(`${c.dim}SPL: ${classifier.splCount} | Token-2022: ${classifier.token2022Count}${c.reset}\n`);

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

    try {
      const walletPubKey = new PublicKey(wallet.address);
      const results = [];

      if (classifier.splCount > 0) {
        const splResult = await fetchTokenAccountsWithRetry(walletPubKey, TOKEN_PROGRAM_ID, 'spl');
        results.push(splResult);
      }

      if (classifier.token2022Count > 0) {
        const t2022Result = await fetchTokenAccountsWithRetry(walletPubKey, TOKEN_2022_PROGRAM_ID, 'token2022');
        results.push(t2022Result);
      }

      const allAccounts = [];
      for (const res of results) {
        if (res.success) {
          allAccounts.push(...(res.data?.value || []));
        } else {
          errors.push(`${wallet.name} [${res.tokenType}]: ${res.error}`);
        }
      }

      totalProcessed += allAccounts.length;

      for (const item of allAccounts) {
        try {
          const parsedInfo = item.account.data.parsed.info;
          const mint = parsedInfo.mint;
          const tokenAmount = parsedInfo.tokenAmount;

          if (classifier.trackedMints.has(mint)) {
            const amount = parseFloat(tokenAmount?.uiAmountString || tokenAmount?.uiAmount || '0');
            const symbol = classifier.getSymbol(mint);
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
          }
        } catch (tokenErr) {}
      }

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

    if (walletNum % CONFIG.rpcHealthCheckInterval === 0) {
      resetFailures('detect');
      resetFailures('token2022');
    }

    if (walletNum < WALLETS.length) await delay(CONFIG.delayBetweenWallets);
  }

  // Write
  console.log(`\n${c.cyan}>> Writing ${rowsToAdd.length} record(s) to Google Sheets...${c.reset}`);

  if (rowsToAdd.length > 0) {
    try {
      await sheet.clear();
      await sheet.setHeaderRow(SHEET_HEADERS);
      await writeBatchToSheet(sheet, rowsToAdd);
      console.log(`${c.green}✓ Successfully written to sheet${c.reset}`);
    } catch (err) {
      console.error(`${c.red}✗ Error writing: ${err.message}${c.reset}`);
      errors.push(`Google Sheets: ${err.message}`);
    }
  } else {
    console.log(`${c.yellow}⚠ No tokens found to write${c.reset}`);
  }

  // Summary
  const execMins = ((Date.now() - startTime) / 1000 / 60).toFixed(2);
  const cacheStats = mintCache.getStats();

  console.log(`\n${c.cyan}${c.bright}╔════════════════════════════════════════════════════════╗${c.reset}`);
  console.log(`${c.cyan}${c.bright}║  EXECUTION SUMMARY                                     ║${c.reset}`);
  console.log(`${c.cyan}${c.bright}╚════════════════════════════════════════════════════════╝${c.reset}`);
  console.log(`Execution Time: ${execMins} minutes`);
  console.log(`Wallets Processed: ${WALLETS.length}`);
  console.log(`Empty Wallets: ${totalEmptyWallets}`);
  console.log(`Cache: ${cacheStats.spl} SPL, ${cacheStats.token2022} Token-2022`);
  console.log(`Total Accounts Found: ${totalProcessed}`);
  console.log(`${c.green}Total Records Added: ${totalAdded}${c.reset}`);

  if (errors.length > 0) {
    console.log(`${c.red}Total Errors: ${errors.length}${c.reset}`);
    if (errors.length <= 15) {
      console.log(`\n${c.red}Errors:${c.reset}`);
      errors.forEach((e, i) => console.log(`${c.red} ${i + 1}. ${e}${c.reset}`));
    }
  } else {
    console.log(`${c.green}Total Errors: 0${c.reset}`);
  }
  console.log(`${c.cyan}${c.bright}════════════════════════════════════════════════════════${c.reset}\n`);

  process.exit(0);
}

main().catch(err => {
  console.error(`${c.red}${c.bright}Fatal Error:${c.reset} ${err.message}`);
  process.exit(1);
});
