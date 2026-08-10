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
// RPC ENDPOINTS - ใช้ร่วมกัน (ไม่แยก SPL/Token-2022)
// ============================================
const RPC_ENDPOINTS = [
  'https://solana-rpc.publicnode.com',     // PublicNode - ฟรี
  'https://api.mainnet-beta.solana.com',   // Official - ฟรี
  'https://rpc.ankr.com/solana',           // Ankr - ฟรี
  'https://solana.drpc.org',               // dRPC - ฟรี (aggregator)
];

let currentRPCIndex = 0;
let rpcFailures = {};

RPC_ENDPOINTS.forEach((_, i) => { rpcFailures[i] = { count: 0, lastFail: null }; });

const CONFIG = {
  delayBetweenRequests: 200,
  delayBetweenWallets: 500,
  timeoutMs: 15000,
  maxRetriesPerWallet: 3,
  rpcHealthCheckInterval: 5,
  batchWriteSize: 500,
  mintTypeCacheExpiry: 30 * 60 * 1000,
  multiAccountBatchSize: 100,
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

function switchToHealthiest() {
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

function recordFailure() {
  rpcFailures[currentRPCIndex].count++;
  rpcFailures[currentRPCIndex].lastFail = Date.now();
}

function resetFailures() {
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
  } catch { return false; }
}

// ============================================
// Fetch Function (ใช้ RPC ร่วม)
// ============================================

async function fetchTokenAccountsWithRetry(walletPubKey, programId) {
  let attemptConn = new Connection(RPC_ENDPOINTS[currentRPCIndex], 'confirmed');
  let lastError;

  for (let attempt = 0; attempt < CONFIG.maxRetriesPerWallet; attempt++) {
    try {
      await delay(CONFIG.delayBetweenRequests);

      const result = await Promise.race([
        attemptConn.getParsedTokenAccountsByOwner(walletPubKey, { programId }),
        new Promise((_, reject) => setTimeout(() => reject(new Error('timeout')), CONFIG.timeoutMs))
      ]);

      return { success: true, data: result };

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
        recordFailure();
        if (attempt < CONFIG.maxRetriesPerWallet - 1) {
          attemptConn = switchToHealthiest();
          const waitTime = 500 + (attempt * 1000);
          console.log(`${c.dim} ⟳ Retry ${attempt + 1}/${CONFIG.maxRetriesPerWallet}${c.reset}`);
          await delay(waitTime);
        }
      } else {
        if (attempt < CONFIG.maxRetriesPerWallet - 1) await delay(300);
      }
    }
  }

  return { success: false, error: lastError?.message || 'Max retries' };
}

// ============================================
// Auto-Detect Token Type
// ============================================

class MintTypeCache {
  constructor() {
    this.cache = new Map();
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

async function batchDetectMintTypes(mints) {
  const results = new Map();
  const toDetect = [];

  for (const mint of mints) {
    const cached = mintCache.get(mint);
    if (cached) results.set(mint, cached);
    else toDetect.push(mint);
  }

  if (toDetect.length === 0) {
    console.log(`${c.dim}  ✓ All ${mints.length} mints cached${c.reset}`);
    return results;
  }

  console.log(`${c.cyan}  🔍 Auto-detecting ${toDetect.length} mint(s)...${c.reset}`);

  let conn = new Connection(RPC_ENDPOINTS[currentRPCIndex], 'confirmed');
  let detected = 0;

  for (let i = 0; i < toDetect.length; i += CONFIG.multiAccountBatchSize) {
    const batch = toDetect.slice(i, i + CONFIG.multiAccountBatchSize);
    const pubKeys = batch.map(m => new PublicKey(m));

    let success = false;
    let retries = 0;

    while (!success && retries < CONFIG.maxRetriesPerWallet) {
      try {
        await delay(300); // ช้าหน่อยเพื่อไม่โดน rate limit

        const accounts = await Promise.race([
          conn.getMultipleAccountsInfo(pubKeys),
          new Promise((_, reject) => setTimeout(() => reject(new Error('timeout')), CONFIG.timeoutMs))
        ]);

        for (let j = 0; j < batch.length; j++) {
          const mint = batch[j];
          const account = accounts[j];

          if (!account) {
            mintCache.set(mint, 'spl');
            results.set(mint, 'spl');
            continue;
          }

          const owner = account.owner.toBase58();

          if (owner === TOKEN_2022_PROGRAM_ID.toBase58()) {
            mintCache.set(mint, 'token2022');
            results.set(mint, 'token2022');
            detected++;
          } else {
            mintCache.set(mint, 'spl');
            results.set(mint, 'spl');
          }
        }

        success = true;

      } catch (e) {
        retries++;
        const isRateLimit = e.message && e.message.includes('429');
        if (isRateLimit && retries < CONFIG.maxRetriesPerWallet) {
          recordFailure();
          conn = switchToHealthiest();
          await delay(1000 * Math.pow(2, retries));
        } else {
          // Fallback ทั้ง batch
          for (const mint of batch) {
            mintCache.set(mint, 'spl');
            results.set(mint, 'spl');
          }
          success = true;
        }
      }
    }

    const progress = Math.min(i + CONFIG.multiAccountBatchSize, toDetect.length);
    if (progress % 100 === 0 || progress === toDetect.length) {
      process.stdout.write(`${c.dim}  ${progress}/${toDetect.length} processed...${c.reset}\r`);
    }
  }

  const stats = mintCache.getStats();
  console.log(`\n${c.green}  ✓ Detected: ${stats.spl} SPL, ${stats.token2022} Token-2022${c.reset}`);

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

  getSymbol(mint) {
    return this.token2022Tokens.get(mint) || this.splTokens.get(mint) || this.unknownTokens.get(mint);
  }

  get trackedMints() {
    return new Set([...this.splTokens.keys(), ...this.token2022Tokens.keys()]);
  }

  get splCount() { return this.splTokens.size; }
  get token2022Count() { return this.token2022Tokens.size; }
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
  console.log(`${c.cyan}${c.bright}║  SOLANA TOKEN TRACKER (Free RPC - Unified)             ║${c.reset}`);
  console.log(`${c.cyan}${c.bright}╚════════════════════════════════════════════════════════╝${c.reset}\n`);

  if (!GOOGLE_SERVICE_ACCOUNT_EMAIL || !GOOGLE_PRIVATE_KEY || !SPREADSHEET_ID) {
    console.error(`${c.red}✗ Missing environment variables${c.reset}`);
    process.exit(1);
  }

  // Google Sheets
  console.log(`${c.cyan}[1/6] Connecting to Google Sheets...${c.reset}`);
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

  // Load custom RPC from nodes sheet
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
            RPC_ENDPOINTS.unshift(String(urlCell.value).trim());
          }
        }
      }
    } catch {}
  }

  // Check health
  console.log(`${c.cyan}[2/6] Checking RPC health...${c.reset}`);
  let conn = new Connection(RPC_ENDPOINTS[0], 'confirmed');
  const isHealthy = await checkRPCHealth(conn);
  if (!isHealthy) {
    console.log(`${c.yellow}⚠ Primary RPC unhealthy, switching...${c.reset}`);
    conn = switchToHealthiest();
  } else {
    console.log(`${c.green}✓ RPC is healthy: ${RPC_ENDPOINTS[0].split('/')[2]}${c.reset}`);
  }

  // Load Wallets
  console.log(`${c.cyan}[3/6] Loading wallets...${c.reset}`);
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
  console.log(`${c.cyan}[4/6] Loading token subscriptions...${c.reset}`);
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

  if (!classifier.trackedMints.size && !classifier.unknownTokens.size) {
    console.error(`${c.red}✗ No valid tokens found${c.reset}`);
    process.exit(0);
  }
  console.log(`${c.green}✓ Loaded ${classifier.unknownTokens.size} tokens (pending auto-detect)${c.reset}`);

  // Auto-Detect
  console.log(`${c.cyan}[5/6] Auto-detecting token types...${c.reset}`);
  const allMints = Array.from(classifier.unknownTokens.keys());
  const detections = await batchDetectMintTypes(allMints);
  await classifier.classifyFromDetections(detections);

  console.log(`${c.green}✓ Classification:${c.reset}`);
  console.log(`  ${c.cyan}• SPL: ${classifier.splCount}${c.reset}`);
  console.log(`  ${c.magenta}• Token-2022: ${classifier.token2022Count}${c.reset}`);

  // Setup Sheet
  console.log(`${c.cyan}[6/6] Setting up Google Sheet...${c.reset}`);
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
      const allAccounts = [];

      // 🆕 Fetch SPL (ใช้ RPC ร่วม)
      const splResult = await fetchTokenAccountsWithRetry(walletPubKey, TOKEN_PROGRAM_ID);
      if (splResult.success) {
        allAccounts.push(...splResult.data.value);
      } else {
        errors.push(`${wallet.name} [SPL]: ${splResult.error}`);
      }

      // 🆕 Fetch Token-2022 (ใช้ RPC ร่วมกัน!)
      const t2022Result = await fetchTokenAccountsWithRetry(walletPubKey, TOKEN_2022_PROGRAM_ID);
      if (t2022Result.success) {
        allAccounts.push(...t2022Result.data.value);
      } else {
        errors.push(`${wallet.name} [Token-2022]: ${t2022Result.error}`);
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
      errors.push(`${wallet.name}: ${err.message?.substring(0, 40)}`);
      console.log(`${c.red}✗ Error${c.reset}`);
    }

    if (walletNum % CONFIG.rpcHealthCheckInterval === 0) resetFailures();
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
