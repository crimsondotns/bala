import { Connection, PublicKey } from '@solana/web3.js';
import { getAssociatedTokenAddress } from '@solana/spl-token';
import { GoogleSpreadsheet } from 'google-spreadsheet';
import { JWT } from 'google-auth-library';
import dotenv from 'dotenv';
dotenv.config();

// ============================================================================
// 1. SETTINGS & APP CONFIGURATION
// ============================================================================
const RPC_ENDPOINT = process.env.RPC_ENDPOINT || 'https://solana-rpc.publicnode.com';
const INTERVAL_MS = parseInt(process.env.INTERVAL_MS, 10) || 300000; 
const DELAY_BETWEEN_CHUNKS_MS = parseInt(process.env.DELAY_BETWEEN_WALLETS_MS, 10) || 1000;

// ============================================================================
// 2. OUTPUT CONFIGURATION (Google Sheets)
// ============================================================================
const SPREADSHEET_ID = process.env.SPREADSHEET_ID;
const GOOGLE_SERVICE_ACCOUNT_EMAIL = process.env.GOOGLE_SERVICE_ACCOUNT_EMAIL;
const GOOGLE_PRIVATE_KEY = process.env.GOOGLE_PRIVATE_KEY;
const SHEET_TAB_NAME = 'Solana_Tracker';
const SUBSCRIPTION_SPL_TAB = process.env.SUBSCRIPTION_SPL_TAB || '';
const SHEET_HEADERS = ['Symbol', 'Token Mint', 'Amount', 'Wallet Name', 'Wallet Address', 'Timestamp'];

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
let tokenMap = new Map();

async function fetchTokenList() {
  try {
    const response = await fetch('https://api.jup.ag/tokens/v1/tagged/verified', {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'application/json'
      }
    });
    if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
    const data = await response.json();
    
    const tokenArray = Array.isArray(data) ? data : (data.tokens || []);
    tokenArray.forEach(token => {
      tokenMap.set(token.address, token.symbol);
    });
  } catch (error) {
    console.error('Failed to fetch Jupiter token list (Symbols might be missing):', error.message);
  }
}

function delay(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function formatTimestamp(date = new Date()) {
  const month = String(date.getMonth() + 1).padStart(2, '0');
  const day = String(date.getDate()).padStart(2, '0');
  const year = date.getFullYear();
  const hours = String(date.getHours()).padStart(2, '0');
  const minutes = String(date.getMinutes()).padStart(2, '0');
  const seconds = String(date.getSeconds()).padStart(2, '0');
  return `${month}/${day}/${year} ${hours}:${minutes}:${seconds}`;
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

async function getSubscriptionSheet() {
  const serviceAccountAuth = new JWT({
    email: GOOGLE_SERVICE_ACCOUNT_EMAIL,
    key: GOOGLE_PRIVATE_KEY ? GOOGLE_PRIVATE_KEY.replace(/\\n/g, '\n') : '',
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
  });

  const doc = new GoogleSpreadsheet(SPREADSHEET_ID, serviceAccountAuth);
  await doc.loadInfo(); 
  
  let sheet = doc.sheetsByTitle[SUBSCRIPTION_SPL_TAB];
  if (!sheet) {
    throw new Error(`Sheet '${SUBSCRIPTION_SPL_TAB}' not found in Google Spreadsheet`);
  }
  await sheet.loadHeaderRow();
  return sheet;
}

async function loadTokensFromSubscriptionSheet() {
  const tokens = [];
  try {
    const sheet = await getSubscriptionSheet();
    const rows = await sheet.getRows({ offset: 0 });
    
    for (let i = 0; i < rows.length; i++) {
      const row = rows[i];
      const rowData = row._rawData || [];
      const symbol = rowData[0]; // Column A (NAME)
      const contracts = rowData[2]; // Column C (CONTRACTS)
      
      if (symbol && contracts) {
        let mintAddresses = [];
        try {
          mintAddresses = JSON.parse(contracts);
          if (!Array.isArray(mintAddresses)) {
            mintAddresses = [contracts];
          }
        } catch {
          mintAddresses = [contracts];
        }
        
        for (const mint of mintAddresses) {
          if (mint && typeof mint === 'string' && mint.trim() !== '') {
            tokens.push({
              symbol,
              mint: mint.trim()
            });
            tokenMap.set(mint.trim(), symbol);
          }
        }
      }
    }
    console.log(`[INFO] Loaded ${tokens.length} tokens from '${SUBSCRIPTION_SPL_TAB}' sheet.`);
  } catch (err) {
    console.error('[FATAL] Failed to load tokens from subscription sheet:', err.message);
    process.exit(1);
  }
  return tokens;
}

// ============================================================================
// 5. BATCH PROCESSING LOGIC
// ============================================================================

async function loadSheetCache(sheet) {
  const cache = new Map();
  try {
    const rows = await sheet.getRows();
    console.log(`[DEBUG] Cache load: Found ${rows.length} rows in sheet`);
    for (const row of rows) {
      // Use _rawData to access by column index since header mapping is not working
      const rowData = row._rawData || [];
      const symbol = rowData[0]; // Column A: Symbol
      const tokenMint = rowData[1]; // Column B: Token Mint
      const amount = rowData[2]; // Column C: Amount
      const walletName = rowData[3]; // Column D: Wallet Name
      const walletAddr = rowData[4]; // Column E: Wallet Address
      const timestamp = rowData[5]; // Column F: Timestamp
      
      if (walletAddr && tokenMint) {
        const key = `${walletAddr}_${tokenMint}`;
        console.log(`[DEBUG] Cache load: Row with key='${key}'`);
        cache.set(key, row);
      }
    }
    console.log(`[INFO] Loaded ${cache.size} existing rows from Google Sheets cache.`);
  } catch (err) {
    console.warn('[WARN] Failed to load sheet cache (sheet might be empty):', err.message);
  }
  return cache;
}

async function prepareATARequests(tokens) {
  const requests = [];
  for (const wallet of WALLETS) {
    for (const token of tokens) {
      try {
        const ata = await getAssociatedTokenAddress(
          new PublicKey(token.mint), 
          new PublicKey(wallet.address)
        );
        requests.push({
          ataAddress: ata.toString(),
          wallet,
          tokenMint: token.mint,
          tokenSymbol: token.symbol
        });
      } catch (err) {
        console.warn(`[WARN] Invalid ATA calculation for wallet ${wallet.name} and token ${token.symbol}`);
      }
    }
  }
  return requests;
}

let roundCounter = 0;

function generateSummary({ round, execTime, nextSyncStr, totalWallets, totalTokensChecked, rowsUpdated, rowsAdded, fails }) {
  const border = '--------------------------------------------------';
  const now = formatTimestamp();
  
  let out = `\n${border}\n[${now}] PROCESS SUMMARY: SOLANA_TRACKER (ROUND #${round})\n${border}\n`;
  out += `Status: Completed\nTotal Wallets: ${totalWallets}\nTotal ATAs Checked: ${totalTokensChecked}\n`;
  out += `Rows Updated: ${rowsUpdated}\nRows Added: ${rowsAdded}\nFailed: ${fails.length}\n`;

  if (fails.length > 0) {
    out += `\nFailed Items List:\n`;
    for (const f of fails) {
      out += `- [Solana] ${f.error}\n`;
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

  const failList = [];
  let rowsUpdated = 0;
  let rowsAdded = 0;

  const sheetCache = await loadSheetCache(sheet);

  const tokens = await loadTokensFromSubscriptionSheet();
  const requests = await prepareATARequests(tokens);
  const totalTokensChecked = requests.length;
  
  const CHUNK_SIZE = 10;
  const chunks = [];
  for (let i = 0; i < requests.length; i += CHUNK_SIZE) {
    chunks.push(requests.slice(i, i + CHUNK_SIZE));
  }

  console.log(`[INFO] Prepared ${requests.length} ATA addresses, split into ${chunks.length} chunks.`);

  const timestamp = formatTimestamp();

  // Buffers for batch writing
  const rowsToUpdate = [];
  const rowsToAdd = [];
  const BUFFER_FLUSH_INTERVAL = 5;

  async function flushBuffers() {
    if (rowsToUpdate.length > 0) {
      for (const row of rowsToUpdate) {
        try {
          await row.save();
          rowsUpdated++;
        } catch (saveErr) {
          console.error('[ERROR] Failed to save row:', saveErr.message);
        }
      }
      rowsToUpdate.length = 0;
      console.log(`[BUFFER] Flushed ${rowsUpdated} updates to Google Sheets...`);
    }
    if (rowsToAdd.length > 0) {
      try {
        const addedRows = await sheet.addRows(rowsToAdd);
        rowsAdded += rowsToAdd.length;
        // Update cache with actual row objects from Google Sheets
        for (let i = 0; i < addedRows.length; i++) {
          const newRow = addedRows[i];
          const rowData = rowsToAdd[i];
          const uniqueKey = `${rowData['Wallet Address']}_${rowData['Token Mint']}`;
          sheetCache.set(uniqueKey, newRow);
        }
        console.log(`[BUFFER] Flushed ${rowsToAdd.length} new rows to Google Sheets...`);
      } catch (addErr) {
        console.error('[ERROR] Failed to add rows:', addErr.message);
      }
      rowsToAdd.length = 0;
    }
  }

  for (let i = 0; i < chunks.length; i++) {
    const chunk = chunks[i];
    console.log(`[PROGRESS] Fetching chunk ${i + 1} of ${chunks.length} (${chunk.length} addresses)...`);
    
    try {
      const pubkeys = chunk.map(req => new PublicKey(req.ataAddress));
      const response = await connection.getMultipleParsedAccounts(pubkeys);

      for (let j = 0; j < response.value.length; j++) {
        const accountInfo = response.value[j];
        const req = chunk[j];
        const symbol = req.tokenSymbol || tokenMap.get(req.tokenMint) || 'Unknown';
        const uniqueKey = `${req.wallet.address}_${req.tokenMint}`;

        if (accountInfo === null) continue;

        const parsedInfo = accountInfo.data.parsed.info;
        const tokenAmount = parsedInfo.tokenAmount;
        
        if (tokenAmount.uiAmount === 0) continue;

        const rowData = {
          Symbol: symbol,
          'Token Mint': req.tokenMint,
          Amount: tokenAmount.uiAmountString,
          'Wallet Name': req.wallet.name,
          'Wallet Address': req.wallet.address,
          Timestamp: timestamp
        };

        if (sheetCache.has(uniqueKey)) {
          const existingRow = sheetCache.get(uniqueKey);
          // Diff check: only update if Amount changed
          if (existingRow.Amount !== rowData.Amount) {
            existingRow.assign({ Amount: rowData.Amount, Timestamp: rowData.Timestamp });
            rowsToUpdate.push(existingRow);
          }
        } else {
          console.log(`[DEBUG] Adding new row: ${JSON.stringify(rowData)}`);
          rowsToAdd.push(rowData);
          sheetCache.set(uniqueKey, rowData);
        }
      }
    } catch (err) {
      for (const req of chunk) {
        const symbol = req.tokenSymbol || tokenMap.get(req.tokenMint) || 'Unknown';
        const uniqueKey = `${req.wallet.address}_${req.tokenMint}`;
        const rowData = {
          Symbol: symbol,
          'Token Mint': req.tokenMint,
          Amount: err.message,
          'Wallet Name': req.wallet.name,
          'Wallet Address': req.wallet.address,
          Timestamp: timestamp
        };

        if (sheetCache.has(uniqueKey)) {
          const existingRow = sheetCache.get(uniqueKey);
          // Diff check: only update if Amount changed
          if (existingRow.Amount !== rowData.Amount) {
            existingRow.assign({ Amount: rowData.Amount, Timestamp: rowData.Timestamp });
            rowsToUpdate.push(existingRow);
          }
        } else {
          rowsToAdd.push(rowData);
          sheetCache.set(uniqueKey, rowData);
        }
      }

      failList.push({
        error: `Chunk ${i + 1} failed: ${err.message}`
      });
    }

    // Flush buffers every BUFFER_FLUSH_INTERVAL chunks
    if ((i + 1) % BUFFER_FLUSH_INTERVAL === 0 || i === chunks.length - 1) {
      await flushBuffers();
    }

    if (i < chunks.length - 1) {
      await delay(DELAY_BETWEEN_CHUNKS_MS);
    }
  }

  const endTime = Date.now();
  const execTimeSeconds = ((endTime - startTime) / 1000).toFixed(2) + ' seconds';
  
  const nextSyncDate = new Date(endTime + INTERVAL_MS);
  const nextSyncStr = formatTimestamp(nextSyncDate);

  const summary = generateSummary({
    round: roundCounter,
    execTime: execTimeSeconds,
    nextSyncStr,
    totalWallets: WALLETS.length,
    totalTokensChecked,
    rowsUpdated,
    rowsAdded,
    fails: failList
  });
  
  console.log(summary);
}

async function main() {
  if (!SPREADSHEET_ID || !process.env.GOOGLE_SERVICE_ACCOUNT_EMAIL || !process.env.GOOGLE_PRIVATE_KEY) {
    console.error('[FATAL] Missing Google Sheets configuration in .env!');
    process.exit(1);
  }

  await fetchTokenList();
  await runTracker();

  setInterval(runTracker, INTERVAL_MS);
}

main().catch(err => {
  console.error('[FATAL] Unhandled Exception:', err);
});