import { Connection, PublicKey } from '@solana/web3.js';
import { getAssociatedTokenAddress } from '@solana/spl-token';
import { GoogleSpreadsheet } from 'google-spreadsheet';
import { JWT } from 'google-auth-library';
import dotenv from 'dotenv';
dotenv.config();

// 1. CONFIGURATION (Strictly Environment Variables)
const WALLETS_RAW = process.env.WALLETS_RAW;
const RPC_ENDPOINT = process.env.RPC_ENDPOINT || 'https://solana-rpc.publicnode.com';
const GOOGLE_SERVICE_ACCOUNT_EMAIL = process.env.GOOGLE_SERVICE_ACCOUNT_EMAIL;
const GOOGLE_PRIVATE_KEY = process.env.GOOGLE_PRIVATE_KEY ? process.env.GOOGLE_PRIVATE_KEY.replace(/\\n/g, '\n') : '';
const SPREADSHEET_ID = process.env.SPREADSHEET_ID;

const SHEET_TAB_NAME = 'Solana_Tracker';
const SUBSCRIPTION_SPL_TAB = 'SUBSCRIPTION SPL';
const SHEET_HEADERS = ['Symbol', 'Network', 'Token Mint', 'Amount', 'Wallet Name', 'Wallet Address', 'Timestamp'];

// Utility
function chunkArray(array, size) {
  const chunked = [];
  for (let i = 0; i < array.length; i += size) {
    chunked.push(array.slice(i, i + size));
  }
  return chunked;
}

function delay(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function formatDate(date) {
  const pad = (n) => String(n).padStart(2, '0');
  return `${pad(date.getMonth() + 1)}/${pad(date.getDate())}/${date.getFullYear()} ${pad(date.getHours())}:${pad(date.getMinutes())}:${pad(date.getSeconds())}`;
}

async function main() {
  const startTime = Date.now();

  if (!WALLETS_RAW || !RPC_ENDPOINT || !GOOGLE_SERVICE_ACCOUNT_EMAIL || !GOOGLE_PRIVATE_KEY || !SPREADSHEET_ID) {
    console.error('Fatal Error: Missing required environment variables.');
    process.exit(1);
  }

  let WALLETS;
  try {
    let rawWallets = WALLETS_RAW.trim();
    if (!rawWallets.startsWith('[')) {
      // Remove trailing comma if present to ensure valid array
      rawWallets = rawWallets.replace(/,\s*$/, '');
      rawWallets = `[${rawWallets}]`;
    }
    const parsedWallets = new Function('return ' + rawWallets)();
    WALLETS = [];
    for (const w of parsedWallets) {
      try {
        new PublicKey(w.address);
        WALLETS.push(w);
      } catch (e) { }
    }
  } catch (err) {
    console.error('Fatal Error: Parsing failed for WALLETS_RAW.', err.message);
    process.exit(1);
  }

  if (WALLETS.length === 0) {
    console.error('Fatal Error: Wallets array is empty after validation.');
    process.exit(1);
  }

  // 1. Jupiter API Metadata
  const tokenMap = new Map();
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
    console.error('Warning: Failed to fetch Jupiter token list:', error.message);
  }

  // 2. Google Sheets Init
  const serviceAccountAuth = new JWT({
    email: GOOGLE_SERVICE_ACCOUNT_EMAIL,
    key: GOOGLE_PRIVATE_KEY,
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
  });

  const doc = new GoogleSpreadsheet(SPREADSHEET_ID, serviceAccountAuth);
  try {
    await doc.loadInfo();
  } catch (err) {
    console.error('Fatal Error: Failed to connect to Google Sheets', err.message);
    process.exit(1);
  }

  // 3. Load Tokens from SUBSCRIPTION SPL
  const subsSheet = doc.sheetsByTitle[SUBSCRIPTION_SPL_TAB];
  if (!subsSheet) {
    console.error(`Fatal Error: Sheet '${SUBSCRIPTION_SPL_TAB}' not found.`);
    process.exit(1);
  }

  const tokensToTrack = [];
  try {
    const maxRows = subsSheet.rowCount;
    if (maxRows >= 2) {
      await subsSheet.loadCells(`A1:C${maxRows}`);
      for (let r = 1; r < maxRows; r++) { // Assume row 1 is header
        const symCell = subsSheet.getCell(r, 0); // Column A
        const mintCell = subsSheet.getCell(r, 2); // Column C

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
        
        const symVal = symCell && symCell.value ? String(symCell.value).trim() : '';
        for (const m of mints) {
          try {
            new PublicKey(m);
            tokensToTrack.push({ symbol: symVal, mint: m });
            if (symVal && !tokenMap.has(m)) tokenMap.set(m, symVal);
          } catch (e) {
            // Invalid mint
          }
        }
      }
    }
  } catch (err) {
    console.error(`Fatal Error: Failed to read from ${SUBSCRIPTION_SPL_TAB}.`, err.message);
    process.exit(1);
  }

  if (tokensToTrack.length === 0) {
    console.error(`Fatal Error: No valid tokens found to track.`);
    process.exit(1);
  }

  // 4. Cache-Driven Upsert from Solana_Tracker
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

  let existingRows;
  try {
    existingRows = await sheet.getRows();
  } catch (err) {
    console.error('Fatal Error: Failed to load rows from Google Sheets', err.message);
    process.exit(1);
  }

  const cacheMap = new Map();
  existingRows.forEach(row => {
    const rawData = row._rawData || [];
    const tMint = rawData[2];
    const amt = rawData[3];
    const wAddr = rawData[5];
    if (wAddr && tMint) {
      const uniqueKey = `${wAddr}_${tMint}`;
      cacheMap.set(uniqueKey, {
        rowIdx: row.rowIndex - 1, // 0-based index for getCell
        amount: amt
      });
    }
  });

  try {
    const maxRowIndex = sheet.rowCount > 0 ? sheet.rowCount : 1;
    await sheet.loadCells(`A1:G${maxRowIndex}`);
  } catch (err) {
    console.error('Fatal Error: Failed to load cells for batch updates', err.message);
    process.exit(1);
  }

  // 5. Build Requests
  const requests = [];
  for (const wallet of WALLETS) {
    for (const token of tokensToTrack) {
      try {
        const ata = await getAssociatedTokenAddress(
          new PublicKey(token.mint),
          new PublicKey(wallet.address)
        );
        requests.push({
          ataAddress: ata.toString(),
          wallet,
          tokenMint: token.mint,
          tokenSymbol: token.symbol || tokenMap.get(token.mint) || 'Unknown'
        });
      } catch (e) { }
    }
  }

  // 6. Bulk Processing
  console.log(`\n>> Network: SOLANA`);
  
  let totalAdded = 0;
  let totalUpdated = 0;
  let totalIdle = 0;
  const errors = [];
  const rowsToAdd = [];

  const connection = new Connection(RPC_ENDPOINT, 'confirmed');
  const mainChunks = chunkArray(requests, 500);

  for (let c = 0; c < mainChunks.length; c++) {
    const batch500 = mainChunks[c];
    process.stdout.write(`[${String(c + 1).padStart(2, '0')}/${String(mainChunks.length).padStart(2, '0')}] Processing ${batch500.length} ATAs... `);
    
    let added = 0, updated = 0, idle = 0;
    const subChunks = chunkArray(batch500, 100);

    for (let s = 0; s < subChunks.length; s++) {
      const subChunk = subChunks[s];
      try {
        const pubkeys = subChunk.map(req => new PublicKey(req.ataAddress));
        const response = await connection.getMultipleParsedAccounts(pubkeys);

        for (let j = 0; j < response.value.length; j++) {
          const accountInfo = response.value[j];
          const req = subChunk[j];

          if (accountInfo === null) continue;

          const parsedInfo = accountInfo.data.parsed.info;
          const tokenAmount = parsedInfo.tokenAmount;
          
          if (tokenAmount.uiAmount === 0) continue;

          const balanceStr = tokenAmount.uiAmountString;
          const uniqueKey = `${req.wallet.address}_${req.tokenMint}`;
          const nowStr = formatDate(new Date());

          if (cacheMap.has(uniqueKey)) {
            const cached = cacheMap.get(uniqueKey);
            if (cached.amount !== balanceStr) {
              const cellAmount = sheet.getCell(cached.rowIdx, 3);
              const cellTimestamp = sheet.getCell(cached.rowIdx, 6);
              cellAmount.value = balanceStr;
              cellTimestamp.value = nowStr;
              updated++;
              totalUpdated++;
              cached.amount = balanceStr;
            } else {
              idle++;
              totalIdle++;
            }
          } else {
            rowsToAdd.push({
              'Symbol': req.tokenSymbol,
              'Network': 'Solana',
              'Token Mint': req.tokenMint,
              'Amount': balanceStr,
              'Wallet Name': req.wallet.name,
              'Wallet Address': req.wallet.address,
              'Timestamp': nowStr
            });
            added++;
            totalAdded++;
            cacheMap.set(uniqueKey, { rowIdx: -1, amount: balanceStr });
          }
        }
      } catch (err) {
        errors.push(`Batch ${c+1}.${s+1} failed: ${err.message}`);
      }

      if (s < subChunks.length - 1) {
        await delay(1000);
      }
    }
    
    console.log(`+ Added: ${added} | ~ Updated: ${updated} | . Idle: ${idle}`);
  }

  // 7. Batch Write
  if (totalUpdated > 0) {
    try {
      await sheet.saveUpdatedCells();
    } catch (err) {
      errors.push(`Failed to save updated cells: ${err.message}`);
    }
  }

  if (rowsToAdd.length > 0) {
    const newChunks = chunkArray(rowsToAdd, 2000);
    for (const rChunk of newChunks) {
      try {
        await sheet.addRows(rChunk);
      } catch (err) {
        errors.push(`Failed to add new rows: ${err.message}`);
      }
    }
  }

  const execSecs = ((Date.now() - startTime) / 1000).toFixed(2);

  console.log(`\n--------------------------------------------------`);
  console.log(`PROCESS SUMMARY: SOLANA WORKER`);
  console.log(`--------------------------------------------------`);
  console.log(`Execution Time: ${execSecs} seconds`);
  console.log(`Total Added:    ${totalAdded}`);
  console.log(`Total Updated:  ${totalUpdated}`);
  console.log(`Total Idle:     ${totalIdle}`);
  
  if (errors.length > 0) {
    console.log(`Total Errors:   ${errors.length}`);
    console.log(`\nErrors encountered:`);
    errors.forEach(e => console.log(`- ${e}`));
  } else {
    console.log(`Total Errors:   0`);
  }
  console.log(`--------------------------------------------------`);
  
  if (errors.length > 0) {
    // If fatal error, it should exit(1). But for partial RPC errors, process.exit(0) is acceptable to not fail the pipeline entirely.
    // GitHub actions might want to know if there's any error, but the instruction says process.exit(0) on success or process.exit(1) on fatal error.
  }
  process.exit(0);
}

main();