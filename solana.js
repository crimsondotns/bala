import { Connection, PublicKey } from '@solana/web3.js';
import { getAssociatedTokenAddress } from '@solana/spl-token';
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

// 1. CONFIGURATION (Strictly Environment Variables)
const GOOGLE_SERVICE_ACCOUNT_EMAIL = process.env.GOOGLE_SERVICE_ACCOUNT_EMAIL;
const GOOGLE_PRIVATE_KEY = process.env.GOOGLE_PRIVATE_KEY ? process.env.GOOGLE_PRIVATE_KEY.replace(/\\n/g, '\n') : '';
const SPREADSHEET_ID = process.env.SPREADSHEET_ID;

const SHEET_TAB_NAME = 'Solana_Tracker';
const SUBSCRIPTION_SPL_TAB = 'SUBSCRIPTION SPL';
const SUBSCRIPTION_WALLET_TAB = 'SUBSCRIPTION WALLET';
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

  if (!GOOGLE_SERVICE_ACCOUNT_EMAIL || !GOOGLE_PRIVATE_KEY || !SPREADSHEET_ID) {
    console.error('Fatal Error: Missing required environment variables.');
    process.exit(1);
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

  // Load RPC_ENDPOINT from 'nodes' tab
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
      console.log(`${c.red}Warning: Failed to read from 'nodes' tab: ${err.message}${c.reset}`);
    }
  } else {
    console.log(`${c.yellow}Warning: Sheet 'nodes' not found.${c.reset}`);
  }

  if (!RPC_ENDPOINT) {
    console.log(`${c.red}No Solana RPC found in 'nodes' tab. Exiting.${c.reset}`);
    process.exit(0);
  }

  // 3. Load Wallets from SUBSCRIPTION WALLET
  let WALLETS = [];
  const walletSheet = doc.sheetsByTitle[SUBSCRIPTION_WALLET_TAB];
  if (walletSheet) {
    try {
      const maxRows = walletSheet.rowCount;
      if (maxRows >= 3) {
        await walletSheet.loadCells(`A1:B${maxRows}`);
        for (let r = 2; r < maxRows; r++) { // Row 3 is index 2
          const nameCell = walletSheet.getCell(r, 0); // Column A
          const addrCell = walletSheet.getCell(r, 1); // Column B
          
          const addrVal = (addrCell && addrCell.value && typeof addrCell.value === 'string') ? addrCell.value.trim() : '';
          const nameVal = (nameCell && nameCell.value) ? String(nameCell.value).trim() : 'Unknown Wallet';

          if (addrVal) {
            try {
              new PublicKey(addrVal); // Validate Solana address
              WALLETS.push({ name: nameVal, address: addrVal });
            } catch (e) {
              // Invalid address, skip
            }
          }
        }
      }
    } catch (err) {
      console.log(`${c.red}Warning: Failed to read from ${SUBSCRIPTION_WALLET_TAB}: ${err.message}${c.reset}`);
    }
  } else {
    console.log(`${c.yellow}Warning: Sheet '${SUBSCRIPTION_WALLET_TAB}' not found.${c.reset}`);
  }

  if (WALLETS.length === 0) {
    console.log(`${c.red}No valid wallets found. Exiting.${c.reset}`);
    process.exit(0);
  }
  console.log(`${c.gray}Loaded ${WALLETS.length} wallet(s)${c.reset}`);

  // 4. Load Tokens from SUBSCRIPTION SPL
  const tokensToTrack = [];
  const subsSheet = doc.sheetsByTitle[SUBSCRIPTION_SPL_TAB];
  if (subsSheet) {
    try {
      const maxRows = subsSheet.rowCount;
      if (maxRows >= 2) {
        await subsSheet.loadCells(`A1:C${maxRows}`);
        for (let r = 1; r < maxRows; r++) { // Assume row 2 is index 1
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
            } catch (e) {
              // Invalid mint
            }
          }
        }
      }
    } catch (err) {
      console.log(`${c.red}Warning: Failed to read from ${SUBSCRIPTION_SPL_TAB}: ${err.message}${c.reset}`);
    }
  } else {
    console.log(`${c.yellow}Warning: Sheet '${SUBSCRIPTION_SPL_TAB}' not found.${c.reset}`);
  }

  if (tokensToTrack.length === 0) {
    console.log(`${c.red}No valid tokens found to track. Exiting.${c.reset}`);
    process.exit(0);
  }
  console.log(`${c.gray}Loaded ${tokensToTrack.length} token(s)${c.reset}`);

  // 5. Cache-Driven Upsert from Solana_Tracker
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
        rowIdx: row.rowNumber - 1, // 0-based index for getCell
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

  // 6. Build Requests
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
          tokenSymbol: token.symbol || 'Unknown'
        });
      } catch (e) { }
    }
  }

  // 7. Bulk Processing with error handling
  console.log(`\n${c.cyan}${c.bright}>> Network: SOLANA${c.reset}`);
  
  let totalAdded = 0;
  let totalUpdated = 0;
  let totalIdle = 0;
  let totalEmpty = 0;
  const errors = [];
  const rowsToAdd = [];

  let connection;
  try {
    connection = new Connection(RPC_ENDPOINT, 'confirmed');
  } catch (err) {
    console.log(`${c.red}Failed to connect to Solana RPC: ${err.message}. Exiting.${c.reset}`);
    process.exit(0);
  }

  const mainChunks = chunkArray(requests, 500);

  for (let chunkIdx = 0; chunkIdx < mainChunks.length; chunkIdx++) {
    const batch500 = mainChunks[chunkIdx];
    const batchInfo = `${c.gray}[${String(chunkIdx + 1).padStart(2, '0')}/${String(mainChunks.length).padStart(2, '0')}]${c.reset}`;
    const processInfo = `Processing ${String(batch500.length).padStart(3, ' ')} ATAs... `;
    process.stdout.write(`   ${batchInfo} ${processInfo} `);
    
    let added = 0, updated = 0, idle = 0, empty = 0;
    const subChunks = chunkArray(batch500, 10);

    for (let s = 0; s < subChunks.length; s++) {
      const subChunk = subChunks[s];
      try {
        const pubkeys = subChunk.map(req => new PublicKey(req.ataAddress));
        const response = await connection.getMultipleParsedAccounts(pubkeys);

        for (let j = 0; j < response.value.length; j++) {
          const accountInfo = response.value[j];
          const req = subChunk[j];

          if (accountInfo === null) { empty++; totalEmpty++; continue; }

          const parsedInfo = accountInfo.data.parsed.info;
          const tokenAmount = parsedInfo.tokenAmount;
          
          if (tokenAmount.uiAmount === 0) { empty++; totalEmpty++; continue; }

          const balanceStr = tokenAmount.uiAmountString;
          const balanceFloat = parseFloat(balanceStr);
          const uniqueKey = `${req.wallet.address}_${req.tokenMint}`;
          const nowStr = formatDate(new Date());

          if (cacheMap.has(uniqueKey)) {
            const cached = cacheMap.get(uniqueKey);
            if (parseFloat(cached.amount) !== balanceFloat) {
              const cellAmount = sheet.getCell(cached.rowIdx, 3);
              const cellTimestamp = sheet.getCell(cached.rowIdx, 6);
              cellAmount.value = balanceFloat;
              cellTimestamp.value = nowStr;
              updated++;
              totalUpdated++;
              cached.amount = balanceFloat;
            } else {
              idle++;
              totalIdle++;
            }
          } else {
            rowsToAdd.push({
              'Symbol': req.tokenSymbol,
              'Network': 'Solana',
              'Token Mint': req.tokenMint,
              'Amount': balanceFloat,
              'Wallet Name': req.wallet.name,
              'Wallet Address': req.wallet.address,
              'Timestamp': nowStr
            });
            added++;
            totalAdded++;
            cacheMap.set(uniqueKey, { rowIdx: -1, amount: balanceFloat });
          }
        }
      } catch (err) {
        errors.push(`Batch ${chunkIdx+1}.${s+1} failed: ${err.message}`);
        console.log(`${c.red}Batch ${chunkIdx+1}.${s+1} failed: ${err.message}${c.reset}`);
      }

      if (s < subChunks.length - 1) {
        await delay(1000);
      }
    }
    
    const addedPad = String(added).padStart(3, '0');
    const updatedPad = String(updated).padStart(3, '0');
    const idlePad = String(idle).padStart(3, '0');
    const emptyPad = String(empty).padStart(3, '0');

    const addedText = added > 0 ? `${c.green}+ Added: ${addedPad}${c.reset}` : `${c.gray}+ Added: ${addedPad}${c.reset}`;
    const updatedText = updated > 0 ? `${c.yellow}~ Updated: ${updatedPad}${c.reset}` : `${c.gray}~ Updated: ${updatedPad}${c.reset}`;
    const idleText = `${c.gray}. Idle: ${idlePad}${c.reset}`;
    const emptyText = `${c.gray}o Empty: ${emptyPad}${c.reset}`;

    console.log(`${addedText} | ${updatedText} | ${idleText} | ${emptyText}`);
  }

  // 8. Batch Write
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

  console.log(`\n${c.gray}--------------------------------------------------${c.reset}`);
  console.log(`${c.cyan}${c.bright}PROCESS SUMMARY: SOLANA WORKER${c.reset}`);
  console.log(`${c.gray}--------------------------------------------------${c.reset}`);
  console.log(`Execution Time: ${execSecs} seconds`);
  console.log(`${c.green}Total Added:    ${totalAdded}${c.reset}`);
  console.log(`${c.yellow}Total Updated:  ${totalUpdated}${c.reset}`);
  console.log(`${c.gray}Total Idle:     ${totalIdle}${c.reset}`);
  console.log(`${c.gray}Total Empty:    ${totalEmpty}${c.reset}`);
  
  if (errors.length > 0) {
    console.log(`${c.red}Total Errors:   ${errors.length}${c.reset}`);
    console.log(`\n${c.red}Errors encountered:${c.reset}`);
    errors.forEach(e => console.log(`${c.red}- ${e}${c.reset}`));
  } else {
    console.log(`${c.gray}Total Errors:   0${c.reset}`);
  }
  console.log(`${c.gray}--------------------------------------------------${c.reset}`);
  
  process.exit(0);
}

main();