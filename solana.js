import { Connection, PublicKey } from '@solana/web3.js';
import { getAssociatedTokenAddressSync } from '@solana/spl-token';
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

// 1. CONFIGURATION
const GOOGLE_SERVICE_ACCOUNT_EMAIL = process.env.GOOGLE_SERVICE_ACCOUNT_EMAIL;
const GOOGLE_PRIVATE_KEY = process.env.GOOGLE_PRIVATE_KEY ? process.env.GOOGLE_PRIVATE_KEY.replace(/\\n/g, '\n') : '';
const SPREADSHEET_ID = process.env.SPREADSHEET_ID;

const SHEET_TAB_NAME = 'Solana_Tracker';
const SUBSCRIPTION_SPL_TAB = 'SUBSCRIPTION SPL';
const SUBSCRIPTION_WALLET_TAB = 'SUBSCRIPTION WALLET';
const SHEET_HEADERS = ['Symbol', 'Network', 'Token Mint', 'Amount', 'Wallet Name', 'Wallet Address', 'Timestamp'];

// RPC Endpoints สำรองกรณี Public Endpoint ติด Rate Limit
const RPC_ENDPOINTS = [
  'https://api.mainnet-beta.solana.com',
  'https://solana-rpc.publicnode.com',
  'https://rpc.solflare.com',
  'https://api.orca.so'
];

let currentRPCIndex = 0;

// Utility Functions
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
  console.log(`${c.yellow}Switching RPC to: ${RPC_ENDPOINTS[currentRPCIndex]}${c.reset}`);
  return new Connection(RPC_ENDPOINTS[currentRPCIndex], 'confirmed');
}

// Fetch Accounts With Automatic Retry & RPC Fallback
async function getMultipleParsedAccountsWithRetry(connectionState, pubkeys, maxRetries = 3) {
  let conn = connectionState.conn;

  for (let attempt = 0; attempt < maxRetries; attempt++) {
    try {
      return await conn.getMultipleParsedAccounts(pubkeys);
    } catch (e) {
      console.log(`\n${c.yellow}Attempt ${attempt + 1}/${maxRetries} failed: ${e.message}${c.reset}`);
      
      if (e.message && (e.message.includes('429') || e.message.includes('Too Many Requests'))) {
        conn = switchRPC();
        connectionState.conn = conn; // อัปเดต Reference
        await delay(3000);
      } else if (attempt < maxRetries - 1) {
        const waitTime = 2000 * Math.pow(1.5, attempt);
        await delay(waitTime);
      } else {
        throw e;
      }
    }
  }
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

  // Load RPC_ENDPOINT from 'nodes' tab if available
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
  }

  if (!RPC_ENDPOINT) {
    RPC_ENDPOINT = RPC_ENDPOINTS[0];
    console.log(`${c.yellow}No Solana RPC found in 'nodes' tab. Using default: ${RPC_ENDPOINT}${c.reset}`);
  }

  // 3. Load Wallets
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
              // Address ไม่ถูกต้อง ให้ข้ามไป
            }
          }
        }
      }
    } catch (err) {
      console.log(`${c.red}Warning: Failed to read from ${SUBSCRIPTION_WALLET_TAB}: ${err.message}${c.reset}`);
    }
  }

  if (WALLETS.length === 0) {
    console.log(`${c.red}No valid wallets found. Exiting.${c.reset}`);
    process.exit(0);
  }
  console.log(`${c.gray}Loaded ${WALLETS.length} wallet(s)${c.reset}`);

  // 4. Load Tokens
  const tokensToTrack = [];
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
          
          const symVal = symCell && symCell.value ? String(symCell.value).trim() : '';
          for (const m of mints) {
            try {
              new PublicKey(m);
              tokensToTrack.push({ symbol: symVal, mint: m });
            } catch (e) {
              // Mint ไม่ถูกต้อง ให้ข้ามไป
            }
          }
        }
      }
    } catch (err) {
      console.log(`${c.red}Warning: Failed to read from ${SUBSCRIPTION_SPL_TAB}: ${err.message}${c.reset}`);
    }
  }

  if (tokensToTrack.length === 0) {
    console.log(`${c.red}No valid tokens found to track. Exiting.${c.reset}`);
    process.exit(0);
  }
  console.log(`${c.gray}Loaded ${tokensToTrack.length} token(s)${c.reset}`);

  // 5. Setup Target Sheet
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

  // 6. Build Requests (คำนวณ ATA แบบ Sync)
  const requests = [];
  for (const wallet of WALLETS) {
    for (const token of tokensToTrack) {
      try {
        const ata = getAssociatedTokenAddressSync(
          new PublicKey(token.mint),
          new PublicKey(wallet.address)
        );
        requests.push({
          ataAddress: ata.toString(),
          wallet,
          tokenMint: token.mint,
          tokenSymbol: token.symbol || 'Unknown'
        });
      } catch (e) {
        console.log(`${c.yellow}Warning: Cannot derive ATA for ${token.symbol} and wallet ${wallet.name}${c.reset}`);
      }
    }
  }

  // 7. Processing Requests
  console.log(`\n${c.cyan}${c.bright}>> Network: SOLANA${c.reset}`);
  console.log(`${c.gray}RPC Endpoint: ${RPC_ENDPOINT}${c.reset}`);
  
  let totalAdded = 0;
  let totalEmpty = 0;
  const errors = [];
  const rowsToAdd = [];

  const connectionState = { conn: new Connection(RPC_ENDPOINT, 'confirmed') };

  // แบ่ง Batch ครั้งละ 50 ATAs (กำลังดีสำหรับ Public RPC)
  const batches = chunkArray(requests, 50);

  for (let i = 0; i < batches.length; i++) {
    const batch = batches[i];
    const batchInfo = `${c.gray}[${String(i + 1).padStart(2, '0')}/${String(batches.length).padStart(2, '0')}]${c.reset}`;
    process.stdout.write(`   ${batchInfo} Processing ${String(batch.length).padStart(3, ' ')} ATAs... `);

    let added = 0;
    let empty = 0;

    try {
      const pubkeys = batch.map(req => new PublicKey(req.ataAddress));
      const response = await getMultipleParsedAccountsWithRetry(connectionState, pubkeys, 3);

      for (let j = 0; j < response.value.length; j++) {
        const accountInfo = response.value[j];
        const req = batch[j];

        // กรณีไม่มี บัญชีบน Solana (Uninitialized ATA)
        if (!accountInfo) {
          empty++;
          totalEmpty++;
          continue;
        }

        try {
          // ตรวจสอบว่ามีโครงสร้าง Parsed Data หรือไม่
          const parsedData = accountInfo.data?.parsed;
          if (!parsedData || !parsedData.info) {
            empty++;
            totalEmpty++;
            continue;
          }

          const tokenAmount = parsedData.info.tokenAmount;
          if (!tokenAmount || tokenAmount.uiAmount === 0 || tokenAmount.uiAmount === null) {
            empty++;
            totalEmpty++;
            continue;
          }

          const balanceFloat = parseFloat(tokenAmount.uiAmountString || tokenAmount.uiAmount);
          const nowStr = formatDate(new Date());

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
        } catch (parseErr) {
          errors.push(`Parse error for ${req.ataAddress}: ${parseErr.message}`);
          empty++;
          totalEmpty++;
        }
      }
    } catch (batchErr) {
      errors.push(`Batch ${i + 1} failed: ${batchErr.message}`);
      console.log(`${c.red}Batch ${i + 1} failed: ${batchErr.message}${c.reset}`);
    }

    const addedText = added > 0 ? `${c.green}+ Added: ${String(added).padStart(3, '0')}${c.reset}` : `${c.gray}+ Added: 000${c.reset}`;
    const emptyText = `${c.gray}o Empty: ${String(empty).padStart(3, '0')}${c.reset}`;

    console.log(`${addedText} | ${emptyText}`);

    // พักระหว่าง Batch เล็กน้อยเพื่อไม่ให้โดนบล็อก Rate Limit
    if (i < batches.length - 1) {
      await delay(500);
    }
  }

  // 8. Clear old data & Write new data to Google Sheets
  if (rowsToAdd.length > 0) {
    try {
      await sheet.clear();
      await sheet.setHeaderRow(SHEET_HEADERS);
      console.log(`\n${c.gray}Cleared existing data from ${SHEET_TAB_NAME}${c.reset}`);
    } catch (err) {
      console.error('Fatal Error: Failed to clear sheet data', err.message);
      process.exit(1);
    }

    const writeChunks = chunkArray(rowsToAdd, 500);
    for (const rChunk of writeChunks) {
      try {
        await sheet.addRows(rChunk);
      } catch (err) {
        errors.push(`Failed to add new rows: ${err.message}`);
      }
    }
  } else {
    console.log(`\n${c.yellow}No non-zero balances found to write to sheet.${c.reset}`);
  }

  const execSecs = ((Date.now() - startTime) / 1000).toFixed(2);

  console.log(`\n${c.gray}--------------------------------------------------${c.reset}`);
  console.log(`${c.cyan}${c.bright}PROCESS SUMMARY: SOLANA WORKER (RPC Optimized)${c.reset}`);
  console.log(`${c.gray}--------------------------------------------------${c.reset}`);
  console.log(`Execution Time: ${execSecs} seconds`);
  console.log(`${c.green}Total Added:    ${totalAdded}${c.reset}`);
  console.log(`${c.gray}Total Empty:    ${totalEmpty}${c.reset}`);
  
  if (errors.length > 0) {
    console.log(`${c.red}Total Errors:   ${errors.length}${c.reset}`);
    console.log(`\n${c.red}Errors encountered:${c.reset}`);
    errors.slice(0, 10).forEach(e => console.log(`${c.red}- ${e}${c.reset}`));
  } else {
    console.log(`${c.gray}Total Errors:   0${c.reset}`);
  }
  console.log(`${c.gray}--------------------------------------------------${c.reset}`);
  
  process.exit(0);
}

main();
