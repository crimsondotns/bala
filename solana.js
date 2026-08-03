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

// 1. CONFIGURATION
const GOOGLE_SERVICE_ACCOUNT_EMAIL = process.env.GOOGLE_SERVICE_ACCOUNT_EMAIL;
const GOOGLE_PRIVATE_KEY = process.env.GOOGLE_PRIVATE_KEY ? process.env.GOOGLE_PRIVATE_KEY.replace(/\\n/g, '\n') : '';
const SPREADSHEET_ID = process.env.SPREADSHEET_ID;

const SHEET_TAB_NAME = 'Solana_Tracker';
const SUBSCRIPTION_SPL_TAB = 'SUBSCRIPTION SPL';
const SUBSCRIPTION_WALLET_TAB = 'SUBSCRIPTION WALLET';
const SHEET_HEADERS = ['Symbol', 'Network', 'Token Mint', 'Amount', 'Wallet Name', 'Wallet Address', 'Timestamp'];

const RPC_ENDPOINTS = [
  'https://api.mainnet-beta.solana.com',
  'https://solana-rpc.publicnode.com',
  'https://rpc.solflare.com',
  'https://api.orca.so'
];

let currentRPCIndex = 0;

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
  console.log(`\n${c.yellow}Switching RPC to: ${RPC_ENDPOINTS[currentRPCIndex]}${c.reset}`);
  return new Connection(RPC_ENDPOINTS[currentRPCIndex], 'confirmed');
}

// Helper query ทั้ง Standard SPL และ Token-2022
async function getWalletTokenAccountsWithRetry(connectionState, walletPubKey, programId, maxRetries = 3) {
  let conn = connectionState.conn;

  for (let attempt = 0; attempt < maxRetries; attempt++) {
    try {
      return await conn.getParsedTokenAccountsByOwner(walletPubKey, { programId });
    } catch (e) {
      const isBlocked = e.message && (
        e.message.includes('429') || 
        e.message.includes('403') || 
        e.message.includes('blocked') || 
        e.message.includes('Too Many Requests')
      );

      if (isBlocked) {
        conn = switchRPC();
        connectionState.conn = conn;
        await delay(2000);
      } else if (attempt < maxRetries - 1) {
        await delay(1000 * (attempt + 1));
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
  }

  if (!RPC_ENDPOINT) {
    RPC_ENDPOINT = RPC_ENDPOINTS[0];
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
              // Invalid address, skip
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

  // 4. Load Tokens & Map สำหรับค้นหาแบบ O(1)
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
              // Invalid mint, skip
            }
          }
        }
      }
    } catch (err) {
      console.log(`${c.red}Warning: Failed to read from ${SUBSCRIPTION_SPL_TAB}: ${err.message}${c.reset}`);
    }
  }

  if (tokenMap.size === 0) {
    console.log(`${c.red}No valid tokens found to track. Exiting.${c.reset}`);
    process.exit(0);
  }
  console.log(`${c.gray}Loaded ${tokenMap.size} token mint(s) to track${c.reset}`);

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

  // 6. Processing Wallets
  console.log(`\n${c.cyan}${c.bright}>> Network: SOLANA (Scanning SPL + Token-2022)${c.reset}`);
  console.log(`${c.gray}Initial RPC Endpoint: ${RPC_ENDPOINT}${c.reset}`);
  
  let totalAdded = 0;
  const errors = [];
  const rowsToAdd = [];

  const connectionState = { conn: new Connection(RPC_ENDPOINT, 'confirmed') };

  for (let wIdx = 0; wIdx < WALLETS.length; wIdx++) {
    const wallet = WALLETS[wIdx];
    const walletInfo = `${c.gray}[${String(wIdx + 1).padStart(3, '0')}/${String(WALLETS.length).padStart(3, '0')}]${c.reset}`;
    process.stdout.write(`   ${walletInfo} Scanning ${wallet.name.padEnd(20, ' ')} `);

    let walletAdded = 0;

    try {
      const walletPubKey = new PublicKey(wallet.address);

      // ดึงข้อมูลเหรียญทั้ง Standard SPL (TOKEN_PROGRAM_ID) และ Token-2022 (TOKEN_2022_PROGRAM_ID)
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

          // เช็คว่าเหรียญตรงกับที่เราต้องการติดตามหรือไม่ และยอดต้องมากกว่า 0
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
          // Parse error บนบางบัญชี ให้ข้ามไป
        }
      }

      console.log(`${walletAdded > 0 ? c.green : c.gray}+ Found: ${String(walletAdded).padStart(2, '0')} tokens${c.reset}`);

    } catch (err) {
      errors.push(`Wallet ${wallet.name} (${wallet.address}) failed: ${err.message}`);
      console.log(`${c.red}Failed: ${err.message}${c.reset}`);
    }

    // Delay เล็กน้อยระหว่างกระเป๋า
    await delay(150);
  }

  // 7. Clear old data & Write new data to Google Sheets
  if (rowsToAdd.length > 0) {
    try {
      await sheet.clear();
      await sheet.setHeaderRow(SHEET_HEADERS);
      console.log(`\n${c.gray}Cleared existing data from ${SHEET_TAB_NAME}${c.reset}`);
      
      await sheet.addRows(rowsToAdd);
      console.log(`${c.green}Successfully written ${rowsToAdd.length} row(s) to Google Sheets!${c.reset}`);
    } catch (err) {
      errors.push(`Failed to write rows to sheet: ${err.message}`);
      console.error(`${c.red}Error writing to Google Sheets: ${err.message}${c.reset}`);
    }
  } else {
    console.log(`\n${c.yellow}No matching token balances (> 0) found to write to sheet.${c.reset}`);
  }

  const execSecs = ((Date.now() - startTime) / 1000).toFixed(2);

  console.log(`\n${c.gray}--------------------------------------------------${c.reset}`);
  console.log(`${c.cyan}${c.bright}PROCESS SUMMARY: SOLANA WORKER (Token-2022 Native)${c.reset}`);
  console.log(`${c.gray}--------------------------------------------------${c.reset}`);
  console.log(`Execution Time: ${execSecs} seconds`);
  console.log(`${c.green}Total Added:    ${totalAdded}${c.reset}`);
  
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
