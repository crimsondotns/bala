import { JsonRpcProvider, Contract, Interface, formatUnits, isAddress } from 'ethers';
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
const SHEET_TAB_NAME = 'EVM_Tracker';
const SUBSCRIPTION_WALLET_TAB = 'SUBSCRIPTION WALLET';
const SUBSCRIPTION_ERC20_TAB = 'SUBSCRIPTION ERC20';
const SHEET_HEADERS = ['Tokens Name', 'Network', 'Tokens Address', 'Amount', 'Wallet Name', 'Wallet Address', 'Timestamp'];
const MULTICALL3_ADDRESS = '0xcA11bde05977b3631167028862bE2a173976CA11';
const MULTICALL_BATCH_SIZE = 500;

// Multicall3 ABI
const MULTICALL3_ABI = [
  {
    "inputs": [
      {
        "components": [
          { "name": "target", "type": "address" },
          { "name": "allowFailure", "type": "bool" },
          { "name": "callData", "type": "bytes" }
        ],
        "name": "calls",
        "type": "tuple[]"
      }
    ],
    "name": "aggregate3",
    "outputs": [
      {
        "components": [
          { "name": "success", "type": "bool" },
          { "name": "returnData", "type": "bytes" }
        ],
        "name": "returnData",
        "type": "tuple[]"
      }
    ],
    "stateMutability": "payable",
    "type": "function"
  }
];

const ERC20_INTERFACE = new Interface([
  "function symbol() view returns (string)",
  "function decimals() view returns (uint8)",
  "function balanceOf(address owner) view returns (uint256)"
]);

// Utility
function chunkArray(array, size) {
  const chunked = [];
  for (let i = 0; i < array.length; i += size) {
    chunked.push(array.slice(i, i + size));
  }
  return chunked;
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

  // Connect Google Sheets
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

  // Load RPC_URLS from 'nodes' tab
  const nodesSheet = doc.sheetsByTitle['nodes'];
  if (!nodesSheet) {
    console.error("Fatal Error: Sheet 'nodes' not found.");
    process.exit(1);
  }

  let RPC_URLS = {};
  try {
    const maxRows = nodesSheet.rowCount;
    if (maxRows >= 2) {
      await nodesSheet.loadCells(`A1:B${maxRows}`);
      for (let r = 1; r < maxRows; r++) {
        const netCell = nodesSheet.getCell(r, 0);
        const urlCell = nodesSheet.getCell(r, 1);
        if (netCell && netCell.value && urlCell && urlCell.value) {
          const networkName = String(netCell.value).trim();
          const rpcUrl = String(urlCell.value).trim();
          if (networkName.toLowerCase() !== 'solana') {
             // Use original case, but capitalize first letter if needed
             let name = networkName;
             if (name.toLowerCase() === 'bsc') name = 'Bsc';
             else name = name.charAt(0).toUpperCase() + name.slice(1);
             RPC_URLS[name] = rpcUrl;
          }
        }
      }
    }
  } catch (err) {
    console.error("Fatal Error: Failed to read from 'nodes' tab.", err.message);
    process.exit(1);
  }

  if (Object.keys(RPC_URLS).length === 0) {
    console.error("Fatal Error: No EVM RPCs found in 'nodes' tab.");
    process.exit(1);
  }

  // Load Wallets from SUBSCRIPTION WALLET (Col E & F)
  const walletSheet = doc.sheetsByTitle[SUBSCRIPTION_WALLET_TAB];
  if (!walletSheet) {
    console.error(`Fatal Error: Sheet '${SUBSCRIPTION_WALLET_TAB}' not found.`);
    process.exit(1);
  }

  let WALLETS = [];
  try {
    const maxRows = walletSheet.rowCount;
    if (maxRows >= 3) {
      await walletSheet.loadCells(`E1:F${maxRows}`);
      for (let r = 2; r < maxRows; r++) { // Row 3 is index 2
        const nameCell = walletSheet.getCell(r, 4); // Column E
        const addrCell = walletSheet.getCell(r, 5); // Column F
        
        const addrVal = (addrCell && addrCell.value && typeof addrCell.value === 'string') ? addrCell.value.trim() : '';
        const nameVal = (nameCell && nameCell.value) ? String(nameCell.value).trim() : 'Unknown Wallet';

        if (addrVal && isAddress(addrVal)) {
          WALLETS.push({ name: nameVal, address: addrVal });
        }
      }
    }
  } catch (err) {
    console.error(`Fatal Error: Failed to read from ${SUBSCRIPTION_WALLET_TAB}.`, err.message);
    process.exit(1);
  }

  if (WALLETS.length === 0) {
    console.error('Fatal Error: Wallets array is empty after validation from Google Sheets.');
    process.exit(1);
  }

  // Load Tokens from SUBSCRIPTION ERC20 (Col C)
  const tokenSheet = doc.sheetsByTitle[SUBSCRIPTION_ERC20_TAB];
  if (!tokenSheet) {
    console.error(`Fatal Error: Sheet '${SUBSCRIPTION_ERC20_TAB}' not found.`);
    process.exit(1);
  }

  let TOKENS = [];
  try {
    const maxRows = tokenSheet.rowCount;
    if (maxRows >= 3) {
      await tokenSheet.loadCells(`C1:C${maxRows}`);
      for (let r = 2; r < maxRows; r++) { // Row 3 is index 2
        const addrCell = tokenSheet.getCell(r, 2); // Column C
        
        const addrVal = (addrCell && addrCell.value && typeof addrCell.value === 'string') ? addrCell.value.trim() : '';

        if (addrVal && isAddress(addrVal)) {
          TOKENS.push(addrVal);
        }
      }
    }
  } catch (err) {
    console.error(`Fatal Error: Failed to read from ${SUBSCRIPTION_ERC20_TAB}.`, err.message);
    process.exit(1);
  }

  if (TOKENS.length === 0) {
    console.error('Fatal Error: Tokens array is empty after validation from Google Sheets.');
    process.exit(1);
  }

  // Load existing data for Cache-Driven Upsert
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
    const net = rawData[1];
    const tAddr = rawData[2];
    const amt = rawData[3];
    const wAddr = rawData[5];
    if (wAddr && net && tAddr) {
      const uniqueKey = `${wAddr}_${net}_${tAddr}`.toLowerCase();
      cacheMap.set(uniqueKey, {
        rowIdx: row.rowNumber - 1, // 0-based index for getCell
        amount: amt
      });
    }
  });

  // Prepare sheet cells for batch updates
  try {
    const maxRowIndex = sheet.rowCount > 0 ? sheet.rowCount : 1;
    await sheet.loadCells(`A1:G${maxRowIndex}`);
  } catch (err) {
    console.error('Fatal Error: Failed to load cells for batch updates', err.message);
    process.exit(1);
  }

  let totalAdded = 0;
  let totalUpdated = 0;
  let totalIdle = 0;
  let totalEmpty = 0;
  const errors = [];
  const rowsToAdd = [];

  const networks = Object.keys(RPC_URLS);

  for (const network of networks) {
    console.log(`\n${c.cyan}${c.bright}>> Network: ${network}${c.reset}`);
    const rpcUrl = RPC_URLS[network];
    const provider = new JsonRpcProvider(rpcUrl, undefined, { staticNetwork: true });
    const multicall = new Contract(MULTICALL3_ADDRESS, MULTICALL3_ABI, provider);

    // 1. Resolve Metadata via Multicall (No Redis)
    const networkTokens = TOKENS.map(token => ({ address: token }));
    const metaChunks = chunkArray(networkTokens, MULTICALL_BATCH_SIZE / 2); // 2 calls per token

    for (const chunk of metaChunks) {
      const metaCalls = [];
      for (const pt of chunk) {
        metaCalls.push({ target: pt.address, allowFailure: true, callData: ERC20_INTERFACE.encodeFunctionData("symbol") });
        metaCalls.push({ target: pt.address, allowFailure: true, callData: ERC20_INTERFACE.encodeFunctionData("decimals") });
      }
      try {
        const results = await multicall.aggregate3.staticCall(metaCalls);
        for (let i = 0; i < chunk.length; i++) {
          const pt = chunk[i];
          const symRes = results[i * 2];
          const decRes = results[i * 2 + 1];

          if (symRes.success && decRes.success) {
            try {
              pt.symbol = ERC20_INTERFACE.decodeFunctionResult("symbol", symRes.returnData)[0];
              pt.decimals = Number(ERC20_INTERFACE.decodeFunctionResult("decimals", decRes.returnData)[0]);
              pt.success = true;
            } catch (e) {
              pt.success = false;
            }
          } else {
            pt.success = false;
          }
        }
      } catch (err) {
        const errMsg = err.shortMessage || err.message.split(' (')[0];
        errors.push(`[${network}] Metadata Multicall failed: ${errMsg}`);
        for (const pt of chunk) { pt.success = false; }
      }
    }

    // 2. Fetch Balances
    const validTokens = networkTokens.filter(t => t.success);
    if (validTokens.length === 0) {
      console.log(`No valid tokens found for ${network}.`);
      continue;
    }

    const balanceCalls = [];
    const callMappings = [];

    for (const token of validTokens) {
      for (const wallet of WALLETS) {
        balanceCalls.push({
          target: token.address,
          allowFailure: true,
          callData: ERC20_INTERFACE.encodeFunctionData("balanceOf", [wallet.address])
        });
        callMappings.push({ token, wallet });
      }
    }

    const balChunks = chunkArray(balanceCalls, MULTICALL_BATCH_SIZE);
    const mapChunks = chunkArray(callMappings, MULTICALL_BATCH_SIZE);

    for (let c = 0; c < balChunks.length; c++) {
      const chunk = balChunks[c];
      const mapping = mapChunks[c];
      let added = 0, updated = 0, idle = 0, empty = 0;

      process.stdout.write(`${c.gray}[${String(c + 1).padStart(2, '0')}/${String(balChunks.length).padStart(2, '0')}]${c.reset} Processing ${chunk.length} calls... `);

      try {
        const results = await multicall.aggregate3.staticCall(chunk);
        
        for (let k = 0; k < results.length; k++) {
          const res = results[k];
          const m = mapping[k];

          if (res.success) {
            try {
              const balanceWei = ERC20_INTERFACE.decodeFunctionResult("balanceOf", res.returnData)[0];
              const balanceStr = formatUnits(balanceWei, m.token.decimals);

              if (parseFloat(balanceStr) > 0) {
                const uniqueKey = `${m.wallet.address}_${network}_${m.token.address}`.toLowerCase();
                const nowStr = formatDate(new Date());

                if (cacheMap.has(uniqueKey)) {
                  const cached = cacheMap.get(uniqueKey);
                  if (cached.amount !== balanceStr) {
                    // Update
                    const cellAmount = sheet.getCell(cached.rowIdx, 3);
                    const cellTimestamp = sheet.getCell(cached.rowIdx, 6);
                    cellAmount.value = balanceStr;
                    cellTimestamp.value = nowStr;
                    updated++;
                    totalUpdated++;
                    cached.amount = balanceStr; // update local cache
                  } else {
                    idle++;
                    totalIdle++;
                  }
                } else {
                  // Add
                  rowsToAdd.push({
                    'Tokens Name': m.token.symbol,
                    'Network': network,
                    'Tokens Address': m.token.address,
                    'Amount': balanceStr,
                    'Wallet Name': m.wallet.name,
                    'Wallet Address': m.wallet.address,
                    'Timestamp': nowStr
                  });
                  added++;
                  totalAdded++;
                  cacheMap.set(uniqueKey, { rowIdx: -1, amount: balanceStr }); 
                }
              } else {
                empty++;
                totalEmpty++;
              }
            } catch (e) {
              // Ignore decode fail for successful call
            }
          }
        }
        
        const addedText = added > 0 ? `${c.green}+ Added: ${added}${c.reset}` : `${c.gray}+ Added: ${added}${c.reset}`;
        const updatedText = updated > 0 ? `${c.yellow}~ Updated: ${updated}${c.reset}` : `${c.gray}~ Updated: ${updated}${c.reset}`;
        console.log(`${addedText} ${c.gray}|${c.reset} ${updatedText} ${c.gray}| . Idle: ${idle} | 0 Empty: ${empty}${c.reset}`);
      } catch (err) {
        console.log(`${c.red}FAILED!${c.reset}`);
        const errMsg = err.shortMessage || err.message.split(' (')[0];
        errors.push(`[${network}] Batch Multicall failed: ${errMsg}`);
      }
    }
  }

  // 3. Batch Write
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



  const endTime = Date.now();
  const execSecs = ((endTime - startTime) / 1000).toFixed(2);

  console.log(`\n${c.gray}--------------------------------------------------${c.reset}`);
  console.log(`${c.cyan}${c.bright}PROCESS SUMMARY: EVM WORKER${c.reset}`);
  console.log(`${c.gray}--------------------------------------------------${c.reset}`);
  console.log(`Execution Time: ${execSecs} seconds`);
  console.log(`${c.green}Total Added:    ${totalAdded}${c.reset}`);
  console.log(`${c.yellow}Total Updated:  ${totalUpdated}${c.reset}`);
  console.log(`${c.gray}Total Idle:     ${totalIdle}${c.reset}`);
  console.log(`${c.gray}Total Empty:    ${totalEmpty}${c.reset}`);
  
  if (errors.length > 0) {
    console.log(`\n${c.red}Errors encountered:${c.reset}`);
    errors.forEach(e => console.log(`${c.red}- ${e}${c.reset}`));
  }
  console.log(`${c.gray}--------------------------------------------------${c.reset}`);
  
  process.exit(0);
}

main();
