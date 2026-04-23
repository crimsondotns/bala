import { JsonRpcProvider, Contract, Interface, formatUnits, isAddress } from 'ethers';
import { GoogleSpreadsheet } from 'google-spreadsheet';
import { JWT } from 'google-auth-library';
import { createClient } from 'redis';
import dotenv from 'dotenv';
dotenv.config();

// 1. CONFIGURATION (Strictly Environment Variables)
const RPC_CONFIG_RAW = process.env.RPC_CONFIG;
const GOOGLE_SERVICE_ACCOUNT_EMAIL = process.env.GOOGLE_SERVICE_ACCOUNT_EMAIL;
const GOOGLE_PRIVATE_KEY = process.env.GOOGLE_PRIVATE_KEY ? process.env.GOOGLE_PRIVATE_KEY.replace(/\\n/g, '\n') : '';
const SPREADSHEET_ID = process.env.SPREADSHEET_ID;
const REDIS_URL = process.env.REDIS_URL;

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

  if (!RPC_CONFIG_RAW || !GOOGLE_SERVICE_ACCOUNT_EMAIL || !GOOGLE_PRIVATE_KEY || !SPREADSHEET_ID || !REDIS_URL) {
    console.error('Fatal Error: Missing required environment variables.');
    process.exit(1);
  }

  let RPC_URLS;
  try {
    RPC_URLS = JSON.parse(RPC_CONFIG_RAW);
  } catch (err) {
    console.error('Fatal Error: JSON Parsing failed for RPC_CONFIG.', err.message);
    process.exit(1);
  }

  // Connect Redis
  const redis = createClient({ url: REDIS_URL });
  redis.on('error', (err) => {
    console.error('Fatal Error: Redis Client Error', err);
    process.exit(1);
  });
  
  try {
    await redis.connect();
  } catch (err) {
    console.error('Fatal Error: Failed to connect to Redis', err.message);
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
  const errors = [];
  const rowsToAdd = [];

  const networks = Object.keys(RPC_URLS);

  for (const network of networks) {
    console.log(`\n>> Network: ${network}`);
    const rpcUrl = RPC_URLS[network];
    const provider = new JsonRpcProvider(rpcUrl, undefined, { staticNetwork: true });
    const multicall = new Contract(MULTICALL3_ADDRESS, MULTICALL3_ABI, provider);

    // 1. Resolve Metadata
    const networkTokens = [];
    for (const token of TOKENS) {
      const redisKey = `meta:${network}:${token}`.toLowerCase();
      let cached;
      try {
        cached = await redis.get(redisKey);
      } catch (err) {
        // Ignored redis read error
      }
      
      if (cached) {
        networkTokens.push({ address: token, ...JSON.parse(cached) });
      } else {
        networkTokens.push({ address: token, pending: true });
      }
    }

    const pendingTokens = networkTokens.filter(t => t.pending);
    if (pendingTokens.length > 0) {
      const metaChunks = chunkArray(pendingTokens, MULTICALL_BATCH_SIZE / 2); // 2 calls per token
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
                const symbol = ERC20_INTERFACE.decodeFunctionResult("symbol", symRes.returnData)[0];
                const decimals = Number(ERC20_INTERFACE.decodeFunctionResult("decimals", decRes.returnData)[0]);
                const meta = { success: true, symbol, decimals };
                pt.success = true;
                pt.symbol = symbol;
                pt.decimals = decimals;
                await redis.set(`meta:${network}:${pt.address}`.toLowerCase(), JSON.stringify(meta));
              } catch (e) {
                const meta = { success: false };
                pt.success = false;
                await redis.set(`meta:${network}:${pt.address}`.toLowerCase(), JSON.stringify(meta));
              }
            } else {
              const meta = { success: false };
              pt.success = false;
              await redis.set(`meta:${network}:${pt.address}`.toLowerCase(), JSON.stringify(meta));
            }
          }
        } catch (err) {
          errors.push(`[${network}] Metadata Multicall failed: ${err.message}`);
          for (const pt of chunk) { pt.success = false; }
        }
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
      let added = 0, updated = 0, idle = 0;

      process.stdout.write(`[${String(c + 1).padStart(2, '0')}/${String(balChunks.length).padStart(2, '0')}] Processing ${chunk.length} calls... `);

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
              }
            } catch (e) {
              // Ignore decode fail for successful call
            }
          }
        }
        console.log(`+ Added: ${added} | ~ Updated: ${updated} | . Idle: ${idle}`);
      } catch (err) {
        console.log(`FAILED!`);
        errors.push(`[${network}] Batch Multicall failed: ${err.message}`);
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

  await redis.quit();

  const endTime = Date.now();
  const execSecs = ((endTime - startTime) / 1000).toFixed(2);

  console.log(`\n--------------------------------------------------`);
  console.log(`PROCESS SUMMARY: EVM WORKER`);
  console.log(`--------------------------------------------------`);
  console.log(`Execution Time: ${execSecs} seconds`);
  console.log(`Total Added:    ${totalAdded}`);
  console.log(`Total Updated:  ${totalUpdated}`);
  console.log(`Total Idle:     ${totalIdle}`);
  
  if (errors.length > 0) {
    console.log(`\nErrors encountered:`);
    errors.forEach(e => console.log(`- ${e}`));
  }
  console.log(`--------------------------------------------------`);
  
  process.exit(0);
}

main();
