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
  // Use Intl.DateTimeFormat for Asia/Bangkok (UTC+7) timezone
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

  // Load Wallets from SUBSCRIPTION WALLET (Col E: Name, F: Address)
  let WALLETS = [];
  const walletSheet = doc.sheetsByTitle[SUBSCRIPTION_WALLET_TAB];
  if (walletSheet) {
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
      console.log(`${c.red}Warning: Failed to read wallets from ${SUBSCRIPTION_WALLET_TAB}: ${err.message}${c.reset}`);
    }
  } else {
    console.log(`${c.yellow}Warning: Sheet '${SUBSCRIPTION_WALLET_TAB}' not found.${c.reset}`);
  }

  if (WALLETS.length === 0) {
    console.log(`${c.red}No valid wallets found. Exiting.${c.reset}`);
    process.exit(0);
  }
  console.log(`${c.gray}Loaded ${WALLETS.length} wallet(s)${c.reset}`);

  // Load Tokens from SUBSCRIPTION ERC20 (Col A: symbol, B: name, C: address)
  let TOKENS = [];
  const tokenSheet = doc.sheetsByTitle[SUBSCRIPTION_ERC20_TAB];
  if (tokenSheet) {
    try {
      const maxRows = tokenSheet.rowCount;
      if (maxRows >= 3) {
        await tokenSheet.loadCells(`A1:C${maxRows}`);
        for (let r = 2; r < maxRows; r++) { // Row 3 is index 2
          const addrCell = tokenSheet.getCell(r, 2); // Column C - Token Address
          const symCell = tokenSheet.getCell(r, 0); // Column A - Symbol (optional)
          
          const addrVal = (addrCell && addrCell.value && typeof addrCell.value === 'string') ? addrCell.value.trim() : '';
          const symVal = (symCell && symCell.value) ? String(symCell.value).trim() : '';

          if (addrVal && isAddress(addrVal)) {
            TOKENS.push({ address: addrVal, symbol: symVal });
          }
        }
      }
    } catch (err) {
      console.log(`${c.red}Warning: Failed to read tokens from ${SUBSCRIPTION_ERC20_TAB}: ${err.message}${c.reset}`);
    }
  } else {
    console.log(`${c.yellow}Warning: Sheet '${SUBSCRIPTION_ERC20_TAB}' not found.${c.reset}`);
  }

  if (TOKENS.length === 0) {
    console.log(`${c.red}No valid tokens found. Exiting.${c.reset}`);
    process.exit(0);
  }
  console.log(`${c.gray}Loaded ${TOKENS.length} token(s)${c.reset}`);

  // Prepare output sheet (data will be cleared and rewritten after fetching all balances)
  let sheet = doc.sheetsByTitle[SHEET_TAB_NAME];
  if (!sheet) {
    sheet = await doc.addSheet({ title: SHEET_TAB_NAME, headerValues: SHEET_HEADERS });
  }

  let totalFound = 0;
  let totalEmpty = 0;
  const errors = [];
  const allResults = [];

  const networks = Object.keys(RPC_URLS);

  // Process each network with error handling - skip failed networks
  for (const network of networks) {
    try {
      console.log(`\n${c.cyan}${c.bright}>> Network: ${network.toUpperCase()}${c.reset}`);
      const rpcUrl = RPC_URLS[network];
      const provider = new JsonRpcProvider(rpcUrl, undefined, { staticNetwork: true });
      const multicall = new Contract(MULTICALL3_ADDRESS, MULTICALL3_ABI, provider);

      // 1. Resolve Metadata via Multicall (No Redis - fetch fresh each run)
      const networkTokens = TOKENS.map(t => ({ address: t.address, sheetSymbol: t.symbol }));
      const metaChunks = chunkArray(networkTokens, Math.floor(MULTICALL_BATCH_SIZE / 2));

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
                pt.symbol = pt.sheetSymbol || 'Unknown';
                pt.success = false;
              }
            } else {
              pt.symbol = pt.sheetSymbol || 'Unknown';
              pt.success = false;
            }
          }
        } catch (err) {
          const errMsg = err.shortMessage || err.message.split(' (')[0];
          console.log(`${c.gray}   Metadata fetch failed: ${errMsg}${c.reset}`);
          for (const pt of chunk) { 
            pt.symbol = pt.sheetSymbol || 'Unknown';
            pt.success = false; 
          }
        }
      }

      // 2. Fetch Balances
      const validTokens = networkTokens.filter(t => t.success);
      if (validTokens.length === 0) {
        console.log(`${c.gray}   No valid tokens with metadata for ${network}. Skipping.${c.reset}`);
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

      for (let chunkIdx = 0; chunkIdx < balChunks.length; chunkIdx++) {
        const chunk = balChunks[chunkIdx];
        const mapping = mapChunks[chunkIdx];
        let found = 0, empty = 0;

        const batchInfo = `${c.gray}[${String(chunkIdx + 1).padStart(2, '0')}/${String(balChunks.length).padStart(2, '0')}]${c.reset}`;
        const processInfo = `Processing ${String(chunk.length).padStart(3, ' ')} calls...`;
        process.stdout.write(`   ${batchInfo} ${processInfo} `);

        try {
          const results = await multicall.aggregate3.staticCall(chunk);
          
          for (let k = 0; k < results.length; k++) {
            const res = results[k];
            const m = mapping[k];

            if (res.success) {
              try {
                const balanceWei = ERC20_INTERFACE.decodeFunctionResult("balanceOf", res.returnData)[0];
                const balanceStr = formatUnits(balanceWei, m.token.decimals);
                const balanceFloat = parseFloat(balanceStr);

                if (balanceFloat > 0) {
                  const nowStr = formatDate(new Date());
                  allResults.push({
                    'Tokens Name': m.token.symbol,
                    'Network': network,
                    'Tokens Address': m.token.address,
                    'Amount': balanceFloat,
                    'Wallet Name': m.wallet.name,
                    'Wallet Address': m.wallet.address,
                    'Timestamp': nowStr
                  });
                  found++;
                  totalFound++;
                } else {
                  empty++;
                  totalEmpty++;
                }
              } catch (e) {
                // Ignore decode fail for successful call
              }
            }
          }
          
          const foundPad = String(found).padStart(3, '0');
          const emptyPad = String(empty).padStart(3, '0');

          const foundText = found > 0 ? `${c.green}✓ Found: ${foundPad}${c.reset}` : `${c.gray}✓ Found: ${foundPad}${c.reset}`;
          const emptyText = `${c.gray}○ Empty: ${emptyPad}${c.reset}`;

          console.log(`${foundText} | ${emptyText}`);
        } catch (err) {
          console.log(`${c.red}FAILED!${c.reset}`);
          const errMsg = err.shortMessage || err.message.split(' (')[0];
          errors.push(`[${network}] Batch ${chunkIdx + 1} failed: ${errMsg}`);
        }
      }
    } catch (err) {
      // Network-level error - skip to next network
      const errMsg = err.shortMessage || err.message.split(' (')[0];
      console.log(`${c.red}   Network ${network} failed: ${errMsg}. Skipping to next network.${c.reset}`);
      errors.push(`[${network}] Network failed: ${errMsg}`);
      continue;
    }
  }

  // 3. ClearContent + Bulk Write with Rate Limiting
  const WRITE_BATCH_SIZE = 500;
  const RATE_LIMIT_DELAY_MS = 1100; // ~1.1s between writes to stay under 60 req/min

  async function rateLimitDelay() {
    return new Promise(resolve => setTimeout(resolve, RATE_LIMIT_DELAY_MS));
  }

  console.log(`\n${c.cyan}${c.bright}>> Writing to Google Sheets...${c.reset}`);
  console.log(`${c.gray}   Collected ${allResults.length} rows from ${networks.length} network(s)${c.reset}`);

  // Step 1: Clear existing content (values only, preserves sheet)
  try {
    console.log(`${c.yellow}   Clearing existing sheet content...${c.reset}`);
    await sheet.clear();
    await rateLimitDelay();
    await sheet.setHeaderRow(SHEET_HEADERS);
    await rateLimitDelay();
    console.log(`${c.green}   ✓ Sheet cleared and headers set${c.reset}`);
  } catch (err) {
    errors.push(`Failed to clear sheet: ${err.message}`);
    console.log(`${c.red}   ✗ Failed to clear sheet: ${err.message}${c.reset}`);
  }

  // Step 2: Bulk write in batches with rate limiting
  if (allResults.length > 0) {
    const writeChunks = chunkArray(allResults, WRITE_BATCH_SIZE);
    for (let i = 0; i < writeChunks.length; i++) {
      const batch = writeChunks[i];
      try {
        process.stdout.write(`   ${c.gray}[${String(i + 1).padStart(2, '0')}/${String(writeChunks.length).padStart(2, '0')}]${c.reset} Writing ${String(batch.length).padStart(4, ' ')} rows... `);
        await sheet.addRows(batch);
        console.log(`${c.green}✓${c.reset}`);
        if (i < writeChunks.length - 1) {
          await rateLimitDelay();
        }
      } catch (err) {
        console.log(`${c.red}✗${c.reset}`);
        errors.push(`Failed to write batch ${i + 1}: ${err.message}`);
      }
    }
  } else {
    console.log(`${c.yellow}   No data to write.${c.reset}`);
  }

  const endTime = Date.now();
  const execSecs = ((endTime - startTime) / 1000).toFixed(2);

  console.log(`\n${c.gray}--------------------------------------------------${c.reset}`);
  console.log(`${c.cyan}${c.bright}PROCESS SUMMARY: EVM WORKER${c.reset}`);
  console.log(`${c.gray}--------------------------------------------------${c.reset}`);
  console.log(`Execution Time: ${execSecs} seconds`);
  console.log(`${c.green}Total Found:    ${totalFound}${c.reset}`);
  console.log(`${c.gray}Total Empty:    ${totalEmpty}${c.reset}`);
  console.log(`${c.cyan}Total Written:  ${allResults.length}${c.reset}`);
  
  if (errors.length > 0) {
    console.log(`\n${c.red}Errors encountered:${c.reset}`);
    errors.forEach(e => console.log(`${c.red}- ${e}${c.reset}`));
  }
  console.log(`${c.gray}--------------------------------------------------${c.reset}`);
  
  process.exit(0);
}

main();
