import { JsonRpcProvider, Contract, Interface, formatUnits, isAddress } from 'ethers';
import { GoogleSpreadsheet } from 'google-spreadsheet';
import { JWT } from 'google-auth-library';

import dotenv from 'dotenv';
import { pathToFileURL } from 'node:url';
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

// สัดส่วนข้อมูลขั้นต่ำที่ต้องอ่านได้ ถึงจะยอมเขียนทับ Sheet
const MIN_COVERAGE = Number(process.env.MIN_COVERAGE || 0.95);
// timeout ตอนถาม chainId ของแต่ละ RPC
const CHAIN_ID_TIMEOUT_MS = Number(process.env.CHAIN_ID_TIMEOUT_MS || 10000);

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

/**
 * error ที่แปลว่า "ปลายทางนี้ไม่ใช่ EVM JSON-RPC" ไม่ใช่ "ล่มชั่วคราว"
 * เช่น Injective ตอบ 501, Tron ตอบ 405, Sui ตอบ -32601 Method not found
 * แยกออกจากกันเพราะอันแรกจะไม่มีวันหาย ส่วนอันหลังรอแล้วอาจกลับมา
 */
function isNotEvmRpc(err) {
  const msg = String(err?.message || err || '');
  return /not implemented|method not found|-32601|-32701|\b501\b|\b405\b|Method Not Allowed/i.test(msg);
}

/** ปิด provider แบบไม่โยน error (provider ปลอมในเทสต์ไม่มี destroy) */
function destroyProvider(provider) {
  try {
    if (typeof provider?.destroy === 'function') provider.destroy();
  } catch { /* ปิดไม่ได้ก็ปล่อย */ }
}

const withTimeout = (p, ms, label) => Promise.race([
  p,
  new Promise((_, rej) => setTimeout(() => rej(new Error(`timeout (${label})`)), ms)),
]);

/**
 * ตรวจ 2 อย่างในรอบเดียว: chainId และ Multicall3 มีอยู่บนเชนนี้ไหม
 *
 * worker ตัวนี้อ่านยอดผ่าน Multicall3 อย่างเดียว เชนที่ไม่มีสัญญานี้จึงสแกนไม่ได้เลย
 * ไม่ว่าจะรอนานแค่ไหน — ต้องแยกออกจาก "เชนที่ล่มชั่วคราว" ไม่งั้น coverage
 * จะถูกกดต่ำค้างตลอดกาลจนไม่มีวันได้เขียนชีตอีก
 */
async function probeChainId(entry) {
  const provider = new JsonRpcProvider(entry.url, undefined, { staticNetwork: true });
  let net;
  try {
    net = await withTimeout(provider.getNetwork(), CHAIN_ID_TIMEOUT_MS, 'chainId');
  } catch (err) {
    provider.destroy();
    if (isNotEvmRpc(err)) {
      const e = new Error('ปลายทางนี้ไม่ใช่ EVM JSON-RPC');
      e.unsupported = true;
      throw e;
    }
    throw err;
  }

  // มี Multicall3 อยู่จริงไหม — ไม่มี code ที่ที่อยู่นั้น = สแกนด้วย worker ตัวนี้ไม่ได้
  try {
    const code = await withTimeout(
      provider.getCode(MULTICALL3_ADDRESS), CHAIN_ID_TIMEOUT_MS, 'multicall');
    if (!code || code === '0x') {
      provider.destroy();
      const e = new Error(`ไม่มี Multicall3 บนเชนนี้ (chainId ${net.chainId})`);
      e.unsupported = true;
      throw e;
    }
  } catch (err) {
    if (err.unsupported) throw err;
    // ถาม getCode ไม่ได้ = อาจแค่ล่มชั่วคราว ปล่อยให้ลองสแกนต่อ
  }

  return { chainId: String(net.chainId), provider };
}

/**
 * รวมรายการที่เป็นเชนเดียวกันให้เหลือตัวเดียว และคัดเชนที่สแกนไม่ได้ออก
 *
 * คอลัมน์ Network ใน Sheet คือ "ข้อความที่คนพิมพ์" ไม่ใช่ค่าที่มาจากบล็อกเชน
 * ถ้าแท็บ nodes มี 2 แถวที่ URL ชี้ไปเชนเดียวกันแต่ตั้งชื่อคนละอย่าง
 * (เช่น Hyperliquid กับ Hyperevm) โค้ดจะอ่านเชนนั้น 2 รอบและเขียนยอดเดียวกัน 2 แถว
 *
 * การเทียบด้วย chainId ปลอดภัยกว่ารายการชื่อพ้อง เพราะถ้าเป็นคนละเชนจริง
 * chainId จะต่างกันและไม่มีอะไรถูกรวม — ไม่มีทางรวมผิด
 *
 * แถวที่อยู่บนสุดในแท็บ nodes เป็นตัวที่ถูกเก็บไว้ (สลับได้โดยย้ายลำดับแถว)
 *
 * คืน 3 กลุ่ม: networks (สแกนต่อ) · merged (เชนซ้ำ) · unsupported (สแกนไม่ได้เลย)
 */
async function dedupeNetworksByChainId(entries, probe = probeChainId) {
  const probes = await Promise.all(entries.map(async (e) => {
    try {
      const { chainId, provider } = await probe(e);
      return { ...e, chainId: String(chainId), provider };
    } catch (err) {
      if (err?.unsupported) {
        // worker ตัวนี้อ่านเชนนี้ไม่ได้เลย ไม่ใช่เรื่องชั่วคราว
        return { ...e, unsupported: true, error: err.message };
      }
      // ถาม chainId ไม่ได้ ก็ยังปล่อยให้ลองสแกน แล้วไปพังตอนนั้นพร้อมรายงาน error
      // ใช้ url เป็นคีย์แทน เพื่อไม่ให้ถูกรวมกับใครโดยไม่มีหลักฐาน
      return { ...e, chainId: null, key: e.url, error: err?.message || String(err) };
    }
  }));

  const byChain = new Map();
  const merged = [];
  const unsupported = [];
  for (const p of probes) {
    if (p.unsupported) { unsupported.push(p); continue; }
    const key = p.chainId ?? p.key;
    if (byChain.has(key)) {
      merged.push({ kept: byChain.get(key).name, dropped: p.name, chainId: p.chainId });
      destroyProvider(p.provider); // ตัวที่ถูกรวมทิ้ง ต้องปิดไม่ให้ค้าง event loop
      continue;
    }
    byChain.set(key, p);
  }
  return { networks: [...byChain.values()], merged, unsupported };
}

/**
 * สัดส่วนข้อมูลที่อ่านได้จริงในรอบนี้
 *   metadata — ยิงถาม symbol/decimals สำเร็จกี่ token (token ที่ไม่มีบนเชนนั้น
 *              ยังนับว่าสำเร็จ เพราะเรารู้คำตอบแล้วว่า "ไม่มี")
 *   balance  — ยิงถามยอดสำเร็จกี่ช่อง (token × wallet)
 * คูณกันเพื่อให้ความล้มเหลวของทั้งสองขั้นสะท้อนออกมาทั้งคู่
 */
function computeEvmCoverage({ metaIntended, metaOk, balIntended, balOk }) {
  const meta = metaIntended ? metaOk / metaIntended : 0;
  // ไม่มี balance call เลยทั้งที่ metadata ผ่าน = ไม่มี token บนเชนไหนเลย ถือว่าครบ
  const bal = balIntended ? balOk / balIntended : (metaIntended ? 1 : 0);
  return { meta, bal, total: meta * bal };
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

  // เก็บเป็น array เพื่อรักษาลำดับแถวใน Sheet ไว้ใช้ตัดสินว่าชื่อไหนถูกเก็บตอนรวมเชน
  let rpcEntries = [];
  try {
    const maxRows = nodesSheet.rowCount;
    if (maxRows >= 2) {
      await nodesSheet.loadCells(`A1:B${maxRows}`);
      const seenName = new Set();
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
             if (seenName.has(name)) continue; // ชื่อซ้ำเป๊ะ — เดิม object key ก็ทับกันอยู่แล้ว
             seenName.add(name);
             rpcEntries.push({ name, url: rpcUrl });
          }
        }
      }
    }
  } catch (err) {
    console.error("Fatal Error: Failed to read from 'nodes' tab.", err.message);
    process.exit(1);
  }

  if (rpcEntries.length === 0) {
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
    process.exit(1); // ไม่มี wallet = ตั้งค่าผิด ไม่ใช่ "สำเร็จแต่ไม่มีอะไรทำ"
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
    process.exit(1); // ไม่มี token = ตั้งค่าผิด ไม่ใช่ "สำเร็จแต่ไม่มีอะไรทำ"
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

  // ---- รวมเครือข่ายที่เป็นเชนเดียวกัน ก่อนเริ่มอ่านยอด ----
  console.log(`\n${c.cyan}${c.bright}>> Checking chain IDs of ${rpcEntries.length} network(s)...${c.reset}`);
  const { networks, merged, unsupported } = await dedupeNetworksByChainId(rpcEntries);

  for (const m of merged) {
    console.log(`   ${c.yellow}⏭${c.reset} ${c.gray}${m.dropped.padEnd(14)}${c.reset} ` +
      `เป็นเชนเดียวกับ ${c.bright}${m.kept}${c.reset} (chainId ${m.chainId}) — ` +
      `${c.gray}อ่านรอบเดียวพอ ไม่งั้นยอดเดิมจะถูกเขียน 2 แถว${c.reset}`);
  }
  for (const u of unsupported) {
    // ไม่นับใน coverage เพราะไม่ใช่ความล้มเหลวชั่วคราว ถ้านับ coverage จะต่ำค้าง
    // ตลอดกาลจนไม่มีวันได้เขียนชีตอีก — แต่ต้องบอกให้ชัดว่าเชนนี้ไม่ได้ถูกอ่าน
    console.log(`   ${c.yellow}⊘${c.reset} ${c.gray}${u.name.padEnd(14)}${c.reset} ` +
      `${c.yellow}ข้ามทั้งเชน${c.reset} — ${u.error} ` +
      `${c.gray}(worker นี้อ่านผ่าน Multicall3 บน EVM เท่านั้น)${c.reset}`);
  }
  for (const n of networks) {
    if (n.chainId) {
      console.log(`   ${c.green}•${c.reset} ${c.gray}${n.name.padEnd(14)} chainId ${n.chainId}${c.reset}`);
    } else {
      console.log(`   ${c.yellow}?${c.reset} ${c.gray}${n.name.padEnd(14)} ` +
        `ถาม chainId ไม่ได้ (${n.error}) — จะลองสแกนต่อ${c.reset}`);
      errors.push(`[${n.name}] chainId probe failed: ${n.error}`);
    }
  }

  // นับ "ช่องข้อมูล" ที่ตั้งใจอ่าน เทียบกับที่อ่านได้จริง เพื่อกันการเขียนทับด้วยข้อมูลไม่ครบ
  let metaIntended = 0, metaOk = 0;
  let balIntended = 0, balOk = 0;
  // ตาข่ายกันพลาดชั้นสุดท้าย: chainId|token|wallet ต้องไม่ซ้ำ
  const seenCell = new Set();
  let dupSkipped = 0;

  // Process each network with error handling - skip failed networks
  for (const netEntry of networks) {
    const network = netEntry.name;
    const chainKey = netEntry.chainId ?? netEntry.url;
    const snapMeta = metaIntended, snapBal = balIntended;
    try {
      console.log(`\n${c.cyan}${c.bright}>> Network: ${network.toUpperCase()}${c.reset}`);
      const provider = netEntry.provider
        ?? new JsonRpcProvider(netEntry.url, undefined, { staticNetwork: true });
      const multicall = new Contract(MULTICALL3_ADDRESS, MULTICALL3_ABI, provider);

      // 1. Resolve Metadata via Multicall (No Redis - fetch fresh each run)
      const networkTokens = TOKENS.map(t => ({ address: t.address, sheetSymbol: t.symbol }));
      const metaChunks = chunkArray(networkTokens, Math.floor(MULTICALL_BATCH_SIZE / 2));

      for (const chunk of metaChunks) {
        metaIntended += chunk.length;
        const metaCalls = [];
        for (const pt of chunk) {
          metaCalls.push({ target: pt.address, allowFailure: true, callData: ERC20_INTERFACE.encodeFunctionData("symbol") });
          metaCalls.push({ target: pt.address, allowFailure: true, callData: ERC20_INTERFACE.encodeFunctionData("decimals") });
        }
        try {
          const results = await multicall.aggregate3.staticCall(metaCalls);
          // ยิงสำเร็จแล้ว — token ที่ success:false คือ "ไม่มีบนเชนนี้" ไม่ใช่ความล้มเหลว
          metaOk += chunk.length;
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
          console.log(`${c.red}   Metadata fetch failed: ${errMsg}${c.reset}`);
          // ต่างจากกรณีบน: ยิงไม่สำเร็จ = ไม่รู้ว่า token เหล่านี้มีบนเชนนี้หรือไม่
          errors.push(`[${network}] Metadata batch failed (${chunk.length} tokens): ${errMsg}`);
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
        balIntended += chunk.length;

        try {
          const results = await multicall.aggregate3.staticCall(chunk);
          balOk += chunk.length;

          for (let k = 0; k < results.length; k++) {
            const res = results[k];
            const m = mapping[k];

            if (res.success) {
              try {
                const balanceWei = ERC20_INTERFACE.decodeFunctionResult("balanceOf", res.returnData)[0];
                const balanceStr = formatUnits(balanceWei, m.token.decimals);
                const balanceFloat = parseFloat(balanceStr);

                if (balanceFloat > 0) {
                  // ตาข่ายชั้นสุดท้าย เผื่อมีทางอื่นที่ทำให้ช่องเดิมถูกอ่านซ้ำ
                  const cellKey = `${chainKey}|${m.token.address.toLowerCase()}|${m.wallet.address.toLowerCase()}`;
                  if (seenCell.has(cellKey)) { dupSkipped++; continue; }
                  seenCell.add(cellKey);

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
      // เชนนี้ไม่ได้ข้อมูลเลย — ต้องนับเป็นช่องที่ตั้งใจอ่านแต่อ่านไม่ได้
      // ไม่งั้น coverage จะดูสมบูรณ์ทั้งที่ข้อมูลทั้งเชนหายไป แล้วไปเขียนทับของเดิม
      // เติมเฉพาะส่วนที่ยังไม่ถูกนับไปก่อนหน้า เพื่อไม่ให้นับซ้ำถ้าพังกลางทาง
      metaIntended += Math.max(0, TOKENS.length - (metaIntended - snapMeta));
      balIntended += Math.max(0, (TOKENS.length * WALLETS.length) - (balIntended - snapBal));
      continue;
    }
  }

  // 3. ClearContent + Bulk Write with Rate Limiting
  const WRITE_BATCH_SIZE = 500;
  const RATE_LIMIT_DELAY_MS = 1100; // ~1.1s between writes to stay under 60 req/min

  async function rateLimitDelay() {
    return new Promise(resolve => setTimeout(resolve, RATE_LIMIT_DELAY_MS));
  }

  /**
   * Coverage guard — เขียนทับ Sheet ต่อเมื่ออ่านข้อมูลได้ครบพอ
   *
   * ของเดิม sheet.clear() ทำงานทุกครั้งไม่ว่าเกิดอะไรขึ้น เครือข่ายที่พังทั้งเชน
   * ถูก `continue` ข้ามไปเงียบ ๆ แล้วข้อมูลของเชนนั้นก็หายไปจาก Sheet
   * โดยไม่มีอะไรบอก — ลบของดีทิ้งแล้วใส่ข้อมูลไม่ครบแทน
   */
  const {
    meta: metaCoverage, bal: balCoverage, total: coverage,
  } = computeEvmCoverage({ metaIntended, metaOk, balIntended, balOk });

  console.log(`\n${c.cyan}${c.bright}>> Coverage: ${(coverage * 100).toFixed(1)}%${c.reset} ` +
    `${c.gray}(metadata ${(metaCoverage * 100).toFixed(1)}% × balance ${(balCoverage * 100).toFixed(1)}%)${c.reset}`);
  console.log(`${c.gray}   Collected ${allResults.length} rows from ${networks.length} network(s)${c.reset}`);
  if (dupSkipped > 0) {
    console.log(`${c.yellow}   ⏭ ข้ามแถวซ้ำ ${dupSkipped} แถว (ช่องเดิมถูกอ่านมากกว่าหนึ่งครั้ง)${c.reset}`);
  }

  let wrote = false;
  if (coverage < MIN_COVERAGE) {
    console.log(`${c.red}   ✗ Coverage ต่ำกว่าเกณฑ์ ${(MIN_COVERAGE * 100).toFixed(0)}% — ` +
      `ยกเลิกการเขียนทับ Sheet เพื่อกันข้อมูลหาย${c.reset}`);
    console.log(`${c.gray}     ข้อมูลเดิมใน Sheet ยังอยู่ครบ ปรับเกณฑ์ได้ด้วย MIN_COVERAGE${c.reset}`);
    process.exitCode = 1;
  } else if (allResults.length === 0) {
    console.log(`${c.yellow}   ⚠ No data to write — ข้ามการเขียนทับ${c.reset}`);
  } else {
    console.log(`\n${c.cyan}${c.bright}>> Writing to Google Sheets...${c.reset}`);

    // Step 1: Clear existing content (values only, preserves sheet)
    let cleared = false;
    try {
      console.log(`${c.yellow}   Clearing existing sheet content...${c.reset}`);
      await sheet.clear();
      await rateLimitDelay();
      await sheet.setHeaderRow(SHEET_HEADERS);
      await rateLimitDelay();
      cleared = true;
      console.log(`${c.green}   ✓ Sheet cleared and headers set${c.reset}`);
    } catch (err) {
      errors.push(`Failed to clear sheet: ${err.message}`);
      console.log(`${c.red}   ✗ Failed to clear sheet: ${err.message}${c.reset}`);
      process.exitCode = 1;
    }

    // Step 2: Bulk write in batches with rate limiting
    if (cleared) {
      const writeChunks = chunkArray(allResults, WRITE_BATCH_SIZE);
      let written = 0;
      for (let i = 0; i < writeChunks.length; i++) {
        const batch = writeChunks[i];
        try {
          process.stdout.write(`   ${c.gray}[${String(i + 1).padStart(2, '0')}/${String(writeChunks.length).padStart(2, '0')}]${c.reset} Writing ${String(batch.length).padStart(4, ' ')} rows... `);
          await sheet.addRows(batch);
          written += batch.length;
          console.log(`${c.green}✓${c.reset}`);
          if (i < writeChunks.length - 1) {
            await rateLimitDelay();
          }
        } catch (err) {
          console.log(`${c.red}✗${c.reset}`);
          errors.push(`Failed to write batch ${i + 1}: ${err.message}`);
          process.exitCode = 1;
        }
      }
      wrote = written === allResults.length;
      if (!wrote) {
        console.log(`${c.red}   ✗ เขียนได้ ${written}/${allResults.length} แถว — Sheet ไม่ครบ${c.reset}`);
      }
    }
  }

  const endTime = Date.now();
  const execSecs = ((endTime - startTime) / 1000).toFixed(2);

  console.log(`\n${c.gray}--------------------------------------------------${c.reset}`);
  console.log(`${c.cyan}${c.bright}PROCESS SUMMARY: EVM WORKER${c.reset}`);
  console.log(`${c.gray}--------------------------------------------------${c.reset}`);
  console.log(`Execution Time: ${execSecs} seconds`);
  console.log(`Coverage:       ${(coverage * 100).toFixed(1)}%  |  Wrote sheet: ${wrote ? 'yes' : 'no'}`);
  console.log(`Networks:       ${networks.length} scanned` +
    (merged.length ? `, ${merged.length} merged as same chain` : '') +
    (unsupported.length ? `, ${unsupported.length} skipped (ไม่รองรับ)` : ''));
  if (unsupported.length) {
    console.log(`${c.yellow}Skipped:        ${unsupported.map(u => u.name).join(', ')}` +
      ` — ไม่ได้ถูกอ่านและไม่ถูกนับใน Coverage${c.reset}`);
  }
  console.log(`${c.green}Total Found:    ${totalFound}${c.reset}`);
  console.log(`${c.gray}Total Empty:    ${totalEmpty}${c.reset}`);
  if (dupSkipped > 0) console.log(`${c.yellow}Duplicates:     ${dupSkipped} skipped${c.reset}`);
  console.log(`${c.cyan}Total Written:  ${wrote ? allResults.length : 0}${c.reset}`);

  if (errors.length > 0) {
    console.log(`\n${c.red}Errors encountered: ${errors.length}${c.reset}`);
    errors.slice(0, 15).forEach(e => console.log(`${c.red}- ${e}${c.reset}`));
    if (errors.length > 15) console.log(`${c.red}- ... and ${errors.length - 15} more${c.reset}`);
    // มี error = job ต้องไม่ขึ้นเขียว ของเดิม process.exit(0) เสมอ ทำให้ไม่มีใครรู้ว่าพัง
    process.exitCode = 1;
  }
  console.log(`${c.gray}--------------------------------------------------${c.reset}`);

  /**
   * ปิด provider ทุกตัว ไม่งั้นโปรเซสไม่จบ
   *
   * ethers จะพยายามต่อใหม่ทุกวินาทีไปเรื่อย ๆ กับปลายทางที่ตอบไม่ได้
   * ("failed to detect network ... retry in 1s") timer พวกนี้ค้าง event loop ไว้
   * ตอนที่ยังมี process.exit(0) ปิดท้าย ปัญหานี้ถูกกลบไว้ พอเอาออกเพื่อให้
   * exit code ทำงานถูกต้อง มันเลยโผล่มาเป็น job ที่ค้างจนถูก cancel
   */
  for (const n of networks) destroyProvider(n.provider);

  // กันเหนียว: ถ้ายังมี handle อะไรค้างอยู่ ให้จบใน 5 วินาทีด้วย exit code เดิม
  // unref() ทำให้ timer นี้ไม่ยื้อโปรเซสเอง ถ้าทุกอย่างสะอาดก็จบทันทีไม่ต้องรอ
  setTimeout(() => process.exit(process.exitCode ?? 0), 5000).unref();
}

// รันเฉพาะตอนถูกเรียกเป็นสคริปต์ — ทำให้ import มาเทสต์ฟังก์ชันย่อยได้โดยไม่สแกนจริง
const isCli = process.argv[1]
  && import.meta.url === pathToFileURL(process.argv[1]).href;

if (isCli) {
  main().catch(err => {
    console.error(`${c.red}Fatal Error:${c.reset} ${err?.stack || err?.message || err}`);
    process.exitCode = 1;
  });
}

export { dedupeNetworksByChainId, computeEvmCoverage, isNotEvmRpc, MIN_COVERAGE };
