import { JsonRpcProvider, Contract, formatEther, formatUnits } from 'ethers';
import { GoogleSpreadsheet } from 'google-spreadsheet';
import { JWT } from 'google-auth-library';
import dotenv from 'dotenv';
dotenv.config();

// RPC Configuration
const RPC_URLS = {
  'Ethereum': 'https://ethereum-rpc.publicnode.com',
  'BSC': 'https://bsc-rpc.publicnode.com',
  'Polygon': 'https://polygon-bor-rpc.publicnode.com',
  'Arbitrum': 'https://arbitrum-one-rpc.publicnode.com',
  'Optimism': 'https://optimism-rpc.publicnode.com',
  'Base': 'https://base-rpc.publicnode.com',
  'Avalanche': 'https://avalanche-c-chain-rpc.publicnode.com',
  'Fantom': 'https://fantom-public.nodies.app',
  'Linea': 'https://linea-rpc.publicnode.com',
  'Blast': 'https://blast-rpc.publicnode.com',
  'Abstract': 'https://api.mainnet.abs.xyz',
  'Ink': 'https://ink.api.pocket.network',
  'Merlin': 'https://rpc.merlinchain.io',
  'Sonic': 'https://sonic-rpc.publicnode.com:443',
  'Sei V2': 'https://sei-evm-rpc.publicnode.com',
  'Degen': 'https://rpc.degen.tips',
  'Dogechain': 'https://rpc.dogechain.dog'
};

// Wallets Configuration
// Parse from env
let WALLETS = [];
try {
  let rawVal = process.env.EVM_WALLETS_RAW || process.env.EVM_WALLETS_JSON || '[]';
  if (!rawVal.trim().startsWith('[')) {
    rawVal = '[' + rawVal + ']';
  }
  WALLETS = new Function('return ' + rawVal)();
  if (!Array.isArray(WALLETS) || WALLETS.length === 0) {
    throw new Error('EVM_WALLETS array is empty.');
  }
} catch (err) {
  console.error('[FATAL] Failed to parse EVM_WALLETS_RAW from .env:', err.message);
  process.exit(1);
}

// ERC20 Config (Optional for future use)
const ERC20_TOKENS = {
  // 'Ethereum': [{ address: '0xdAC17F958D2ee523a2206206994597C13D831ec7', symbol: 'USDT', decimals: 6 }]
};

const ERC20_ABI = [
  "function balanceOf(address owner) view returns (uint256)"
];

const DELAY_BETWEEN_RPC_MS = parseInt(process.env.EVM_DELAY_RPC_MS, 10) || 2000;
const DELAY_BETWEEN_WALLET_MS = parseInt(process.env.EVM_DELAY_WALLET_MS, 10) || 500;
const INTERVAL_MS = parseInt(process.env.EVM_INTERVAL_MS, 10) || 300000;
const SPREADSHEET_ID = process.env.SPREADSHEET_ID;

function delay(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function getGoogleSheet() {
  const serviceAccountAuth = new JWT({
    email: process.env.GOOGLE_SERVICE_ACCOUNT_EMAIL,
    key: process.env.GOOGLE_PRIVATE_KEY ? process.env.GOOGLE_PRIVATE_KEY.replace(/\\n/g, '\n') : '',
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
  });

  const doc = new GoogleSpreadsheet(SPREADSHEET_ID, serviceAccountAuth);
  await doc.loadInfo(); 
  
  const sheetTitle = 'EVM_Tracker';
  let sheet = doc.sheetsByTitle[sheetTitle];
  if (!sheet) {
    sheet = await doc.addSheet({ title: sheetTitle, headerValues: ['Timestamp', 'Network', 'Wallet Name', 'Wallet Address', 'Token', 'Amount'] });
  } else {
    try {
      await sheet.loadHeaderRow();
    } catch {
      await sheet.setHeaderRow(['Timestamp', 'Network', 'Wallet Name', 'Wallet Address', 'Token', 'Amount']);
    }
  }
  return sheet;
}

async function runTracker() {
  console.log(`\n--- Starting EVM Tracker Cycle at ${new Date().toISOString()} ---`);
  
  let sheet;
  try {
    sheet = await getGoogleSheet();
  } catch (err) {
    console.error('[FATAL] Failed to connect to Google Sheets:', err.message);
    return;
  }

  const allRows = [];
  const timestamp = new Date().toISOString();
  const networks = Object.keys(RPC_URLS);

  for (let i = 0; i < networks.length; i++) {
    const network = networks[i];
    const rpcUrl = RPC_URLS[network];
    console.log(`[Network: ${network}] Initializing provider...`);
    
    // We recreate provider for each network
    const provider = new JsonRpcProvider(rpcUrl);

    for (let j = 0; j < WALLETS.length; j++) {
      const wallet = WALLETS[j];
      try {
        console.log(`  -> Fetching Native Balance for ${wallet.name} (${wallet.address}) on ${network}`);
        const balanceWei = await provider.getBalance(wallet.address);
        const balanceEth = formatEther(balanceWei);
        
        allRows.push({
          Timestamp: timestamp,
          Network: network,
          'Wallet Name': wallet.name,
          'Wallet Address': wallet.address,
          Token: 'Native',
          Amount: balanceEth
        });

        // Loop ERC20 tokens if defined for this network
        if (ERC20_TOKENS[network]) {
          for (const token of ERC20_TOKENS[network]) {
            console.log(`  -> Fetching ERC20 ${token.symbol} for ${wallet.name} on ${network}`);
            const contract = new Contract(token.address, ERC20_ABI, provider);
            const tokenBalanceWei = await contract.balanceOf(wallet.address);
            const tokenBalance = formatUnits(tokenBalanceWei, token.decimals || 18);
            
            allRows.push({
              Timestamp: timestamp,
              Network: network,
              'Wallet Name': wallet.name,
              'Wallet Address': wallet.address,
              Token: token.symbol,
              Amount: tokenBalance
            });
            await delay(DELAY_BETWEEN_WALLET_MS);
          }
        }
        
      } catch (err) {
        const errorMsg = `[ERROR] Failed to fetch data for ${wallet.name} (${wallet.address}) on ${network}: ${err.message}`;
        console.error(errorMsg);
        
        // Push the error as the Amount so it doesn't default to 0 and records it for debugging
        allRows.push({
          Timestamp: timestamp,
          Network: network,
          'Wallet Name': wallet.name,
          'Wallet Address': wallet.address,
          Token: 'Fetch Error',
          Amount: errorMsg
        });
      }
      
      if (j < WALLETS.length - 1) {
        await delay(DELAY_BETWEEN_WALLET_MS);
      }
    }
    
    if (i < networks.length - 1) {
      await delay(DELAY_BETWEEN_RPC_MS);
    }
  }

  if (allRows.length > 0) {
    try {
      console.log(`Saving ${allRows.length} rows to Google Sheets (Sheet: EVM_Tracker)...`);
      await sheet.addRows(allRows);
      console.log('Successfully saved EVM data to Google Sheets.');
    } catch (err) {
      console.error('[ERROR] Failed to save rows to Google Sheets:', err.message);
    }
  } else {
    console.log('No EVM token data to save in this cycle.');
  }

  console.log(`--- Finished EVM Tracker Cycle ---`);
}

async function main() {
  if (!SPREADSHEET_ID || !process.env.GOOGLE_SERVICE_ACCOUNT_EMAIL || !process.env.GOOGLE_PRIVATE_KEY) {
    console.error('[FATAL] Missing Google Sheets configuration in .env!');
    process.exit(1);
  }

  // Run once immediately
  await runTracker();

  console.log(`Started continuous EVM tracking every ${INTERVAL_MS / 1000} seconds.`);
  setInterval(runTracker, INTERVAL_MS);
}

main().catch(err => {
  console.error('[FATAL] Unhandled Exception in EVM Tracker:', err);
});
