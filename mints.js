import { Connection, PublicKey } from '@solana/web3.js';
import { TOKEN_PROGRAM_ID, TOKEN_2022_PROGRAM_ID } from '@solana/spl-token';

const conn = new Connection('https://api.mainnet-beta.solana.com');

async function checkMint(mintAddress) {
  const mintPubKey = new PublicKey(mintAddress);
  
  try {
    const accountInfo = await conn.getAccountInfo(mintPubKey);
    
    if (!accountInfo) {
      console.log('❌ Account not found');
      return;
    }
    
    const owner = accountInfo.owner.toBase58();
    const dataLength = accountInfo.data.length;
    
    console.log('Mint:', mintAddress);
    console.log('Owner Program:', owner);
    console.log('Data Length:', dataLength);
    
    if (owner === TOKEN_PROGRAM_ID.toBase58()) {
      console.log('✅ Type: SPL Token');
    } else if (owner === TOKEN_2022_PROGRAM_ID.toBase58()) {
      console.log('✅ Type: Token-2022');
    } else {
      console.log('⚠️ Type: Unknown / Other Program');
      console.log('Owner:', owner);
    }
    
    // เช็คว่าเป็น Pump.fun หรือไม่
    if (mintAddress.endsWith('pump')) {
      console.log('🎯 Likely Pump.fun token');
    }
    
  } catch (e) {
    console.log('Error:', e.message);
  }
}

checkMint('EmcxFTNVDqyLHp11NvwvLZ4D7LKGbG9i7B8RF7dwpump');
