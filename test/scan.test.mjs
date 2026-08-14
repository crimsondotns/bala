/**
 * เทสต์ partial-batch recovery และ coverage guard
 * รันด้วย: npm test
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { PublicKey, Keypair } from '@solana/web3.js';
import { AccountLayout, TOKEN_PROGRAM_ID, getAssociatedTokenAddressSync } from '@solana/spl-token';
import { computeCoverage, scanWalletByAta, CONFIG } from '../solana.js';

const WALLET = { name: 'TEST WALLET', address: Keypair.generate().publicKey.toBase58() };

/** mintInfoMap ปลอมขนาด n mint (SPL ทั้งหมด, decimals 6) */
function makeMintMap(n) {
  const m = new Map();
  for (let i = 0; i < n; i++) {
    m.set(Keypair.generate().publicKey.toBase58(), {
      programId: TOKEN_PROGRAM_ID, decimals: 6,
    });
  }
  return m;
}

/** สร้าง AccountInfo ของ token account ที่ถือ amount หน่วย (raw) */
function tokenAccountInfo(mint, owner, amount) {
  const data = Buffer.alloc(AccountLayout.span);
  AccountLayout.encode({
    mint: new PublicKey(mint),
    owner: new PublicKey(owner),
    amount: BigInt(amount),
    delegateOption: 0, delegate: PublicKey.default,
    state: 1,
    isNativeOption: 0, isNative: 0n,
    delegatedAmount: 0n,
    closeAuthorityOption: 0, closeAuthority: PublicKey.default,
  }, data);
  return { owner: TOKEN_PROGRAM_ID, data, lamports: 2039280, executable: false };
}

/**
 * pool ปลอม: batchIdx ที่อยู่ใน failBatches จะโยน error
 * batch อื่นคืน token account ที่มียอดครบทุกช่อง
 */
function fakePool(mintMap, failBatches = new Set()) {
  let batchIdx = 0;
  const ataToMint = new Map();
  for (const [mint, meta] of mintMap) {
    const ata = getAssociatedTokenAddressSync(
      new PublicKey(mint), new PublicKey(WALLET.address), true, meta.programId);
    ataToMint.set(ata.toBase58(), mint);
  }
  return {
    calls: 0,
    async call(fn) {
      const idx = batchIdx++;
      this.calls++;
      if (failBatches.has(idx)) throw new Error('500 Internal Server Error: boom');
      return fn({
        getMultipleAccountsInfo: async (keys) => keys.map((k) => {
          const mint = ataToMint.get(k.toBase58());
          return mint ? tokenAccountInfo(mint, WALLET.address, 1_500_000) : null;
        }),
      });
    },
  };
}

test('batch ที่สำเร็จต้องรอด แม้ batch อื่นพัง (Bug 4)', async () => {
  const mintMap = makeMintMap(250); // batchSize 100 → 3 batches
  const pool = fakePool(mintMap, new Set([1])); // batch กลางพัง

  const r = await scanWalletByAta(pool, WALLET, mintMap);

  assert.equal(r.totalBatches, 3);
  assert.equal(r.failedBatches, 1);
  assert.equal(r.attempted, 250);
  assert.equal(r.checked, 150, 'batch 0 (100) + batch 2 (50)');
  assert.equal(r.found.length, 150, 'ของเดิมจะได้ 0 เพราะ throw ทิ้งทั้ง wallet');
  assert.equal(r.found[0].amount, 1.5, 'toUiAmount แปลง decimals ถูก');
});

test('ทุก batch พัง → failedBatches === totalBatches (นับเป็น FAILED)', async () => {
  const mintMap = makeMintMap(250);
  const pool = fakePool(mintMap, new Set([0, 1, 2]));

  const r = await scanWalletByAta(pool, WALLET, mintMap);
  assert.equal(r.failedBatches, r.totalBatches);
  assert.equal(r.checked, 0);
  assert.equal(r.found.length, 0);
  assert.match(r.lastError.message, /boom/);
});

test('สแกนสำเร็จหมด → ไม่มี partial', async () => {
  const mintMap = makeMintMap(150);
  const r = await scanWalletByAta(fakePool(mintMap), WALLET, mintMap);
  assert.equal(r.failedBatches, 0);
  assert.equal(r.checked, r.attempted);
  assert.equal(r.checked, 150);
});

test('scanWalletByAta ไม่ throw ออกมาแม้ RPC ตายหมด', async () => {
  const mintMap = makeMintMap(100);
  const pool = { async call() { throw new Error('RPC endpoint ใช้ไม่ได้ทั้งหมด'); } };
  // ต้องคืนผลลัพธ์ให้ caller ตัดสินใจ ไม่ใช่ throw ให้ catch ข้างนอกกลืน
  const r = await scanWalletByAta(pool, WALLET, mintMap);
  assert.equal(r.found.length, 0);
  assert.equal(r.failedBatches, 1);
});

test('regression: mint ที่ resolve ไม่ได้ ต้องดึง coverage ลง (Bug 5)', () => {
  // เคสอันตราย: ทุก wallet สแกนผ่าน แต่ 1 ใน 3 ของ mint หายไปตั้งแต่ต้น
  // ของเดิม coverage = okCount/wallets = 100% → เขียนทับ Sheet ด้วยข้อมูลไม่ครบ
  const cov = computeCoverage({
    tokenCount: 599, mintFailed: 200,
    cellsChecked: 38703, cellsAttempted: 38703, // 97 wallet × 399 mint ที่เหลือ
  });
  assert.equal(cov.cells, 1, 'ระดับ cell ดูเหมือนสมบูรณ์');
  assert.ok(cov.total < 0.95, `ต้องต่ำกว่าเกณฑ์ แต่ได้ ${cov.total}`);
  assert.ok(Math.abs(cov.total - 0.666) < 0.01);
});

test('coverage รวมผลของ partial batch', () => {
  const cov = computeCoverage({
    tokenCount: 599, mintFailed: 0,
    cellsChecked: 50000, cellsAttempted: 58103,
  });
  assert.equal(cov.mint, 1);
  assert.ok(cov.total < 0.95);
});

test('coverage = 100% เมื่อไม่มีอะไรพลาด', () => {
  const cov = computeCoverage({
    tokenCount: 599, mintFailed: 0, cellsChecked: 58103, cellsAttempted: 58103,
  });
  assert.equal(cov.total, 1);
});

test('coverage = 0 เมื่อไม่ได้สแกนอะไรเลย (ไม่ใช่ 100%)', () => {
  const cov = computeCoverage({
    tokenCount: 599, mintFailed: 0, cellsChecked: 0, cellsAttempted: 0,
  });
  assert.equal(cov.total, 0, 'pool ตายตั้งแต่ต้น ต้องไม่ผ่าน guard');
});

test('batchSize ยังอยู่ในลิมิตของ getMultipleAccounts', () => {
  assert.ok(CONFIG.batchSize <= 100, 'RPC จำกัด 100 key ต่อคำขอ');
});
