/**
 * เทสต์ evm.js: รวมเครือข่ายที่เป็นเชนเดียวกัน และ coverage guard
 * รันด้วย: npm test
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { dedupeNetworksByChainId, computeEvmCoverage, MIN_COVERAGE } from '../evm.js';

/** probe ปลอม: map ชื่อ -> chainId ถ้าไม่มีในแมพให้ถือว่าถาม chainId ไม่ได้ */
function fakeProbe(map) {
  return async (entry) => {
    if (!(entry.name in map)) throw new Error('timeout');
    return { chainId: map[entry.name], provider: { __for: entry.name } };
  };
}

test('regression: Hyperliquid กับ Hyperevm chainId เดียวกัน ต้องเหลือรอบสแกนเดียว', async () => {
  // นี่คือเคสจริงจาก EVM_Tracker: ยอดเดียวกันถูกเขียน 2 แถวต่างกันแค่ชื่อ Network
  const entries = [
    { name: 'Hyperliquid', url: 'https://rpc.example/evm' },
    { name: 'Hyperevm', url: 'https://rpc2.example/evm' },
    { name: 'Ethereum', url: 'https://eth.example' },
  ];
  const { networks, merged } = await dedupeNetworksByChainId(entries, fakeProbe({
    Hyperliquid: '999', Hyperevm: '999', Ethereum: '1',
  }));

  assert.deepEqual(networks.map((n) => n.name), ['Hyperliquid', 'Ethereum']);
  assert.equal(merged.length, 1);
  assert.deepEqual(merged[0], { kept: 'Hyperliquid', dropped: 'Hyperevm', chainId: '999' });
});

test('แถวบนสุดใน nodes เป็นตัวที่ถูกเก็บ (สลับได้โดยย้ายลำดับ)', async () => {
  const probe = fakeProbe({ Hyperliquid: '999', Hyperevm: '999' });
  const a = await dedupeNetworksByChainId(
    [{ name: 'Hyperevm', url: 'u1' }, { name: 'Hyperliquid', url: 'u2' }], probe);
  assert.deepEqual(a.networks.map((n) => n.name), ['Hyperevm']);
  assert.equal(a.merged[0].kept, 'Hyperevm');
});

test('คนละเชนจริงต้องไม่ถูกรวม แม้ชื่อจะคล้ายกัน', async () => {
  const entries = [
    { name: 'Optimism', url: 'u1' },
    { name: 'Op', url: 'u2' },
  ];
  const { networks, merged } = await dedupeNetworksByChainId(entries,
    fakeProbe({ Optimism: '10', Op: '8453' }));

  assert.equal(networks.length, 2, 'chainId ต่างกัน = คนละเชน ห้ามรวม');
  assert.equal(merged.length, 0);
});

test('ถาม chainId ไม่ได้ ต้องไม่ถูกรวมกับใครโดยไม่มีหลักฐาน', async () => {
  const entries = [
    { name: 'Ethereum', url: 'u1' },
    { name: 'Mystery', url: 'u2' },   // probe fail
    { name: 'Mystery2', url: 'u3' },  // probe fail เหมือนกัน แต่คนละ url
  ];
  const { networks, merged } = await dedupeNetworksByChainId(entries,
    fakeProbe({ Ethereum: '1' }));

  assert.equal(networks.length, 3, 'ทั้งสามต้องยังถูกสแกนต่อ');
  assert.equal(merged.length, 0, 'ไม่มีหลักฐานว่าเป็นเชนเดียวกัน ห้ามรวม');
  const mystery = networks.find((n) => n.name === 'Mystery');
  assert.equal(mystery.chainId, null);
  assert.ok(mystery.error, 'ต้องเก็บเหตุผลไว้รายงาน');
});

test('chainId เดียวกัน 3 แถว เหลือ 1 และรายงานที่ถูกรวมครบ', async () => {
  const { networks, merged } = await dedupeNetworksByChainId([
    { name: 'A', url: 'u1' }, { name: 'B', url: 'u2' }, { name: 'C', url: 'u3' },
  ], fakeProbe({ A: '999', B: '999', C: '999' }));

  assert.equal(networks.length, 1);
  assert.deepEqual(merged.map((m) => m.dropped), ['B', 'C']);
});

test('รายการว่างไม่ throw', async () => {
  const { networks, merged } = await dedupeNetworksByChainId([], fakeProbe({}));
  assert.deepEqual(networks, []);
  assert.deepEqual(merged, []);
});

// ---- coverage guard ----

test('regression: เครือข่ายพังทั้งเชน ต้องดึง coverage ลงจนไม่เขียนทับ', async () => {
  // 4 เชน เชนหนึ่งพังหมด: metadata ของเชนนั้นอ่านไม่ได้เลย
  // ของเดิม sheet.clear() ทำงานอยู่ดี ข้อมูลทั้งเชนหายเงียบ ๆ
  const cov = computeEvmCoverage({
    metaIntended: 400, metaOk: 300,      // 1 ใน 4 เชนอ่าน metadata ไม่ได้
    balIntended: 3000, balOk: 3000,
  });
  assert.equal(cov.bal, 1, 'ช่องที่ยิงจริงสำเร็จหมด');
  assert.ok(cov.total < MIN_COVERAGE, `ต้องต่ำกว่าเกณฑ์ แต่ได้ ${cov.total}`);
  assert.equal(cov.total, 0.75);
});

test('batch ยอดพังบางส่วนก็ดึง coverage ลง', () => {
  const cov = computeEvmCoverage({
    metaIntended: 400, metaOk: 400,
    balIntended: 4000, balOk: 3600,
  });
  assert.equal(cov.meta, 1);
  assert.equal(cov.total, 0.9);
  assert.ok(cov.total < MIN_COVERAGE);
});

test('อ่านได้ครบ = 100%', () => {
  const cov = computeEvmCoverage({
    metaIntended: 400, metaOk: 400, balIntended: 4000, balOk: 4000,
  });
  assert.equal(cov.total, 1);
});

test('ไม่ได้อ่านอะไรเลยต้องเป็น 0 ไม่ใช่ 100%', () => {
  const cov = computeEvmCoverage({
    metaIntended: 0, metaOk: 0, balIntended: 0, balOk: 0,
  });
  assert.equal(cov.total, 0, 'ทุกเชนพังตั้งแต่ต้น ต้องไม่ผ่าน guard');
});

test('metadata ผ่านแต่ไม่มี token บนเชนไหนเลย ถือว่าครบ', () => {
  // เรารู้คำตอบแล้วว่า "ไม่มี" ซึ่งเป็นข้อมูลที่สมบูรณ์ ไม่ใช่ความล้มเหลว
  const cov = computeEvmCoverage({
    metaIntended: 400, metaOk: 400, balIntended: 0, balOk: 0,
  });
  assert.equal(cov.total, 1);
});
