/**
 * เทสต์การข้าม endpoint ที่รู้แล้วว่าใช้ไม่ได้
 * รันด้วย: npm test
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { filterKnownBlocked, KNOWN_BLOCKED_HOSTS } from '../solana.js';

const GOOD = 'https://api.mainnet-beta.solana.com';
const BLOCKED = 'https://solana-rpc.publicnode.com';

/** เก็บ stdout ไว้ไม่ให้รบกวนผล test */
function quiet(fn) {
  const orig = console.log;
  console.log = () => {};
  try { return fn(); } finally { console.log = orig; }
}

test('endpoint ที่รู้ว่าถูกบล็อกถูกข้าม ตัวที่ดีถูกเก็บไว้', () => {
  const out = quiet(() => filterKnownBlocked([BLOCKED, GOOD]));
  assert.deepEqual(out, [GOOD]);
});

test('แถวที่ค้างใน Sheet ไม่ทำให้ startup เสียเวลาอีก', () => {
  // จำลองสิ่งที่เกิดจริง: แท็บ nodes ยังมี publicnode อยู่ และถูก unshift มาก่อน default
  const fromSheet = [BLOCKED];
  const out = quiet(() => filterKnownBlocked([...fromSheet, GOOD]));
  assert.ok(!out.includes(BLOCKED), 'ต้องไม่ถูกนำไปยิงเลย ไม่ใช่ค่อยไปปิดตอนรัน');
  assert.equal(out.length, 1);
});

test('ทั้งสามตัวที่ error rate 100% ใน log ถูกครอบคลุม', () => {
  for (const host of ['solana-rpc.publicnode.com', 'solana.drpc.org', 'endpoints.omniatech.io']) {
    assert.ok(KNOWN_BLOCKED_HOSTS.has(host), `${host} ควรอยู่ในรายการ`);
    assert.ok(KNOWN_BLOCKED_HOSTS.get(host).length > 0, `${host} ต้องมีเหตุผลกำกับ`);
  }
});

test('host ที่มี path หรือ query ก็ยังถูกจับได้', () => {
  const out = quiet(() => filterKnownBlocked([
    'https://endpoints.omniatech.io/v1/sol/mainnet/public',
    GOOD,
  ]));
  assert.deepEqual(out, [GOOD], 'ต้องเทียบที่ host ไม่ใช่ทั้ง URL');
});

test('URL ที่พังถูกปล่อยผ่านให้ RpcPool จัดการ ไม่ถูกกลืนหาย', () => {
  const out = quiet(() => filterKnownBlocked(['ไม่ใช่ url', GOOD]));
  assert.equal(out.length, 2, 'ไม่ควรตัดทิ้งเงียบ ๆ เพราะจะซ่อนความผิดพลาดของผู้ใช้');
});

test('endpoint ที่ไม่รู้จักไม่ถูกข้าม', () => {
  const custom = 'https://my-private-node.example.com';
  const out = quiet(() => filterKnownBlocked([custom]));
  assert.deepEqual(out, [custom], 'รายการนี้ต้องไม่กลายเป็น allowlist');
});

test('รายชื่อว่างคืนค่าว่าง ไม่ throw', () => {
  assert.deepEqual(quiet(() => filterKnownBlocked([])), []);
});
