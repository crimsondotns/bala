/**
 * เทสต์ RpcPool ด้วย error จริงจาก log run 2026-08-14 (coverage 53.6%)
 * รันด้วย: node --test test/
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { RpcPool, classifyError, httpStatusOf, CONFIG } from '../solana.js';

// ข้อความ error ตามที่ web3.js โยนออกมาจริงใน log
const ERR = {
  drpc400: '400 Bad Request: {"id":"ff14","jsonrpc":"2.0","error":{"message":"chain is not available on free plan, please upgrade to paid plan"}}',
  omnia521: '521 <none>: <!DOCTYPE html>\n<html class="no-js" lang="en-US">',
  publicnode403: '403 Forbidden: {"jsonrpc":"2.0","error":{"code":-32602,"message":"Request blocked"},"id":"cec7"}',
  mainnet429: '429 Too Many Requests:  {"jsonrpc":"2.0","error":{"code": 429, "message":"Connection rate limits exceeded"}, "id": "6536" }',
  serverErr: '500 Internal Server Error: upstream failure',
};
const abortErr = () => Object.assign(new Error('This operation was aborted'), { name: 'AbortError' });

const URLS = [
  'https://solana-rpc.publicnode.com',
  'https://api.mainnet-beta.solana.com',
  'https://solana.drpc.org',
  'https://endpoints.omniatech.io/v1/sol/mainnet/public',
];

/** สร้าง pool ที่ fn() จะโยน error ตาม host — จำลองพฤติกรรมของ log จริง */
function makePool(behavior) {
  const pool = new RpcPool(URLS, 1000);
  // conn จริงถูกแทนด้วย marker ของ host เพื่อให้ fn รู้ว่าถูกยิงไปที่ไหน
  for (const n of pool.nodes) n.conn = { __host: n.host };
  const hits = [];
  const call = (label = 'test') => pool.call(async (conn) => {
    hits.push(conn.__host);
    const r = behavior(conn.__host);
    if (r instanceof Error) throw r;
    return r;
  }, label);
  return { pool, hits, call };
}

test('httpStatusOf ดึง status ออกจาก error ของ web3.js ได้', () => {
  assert.equal(httpStatusOf({ message: ERR.drpc400 }), 400);
  assert.equal(httpStatusOf({ message: ERR.omnia521 }), 521);
  assert.equal(httpStatusOf({ message: ERR.publicnode403 }), 403);
  assert.equal(httpStatusOf({ message: ERR.mainnet429 }), 429);
  assert.equal(httpStatusOf({ message: 'socket hang up' }), 0);
});

test('classifyError แยก permanent / ratelimit / transient ถูกต้อง', () => {
  assert.equal(classifyError({ message: ERR.publicnode403 }), 'permanent', '403 Request blocked');
  assert.equal(classifyError({ message: ERR.omnia521 }), 'permanent', '521 Cloudflare');
  assert.equal(classifyError({ message: ERR.drpc400 }), 'permanent', '400 free plan');
  assert.equal(classifyError({ message: ERR.mainnet429 }), 'ratelimit');
  assert.equal(classifyError({ message: ERR.serverErr }), 'transient', '500 ต้อง retry ได้');
  assert.equal(classifyError(abortErr()), 'transient', 'timeout ต้อง retry ได้');
  // 400 ที่เกิดจาก request ของเราผิดเอง ต้องไม่ไปปิด endpoint ทิ้ง
  assert.equal(classifyError({ message: '400 Bad Request: invalid param' }), 'transient');
});

test('regression: 3 endpoint ตาย 1 endpoint ดี → ต้องสำเร็จ ไม่ FAILED', async () => {
  // นี่คือสถานการณ์ที่ทำให้ 45/97 wallet พังใน log เดิม
  const { pool, hits, call } = makePool((host) => {
    if (host === 'api.mainnet-beta.solana.com') return 'OK';
    if (host === 'solana.drpc.org') return new Error(ERR.drpc400);
    if (host === 'endpoints.omniatech.io') return new Error(ERR.omnia521);
    return new Error(ERR.publicnode403);
  });

  // ทุกคำขอต้องสำเร็จ ไม่มี FAILED แม้แต่ครั้งเดียว
  for (let i = 0; i < 20; i++) assert.equal(await call(), 'OK');

  // endpoint ที่ตายต้องถูกปิดถาวร ไม่ใช่แค่ breaker 30s
  const dead = pool.stats().filter((s) => s.disabled).map((s) => s.host).sort();
  assert.deepEqual(dead, [
    'endpoints.omniatech.io', 'solana-rpc.publicnode.com', 'solana.drpc.org',
  ].sort(), 'endpoint ที่ error rate 100% ต้องถูกปิดครบทุกตัว');
  assert.equal(pool.usable().length, 1);

  // การ probe endpoint ใหม่ต้อง "จบเร็ว": ตัวที่ตายถูกแตะได้ตัวละครั้งเดียว
  const probes = hits.filter((h) => h !== 'api.mainnet-beta.solana.com');
  assert.equal(probes.length, 3, `probe เกินจำเป็น: ${probes.join(',')}`);

  // หลัง converge แล้ว traffic ต้องไปที่ endpoint ที่ดีอย่างเดียว
  hits.length = 0;
  for (let i = 0; i < 20; i++) assert.equal(await call(), 'OK');
  assert.deepEqual([...new Set(hits)], ['api.mainnet-beta.solana.com']);
});

test('permanent error ไม่กิน retry budget', async () => {
  // ตายถาวร 3 ตัว + ตัวดีที่ transient-fail 1 ครั้งแรก
  let mainnetCalls = 0;
  const { call, hits } = makePool((host) => {
    if (host === 'api.mainnet-beta.solana.com') {
      return ++mainnetCalls === 1 ? new Error(ERR.serverErr) : 'OK';
    }
    if (host === 'solana.drpc.org') return new Error(ERR.drpc400);
    if (host === 'endpoints.omniatech.io') return new Error(ERR.omnia521);
    return new Error(ERR.publicnode403);
  });

  // ถ้า permanent นับเป็น retry (พฤติกรรมเดิม) 4 retries จะหมดก่อนถึงรอบที่ 2
  // ของ mainnet และ call() จะ throw
  assert.equal(await call(), 'OK');
  assert.equal(mainnetCalls, 2, 'ต้องได้ลอง mainnet ซ้ำหลัง transient fail');
  assert.ok(hits.length <= URLS.length + CONFIG.maxRetries);
});

test('pick() จัดลำดับด้วย error rate ไม่ใช่ inflight', () => {
  const pool = new RpcPool(URLS.slice(0, 2), 1000);
  const [bad, good] = pool.nodes;
  // bad: fail หมด แต่ inflight = 0 — ของเดิมเรียงด้วย inflight จึงเลือกตัวนี้ก่อน
  bad.calls = 50; bad.errors = 50; bad.inflight = 0;
  // good: สุขภาพดี แต่กำลังมีงานค้างอยู่
  good.calls = 50; good.errors = 1; good.inflight = 3;

  assert.equal(pool.pick(new Set()).host, good.host);
});

test('endpoint ที่ยังไม่เคยลอง ได้สิทธิ์ probe ก่อนตัวที่เคย error', () => {
  // ตรงข้ามกับเทสต์บน: rate 0 ของตัวที่ calls=0 ต้องไม่ถูกตีความว่า "แย่"
  // ไม่งั้น endpoint ที่ผู้ใช้เพิ่มเข้ามาใหม่จะไม่มีวันถูกใช้
  const pool = new RpcPool(URLS.slice(0, 2), 1000);
  const [fresh, used] = pool.nodes;
  used.calls = 10; used.errors = 3;
  assert.equal(pool.pick(new Set()).host, fresh.host);
});

test('exclude กัน retry ซ้ำ endpoint เดิมในคำขอเดียวกัน', () => {
  const pool = new RpcPool(URLS, 1000);
  const first = pool.pick(new Set());
  const second = pool.pick(new Set([first.host]));
  assert.notEqual(second.host, first.host);
});

test('ทุก endpoint ตายถาวร → error ชัดเจน ไม่วนไม่รู้จบ', async () => {
  const { pool, call } = makePool(() => new Error(ERR.publicnode403));
  await assert.rejects(call(), /ใช้ไม่ได้ทั้งหมด/);
  assert.equal(pool.usable().length, 0);
});

test('429 ถูก retry แบบ ratelimit ไม่ปิด endpoint ทิ้ง', async () => {
  let n = 0;
  const { pool, call } = makePool((host) => {
    if (host !== 'api.mainnet-beta.solana.com') return new Error(ERR.publicnode403);
    return ++n < 2 ? new Error(ERR.mainnet429) : 'OK';
  });
  assert.equal(await call(), 'OK');
  const mainnet = pool.stats().find((s) => s.host === 'api.mainnet-beta.solana.com');
  assert.equal(mainnet.disabled, false, '429 เป็นเรื่องชั่วคราว ห้ามปิด endpoint');
});
