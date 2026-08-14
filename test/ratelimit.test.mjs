/**
 * เทสต์ token bucket และการเคารพ Retry-After
 * รันด้วย: npm test
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { createRateLimiter, parseRetryAfter, RpcPool } from '../solana.js';

const URLS = ['https://a.example', 'https://b.example'];

test('parseRetryAfter อ่านได้ทั้งวินาทีและ HTTP-date', () => {
  assert.equal(parseRetryAfter('5'), 5000);
  assert.equal(parseRetryAfter('0'), 0);
  assert.equal(parseRetryAfter(null), 0);
  assert.equal(parseRetryAfter(''), 0);
  assert.equal(parseRetryAfter('ไม่ใช่เวลา'), 0);
  assert.equal(parseRetryAfter('-3'), 0, 'ค่าติดลบต้องไม่ทำให้ delay เพี้ยน');

  const ms = parseRetryAfter(new Date(Date.now() + 4000).toUTCString());
  assert.ok(ms > 2500 && ms <= 4000, `HTTP-date ต้องได้ ~4s แต่ได้ ${ms}`);
  assert.equal(parseRetryAfter(new Date(Date.now() - 9000).toUTCString()), 0,
    'วันที่ในอดีตต้องเป็น 0 ไม่ใช่ค่าติดลบ');
});

test('burst ใช้ได้ทันที แล้วค่อยถูกจำกัดตามอัตรา', async () => {
  const acquire = createRateLimiter(20, 3); // 20/s, burst 3
  const t0 = Date.now();
  await acquire(); await acquire(); await acquire();
  assert.ok(Date.now() - t0 < 30, 'burst 3 ตัวแรกต้องผ่านทันที');

  await acquire(); // ตัวที่ 4 ต้องรอ ~50ms
  assert.ok(Date.now() - t0 >= 40, `ต้องถูกหน่วง แต่ใช้ ${Date.now() - t0}ms`);
});

test('อัตราเฉลี่ยไม่เกินเพดานเมื่อยิงพร้อมกัน', async () => {
  const rps = 25;
  const acquire = createRateLimiter(rps, 1);
  const t0 = Date.now();
  // ยิง 15 คำขอพร้อมกัน — สถานการณ์เดียวกับ concurrency limiter ปล่อยชุดใหม่
  await Promise.all(Array.from({ length: 15 }, () => acquire()));
  const elapsed = (Date.now() - t0) / 1000;
  const observed = 15 / Math.max(elapsed, 1e-6);
  assert.ok(observed <= rps * 1.35,
    `อัตราจริง ${observed.toFixed(1)}/s ต้องไม่เกิน ${rps}/s มากนัก`);
});

test('token bucket ไม่ปล่อย token เดียวให้ผู้เรียกพร้อมกันหลายตัว', async () => {
  const acquire = createRateLimiter(1000, 2);
  const order = [];
  await Promise.all([1, 2, 3, 4].map(async (i) => { await acquire(); order.push(i); }));
  assert.equal(order.length, 4, 'ทุกคำขอต้องได้ token ครบ ไม่มีตัวไหนค้าง');
});

test('regression: retry ต้องกิน quota ด้วย', async () => {
  // นี่คือหัวใจ — ถ้า retry ไม่ผ่าน rate limiter 429 หนึ่งครั้งจะสร้าง
  // request ชุดใหม่ที่ไปกิน quota ซ้ำจนเกิด 429 ต่อเนื่อง (log 05:55: 247 ครั้ง)
  let acquires = 0;
  const pool = new RpcPool(URLS, 1000, { rateLimit: async () => { acquires++; } });
  for (const n of pool.nodes) n.conn = {};

  let calls = 0;
  const res = await pool.call(async () => {
    if (++calls < 3) throw new Error('429 Too Many Requests: rate');
    return 'OK';
  });

  assert.equal(res, 'OK');
  assert.equal(calls, 3, 'ยิงจริง 3 ครั้ง (fail 2 + สำเร็จ 1)');
  assert.equal(acquires, 3, 'ต้อง acquire ครบทุก attempt ไม่ใช่แค่ครั้งแรก');
});

test('permanent error ก็ยังกิน quota เพราะยิงออกไปจริง', async () => {
  let acquires = 0;
  const pool = new RpcPool(URLS, 1000, { rateLimit: async () => { acquires++; } });
  for (const n of pool.nodes) n.conn = {};

  let calls = 0;
  const res = await pool.call(async () => {
    // endpoint แรกโดนปิด endpoint ที่สองตอบได้
    if (++calls === 1) throw new Error('403 Forbidden: Request blocked');
    return 'OK';
  });
  assert.equal(res, 'OK');
  assert.equal(acquires, calls, 'ทุก request ที่ออกไปต้องผ่าน limiter');
});

test('Retry-After จาก server ชนะ backoff ที่เราเดา', async () => {
  const pool = new RpcPool(URLS, 1000, { rateLimit: async () => {} });
  for (const n of pool.nodes) n.conn = {};

  // จำลองว่า fetch wrapper อ่าน Retry-After: 3 มาได้
  let calls = 0;
  const t0 = Date.now();
  const res = await pool.call(async () => {
    if (++calls === 1) {
      pool.nodes.forEach((n) => { n.retryAfterUntil = Date.now() + 3000; });
      throw new Error('429 Too Many Requests: rate');
    }
    return 'OK';
  });

  const waited = Date.now() - t0;
  assert.equal(res, 'OK');
  // backoff ปกติของ ratelimit รอบแรกคือ ~1.4–2.6s ส่วน Retry-After บอก 3s
  assert.ok(waited >= 2900, `ต้องรออย่างน้อย 3s ตาม Retry-After แต่รอ ${waited}ms`);
  assert.ok(waited < 6000, `ต้องไม่รอนานเกินเหตุ (รอ ${waited}ms)`);
});

test('Retry-After ที่หมดอายุแล้วต้องไม่หน่วง retry ในอนาคต', async () => {
  const pool = new RpcPool(URLS, 1000, { rateLimit: async () => {} });
  for (const n of pool.nodes) n.conn = {};
  // จำลอง 429 เมื่อ 30 วินาทีก่อนที่บอก Retry-After: 20s — หมดอายุไปแล้ว
  pool.nodes.forEach((n) => { n.retryAfterUntil = Date.now() - 10_000; });

  let calls = 0;
  const t0 = Date.now();
  const res = await pool.call(async () => {
    if (++calls === 1) throw new Error('500 Internal Server Error: x');
    return 'OK';
  });

  assert.equal(res, 'OK');
  // ต้องใช้ backoff ปกติของ transient (~0.35–0.65s) ไม่ใช่ค่าเก่าที่ค้าง
  const waited = Date.now() - t0;
  assert.ok(waited < 2000, `ค่าที่หมดอายุไม่ควรหน่วง แต่รอ ${waited}ms`);
  // node ที่ค่าค้างอยู่แต่ไม่ได้ error ไม่จำเป็นต้องถูกล้าง เพราะเป็นเวลาในอดีต
  // สิ่งที่ต้องไม่เกิดคือมี node ไหนถือเวลาในอนาคตทั้งที่ไม่มี 429 เกิดขึ้น
  assert.ok(pool.nodes.every((n) => n.retryAfterUntil <= Date.now()),
    'ต้องไม่มี node ไหนถูกหน่วงค้างไว้');
});
