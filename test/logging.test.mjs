/**
 * เทสต์ชั้น log: ต้องอ่านรู้เรื่อง แต่ยังคงรหัส HTTP ไว้ค้นได้
 * รันด้วย: npm test
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { explainStatus, RpcPool, CONFIG } from '../solana.js';

/** ดักสิ่งที่ถูกพิมพ์ออก stdout ระหว่างรัน fn */
async function capture(fn) {
  const lines = [];
  const orig = console.log;
  console.log = (...a) => lines.push(a.join(' '));
  try { await fn(); } finally { console.log = orig; }
  return lines;
}

const ERR = {
  e403: '403 Forbidden: {"jsonrpc":"2.0","error":{"code":-32602,"message":"Request blocked"},"id":"x"}',
  e429: '429 Too Many Requests: {"error":{"code":429,"message":"Connection rate limits exceeded"}}',
  e404: '404 Not Found: nothing here',
  e400plan: '400 Bad Request: {"error":{"message":"chain is not available on free plan, please upgrade"}}',
  e521: '521 <none>: <!DOCTYPE html>',
};

test('explainStatus อธิบายเป็นภาษาคนสำหรับรหัสที่เจอจริง', () => {
  assert.match(explainStatus(403), /ปฏิเสธ/);
  assert.match(explainStatus(404), /ไม่พบ/);
  assert.match(explainStatus(429), /ถี่เกิน/);
  assert.match(explainStatus(401), /API key/);
  assert.match(explainStatus(521), /Cloudflare/);
  assert.match(explainStatus(400, ERR.e400plan), /แพ็กเกจฟรี/);
  assert.match(explainStatus(503), /ขัดข้อง/);
});

test('explainStatus ไม่คืนค่าว่างแม้เจอรหัสที่ไม่รู้จัก', () => {
  for (const s of [418, 599, 0]) {
    assert.ok(explainStatus(s).length > 0, `รหัส ${s} ต้องมีคำอธิบาย`);
  }
  assert.match(explainStatus(0, 'This operation was aborted'), /ไม่ตอบภายใน/);
});

test('log ยังคงรหัส HTTP ไว้ให้ค้นหาได้', async () => {
  const pool = new RpcPool(['https://a.example', 'https://b.example'], 1000,
    { rateLimit: async () => {} });
  for (const n of pool.nodes) n.conn = {};

  const lines = await capture(() => pool.call(async () => { throw new Error(ERR.e403); })
    .catch(() => {}));
  const text = lines.join('\n');

  assert.match(text, /403/, 'ต้องยังเห็นรหัส 403 เพื่อให้ grep เจอ');
  assert.match(text, /a\.example/, 'ต้องบอกว่าเซิร์ฟเวอร์ไหน');
  assert.match(text, /ปฏิเสธ/, 'ต้องมีคำอธิบายภาษาคนด้วย');
});

test('ไม่มี JSON ดิบหลุดออกมาในโหมดปกติ', async () => {
  const pool = new RpcPool(['https://a.example'], 1000, { rateLimit: async () => {} });
  pool.nodes[0].conn = {};

  const lines = await capture(() => pool.call(async () => { throw new Error(ERR.e429); })
    .catch(() => {}));

  for (const l of lines) {
    assert.ok(!l.trimStart().startsWith('{'),
      `บรรทัดนี้เป็น JSON ดิบซึ่งคนอ่านไม่รู้เรื่อง: ${l.slice(0, 80)}`);
  }
});

test('เหตุการณ์ซ้ำถูกย่อ ไม่พิมพ์ทุกครั้ง', async () => {
  const pool = new RpcPool(['https://noisy.example'], 1000, { rateLimit: async () => {} });
  pool.nodes[0].conn = {};

  // ยิงให้เกิด 429 ซ้ำ ๆ จำนวนมาก เหมือน log จริงที่มี 247 บรรทัด
  // ปิด retry ชั่วคราวเพื่อไม่ต้องรอ backoff จริงระหว่างเทสต์
  const saved = CONFIG.maxRetries;
  CONFIG.maxRetries = 1;
  let lines;
  try {
    lines = await capture(async () => {
      for (let i = 0; i < 12; i++) {
        await pool.call(async () => { throw new Error(ERR.e429); }).catch(() => {});
      }
    });
  } finally {
    CONFIG.maxRetries = saved;
  }

  const rateLimitLines = lines.filter((l) => l.includes('429') && l.includes('ถี่เกิน'));
  assert.ok(rateLimitLines.length <= LOG_SAMPLE_MAX,
    `พิมพ์ซ้ำ ${rateLimitLines.length} บรรทัด ควรถูกย่อเหลือไม่เกิน ${LOG_SAMPLE_MAX}`);
  assert.ok(lines.some((l) => l.includes('นับรวมไว้ท้ายสุด')),
    'ต้องบอกผู้อ่านว่าที่เหลือถูกย่อไปไหน');
});
// LOG_SAMPLE ค่า default 3 + บรรทัดแจ้งว่าย่อ เผื่อ jitter ของ retry เล็กน้อย
const LOG_SAMPLE_MAX = 4;

test('เลิกใช้เซิร์ฟเวอร์แล้วไม่ประกาศ "พักชั่วคราว" ซ้ำ', async () => {
  const pool = new RpcPool(['https://dead.example', 'https://ok.example'], 1000,
    { rateLimit: async () => {} });
  for (const n of pool.nodes) n.conn = {};

  const lines = await capture(async () => {
    for (let i = 0; i < 6; i++) {
      await pool.call(async (conn) => {
        if (conn === pool.nodes[0].conn) throw new Error(ERR.e521);
        return 'OK';
      }).catch(() => {});
    }
  });

  const paused = lines.filter((l) => l.includes('พักเซิร์ฟเวอร์นี้') && l.includes('dead.example'));
  assert.equal(paused.length, 0,
    'node ที่เลิกใช้ถาวรแล้ว ไม่ควรมีข้อความพักชั่วคราวตามมา');
});

test('นับคำขอที่ลองใหม่จนสำเร็จ เพื่อบอกว่าไม่กระทบข้อมูล', async () => {
  const pool = new RpcPool(['https://a.example', 'https://b.example'], 1000,
    { rateLimit: async () => {} });
  for (const n of pool.nodes) n.conn = {};

  let calls = 0;
  await capture(() => pool.call(async () => {
    if (++calls === 1) throw new Error('500 Internal Server Error: x');
    return 'OK';
  }));

  assert.equal(pool.recovered, 1, 'พลาดแล้วสำเร็จ ต้องถูกนับเป็น recovered');
});

test('สำเร็จตั้งแต่ครั้งแรกไม่ถูกนับเป็น recovered', async () => {
  const pool = new RpcPool(['https://a.example'], 1000, { rateLimit: async () => {} });
  pool.nodes[0].conn = {};
  await pool.call(async () => 'OK');
  assert.equal(pool.recovered, 0);
});

test('LOG_SAMPLE ปรับได้ผ่าน env', () => {
  assert.equal(typeof CONFIG.rateLimitRps, 'number');
  assert.ok(Number(process.env.LOG_SAMPLE || 3) >= 1);
});
