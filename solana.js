/**
 * SOLANA TOKEN TRACKER v2 — Public RPC, No API Key
 * ------------------------------------------------
 * กลยุทธ์: เลิกใช้ getProgramAccounts (getTokenAccountsByOwner) ซึ่งเป็น query
 * ที่หนักที่สุดและถูก public RPC throttle/drop เกือบตลอด
 * เปลี่ยนเป็นคำนวณ ATA แบบ offline แล้วดึงยอดด้วย getMultipleAccounts (เบามาก)
 *
 * จุดแก้หลักจากเวอร์ชันเดิม:
 *  1. AbortController — timeout ยกเลิก TCP connection จริง (แก้ socket leak)
 *  2. RpcPool + circuit breaker — failover ระหว่างรัน ไม่ใช่แค่ตอน init
 *  3. ATA + getMultipleAccounts — เร็วขึ้น ~50-90 เท่า
 *  4. ตรวจ program ของ mint จริง — ไม่ต้องเดา SPL vs Token-2022
 *  5. Coverage guard — ไม่ clear() sheet ถ้ารอบนี้ข้อมูลไม่ครบ
 *  6. ไม่ซ่อน error อีกต่อไป — timeout ถูกนับเป็น error เสมอ
 *
 * v2.1 — แก้ตาม log รอบ 2026-08-14 (coverage 53.6%)
 *  1. ตัด endpoint ที่ error rate 100% ออกจาก default list
 *  2. keep-alive agent + จำกัด socket — แก้ 429 "Connection rate limits exceeded"
 *  3. แยก permanent error (403/521/free-plan) ออกจาก transient — ปิด endpoint ถาวร
 *     และไม่ให้กิน retry budget
 *  4. pick() จัดลำดับด้วย error rate + ไม่ retry ซ้ำ endpoint เดิมในคำขอเดียวกัน
 *
 * v2.2
 *  5. batch ที่พังไม่ทิ้งผลของ batch อื่น — รายงานเป็น partial
 *  6. coverage นับ mint ที่ resolve ไม่ได้ + batch ที่พัง (ของเดิมเห็นแค่ wallet)
 *  7. เลขลำดับ log ใช้ index ของ wallet ไม่ใช่ลำดับที่ทำเสร็จ
 */

import { Connection, PublicKey } from '@solana/web3.js';
import {
  TOKEN_PROGRAM_ID,
  TOKEN_2022_PROGRAM_ID,
  getAssociatedTokenAddressSync,
  unpackAccount,
  unpackMint,
} from '@solana/spl-token';
import { GoogleSpreadsheet } from 'google-spreadsheet';
import { JWT } from 'google-auth-library';
import dotenv from 'dotenv';
import { pathToFileURL } from 'node:url';
dotenv.config();

// ============================================================
// CONFIG
// ============================================================

const c = {
  reset: '\x1b[0m', bright: '\x1b[1m', dim: '\x1b[2m',
  cyan: '\x1b[36m', green: '\x1b[32m', yellow: '\x1b[33m',
  red: '\x1b[31m', gray: '\x1b[90m', magenta: '\x1b[35m',
  blue: '\x1b[34m', white: '\x1b[97m',
};

/**
 * GitHub Actions รองรับ ANSI เต็มรูปแบบ แต่ไม่ใช่ TTY
 * การเช็ค isTTY อย่างเดียวจึงปิดสีทิ้งทั้งที่ใช้ได้ — ต้องเช็ค CI ด้วย
 * ปิดสีได้ด้วย NO_COLOR=1 (มาตรฐาน no-color.org) บังคับเปิดด้วย FORCE_COLOR=1
 */
const USE_COLOR = (() => {
  if (process.env.NO_COLOR) return false;
  if (process.env.FORCE_COLOR) return process.env.FORCE_COLOR !== '0';
  return Boolean(process.stdout.isTTY || process.env.GITHUB_ACTIONS || process.env.CI);
})();
if (!USE_COLOR) for (const k of Object.keys(c)) c[k] = '';

/** LOG_JSON=1 กลับไปใช้ JSON บรรทัดเดียวสำหรับเครื่องอ่าน/grep */
const LOG_JSON = process.env.LOG_JSON === '1';
/** แสดงเหตุการณ์ซ้ำ ๆ กี่ครั้งก่อนจะย่อเหลือแค่ตัวนับ */
const LOG_SAMPLE = Number(process.env.LOG_SAMPLE || 3);

const GOOGLE_SERVICE_ACCOUNT_EMAIL = process.env.GOOGLE_SERVICE_ACCOUNT_EMAIL;
const GOOGLE_PRIVATE_KEY = process.env.GOOGLE_PRIVATE_KEY
  ? process.env.GOOGLE_PRIVATE_KEY.replace(/\\n/g, '\n') : '';
const SPREADSHEET_ID = process.env.SPREADSHEET_ID;

const SHEET_TAB_NAME = 'Solana_Tracker';
const SUBSCRIPTION_SPL_TAB = 'SUBSCRIPTION SPL';
const SUBSCRIPTION_WALLET_TAB = 'SUBSCRIPTION WALLET';
const SHEET_HEADERS = ['Symbol', 'Network', 'Token Mint', 'Amount',
  'Wallet Name', 'Wallet Address', 'Timestamp'];

const CONFIG = {
  // timeout ต่อ 1 HTTP request — 15s พอเหลือเฟือ (จาก log เดิม request ที่สำเร็จใช้ ~1s)
  rpcTimeoutMs: Number(process.env.RPC_TIMEOUT_MS || 15000),
  // จำนวน request ที่ยิงพร้อมกัน — เริ่มที่ 4 แล้วค่อยจูนขึ้น/ลงตาม 429
  concurrency: Number(process.env.CONCURRENCY || 4),
  // จำนวน retry ต่อ 1 request (แต่ละครั้งอาจสลับ endpoint)
  maxRetries: Number(process.env.MAX_RETRIES || 4),
  // ต้องสแกน wallet สำเร็จอย่างน้อยกี่ % ถึงจะยอมเขียนทับ Sheet
  minCoverage: Number(process.env.MIN_COVERAGE || 0.95),
  // เปิด full scan (getTokenAccountsByOwner) เพื่อจับ non-ATA — ช้า ใช้เฉพาะตอนตรวจสอบ
  fullScan: process.env.FULL_SCAN === '1',
  // circuit breaker: fail ติดกันกี่ครั้งถึงพัก endpoint นั้น และพักนานเท่าไหร่
  breakerThreshold: 3,
  breakerCooldownMs: 30000,
  // ขนาด batch ของ getMultipleAccounts (ข้อจำกัด RPC = 100)
  batchSize: 100,
  // จำนวน TCP socket สูงสุดต่อ 1 origin
  maxSockets: Number(process.env.MAX_SOCKETS || 6),
  /**
   * เพดาน request ต่อวินาทีทั้งโปรเซส (นับ retry ด้วย)
   *
   * api.mainnet-beta จำกัด 40 request/10s ต่อ IP สำหรับ RPC method เดียว
   * log 2026-08-14 05:55 ยิงไป 830 calls ใน 210s = 39.5/10s → ชนเพดานพอดี
   * และ 247 ครั้ง (30%) กลายเป็น 429 ที่ต้อง retry
   * 3.3 req/s = 33/10s เหลือ headroom ให้ retry โดยไม่ทะลุเพดาน
   */
  rateLimitRps: Number(process.env.RATE_LIMIT_RPS || 3.3),
  rateBurst: Number(process.env.RATE_BURST || 5),
};

/**
 * Public RPC ที่ไม่ต้องใช้ API Key
 *
 * ตัดออกจาก default list เพราะ log 2026-08-14 พบ error rate 100%:
 *   - solana.drpc.org        → 400 "chain is not available on free plan" (83/83)
 *   - endpoints.omniatech.io → 521 Cloudflare origin down (80/80)
 *   - solana-rpc.publicnode.com → 403 "Request blocked" (26/26) บล็อก Azure IP
 *     ของ GitHub runner และถ่วงเวลา ~3s ก่อนตอบ
 * เหลือ endpoint เดียวที่ใช้ได้จริง ดีกว่ามี 4 ตัวที่ 3 ตัวกิน retry budget ทิ้ง
 *
 * เพิ่ม endpoint เองได้ทาง env EXTRA_RPCS (คั่นด้วย comma) หรือแท็บ "nodes" ใน Sheet
 */
const DEFAULT_RPCS = [
  'https://api.mainnet-beta.solana.com',
];

const EXTRA_RPCS = String(process.env.EXTRA_RPCS || '')
  .split(',').map((s) => s.trim()).filter(Boolean);

/**
 * Host ที่ "รู้แล้วว่าใช้ไม่ได้จาก GitHub runner" — ข้ามตั้งแต่ต้นไม่ต้องลองซ้ำ
 *
 * ต่างจากการปิด endpoint ระหว่างรัน (ซึ่งต้องเสียคำขอไป 1 ครั้งเพื่อค้นพบ):
 * publicnode ถ่วงเวลา ~3.1 วินาทีก่อนตอบ 403 ทุกครั้ง จึงกินเวลาเริ่มงาน
 * ทุกรอบทั้งที่รู้ผลอยู่แล้ว
 *
 * ทำที่โค้ดเพื่อให้แถวใน Sheet ที่ยังค้างอยู่ไม่มีผล — คนใช้ Sheet ไม่ต้องตามลบ
 * ถ้าอยากลองอีกครั้ง (เช่นย้ายไปรันที่อื่นที่ IP ไม่ถูกบล็อก) ตั้ง ALLOW_BLOCKED_RPCS=1
 */
const KNOWN_BLOCKED_HOSTS = new Map([
  ['solana-rpc.publicnode.com', 'บล็อก IP ของ GitHub runner (403) และถ่วงเวลา ~3 วินาทีก่อนตอบ'],
  ['solana.drpc.org', 'แพ็กเกจฟรีไม่รองรับ Solana อีกแล้ว (400)'],
  ['endpoints.omniatech.io', 'เซิร์ฟเวอร์ปลายทางไม่ตอบ (521)'],
]);

const ALLOW_BLOCKED_RPCS = process.env.ALLOW_BLOCKED_RPCS === '1';

/** คัด host ที่รู้ว่าใช้ไม่ได้ออก พร้อมบอกเหตุผลให้ชัดว่าทำไมถูกข้าม */
function filterKnownBlocked(urls) {
  if (ALLOW_BLOCKED_RPCS) return urls;
  const kept = [];
  for (const u of urls) {
    let host = '';
    try { host = new URL(u).host; } catch { /* ปล่อยผ่านให้ RpcPool จัดการ */ }
    const reason = KNOWN_BLOCKED_HOSTS.get(host);
    if (reason) {
      console.log(`  ${c.yellow}⏭${c.reset} ${c.gray}${host.padEnd(30)}${c.reset} ` +
        `ข้ามเพราะ${reason} ${c.dim}(บังคับใช้ได้ด้วย ALLOW_BLOCKED_RPCS=1)${c.reset}`);
      continue;
    }
    kept.push(u);
  }
  return kept;
}

// ============================================================
// UTILS
// ============================================================

const delay = (ms) => new Promise((r) => setTimeout(r, ms));

function formatDate(date) {
  const fmt = new Intl.DateTimeFormat('en-US', {
    timeZone: 'Asia/Bangkok',
    year: 'numeric', month: '2-digit', day: '2-digit',
    hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false,
  });
  const p = {};
  fmt.formatToParts(date).forEach((x) => (p[x.type] = x.value));
  return `${p.month}/${p.day}/${p.year} ${p.hour}:${p.minute}:${p.second}`;
}

/** แปลง bigint + decimals -> number โดยไม่เสีย precision ระหว่างทาง */
function toUiAmount(raw, decimals) {
  if (decimals === 0) return Number(raw);
  const s = raw.toString().padStart(decimals + 1, '0');
  const int = s.slice(0, s.length - decimals);
  const frac = s.slice(s.length - decimals);
  return Number(`${int}.${frac}`);
}

/**
 * สัดส่วนข้อมูลที่ได้จริงในรอบนี้ = (mint ที่ resolve ได้) × (ช่องที่ตรวจได้)
 * แยกออกมาเป็นฟังก์ชันบริสุทธิ์เพื่อให้เทสต์ตรรกะ guard ได้โดยไม่ต้องยิง RPC
 */
function computeCoverage({ tokenCount, mintFailed, cellsChecked, cellsAttempted }) {
  const mint = tokenCount ? 1 - mintFailed / tokenCount : 1;
  const cells = cellsAttempted ? cellsChecked / cellsAttempted : 0;
  return { mint, cells, total: mint * cells };
}

/**
 * Token bucket จำกัด "อัตรา" request ทั้งโปรเซส
 *
 * ต่างจาก concurrency limiter: การลด concurrency ไม่ได้จำกัดอัตรา เพราะคำขอที่
 * ตอบเร็วจะถูกยิงชุดใหม่ต่อทันที จึงยังทะลุเพดานต่อวินาทีของ RPC ได้
 * ตัวนี้ต้องครอบ retry ด้วย ไม่งั้น 429 จะสร้าง retry ที่ไปกิน quota ซ้ำ
 */
function createRateLimiter(ratePerSec, burst) {
  let tokens = burst;
  let last = Date.now();
  let chain = Promise.resolve();

  const take = async () => {
    for (;;) {
      const now = Date.now();
      tokens = Math.min(burst, tokens + ((now - last) / 1000) * ratePerSec);
      last = now;
      if (tokens >= 1) { tokens -= 1; return; }
      await delay(Math.max(5, Math.ceil(((1 - tokens) / ratePerSec) * 1000)));
    }
  };

  // ต่อคิวเป็นสายเดียว กัน caller พร้อมกันหลายตัวเห็น token เดียวกันแล้วหยิบซ้ำ
  return () => (chain = chain.then(take));
}

/** แปลงค่า Retry-After (วินาที หรือ HTTP-date) เป็น ms — คืน 0 ถ้าอ่านไม่ได้ */
function parseRetryAfter(value) {
  if (!value) return 0;
  const secs = Number(value);
  if (Number.isFinite(secs)) return Math.max(0, secs * 1000);
  const at = Date.parse(value);
  return Number.isNaN(at) ? 0 : Math.max(0, at - Date.now());
}

/** ตัวจำกัด concurrency แบบ inline — ไม่ต้องลง p-limit เพิ่ม */
function createLimiter(max) {
  let active = 0;
  const queue = [];
  const next = () => {
    if (active >= max || queue.length === 0) return;
    active++;
    const { fn, resolve, reject } = queue.shift();
    Promise.resolve()
      .then(fn)
      .then(resolve, reject)
      .finally(() => { active--; next(); });
  };
  return (fn) => new Promise((resolve, reject) => {
    queue.push({ fn, resolve, reject });
    next();
  });
}

/** log แบบ JSON บรรทัดเดียว — ใช้เมื่อ LOG_JSON=1 หรือสำหรับเครื่องอ่าน */
function jlog(obj) {
  console.log(JSON.stringify({ ts: new Date().toISOString(), ...obj }));
}

// ============================================================
// LOGGING — อ่านรู้เรื่องโดยไม่ต้องอ่านโค้ด
// ============================================================

/**
 * แปลรหัส HTTP เป็นภาษาคน โดย "คงรหัสไว้" เสมอ
 * คนอ่าน log ควรรู้ว่าเกิดอะไรขึ้นโดยไม่ต้องไปเปิดเอกสาร แต่ยังค้นด้วยรหัสได้
 */
function explainStatus(status, msg = '') {
  switch (status) {
    case 400:
      if (/free plan|upgrade/i.test(msg)) return 'แพ็กเกจฟรีของผู้ให้บริการไม่รองรับเครือข่ายนี้';
      if (/method not found|unsupported/i.test(msg)) return 'เซิร์ฟเวอร์ไม่รองรับคำสั่งที่เราเรียก';
      return 'คำขอไม่ถูกต้อง';
    case 401: return 'ต้องมี API key ถึงจะใช้ได้';
    case 402: return 'ต้องเสียเงินถึงจะใช้ได้';
    case 403: return 'เซิร์ฟเวอร์ปฏิเสธคำขอ (มักเป็นการบล็อก IP)';
    case 404: return 'ไม่พบปลายทาง (URL อาจผิด)';
    case 429: return 'เราส่งคำขอถี่เกินที่เซิร์ฟเวอร์ยอมให้';
    case 451: return 'ถูกจำกัดด้วยเหตุผลทางกฎหมาย';
    case 500: case 502: case 503: case 504:
      return 'เซิร์ฟเวอร์ขัดข้องชั่วคราว';
    default:
      if (status >= 520 && status <= 527) return 'เซิร์ฟเวอร์ปลายทางไม่ตอบ (Cloudflare)';
      if (status) return `เซิร์ฟเวอร์ตอบรหัส ${status}`;
      if (/abort/i.test(msg)) return `ไม่ตอบภายใน ${CONFIG.rpcTimeoutMs / 1000} วินาที`;
      return 'ติดต่อเซิร์ฟเวอร์ไม่สำเร็จ';
  }
}

/** เหตุการณ์ซ้ำ ๆ ถูกนับรวมแทนการพิมพ์ทุกครั้ง — 247 บรรทัดเดิม ๆ ไม่ช่วยใคร */
const tally = new Map();

function bumpTally(key, meta) {
  const cur = tally.get(key) || { count: 0, ...meta };
  cur.count++;
  tally.set(key, cur);
  return cur.count;
}

/** พิมพ์ 1 บรรทัดแบบคอลัมน์คงที่: ไอคอน | รหัส | เซิร์ฟเวอร์ | คำอธิบาย */
function logEvent({ icon, color, status, host, text, detail }) {
  const code = status ? String(status) : '—';
  const line =
    `  ${color}${icon}${c.reset} ` +
    `${color}${code.padEnd(3)}${c.reset} ` +
    `${c.gray}${String(host).padEnd(30)}${c.reset} ` +
    `${text}` +
    (detail ? ` ${c.dim}${detail}${c.reset}` : '');
  console.log(line);
}

/** ไอคอน + สี ต่อชนิดเหตุการณ์ */
const STYLE = {
  retry:    { icon: '↻', color: c.yellow },
  ratelimit:{ icon: '⏳', color: c.yellow },
  disabled: { icon: '⏭', color: c.red },
  paused:   { icon: '⏸', color: c.yellow },
  failed:   { icon: '✖', color: c.red },
};

function logRpcEvent(kind, { host, status, msg, text, detail, evt, extra }) {
  if (LOG_JSON) {
    jlog({ evt, host, status, ...extra, msg: (msg || '').slice(0, 160) });
    return;
  }
  const key = `${evt}|${host}|${status || 0}`;
  const n = bumpTally(key, { evt, host, status, text });
  const s = STYLE[kind] || STYLE.retry;

  if (n <= LOG_SAMPLE) {
    logEvent({ ...s, status, host, text, detail });
  } else if (n === LOG_SAMPLE + 1) {
    console.log(`  ${c.dim}⋯ เหตุการณ์แบบเดียวกันนี้จะถูกนับรวมไว้ท้ายสุด แทนการพิมพ์ซ้ำ${c.reset}`);
  }
}

/** ตารางสรุปเหตุการณ์ทั้งหมดที่เกิดขึ้น — แทนการไล่อ่าน log ทีละบรรทัด */
function printTally() {
  if (!tally.size) return;
  console.log(`\n${c.bright}เหตุการณ์ที่พบระหว่างทำงาน${c.reset}`);
  const rows = [...tally.values()].sort((a, b) => b.count - a.count);
  for (const r of rows) {
    const code = r.status ? String(r.status) : '—';
    const color = r.status === 429 ? c.yellow : r.status ? c.red : c.gray;
    console.log(
      `  ${color}${code.padEnd(3)}${c.reset} ` +
      `${c.gray}${String(r.host).padEnd(30)}${c.reset} ` +
      `${String(r.count).padStart(4)} ครั้ง  ${c.dim}${r.text}${c.reset}`
    );
  }
}

/** คำอธิบายสัญลักษณ์ — พิมพ์ครั้งเดียวก่อนเริ่มงานจริง */
function printLegend() {
  if (LOG_JSON) return;
  console.log(`${c.dim}สัญลักษณ์: ` +
    `${c.green}✓${c.dim} สำเร็จ  ` +
    `${c.yellow}⚠${c.dim} ได้ข้อมูลบางส่วน  ` +
    `${c.red}✖${c.dim} ล้มเหลว  ` +
    `${c.yellow}↻${c.dim} ลองใหม่  ` +
    `${c.yellow}⏳${c.dim} ชะลอความเร็ว  ` +
    `${c.yellow}⏸${c.dim} พักชั่วคราว  ` +
    `${c.red}⏭${c.dim} เลิกใช้เซิร์ฟเวอร์นี้${c.reset}`);
}

/**
 * global fetch ของ Node ใช้ undici ที่ "ไม่จำกัด" จำนวน connection ต่อ origin
 * ทำให้ยิงพร้อมกันทีเดียวหลาย socket แล้วโดน 429 "Connection rate limits exceeded"
 * (เป็น limit ที่นับ "การเปิด connection ใหม่" ไม่ใช่จำนวน request)
 * แก้ด้วยการจำกัด socket + เปิด keep-alive ยาว ๆ เพื่อ reuse socket เดิม
 */
async function setupHttpAgent() {
  try {
    const { Agent, setGlobalDispatcher } = await import('undici');
    setGlobalDispatcher(new Agent({
      connections: CONFIG.maxSockets,
      pipelining: 1,
      keepAliveTimeout: 60_000,
      keepAliveMaxTimeout: 300_000,
      connect: { timeout: 10_000 },
    }));
    return true;
  } catch (e) {
    jlog({ evt: 'http_agent_unavailable', msg: (e?.message || '').slice(0, 160) });
    return false;
  }
}

/** ดึง HTTP status จาก error ของ web3.js ซึ่งขึ้นต้นด้วย "<code> <statusText>: <body>" */
function httpStatusOf(err) {
  const m = /^(\d{3})\s/.exec(String(err?.message || ''));
  return m ? Number(m[1]) : 0;
}

/**
 * แยกประเภท error — หัวใจของการไม่เผา retry budget ทิ้ง
 *   permanent → endpoint นี้จะไม่มีวันตอบได้ในรอบนี้ ปิดถาวร ไม่นับเป็น retry
 *   ratelimit → รอแล้วลองใหม่ได้ backoff นานหน่อย
 *   transient → network/timeout/5xx ปกติ retry ได้
 */
function classifyError(err) {
  const msg = String(err?.message || '');
  const status = httpStatusOf(err);

  if (err?.name === 'AbortError' || /abort/i.test(msg)) return 'transient';
  if (status === 429 || /too many requests|rate limit/i.test(msg)) return 'ratelimit';

  // ต้องมี API key / จ่ายเงิน / ถูกบล็อก / ไม่ให้บริการ chain นี้
  if ([401, 402, 403, 404, 410, 451].includes(status)) return 'permanent';
  // Cloudflare 52x = origin ของ provider ตาย ไม่ใช่ปัญหาชั่วคราวระดับวินาที
  if (status >= 520 && status <= 527) return 'permanent';
  // 400 ที่เป็นเรื่องแพ็กเกจ/ความสามารถของ endpoint (ไม่ใช่ request ของเราผิด)
  if (status === 400 && /free plan|not available|upgrade|unsupported|api key|method not found/i.test(msg)) {
    return 'permanent';
  }
  return 'transient';
}

// ============================================================
// RPC LAYER
// ============================================================

/**
 * หัวใจของการแก้ socket leak:
 * Promise.race แค่ทิ้ง promise ที่แพ้ ไม่ได้ปิด connection
 * AbortController ยกเลิก request จริง -> socket ถูกคืน pool เสมอ
 */
function makeConn(endpoint, timeoutMs, onRateLimited) {
  return new Connection(endpoint, {
    commitment: 'confirmed',
    disableRetryOnRateLimit: true, // กัน retry ซ้อน retry ของ web3.js
    fetch: async (url, opts) => {
      const ac = new AbortController();
      const timer = setTimeout(() => ac.abort(), timeoutMs);
      try {
        const res = await fetch(url, { ...opts, signal: ac.signal });
        // header ไปไม่ถึง error ของ web3.js จึงต้องดักตรงนี้
        if (res.status === 429) onRateLimited?.(parseRetryAfter(res.headers.get('retry-after')));
        return res;
      } finally {
        clearTimeout(timer);
      }
    },
  });
}

class RpcPool {
  constructor(urls, timeoutMs, opts = {}) {
    // rateLimit ถูกฉีดเข้ามาได้เพื่อให้เทสต์คุมเวลาเองโดยไม่ต้องรอจริง
    this.acquire = opts.rateLimit
      ?? createRateLimiter(CONFIG.rateLimitRps, CONFIG.rateBurst);
    this.throttled = 0; // จำนวนครั้งที่ RPC ส่ง Retry-After กลับมา
    this.recovered = 0; // คำขอที่พลาดแล้วลองใหม่จนสำเร็จ (ไม่กระทบข้อมูล)

    this.nodes = urls.map((u) => {
      const node = {
        url: u,
        host: (() => { try { return new URL(u).host; } catch { return u; } })(),
        // เก็บเป็น "เวลาสิ้นสุด" ไม่ใช่ระยะเวลา — ค่าที่ค้างจาก 429 เก่าจะเสื่อม
        // ไปเองตามเวลา แทนที่จะไปหน่วง retry ในอนาคตเต็มจำนวน
        retryAfterUntil: 0,
        consecFails: 0,
        openUntil: 0,
        disabled: false,
        disabledReason: '',
        inflight: 0,
        calls: 0,
        errors: 0,
        totalMs: 0,
      };
      node.conn = makeConn(u, timeoutMs, (ms) => {
        // จำค่าที่ server บอกมา ดีกว่าเดา backoff เอง
        if (ms > 0) node.retryAfterUntil = Date.now() + ms;
        this.throttled++;
      });
      return node;
    });
    if (!this.nodes.length) throw new Error('RpcPool: ไม่มี endpoint');
  }

  /** endpoint ที่ยังไม่ถูกปิดถาวร */
  usable() { return this.nodes.filter((n) => !n.disabled); }

  /**
   * เลือก endpoint โดยใช้ "error rate" เป็นเกณฑ์หลัก
   *
   * ของเดิมเรียงด้วย inflight ก่อน ซึ่งกลับหัวกลับหาง: endpoint ที่ตายจะมี
   * inflight = 0 เสมอ (fail ทันที) จึงถูกเลือกก่อนตัวที่ดีที่กำลังทำงานอยู่
   *
   * @param exclude Set ของ host ที่ลองไปแล้วในคำขอนี้ — กัน retry ซ้ำที่เดิม
   */
  pick(exclude) {
    const now = Date.now();
    const usable = this.usable();
    if (!usable.length) return null;

    const open = usable.filter((n) => now > n.openUntil);
    let pool = open.filter((n) => !exclude?.has(n.host));
    if (!pool.length) pool = open;          // ลองครบทุกตัวแล้ว → วนกลับตัวที่ดีที่สุด
    if (!pool.length) pool = usable;        // breaker เปิดหมด → ยอมใช้เท่าที่มี

    const rate = (n) => (n.calls ? n.errors / n.calls : 0);
    return pool.sort((a, b) =>
      rate(a) - rate(b) ||
      a.inflight - b.inflight ||
      a.calls - b.calls
    )[0];
  }

  ok(n) { n.consecFails = 0; }

  fail(n) {
    n.errors++;
    if (++n.consecFails >= CONFIG.breakerThreshold) {
      n.openUntil = Date.now() + CONFIG.breakerCooldownMs;
      n.consecFails = 0;
      // node ที่เลิกใช้ถาวรไปแล้ว ไม่ต้องประกาศว่าพักชั่วคราวซ้ำอีก
      if (!n.disabled) {
        logRpcEvent('paused', {
          evt: 'breaker_open', host: n.host,
          text: `พักเซิร์ฟเวอร์นี้ ${CONFIG.breakerCooldownMs / 1000} วินาที เพราะล้มเหลวติดกัน ${CONFIG.breakerThreshold} ครั้ง`,
          extra: { cooldownMs: CONFIG.breakerCooldownMs },
        });
      }
    }
  }

  /** ปิด endpoint ถาวรสำหรับรอบนี้ — ใช้กับ error ที่ retry ไปก็ไม่มีทางหาย */
  disable(n, reason, why) {
    if (n.disabled) return;
    n.disabled = true;
    n.disabledReason = reason;
    const left = this.usable().length;
    logRpcEvent('disabled', {
      evt: 'endpoint_disabled', host: n.host, status: Number(reason) || 0,
      text: `เลิกใช้เซิร์ฟเวอร์นี้ทั้งรอบ — ${why}`,
      detail: `(เหลือใช้ได้ ${left} เซิร์ฟเวอร์)`,
      extra: { reason, remaining: left },
    });
  }

  /** เรียก RPC พร้อม retry + สลับ endpoint + exponential backoff + jitter */
  async call(fn, label = 'rpc') {
    let lastErr;
    const tried = new Set();
    let attempt = 0;
    // guard กันวนไม่รู้จบ: retry budget + โอกาสปิด endpoint ที่ตายทีละตัว
    let guard = CONFIG.maxRetries + this.nodes.length + 1;

    while (attempt < CONFIG.maxRetries && guard-- > 0) {
      const n = this.pick(tried);
      if (!n) break;

      // สำคัญ: กั้นอัตราก่อน "ทุก" attempt รวม retry ด้วย
      // ไม่งั้น 429 หนึ่งครั้งจะสร้าง retry ที่ไปกิน quota ซ้ำจนเกิด 429 ต่อเนื่อง
      await this.acquire();

      n.inflight++;
      n.calls++;
      const t0 = Date.now();
      try {
        const res = await fn(n.conn);
        n.totalMs += Date.now() - t0;
        this.ok(n);
        if (tried.size) this.recovered++; // พลาดมาก่อน แต่จบด้วยความสำเร็จ
        return res;
      } catch (e) {
        n.totalMs += Date.now() - t0;
        lastErr = e;
        this.fail(n);
        tried.add(n.host);

        const kind = classifyError(e);
        const retryAfter = Math.max(0, n.retryAfterUntil - Date.now());
        n.retryAfterUntil = 0; // ใช้ครั้งเดียว
        const status = httpStatusOf(e);
        const why = explainStatus(status, e?.message);

        if (kind === 'permanent') {
          // endpoint นี้ใช้ไม่ได้เลย — ปิดทิ้งแล้วไปตัวถัดไปทันที
          // ไม่นับเป็น retry เพราะไม่ใช่ความผิดของ request
          this.disable(n, `${status || 'error'}`, why);
          continue;
        }

        attempt++;
        const last = attempt >= CONFIG.maxRetries;
        let wait = 0;
        if (!last) {
          const base = kind === 'ratelimit' ? 2000 : 500;
          const backoff = Math.min(15000, base * 2 ** (attempt - 1)) * (0.7 + Math.random() * 0.6);
          // ถ้า server บอก Retry-After มา ให้เชื่อ server ก่อนการเดาของเรา
          wait = Math.min(30000, Math.max(backoff, retryAfter));
        }

        logRpcEvent(kind === 'ratelimit' ? 'ratelimit' : 'retry', {
          evt: 'rpc_retry', host: n.host, status, msg: e?.message,
          text: why,
          detail: last
            ? `· ครบ ${CONFIG.maxRetries} ครั้งแล้ว ยอมแพ้ (${label})`
            : `· รอ ${(wait / 1000).toFixed(1)} วิ แล้วลองใหม่ (ครั้งที่ ${attempt}/${CONFIG.maxRetries})`,
          extra: { label, attempt, ms: Date.now() - t0, kind, retryAfterMs: retryAfter || undefined },
        });

        if (!last) await delay(wait);
      } finally {
        n.inflight--;
      }
    }

    if (!this.usable().length) {
      throw new Error('RPC endpoint ใช้ไม่ได้ทั้งหมด (ถูกปิดถาวรหมดแล้ว)');
    }
    throw lastErr ?? new Error('ไม่มี RPC endpoint ที่พร้อมใช้งาน');
  }

  stats() {
    return this.nodes.map((n) => ({
      host: n.host, calls: n.calls, errors: n.errors,
      avgMs: n.calls ? Math.round(n.totalMs / n.calls) : 0,
      disabled: n.disabled, disabledReason: n.disabledReason,
    }));
  }
}

// ============================================================
// MINT RESOLUTION
// ============================================================

/**
 * ตรวจว่าแต่ละ mint อยู่ใต้ program ไหน (SPL หรือ Token-2022) และมี decimals เท่าไหร่
 * ทำครั้งเดียวต่อรอบ -> ไม่ต้องยิง 2 รอบต่อ wallet อีกต่อไป
 * ใช้ getMultipleAccountsInfo (raw bytes) แล้ว decode เอง = ไม่พึ่ง jsonParsed ของ RPC
 */
async function resolveMints(pool, mints) {
  const out = new Map(); // mint(string) -> { programId, decimals }
  const limit = createLimiter(CONFIG.concurrency);
  const batches = [];
  for (let i = 0; i < mints.length; i += CONFIG.batchSize) {
    batches.push(mints.slice(i, i + CONFIG.batchSize));
  }

  let failed = 0;
  await Promise.all(batches.map((batch, bi) => limit(async () => {
    const keys = batch.map((m) => new PublicKey(m));
    try {
      const infos = await pool.call(
        (conn) => conn.getMultipleAccountsInfo(keys),
        `resolveMints[${bi}]`
      );
      infos.forEach((info, j) => {
        if (!info) return; // mint ไม่มีอยู่จริงบน chain
        const owner = info.owner;
        const isSpl = owner.equals(TOKEN_PROGRAM_ID);
        const is2022 = owner.equals(TOKEN_2022_PROGRAM_ID);
        if (!isSpl && !is2022) return; // ไม่ใช่ token mint
        try {
          const parsed = unpackMint(keys[j], info, owner);
          out.set(batch[j], { programId: owner, decimals: parsed.decimals });
        } catch { /* decode ไม่ได้ ข้ามไป */ }
      });
    } catch (e) {
      failed += batch.length;
      logRpcEvent('failed', {
        evt: 'resolve_mints_failed', host: `mint ชุดที่ ${bi + 1}`,
        status: httpStatusOf(e), msg: e?.message,
        text: `อ่านข้อมูลเหรียญไม่สำเร็จ — ${explainStatus(httpStatusOf(e), e?.message)}`,
        detail: `· ${batch.length} เหรียญในชุดนี้จะไม่ถูกตรวจ`,
        extra: { batch: bi },
      });
    }
  })));

  return { map: out, failed };
}

// ============================================================
// WALLET SCAN
// ============================================================

/**
 * สแกน 1 wallet ด้วย ATA — 599 mints = ~6 requests
 *
 * batch ที่พังจะไม่ทำให้ batch ที่สำเร็จไปแล้วสูญเปล่าอีกต่อไป
 * (ของเดิม throw ออกจาก loop = ทิ้งผลของ batch ก่อนหน้าทั้งหมด)
 * คืน attempted/checked เพื่อให้ coverage guard รู้ว่า "เช็คไปกี่ช่องจริง ๆ"
 */
async function scanWalletByAta(pool, wallet, mintInfoMap) {
  const owner = new PublicKey(wallet.address);

  // คำนวณ ATA address แบบ offline ทั้งหมด — ไม่ยิง RPC เลยในขั้นนี้
  const targets = [];
  for (const [mint, meta] of mintInfoMap) {
    let ata;
    try {
      ata = getAssociatedTokenAddressSync(
        new PublicKey(mint), owner, true, meta.programId
      );
    } catch { continue; }
    targets.push({ mint, ata, meta });
  }

  const found = [];
  let checked = 0;
  let failedBatches = 0;
  let totalBatches = 0;
  let lastError = null;

  for (let i = 0; i < targets.length; i += CONFIG.batchSize) {
    const batch = targets.slice(i, i + CONFIG.batchSize);
    totalBatches++;
    let infos;
    try {
      infos = await pool.call(
        (conn) => conn.getMultipleAccountsInfo(batch.map((t) => t.ata)),
        `scan:${wallet.name}`
      );
    } catch (e) {
      // เก็บผลของ batch อื่นไว้ แล้วไปต่อ — รายงานเป็น partial ทีหลัง
      failedBatches++;
      lastError = e;
      logRpcEvent('failed', {
        evt: 'batch_failed', host: wallet.name,
        status: httpStatusOf(e), msg: e?.message,
        text: `ตรวจเหรียญชุดที่ ${totalBatches} ไม่สำเร็จ — ${explainStatus(httpStatusOf(e), e?.message)}`,
        detail: `· ขาดไป ${batch.length} เหรียญ`,
        extra: { wallet: wallet.name, batch: totalBatches, size: batch.length },
      });
      continue;
    }
    checked += batch.length;
    infos.forEach((info, j) => {
      if (!info) return; // ATA ยังไม่ถูกสร้าง = ไม่ถือ token นี้
      const t = batch[j];
      try {
        const acc = unpackAccount(t.ata, info, t.meta.programId);
        if (acc.amount === 0n) return;
        found.push({
          mint: t.mint,
          amount: toUiAmount(acc.amount, t.meta.decimals),
        });
      } catch { /* ไม่ใช่ token account ที่ถูกต้อง */ }
    });
  }

  return {
    found, attempted: targets.length, checked,
    failedBatches, totalBatches, lastError,
  };
}

/**
 * โหมดตรวจสอบ (FULL_SCAN=1): ใช้ getTokenAccountsByOwner เพื่อจับ token ที่อยู่ใน
 * account ที่ไม่ใช่ ATA — ช้าและหนัก ใช้เฉพาะตอนอยากเทียบผลว่า ATA ครอบคลุมพอไหม
 */
async function scanWalletFull(pool, wallet, mintInfoMap) {
  const owner = new PublicKey(wallet.address);
  const programs = [TOKEN_PROGRAM_ID, TOKEN_2022_PROGRAM_ID];
  const found = [];
  let okPrograms = 0;
  let failedBatches = 0;
  let lastError = null;

  for (const programId of programs) {
    let res;
    try {
      res = await pool.call(
        (conn) => conn.getTokenAccountsByOwner(owner, { programId }),
        `full:${wallet.name}`
      );
    } catch (e) {
      // program หนึ่งพัง ไม่ควรทิ้งผลของอีก program — เหมือน batch ใน ATA mode
      failedBatches++;
      lastError = e;
      logRpcEvent('failed', {
        evt: 'batch_failed', host: wallet.name,
        status: httpStatusOf(e), msg: e?.message,
        text: `อ่านบัญชีของ ${programId.equals(TOKEN_PROGRAM_ID) ? 'SPL' : 'Token-2022'} ไม่สำเร็จ — ` +
          `${explainStatus(httpStatusOf(e), e?.message)}`,
        extra: { wallet: wallet.name, mode: 'full', program: programId.toBase58() },
      });
      continue;
    }
    okPrograms++;
    for (const { pubkey, account } of res.value) {
      try {
        const acc = unpackAccount(pubkey, account, programId);
        if (acc.amount === 0n) continue;
        const mint = acc.mint.toBase58();
        const meta = mintInfoMap.get(mint);
        if (!meta) continue; // ไม่ได้ subscribe mint นี้
        found.push({ mint, amount: toUiAmount(acc.amount, meta.decimals) });
      } catch { /* skip */ }
    }
  }

  // full scan ไม่ได้ไล่ทีละ mint จึงประมาณ "ช่องที่เช็คได้" จากสัดส่วน program ที่สำเร็จ
  const attempted = mintInfoMap.size;
  return {
    found, attempted,
    checked: Math.round((attempted * okPrograms) / programs.length),
    failedBatches, totalBatches: programs.length, lastError,
  };
}

// ============================================================
// GOOGLE SHEETS
// ============================================================

async function loadSheetsData(doc) {
  // --- custom RPC จากแท็บ nodes ---
  const customRpcs = [];
  const nodesSheet = doc.sheetsByTitle['nodes'];
  if (nodesSheet) {
    try {
      const maxRows = nodesSheet.rowCount;
      if (maxRows >= 2) {
        await nodesSheet.loadCells(`A1:C${maxRows}`);
        for (let r = 1; r < maxRows; r++) {
          const net = nodesSheet.getCell(r, 0)?.value;
          const url = nodesSheet.getCell(r, 1)?.value;
          if (String(net || '').toLowerCase() !== 'solana' || !url) continue;
          const u = String(url).trim();
          try { new URL(u); } catch {
            console.log(`${c.yellow}⚠ nodes: URL ไม่ถูกต้อง ข้าม -> ${u}${c.reset}`);
            continue;
          }
          customRpcs.push(u);
        }
      }
    } catch (e) {
      // ไม่ซ่อน error อีกต่อไป
      console.log(`${c.yellow}⚠ อ่านแท็บ nodes ไม่ได้: ${e.message}${c.reset}`);
    }
  }

  // --- wallets ---
  const wallets = [];
  const walletSheet = doc.sheetsByTitle[SUBSCRIPTION_WALLET_TAB];
  if (!walletSheet) throw new Error(`ไม่พบแท็บ "${SUBSCRIPTION_WALLET_TAB}"`);
  const wRows = walletSheet.rowCount;
  if (wRows >= 3) {
    await walletSheet.loadCells(`A1:B${wRows}`);
    for (let r = 2; r < wRows; r++) {
      const name = String(walletSheet.getCell(r, 0)?.value ?? '').trim() || 'Unknown';
      const addr = String(walletSheet.getCell(r, 1)?.value ?? '').trim();
      if (!addr) continue;
      try {
        new PublicKey(addr);
        wallets.push({ name, address: addr });
      } catch {
        console.log(`${c.yellow}⚠ wallet address ไม่ถูกต้อง แถว ${r + 1}: ${addr}${c.reset}`);
      }
    }
  }

  // --- tokens ---
  const tokenMap = new Map(); // mint -> symbol
  const subsSheet = doc.sheetsByTitle[SUBSCRIPTION_SPL_TAB];
  if (!subsSheet) throw new Error(`ไม่พบแท็บ "${SUBSCRIPTION_SPL_TAB}"`);
  const sRows = subsSheet.rowCount;
  if (sRows >= 2) {
    await subsSheet.loadCells(`A1:C${sRows}`);
    for (let r = 1; r < sRows; r++) {
      const sym = String(subsSheet.getCell(r, 0)?.value ?? '').trim() || 'Unknown';
      const raw = String(subsSheet.getCell(r, 2)?.value ?? '').trim();
      if (!raw) continue;
      let mints;
      try {
        mints = JSON.parse(raw);
        if (!Array.isArray(mints)) mints = [raw];
      } catch { mints = [raw]; }
      for (const m of mints) {
        const s = String(m).trim();
        try { new PublicKey(s); tokenMap.set(s, sym); } catch { /* skip */ }
      }
    }
  }

  return { customRpcs, wallets, tokenMap };
}

// ============================================================
// MAIN
// ============================================================

async function main() {
  const startTime = Date.now();

  console.log(`\n${c.cyan}${c.bright}╔══════════════════════════════════════════════════════════╗${c.reset}`);
  console.log(`${c.cyan}${c.bright}║  SOLANA TOKEN TRACKER v2  (Public RPC / ATA / No Key)    ║${c.reset}`);
  console.log(`${c.cyan}${c.bright}╚══════════════════════════════════════════════════════════╝${c.reset}\n`);

  if (!GOOGLE_SERVICE_ACCOUNT_EMAIL || !GOOGLE_PRIVATE_KEY || !SPREADSHEET_ID) {
    console.error(`${c.red}✗ ขาด env vars (GOOGLE_SERVICE_ACCOUNT_EMAIL / GOOGLE_PRIVATE_KEY / SPREADSHEET_ID)${c.reset}`);
    process.exit(1);
  }

  // ---- [1/5] Google Sheets ----
  console.log(`${c.cyan}[1/5] เชื่อมต่อ Google Sheets...${c.reset}`);
  const auth = new JWT({
    email: GOOGLE_SERVICE_ACCOUNT_EMAIL,
    key: GOOGLE_PRIVATE_KEY,
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
  });
  const doc = new GoogleSpreadsheet(SPREADSHEET_ID, auth);
  await doc.loadInfo();
  console.log(`${c.green}✓ เชื่อมต่อแล้ว: ${doc.title}${c.reset}`);

  // ---- [2/5] โหลด config จาก Sheets ----
  console.log(`${c.cyan}[2/5] โหลด wallets / tokens / nodes...${c.reset}`);
  const { customRpcs, wallets, tokenMap } = await loadSheetsData(doc);

  if (!wallets.length) {
    console.error(`${c.red}✗ ไม่พบ wallet ที่ใช้ได้${c.reset}`);
    process.exit(1);
  }
  if (!tokenMap.size) {
    console.error(`${c.red}✗ ไม่พบ token ที่ใช้ได้${c.reset}`);
    process.exit(1);
  }
  console.log(`${c.green}✓ ${wallets.length} wallet(s), ${tokenMap.size} token(s)${c.reset}`);

  // ---- [3/5] RPC Pool ----
  console.log(`${c.cyan}[3/5] ตั้งค่า RPC pool...${c.reset}`);
  const agentOk = await setupHttpAgent();
  const requested = [...new Set([...customRpcs, ...EXTRA_RPCS, ...DEFAULT_RPCS])];
  const rpcUrls = filterKnownBlocked(requested);
  if (!rpcUrls.length) {
    console.error(`${c.red}✗ ไม่เหลือเซิร์ฟเวอร์ที่ใช้ได้ — endpoint ทั้งหมดอยู่ในรายการที่ถูกข้าม${c.reset}`);
    console.error(`${c.gray}  เพิ่มตัวใหม่ทาง EXTRA_RPCS หรือแท็บ nodes ` +
      `หรือตั้ง ALLOW_BLOCKED_RPCS=1 เพื่อลองใช้ตัวที่ถูกข้าม${c.reset}`);
    process.exit(1);
  }
  const pool = new RpcPool(rpcUrls, CONFIG.rpcTimeoutMs);
  rpcUrls.forEach((u) => console.log(`  ${c.green}•${c.reset} ${c.gray}${u}${c.reset}`));
  console.log(`${c.gray}  timeout=${CONFIG.rpcTimeoutMs}ms concurrency=${CONFIG.concurrency} ` +
    `retries=${CONFIG.maxRetries} maxSockets=${CONFIG.maxSockets} keepAlive=${agentOk ? 'on' : 'off'} ` +
    `rate=${CONFIG.rateLimitRps}req/s${c.reset}`);
  if (!agentOk) {
    console.log(`${c.yellow}⚠ ตั้ง keep-alive agent ไม่ได้ (ไม่พบ undici) — เสี่ยงโดน 429 connection-rate${c.reset}`);
  }

  // ---- [4/5] ตรวจ program + decimals ของทุก mint ----
  console.log(`${c.cyan}[4/5] ตรวจ program/decimals ของ mint...${c.reset}`);
  const t0Mint = Date.now();
  const { map: mintInfoMap, failed: mintFailed } = await resolveMints(pool, [...tokenMap.keys()]);

  let n2022 = 0;
  for (const meta of mintInfoMap.values()) {
    if (meta.programId.equals(TOKEN_2022_PROGRAM_ID)) n2022++;
  }
  console.log(`${c.green}✓ resolve ได้ ${mintInfoMap.size}/${tokenMap.size} mint ` +
    `(SPL: ${mintInfoMap.size - n2022}, ${c.magenta}Token-2022: ${n2022}${c.green}) ` +
    `${c.dim}(${((Date.now() - t0Mint) / 1000).toFixed(1)}s)${c.reset}`);
  if (mintFailed) {
    console.log(`${c.yellow}⚠ resolve ไม่สำเร็จ ${mintFailed} mint — ยอดของ mint เหล่านี้จะไม่ถูกดึง${c.reset}`);
  }
  if (!mintInfoMap.size) {
    console.error(`${c.red}✗ resolve mint ไม่ได้เลย — RPC น่าจะมีปัญหาทั้งหมด${c.reset}`);
    process.exit(1);
  }
  // ถ้า endpoint ถูกปิดถาวรจนหมดตั้งแต่ขั้นนี้ สแกนต่อไปก็ล้มเหลวทุก wallet
  if (!pool.usable().length) {
    console.error(`${c.red}✗ RPC endpoint ถูกปิดถาวรทั้งหมด — หยุดก่อนสแกน${c.reset}`);
    pool.stats().forEach((s) => console.error(`${c.red}    ${s.host}: ${s.disabledReason}${c.reset}`));
    process.exit(1);
  }
  console.log(`${c.gray}  RPC ที่ใช้ได้: ${pool.usable().map((n) => n.host).join(', ')}${c.reset}`);

  // ---- [5/5] เตรียม sheet ----
  console.log(`${c.cyan}[5/5] เตรียม sheet ปลายทาง...${c.reset}`);
  let sheet = doc.sheetsByTitle[SHEET_TAB_NAME];
  if (!sheet) {
    sheet = await doc.addSheet({ title: SHEET_TAB_NAME, headerValues: SHEET_HEADERS });
  }
  console.log(`${c.green}✓ พร้อม${c.reset}`);

  // ============================================================
  // SCAN
  // ============================================================
  const mode = CONFIG.fullScan ? 'FULL (getTokenAccountsByOwner)' : 'ATA (getMultipleAccounts)';
  console.log(`\n${c.cyan}${c.bright}>> เริ่มสแกน ${wallets.length} กระเป๋า — โหมด: ${mode}${c.reset}`);
  printLegend();
  console.log('');

  const limit = createLimiter(CONFIG.concurrency);
  const rowsToAdd = [];
  const errors = [];      // ทำให้ข้อมูลของ wallet นั้นหายทั้งก้อน
  const warnings = [];    // ได้ข้อมูลบางส่วน — ไม่ใช่ความล้มเหลว
  let okCount = 0;        // wallet ที่สแกนครบทุก batch
  let partialCount = 0;   // wallet ที่ได้ข้อมูลบางส่วน
  let t2022Rows = 0;
  let cellsChecked = 0;   // จำนวนช่อง (wallet × mint) ที่เช็คได้จริง
  let cellsAttempted = 0; // จำนวนช่องที่ตั้งใจจะเช็ค

  await Promise.all(wallets.map((wallet, wIdx) => limit(async () => {
    const t0 = Date.now();
    // ใช้ index ของ wallet ไม่ใช่ลำดับที่ทำเสร็จ — ของเดิมเลขสลับเพราะรันขนาน
    const tag0 = `${c.gray}[${String(wIdx + 1).padStart(3, '0')}/${wallets.length}]${c.reset}`;
    try {
      const res = CONFIG.fullScan
        ? await scanWalletFull(pool, wallet, mintInfoMap)
        : await scanWalletByAta(pool, wallet, mintInfoMap);
      const { found, attempted, checked, failedBatches, totalBatches, lastError } = res;

      cellsAttempted += attempted;
      cellsChecked += checked;

      const now = formatDate(new Date());
      let w2022 = 0;
      for (const f of found) {
        const meta = mintInfoMap.get(f.mint);
        if (meta?.programId.equals(TOKEN_2022_PROGRAM_ID)) { w2022++; t2022Rows++; }
        rowsToAdd.push({
          'Symbol': tokenMap.get(f.mint) ?? 'Unknown',
          'Network': 'Solana',
          'Token Mint': f.mint,
          'Amount': f.amount,
          'Wallet Name': wallet.name,
          'Wallet Address': wallet.address,
          'Timestamp': now,
        });
      }

      const allFailed = failedBatches > 0 && failedBatches === totalBatches;
      const tag = w2022 ? ` ${c.magenta}[T2022:${w2022}]${c.reset}` : '';
      const took = `${c.dim}(${((Date.now() - t0) / 1000).toFixed(1)}s)${c.reset}`;

      if (allFailed) {
        errors.push(`${wallet.name}: ${lastError?.message || lastError}`);
        console.log(`${tag0} ${wallet.name.padEnd(35, ' ')} ${c.red}✗ FAILED${c.reset} ${took} ` +
          `${c.red}${String(lastError?.message || lastError).slice(0, 60)}${c.reset}`);
      } else if (failedBatches > 0) {
        // ได้ข้อมูลบางส่วน — เก็บไว้ แต่ต้องไม่นับเป็นสำเร็จเต็ม
        partialCount++;
        warnings.push(`${wallet.name} [ขาด ${attempted - checked}/${attempted} mint]: ${lastError?.message || lastError}`);
        console.log(`${tag0} ${wallet.name.padEnd(35, ' ')} ` +
          `${c.yellow}⚠ ${String(found.length).padStart(2, '0')} tokens (บางส่วน ${checked}/${attempted})${c.reset}${tag} ${took}`);
      } else {
        okCount++;
        console.log(`${tag0} ${wallet.name.padEnd(35, ' ')} ` +
          `${found.length ? c.green : c.gray}✓ ${String(found.length).padStart(2, '0')} tokens${c.reset}${tag} ${took}`);
      }
    } catch (e) {
      // สำคัญ: timeout ก็นับเป็น error — ไม่ซ่อนเหมือนเวอร์ชันเดิม
      cellsAttempted += mintInfoMap.size;
      errors.push(`${wallet.name}: ${e?.message || e}`);
      console.log(
        `${tag0} ${wallet.name.padEnd(35, ' ')} ` +
        `${c.red}✗ FAILED${c.reset} ${c.dim}(${((Date.now() - t0) / 1000).toFixed(1)}s)${c.reset} ` +
        `${c.red}${String(e?.message || e).slice(0, 60)}${c.reset}`
      );
    }
  })));

  // ============================================================
  // COVERAGE GUARD + WRITE
  // ============================================================
  /**
   * Coverage ต้องวัด "ข้อมูลที่ได้จริง" ไม่ใช่แค่ wallet ที่ไม่ throw
   *
   * ของเดิมนับแค่ okCount/wallets ซึ่งมองไม่เห็น 2 ทาง:
   *   - mint ที่ resolve ไม่สำเร็จ → หายจากทุก wallet โดย coverage ยังขึ้น 100%
   *   - batch ที่พังบางส่วน → ตอนนี้ wallet ไม่ throw แล้ว จึงต้องนับระดับช่อง
   * จึงวัดเป็นสัดส่วนของช่อง (wallet × mint) ที่ตรวจได้จริง
   */
  const {
    mint: mintCoverage, cells: cellCoverage, total: coverage,
  } = computeCoverage({
    tokenCount: tokenMap.size, mintFailed, cellsChecked, cellsAttempted,
  });

  console.log(`\n${c.cyan}${c.bright}>> Coverage: ${(coverage * 100).toFixed(1)}%${c.reset}` +
    `${c.gray}  (mint ${(mintCoverage * 100).toFixed(1)}% × cells ${(cellCoverage * 100).toFixed(1)}%` +
    ` | wallet ครบ ${okCount}/${wallets.length}, บางส่วน ${partialCount})${c.reset}`);

  let wrote = false;
  if (coverage < CONFIG.minCoverage) {
    // ถ้าเขียนตอนนี้ = ลบข้อมูลดีของเมื่อวานทิ้ง แล้วใส่ข้อมูลไม่ครบแทน
    console.error(`${c.red}✗ Coverage ต่ำกว่าเกณฑ์ (${(CONFIG.minCoverage * 100).toFixed(0)}%) — ` +
      `ยกเลิกการเขียนทับ Sheet เพื่อกันข้อมูลหาย${c.reset}`);
    process.exitCode = 1;
  } else if (!rowsToAdd.length) {
    console.log(`${c.yellow}⚠ ไม่มีข้อมูลให้เขียน — ข้ามการเขียนทับ${c.reset}`);
  } else {
    console.log(`${c.cyan}>> กำลังเขียน ${rowsToAdd.length} record...${c.reset}`);
    try {
      await sheet.clear();
      await sheet.setHeaderRow(SHEET_HEADERS);
      // แบ่ง chunk กัน payload ใหญ่เกินของ Sheets API
      for (let i = 0; i < rowsToAdd.length; i += 500) {
        await sheet.addRows(rowsToAdd.slice(i, i + 500));
      }
      wrote = true;
      console.log(`${c.green}✓ เขียนแล้ว ${rowsToAdd.length} แถว${c.reset}`);
    } catch (e) {
      console.error(`${c.red}✗ เขียน Sheet ไม่สำเร็จ: ${e.message}${c.reset}`);
      errors.push(`Sheets: ${e.message}`);
      process.exitCode = 1;
    }
  }

  // ============================================================
  // SUMMARY
  // ============================================================
  const secs = (Date.now() - startTime) / 1000;
  console.log(`\n${c.cyan}${c.bright}╔══════════════════════════════════════════════════════════╗${c.reset}`);
  console.log(`${c.cyan}${c.bright}║  EXECUTION SUMMARY                                       ║${c.reset}`);
  console.log(`${c.cyan}${c.bright}╚══════════════════════════════════════════════════════════╝${c.reset}`);
  console.log(`${c.gray}เวลา: ${(secs / 60).toFixed(1)} นาที (${secs.toFixed(0)}s) | Wallets: ${wallets.length}${c.reset}`);
  console.log(`${c.gray}Coverage: ${(coverage * 100).toFixed(1)}% | เขียน Sheet: ${wrote ? 'ใช่' : 'ไม่'}${c.reset}`);
  console.log(`${c.gray}Wallet: ครบ ${okCount} | บางส่วน ${partialCount} | ล้มเหลว ${wallets.length - okCount - partialCount}${c.reset}`);
  console.log(`${c.gray}Cells: ${cellsChecked}/${cellsAttempted} | Mint resolve: ${mintInfoMap.size}/${tokenMap.size} ` +
    `(ล้มเหลว ${mintFailed}, Token-2022 mints: ${n2022})${c.reset}`);
  console.log(`${c.magenta}Token-2022 holdings พบ: ${t2022Rows}${c.reset}`);
  console.log(`${c.green}Records: ${rowsToAdd.length}${c.reset}`);

  console.log(`\n${c.bright}เซิร์ฟเวอร์ที่ใช้${c.reset} ` +
    `${c.dim}(จำกัด ${CONFIG.rateLimitRps} คำขอ/วินาที · ถูกขอให้ชะลอ ${pool.throttled} ครั้ง)${c.reset}`);
  console.log(`  ${c.dim}${'เซิร์ฟเวอร์'.padEnd(36)}${'คำขอ'.padStart(7)}${'พลาด'.padStart(8)}${'ตอบเฉลี่ย'.padStart(11)}${c.reset}`);
  for (const s of pool.stats()) {
    const pct = s.calls ? Math.round((s.errors / s.calls) * 100) : 0;
    const tone = s.disabled ? c.red : pct >= 20 ? c.yellow : c.green;
    const flag = s.disabled ? ` ${c.red}⏭ เลิกใช้ (รหัส ${s.disabledReason})${c.reset}` : '';
    console.log(
      `  ${c.gray}${s.host.padEnd(36)}${c.reset}` +
      `${String(s.calls).padStart(7)}` +
      `${tone}${`${s.errors} (${pct}%)`.padStart(8)}${c.reset}` +
      `${`${s.avgMs}ms`.padStart(11)}${flag}`
    );
  }
  if (pool.recovered) {
    console.log(`  ${c.green}✓${c.reset} ${c.dim}${pool.recovered} คำขอที่พลาดถูกลองใหม่จนสำเร็จ — ไม่กระทบข้อมูล${c.reset}`);
  }

  printTally();

  // แยก Warnings ออกจาก Errors — partial คือ "ข้อมูลไม่ครบ" ไม่ใช่ "งานล้มเหลว"
  // การเอาไปกองรวมกันทำให้ summary ดูเหมือนพังทั้งที่ job ผ่าน
  if (warnings.length) {
    console.log(`${c.yellow}Warnings: ${warnings.length} (wallet ที่ได้ข้อมูลบางส่วน)${c.reset}`);
    warnings.slice(0, 10).forEach((w, i) =>
      console.log(`${c.yellow}  ${i + 1}. ${String(w).replace(/\s+/g, ' ').slice(0, 150)}${c.reset}`));
    if (warnings.length > 10) console.log(`${c.yellow}  ... และอีก ${warnings.length - 10} รายการ${c.reset}`);
  }

  if (errors.length) {
    console.log(`${c.red}Errors: ${errors.length}${c.reset}`);
    errors.slice(0, 15).forEach((e, i) =>
      console.log(`${c.red}  ${i + 1}. ${String(e).replace(/\s+/g, ' ').slice(0, 150)}${c.reset}`));
    if (errors.length > 15) console.log(`${c.red}  ... และอีก ${errors.length - 15} รายการ${c.reset}`);
  } else {
    console.log(`${c.green}Errors: 0${c.reset}`);
  }
  console.log(`${c.cyan}${c.bright}══════════════════════════════════════════════════════════${c.reset}\n`);

  // ไม่ใช้ process.exit() เพื่อให้ stdout flush ครบ (log บรรทัดท้ายไม่หาย)
}

// รันเฉพาะตอนถูกเรียกเป็นสคริปต์ — ทำให้ import มาเทสต์ RpcPool ได้โดยไม่สแกนจริง
const isCli = process.argv[1]
  && import.meta.url === pathToFileURL(process.argv[1]).href;

if (isCli) {
  main().catch((err) => {
    console.error(`${c.red}${c.bright}Fatal Error:${c.reset} ${err?.stack || err?.message || err}`);
    process.exitCode = 1;
  });
}

export {
  RpcPool, classifyError, httpStatusOf, CONFIG,
  computeCoverage, scanWalletByAta,
  createRateLimiter, parseRetryAfter,
  explainStatus, printTally, printLegend, USE_COLOR,
  filterKnownBlocked, KNOWN_BLOCKED_HOSTS,
};
