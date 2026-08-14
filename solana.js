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
};
// ปิดสีอัตโนมัติเมื่อไม่ใช่ TTY (เช่นบน GitHub Actions) เพื่อให้ log อ่านง่าย
const USE_COLOR = process.stdout.isTTY && process.env.NO_COLOR !== '1';
if (!USE_COLOR) for (const k of Object.keys(c)) c[k] = '';

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
  // จำนวน TCP socket สูงสุดต่อ 1 origin — mainnet-beta จำกัด "connection rate"
  // ไม่ใช่ request rate การ reuse socket จึงสำคัญกว่าการลด concurrency
  maxSockets: Number(process.env.MAX_SOCKETS || 6),
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

/** log แบบ JSON บรรทัดเดียว — grep/analyze ได้จริง */
function jlog(obj) {
  console.log(JSON.stringify({ ts: new Date().toISOString(), ...obj }));
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
function makeConn(endpoint, timeoutMs) {
  return new Connection(endpoint, {
    commitment: 'confirmed',
    disableRetryOnRateLimit: true, // กัน retry ซ้อน retry ของ web3.js
    fetch: async (url, opts) => {
      const ac = new AbortController();
      const timer = setTimeout(() => ac.abort(), timeoutMs);
      try {
        return await fetch(url, { ...opts, signal: ac.signal });
      } finally {
        clearTimeout(timer);
      }
    },
  });
}

class RpcPool {
  constructor(urls, timeoutMs) {
    this.nodes = urls.map((u) => ({
      url: u,
      host: (() => { try { return new URL(u).host; } catch { return u; } })(),
      conn: makeConn(u, timeoutMs),
      consecFails: 0,
      openUntil: 0,
      disabled: false,
      disabledReason: '',
      inflight: 0,
      calls: 0,
      errors: 0,
      totalMs: 0,
    }));
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
      jlog({ evt: 'breaker_open', host: n.host, cooldownMs: CONFIG.breakerCooldownMs });
    }
  }

  /** ปิด endpoint ถาวรสำหรับรอบนี้ — ใช้กับ error ที่ retry ไปก็ไม่มีทางหาย */
  disable(n, reason) {
    if (n.disabled) return;
    n.disabled = true;
    n.disabledReason = reason;
    jlog({
      evt: 'endpoint_disabled', host: n.host, reason,
      remaining: this.usable().length,
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

      n.inflight++;
      n.calls++;
      const t0 = Date.now();
      try {
        const res = await fn(n.conn);
        n.totalMs += Date.now() - t0;
        this.ok(n);
        return res;
      } catch (e) {
        n.totalMs += Date.now() - t0;
        lastErr = e;
        this.fail(n);
        tried.add(n.host);

        const kind = classifyError(e);
        jlog({
          evt: 'rpc_retry', label, host: n.host,
          attempt: attempt + 1, ms: Date.now() - t0, kind,
          status: httpStatusOf(e) || undefined,
          msg: (e?.message || '').slice(0, 160),
        });

        if (kind === 'permanent') {
          // endpoint นี้ใช้ไม่ได้เลย — ปิดทิ้งแล้วไปตัวถัดไปทันที
          // ไม่นับเป็น retry เพราะไม่ใช่ความผิดของ request
          this.disable(n, `${httpStatusOf(e) || 'error'}`);
          continue;
        }

        attempt++;
        if (attempt < CONFIG.maxRetries) {
          const base = kind === 'ratelimit' ? 2000 : 500;
          const wait = Math.min(15000, base * 2 ** (attempt - 1)) * (0.7 + Math.random() * 0.6);
          await delay(wait);
        }
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
      jlog({ evt: 'resolve_mints_failed', batch: bi, msg: (e?.message || '').slice(0, 160) });
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
      jlog({
        evt: 'batch_failed', wallet: wallet.name, batch: totalBatches,
        size: batch.length, msg: (e?.message || '').slice(0, 160),
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
      jlog({
        evt: 'batch_failed', wallet: wallet.name, mode: 'full',
        program: programId.toBase58(), msg: (e?.message || '').slice(0, 160),
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
  const rpcUrls = [...new Set([...customRpcs, ...EXTRA_RPCS, ...DEFAULT_RPCS])];
  const pool = new RpcPool(rpcUrls, CONFIG.rpcTimeoutMs);
  rpcUrls.forEach((u) => console.log(`${c.gray}  • ${u}${c.reset}`));
  console.log(`${c.gray}  timeout=${CONFIG.rpcTimeoutMs}ms concurrency=${CONFIG.concurrency} ` +
    `retries=${CONFIG.maxRetries} maxSockets=${CONFIG.maxSockets} keepAlive=${agentOk ? 'on' : 'off'}${c.reset}`);
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
  console.log(`\n${c.cyan}${c.bright}>> เริ่มสแกน ${wallets.length} wallet — โหมด: ${mode}${c.reset}\n`);

  const limit = createLimiter(CONFIG.concurrency);
  const rowsToAdd = [];
  const errors = [];
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
        errors.push(`${wallet.name} [partial ${checked}/${attempted}]: ${lastError?.message || lastError}`);
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

  console.log(`${c.gray}— RPC stats —${c.reset}`);
  for (const s of pool.stats()) {
    const rate = s.calls ? ((s.errors / s.calls) * 100).toFixed(0) : '0';
    const flag = s.disabled ? `${c.red} [DISABLED:${s.disabledReason}]${c.reset}` : '';
    console.log(`${c.gray}  ${s.host.padEnd(42)} calls=${String(s.calls).padStart(5)} ` +
      `errors=${String(s.errors).padStart(4)} (${rate.padStart(3)}%) avg=${s.avgMs}ms${c.reset}${flag}`);
  }

  if (errors.length) {
    console.log(`${c.red}Errors: ${errors.length}${c.reset}`);
    errors.slice(0, 15).forEach((e, i) => console.log(`${c.red}  ${i + 1}. ${e}${c.reset}`));
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
};
