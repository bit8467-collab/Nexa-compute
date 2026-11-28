const TURSO_DEFAULT_URL = "libsql://nexa-compute-nexa-compute.aws-us-west-2.turso.io";
const TURSO_STMT = Symbol("turso-statement");

function tursoEndpoint(connectionUrl) {
  const raw = String(connectionUrl || TURSO_DEFAULT_URL).trim();
  const normalized = raw.startsWith("libsql://") ? "https://" + raw.slice(9) : raw;
  const parsed = new URL(normalized);
  if (parsed.protocol !== "https:") throw new Error("TURSO_DATABASE_URL must use libsql:// or https://.");
  const path = parsed.pathname.replace(new RegExp("/+$"), "");
  return parsed.origin + path + (path.endsWith("/v2/pipeline") ? "" : "/v2/pipeline");
}

function splitSqlStatements(source) {
  const statements = [];
  let start = 0, quote = "", lineComment = false, blockComment = false;
  for (let i = 0; i < source.length; i++) {
    const ch = source[i], next = source[i + 1];
    if (lineComment) { if (ch.charCodeAt(0) === 10) lineComment = false; continue; }
    if (blockComment) { if (ch === "*" && next === "/") { blockComment = false; i++; } continue; }
    if (quote) {
      if (quote !== "]" && ch === quote && next === quote) { i++; continue; }
      if (ch === quote) quote = "";
      continue;
    }
    if (ch === "-" && next === "-") { lineComment = true; i++; continue; }
    if (ch === "/" && next === "*") { blockComment = true; i++; continue; }
    if (ch === "'" || ch === '"' || ch.charCodeAt(0) === 96) { quote = ch; continue; }
    if (ch === "[") { quote = "]"; continue; }
    if (ch === ";") { const part = source.slice(start, i).trim(); if (part) statements.push(part); start = i + 1; }
  }
  const last = source.slice(start).trim();
  if (last) statements.push(last);
  return statements;
}

function tursoBytesBase64(bytes) {
  let binary = "";
  for (let offset = 0; offset < bytes.length; offset += 0x8000) {
    binary += String.fromCharCode(...bytes.subarray(offset, offset + 0x8000));
  }
  return btoa(binary);
}

function tursoArgument(value) {
  if (value == null) return { type: "null" };
  if (typeof value === "boolean") return { type: "integer", value: value ? "1" : "0" };
  if (typeof value === "bigint") return { type: "integer", value: String(value) };
  if (typeof value === "number") return Number.isInteger(value) ? { type: "integer", value: String(value) } : { type: "float", value };
  if (value instanceof ArrayBuffer) return { type: "blob", base64: tursoBytesBase64(new Uint8Array(value)) };
  if (ArrayBuffer.isView(value)) return { type: "blob", base64: tursoBytesBase64(new Uint8Array(value.buffer, value.byteOffset, value.byteLength)) };
  return { type: "text", value: String(value) };
}

function tursoCell(cell) {
  if (cell == null || typeof cell !== "object" || !("type" in cell)) return cell;
  if (cell.type === "null") return null;
  if (cell.type === "integer" || cell.type === "float") return Number(cell.value);
  if (cell.type === "text") return cell.value;
  if (cell.type === "blob") { const binary = atob(cell.base64 || ""); return Uint8Array.from(binary, ch => ch.charCodeAt(0)); }
  return cell.value ?? null;
}

function tursoMeta(result) {
  return {
    duration: 0,
    changes: Number(result?.affected_row_count || 0),
    last_row_id: result?.last_insert_rowid == null ? null : Number(result.last_insert_rowid),
    rows_read: Number(result?.rows_read || 0),
    rows_written: Number(result?.rows_written || 0),
  };
}

function tursoRows(result) {
  const columns = result?.cols || [];
  return (result?.rows || []).map(row => {
    const object = {};
    for (let i = 0; i < columns.length; i++) object[typeof columns[i] === "string" ? columns[i] : columns[i].name] = tursoCell(row[i]);
    return object;
  });
}

function tursoD1Result(result) {
  return { results: tursoRows(result), success: true, meta: tursoMeta(result) };
}

function tursoStatement(sql, args = [], execute) {
  const statement = {
    bind(...values) { return tursoStatement(sql, values, execute); },
    async all() { return tursoD1Result(await execute(sql, args)); },
    async first(column) {
      const result = await execute(sql, args);
      const rows = result?.rows || [], columns = result?.cols || [];
      if (!rows.length) return null;
      const row = {};
      for (let i = 0; i < columns.length; i++) row[typeof columns[i] === "string" ? columns[i] : columns[i].name] = tursoCell(rows[0][i]);
      return column ? row[column] ?? null : row;
    },
    async run() { return tursoD1Result(await execute(sql, args)); },
  };
  Object.defineProperty(statement, TURSO_STMT, { value: { sql, args } });
  return statement;
}

function createTursoD1(connectionUrl, authToken) {
  const endpoint = tursoEndpoint(connectionUrl);
  const token = String(authToken || "").trim();
  async function send(request) {
    const response = await fetch(endpoint, {
      method: "POST",
      headers: { "authorization": "Bearer " + token, "content-type": "application/json" },
      body: JSON.stringify({ requests: [request, { type: "close" }] }),
    });
    let payload;
    try { payload = await response.json(); } catch { payload = null; }
    if (!response.ok) throw new Error("Turso request failed (HTTP " + response.status + ").");
    const first = payload?.results?.[0];
    if (first?.type === "error") throw new Error(first.error?.message || "Turso query failed.");
    if (!first?.response) throw new Error("Unexpected response from Turso.");
    return first.response;
  }
  async function tursoExecute(sql, args = []) {
    const response = await send({ type: "execute", stmt: { sql, args: args.map(tursoArgument) } });
    if (response.type !== "execute" || !response.result) throw new Error("Unexpected Turso execute response.");
    return response.result;
  }
  function preparedStatement(statement) {
    const prepared = statement?.[TURSO_STMT];
    if (!prepared) throw new Error("Invalid statement passed to Turso batch.");
    return { sql: prepared.sql, args: prepared.args.map(tursoArgument) };
  }
  return {
    prepare(sql) { return tursoStatement(String(sql), [], tursoExecute); },
    async batch(statements) {
      if (!statements.length) return [];
      const steps = statements.map((statement, index) => {
        const step = { stmt: preparedStatement(statement) };
        if (index > 0) step.condition = { type: "ok" };
        return step;
      });
      const response = await send({ type: "batch", batch: { steps } });
      const batchResult = response.result || response.batch || response;
      const results = batchResult.step_results || batchResult.results;
      if (!Array.isArray(results)) throw new Error("Unexpected Turso batch response.");
      return results.map(step => {
        if (step?.type === "error" || step?.error) throw new Error(step.error?.message || "Turso batch failed.");
        return tursoD1Result(step.result || step.response?.result || step);
      });
    },
    async exec(sql) {
      const statements = splitSqlStatements(String(sql)).map(text => tursoStatement(text, [], tursoExecute));
      if (statements.length) await this.batch(statements);
      return { count: statements.length, duration: 0 };
    },
  };
}

const DAY = 86400000;
const PLANS = [
  { days: 10, rate: 1.0, min: 50 },
  { days: 30, rate: 1.2, min: 500 },
  { days: 90, rate: 1.5, min: 1000 },
];

function genSalt() {
  const a = new Uint8Array(16);
  crypto.getRandomValues(a);
  return [...a].map(b => b.toString(16).padStart(2, "0")).join("");
}
function makeToken() {
  const a = new Uint8Array(24);
  crypto.getRandomValues(a);
  return [...a].map(b => b.toString(16).padStart(2, "0")).join("");
}
async function hash(pw, salt) {
  const data = new TextEncoder().encode(salt + "::" + pw);
  const buf = await crypto.subtle.digest("SHA-256", data);
  return [...new Uint8Array(buf)].map(b => b.toString(16).padStart(2, "0")).join("");
}

async function sessionUser(request, env) {
  const h = (request.headers.get("x-session-token") || "").trim();
  if (!h) return null;
  const s = await env.DB.prepare("SELECT user_id FROM sessions WHERE token = ?").bind(h).first();
  if (!s) return null;
  return env.DB.prepare("SELECT * FROM users WHERE id = ?").bind(s.user_id).first();
}
async function operatorRoleForUser(env,userId) {
  if(!userId) return {enabled:false,permissions:[]};
  const row=await env.DB.prepare("SELECT permissions,enabled FROM admin_operators WHERE user_id=?").bind(userId).first();
  let permissions=[];
  try { const parsed=JSON.parse(row?.permissions||"[]"); if(Array.isArray(parsed)) permissions=parsed.filter(value=>ADMIN_PERMISSIONS.includes(value)); } catch {}
  return {enabled:Number(row?.enabled)===1,permissions};
}

async function accrue(env, userId) {
  let wallet = await env.DB.prepare("SELECT * FROM wallets WHERE user_id = ?").bind(userId).first();
  if (!wallet) {
    wallet = { user_id: userId, deposit: 0, earnings: 0, total_deposited: 0, total_earned: 0, total_withdrawn: 0 };
    await env.DB.prepare("INSERT INTO wallets (user_id, deposit, earnings, total_deposited, total_earned, total_withdrawn) VALUES (?,0,0,0,0,0)").bind(userId).run();
  }
  const now = Date.now();
  const invs = (await env.DB.prepare("SELECT * FROM investments WHERE user_id = ? AND active = 1").bind(userId).all()).results;
  const updates = [];
  const creditEvents = [];
  let changed = false;
  for (const inv of invs) {
    const elapsed = (now - inv.created) / DAY;
    const target = Math.min(inv.days, elapsed);
    if (target > inv.credited) {
      const gain = (inv.amount * inv.rate / 100) * (target - inv.credited);
      wallet.earnings += gain;
      wallet.total_earned += gain;
      inv.credited = target;
      const active = target < inv.days - 1e-9 ? 1 : 0;
      updates.push(env.DB.prepare("UPDATE investments SET credited = ?, active = ? WHERE id = ?").bind(target, active, inv.id));
      creditEvents.push({ investmentId: inv.id, amount: gain });
      changed = true;
    }
  }
  if (changed) {
    await env.DB.prepare(
      "INSERT INTO wallets (user_id, deposit, earnings, total_deposited, total_earned, total_withdrawn) VALUES (?,?,?,?,?,?) " +
      "ON CONFLICT(user_id) DO UPDATE SET deposit=excluded.deposit, earnings=excluded.earnings, total_deposited=excluded.total_deposited, total_earned=excluded.total_earned, total_withdrawn=excluded.total_withdrawn"
    ).bind(userId, wallet.deposit, wallet.earnings, wallet.total_deposited, wallet.total_earned, wallet.total_withdrawn).run();
    for (let i = 0; i < updates.length; i++) {
      await updates[i].run();
      const credit = creditEvents[i];
      await env.DB.prepare("INSERT INTO earning_credits (user_id, investment_id, amount, created_at) VALUES (?, ?, ?, ?)").bind(userId, credit.investmentId, credit.amount, now).run();
    }
  }
  return wallet;
}

async function accrueServerEarnings(env, userId) {
  await env.DB.prepare("INSERT OR IGNORE INTO wallets (user_id, deposit, earnings, total_deposited, total_earned, total_withdrawn) VALUES (?,0,0,0,0,0)").bind(userId).run();
  const rows = (await env.DB.prepare("SELECT s.purchase_id,s.status,s.updated_at,s.active_since,s.yield_total,s.credited_yield,p.product_id FROM server_instances s JOIN compute_purchases p ON p.id=s.purchase_id WHERE s.user_id=?").bind(userId).all()).results || [];
  const now = Date.now();
  for (const row of rows) {
    const daily = COMPUTE_CATALOG.get(row.product_id)?.daily || 0;
    const activeSince = row.status === "active" ? Number(row.active_since || row.updated_at || now) : 0;
    const total = Number(row.yield_total || 0) + (activeSince ? daily * Math.max(0, now - activeSince) / DAY : 0);
    const credited = Number(row.credited_yield || 0);
    const delta = total - credited;
    if (delta > 1e-10) {
      await env.DB.batch([
        env.DB.prepare("UPDATE server_instances SET credited_yield=? WHERE purchase_id=? AND credited_yield=?").bind(total, row.purchase_id, credited),
        env.DB.prepare("UPDATE wallets SET earnings=earnings+?, total_earned=total_earned+? WHERE user_id=? AND changes()>0").bind(delta, delta, userId),
        env.DB.prepare("INSERT INTO earning_credits (user_id, investment_id, amount, created_at, earning_type, server_purchase_id) SELECT ?,0,?,?,'server',? WHERE changes()>0").bind(userId, delta, now, row.purchase_id)
      ]);
    }
  }
}

async function accrueReferralNetwork(env, referrerId) {
  let frontier = [referrerId];
  const now = Date.now();
  for (let level = 1; level <= 3 && frontier.length; level++) {
    const found = (await env.DB.prepare("SELECT id FROM users WHERE referred_by IN (" + frontier.map(() => "?").join(",") + ")").bind(...frontier).all()).results || [];
    frontier = found.map(row => row.id);
    const rate = level === 1 ? 0.05 : level === 2 ? 0.03 : 0.01;
    for (const memberId of frontier) {
      await accrue(env, memberId);
      await accrueServerEarnings(env, memberId);
      const sources = (await env.DB.prepare("SELECT c.id,c.amount FROM earning_credits c LEFT JOIN referral_commissions r ON r.referrer_id=? AND r.source_kind='earning' AND r.source_id=CAST(c.id AS TEXT) WHERE c.user_id=? AND c.earning_type IN ('investment','server') AND r.id IS NULL AND c.amount>0").bind(referrerId, memberId).all()).results || [];
      for (const source of sources) {
        const amount = Number(source.amount) * rate;
        if (!(amount > 0)) continue;
        await env.DB.batch([
          env.DB.prepare("INSERT OR IGNORE INTO referral_commissions (referrer_id,referred_user_id,source_kind,source_id,amount,applied,created_at) VALUES (?,?, 'earning', ?, ?,0,?)").bind(referrerId, memberId, String(source.id), amount, now),
          env.DB.prepare("UPDATE wallets SET earnings=earnings+(SELECT amount FROM referral_commissions WHERE referrer_id=? AND source_kind='earning' AND source_id=? AND applied=0) WHERE user_id=? AND EXISTS (SELECT 1 FROM referral_commissions WHERE referrer_id=? AND source_kind='earning' AND source_id=? AND applied=0)").bind(referrerId, String(source.id), referrerId, referrerId, String(source.id)),
          env.DB.prepare("INSERT INTO earning_credits (user_id,investment_id,amount,created_at,earning_type) SELECT ?,0,amount,?,'commission' FROM referral_commissions WHERE referrer_id=? AND source_kind='earning' AND source_id=? AND applied=0").bind(referrerId, now, referrerId, String(source.id)),
          env.DB.prepare("UPDATE referral_commissions SET applied=1 WHERE referrer_id=? AND source_kind='earning' AND source_id=? AND applied=0").bind(referrerId, String(source.id))
        ]);
      }
      if (level === 1) {
        const first = await env.DB.prepare("SELECT id,amount FROM deposits WHERE user_id=? ORDER BY date ASC,id ASC LIMIT 1").bind(memberId).first();
        if (first) {
          const exists = await env.DB.prepare("SELECT id FROM referral_commissions WHERE referrer_id=? AND source_kind='deposit' AND source_id=?").bind(referrerId, String(first.id)).first();
          if (!exists) {
            const amount = Number(first.amount) * 0.05;
            await env.DB.batch([
              env.DB.prepare("INSERT OR IGNORE INTO referral_commissions (referrer_id,referred_user_id,source_kind,source_id,amount,applied,created_at) VALUES (?,?, 'deposit', ?, ?,0,?)").bind(referrerId, memberId, String(first.id), amount, now),
              env.DB.prepare("UPDATE wallets SET earnings=earnings+(SELECT amount FROM referral_commissions WHERE referrer_id=? AND source_kind='deposit' AND source_id=? AND applied=0) WHERE user_id=? AND EXISTS (SELECT 1 FROM referral_commissions WHERE referrer_id=? AND source_kind='deposit' AND source_id=? AND applied=0)").bind(referrerId, String(first.id), referrerId, referrerId, String(first.id)),
              env.DB.prepare("INSERT INTO earning_credits (user_id,investment_id,amount,created_at,earning_type) SELECT ?,0,amount,?,'commission' FROM referral_commissions WHERE referrer_id=? AND source_kind='deposit' AND source_id=? AND applied=0").bind(referrerId, now, referrerId, String(first.id)),
              env.DB.prepare("UPDATE referral_commissions SET applied=1 WHERE referrer_id=? AND source_kind='deposit' AND source_id=? AND applied=0").bind(referrerId, String(first.id))
            ]);
          }
        }
      }
    }
  }
}

async function accrueUplineCommissions(env, userId) {
  let parentId = (await env.DB.prepare("SELECT referred_by FROM users WHERE id=?").bind(userId).first())?.referred_by || null;
  for (let level = 0; level < 3 && parentId; level++) {
    await accrueReferralNetwork(env, parentId);
    parentId = (await env.DB.prepare("SELECT referred_by FROM users WHERE id=?").bind(parentId).first())?.referred_by || null;
  }
}

async function getTeam(request, env) {
  const user = await sessionUser(request, env);
  if (!user) return Response.json({ error: "Non autenticato." }, { status: 401 });
  await accrue(env, user.id);
  await accrueServerEarnings(env, user.id);
  await accrueReferralNetwork(env, user.id);
  await accrueUplineCommissions(env, user.id);
  let frontier = [user.id];
  const members = [];
  for (let level = 1; level <= 3 && frontier.length; level++) {
    const placeholders = frontier.map(() => "?").join(",");
    const found = (await env.DB.prepare(
      "SELECT id, username, created_at FROM users WHERE referred_by IN (" + placeholders + ") ORDER BY created_at DESC"
    ).bind(...frontier).all()).results || [];
    frontier = found.map(row => row.id);
    for (const member of found) {
      const wallet = await accrue(env, member.id);
      await accrueServerEarnings(env, member.id);
      const earned = Number((await env.DB.prepare("SELECT total_earned FROM wallets WHERE user_id=?").bind(member.id).first())?.total_earned || wallet.total_earned || 0);
      const first = level === 1
        ? await env.DB.prepare("SELECT amount FROM deposits WHERE user_id=? ORDER BY date ASC, id ASC LIMIT 1").bind(member.id).first()
        : null;
      const earningsCommission = Number(await env.DB.prepare("SELECT COALESCE(SUM(amount),0) FROM referral_commissions WHERE referrer_id=? AND referred_user_id=? AND source_kind='earning' AND applied=1").bind(user.id, member.id).first("COALESCE(SUM(amount),0)"));
      const firstDepositBonus = Number(await env.DB.prepare("SELECT COALESCE(SUM(amount),0) FROM referral_commissions WHERE referrer_id=? AND referred_user_id=? AND source_kind='deposit' AND applied=1").bind(user.id, member.id).first("COALESCE(SUM(amount),0)"));
      members.push({ username: member.username, joined: member.created_at, level,
        earnings: earned, earningsCommission, firstDeposit: Number(first?.amount || 0),
        firstDepositBonus, commission: earningsCommission + firstDepositBonus });
    }
  }
  return Response.json({ members, rates: { 1: 0.05, 2: 0.03, 3: 0.01 }, firstDepositRate: 0.05 });
}

async function fullState(env, user) {
  await accrue(env, user.id);
  await accrueServerEarnings(env, user.id);
  await accrueReferralNetwork(env, user.id);
  await accrueUplineCommissions(env, user.id);
  const wallet = await env.DB.prepare("SELECT * FROM wallets WHERE user_id = ?").bind(user.id).first();
  const investments = (await env.DB.prepare("SELECT * FROM investments WHERE user_id = ? ORDER BY id DESC").bind(user.id).all()).results;
  const deposits = (await env.DB.prepare("SELECT * FROM deposits WHERE user_id = ? ORDER BY id DESC").bind(user.id).all()).results;
  const withdrawals = (await env.DB.prepare("SELECT * FROM withdrawals WHERE user_id = ? ORDER BY id DESC").bind(user.id).all()).results;
  const members = (await env.DB.prepare("SELECT id, username, created_at FROM users WHERE referred_by = ?").bind(user.id).all()).results;
  const team = [];
  for (const m of members) {
    const vol = await env.DB.prepare("SELECT COALESCE(SUM(amount),0) AS v FROM investments WHERE user_id = ?").bind(m.id).first("v");
    team.push({ username: m.username, joined: m.created_at, invested: vol || 0 });
  }
  const s = await env.DB.prepare("SELECT * FROM settings WHERE user_id = ?").bind(user.id).first();
  let withdraw_wallets = {};
  let notifications = true;
  if (s) {
    try { withdraw_wallets = JSON.parse(s.withdraw_wallets || "{}"); } catch { withdraw_wallets = {}; }
    notifications = !!s.notifications;
  }
  return {
    user: { id: user.id, username: user.username, identifier: user.identifier, inviteCode: user.invite_code, referredBy: user.referred_by, email_verified: !!user.email_verified },
    wallet,
    investments,
    deposits,
    withdrawals,
    team,
    acc: { withdraw_wallets, notifications },
  };
}

const websimWorker = {
  async fetch(request, env) {
    const url = new URL(request.url);
    const path = url.pathname;
    const method = request.method;
      if (method === "GET" && path === "/api/pwa-manifest") return new Response(JSON.stringify({ name: "Nexa Compute", short_name: "Nexa", id: "/", start_url: "/", scope: "/", display: "standalone", background_color: "#0b1020", theme_color: "#0b1020", icons: [{ src: "/api/pwa-icon", sizes: "192x192", type: "image/svg+xml", purpose: "any" }, { src: "/api/pwa-icon", sizes: "512x512", type: "image/svg+xml", purpose: "any maskable" }] }), { headers: { "Content-Type": "application/manifest+json; charset=utf-8", "Cache-Control": "no-cache" } });
      if (method === "GET" && path === "/api/pwa-icon") return new Response(`<svg xmlns="http://www.w3.org/2000/svg" width="512" height="512" viewBox="0 0 512 512"><rect width="512" height="512" rx="112" fill="#0b1020"/><rect x="36" y="36" width="440" height="440" rx="88" fill="none" stroke="#c39a46" stroke-width="10"/><text x="256" y="302" text-anchor="middle" font-family="Arial,sans-serif" font-size="190" font-weight="800" fill="#e8c876">NX</text></svg>`, { headers: { "Content-Type": "image/svg+xml; charset=utf-8", "Cache-Control": "public, max-age=86400" } });
      if (method === "GET" && path === "/api/pwa-worker") return new Response(`self.addEventListener("install",event=>event.waitUntil(self.skipWaiting()));self.addEventListener("activate",event=>event.waitUntil(self.clients.claim()));`, { headers: { "Content-Type": "application/javascript; charset=utf-8", "Service-Worker-Allowed": "/", "Cache-Control": "no-cache" } });
    const tursoToken = String(env.TURSO_AUTH_TOKEN || "").trim();
    if (!tursoToken) return Response.json({ error: "Turso is not configured. Set the TURSO_AUTH_TOKEN secret." }, { status: 503 });
    let tursoDB;
    try { tursoDB = createTursoD1(env.TURSO_DATABASE_URL || TURSO_DEFAULT_URL, tursoToken); }
    catch { return Response.json({ error: "TURSO_DATABASE_URL is invalid." }, { status: 503 }); }
    env = new Proxy(env, { get(target, property, receiver) { return property === "DB" ? tursoDB : Reflect.get(target, property, receiver); } });
    await env.DB.exec(`
      CREATE TABLE IF NOT EXISTS users (
        id TEXT PRIMARY KEY,
        username TEXT NOT NULL,
        identifier TEXT NOT NULL,
        password_hash TEXT NOT NULL,
        salt TEXT NOT NULL,
        invite_code TEXT NOT NULL,
        referred_by TEXT,
        created_at INTEGER NOT NULL,
        email_verified INTEGER NOT NULL DEFAULT 0
      );
      CREATE TABLE IF NOT EXISTS email_verifications (
        user_id TEXT PRIMARY KEY,
        email TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'pending',
        created_at INTEGER NOT NULL,
        updated_at INTEGER NOT NULL,
        otp_hash TEXT,
        otp_expires INTEGER,
        attempts INTEGER NOT NULL DEFAULT 0
      );
      CREATE TABLE IF NOT EXISTS kyc_submissions (
        account_id TEXT PRIMARY KEY,
        user_id TEXT NOT NULL,
        username TEXT NOT NULL,
        full_name TEXT NOT NULL,
        birth_date TEXT NOT NULL,
        nationality TEXT NOT NULL,
        document_type TEXT NOT NULL,
        document_number TEXT NOT NULL,
        document_expiry TEXT NOT NULL,
        front_blob_url TEXT NOT NULL,
        front_mime TEXT NOT NULL,
        back_blob_url TEXT NOT NULL,
        back_mime TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'pending',
        created_at INTEGER NOT NULL,
        updated_at INTEGER NOT NULL,
        reviewed_at INTEGER,
        reviewer_id TEXT,
        admin_note TEXT NOT NULL DEFAULT ''
      );
      CREATE INDEX IF NOT EXISTS kyc_submissions_status ON kyc_submissions (status, created_at);
      CREATE TABLE IF NOT EXISTS kyc_keyring (key_id TEXT PRIMARY KEY, secret_b64 TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS sessions (
        token TEXT PRIMARY KEY,
        user_id TEXT NOT NULL,
        created_at INTEGER NOT NULL
      );
      CREATE TABLE IF NOT EXISTS admin_operators (
        user_id TEXT PRIMARY KEY,
        permissions TEXT NOT NULL DEFAULT '[]',
        enabled INTEGER NOT NULL DEFAULT 1,
        updated_at INTEGER NOT NULL
      );
      CREATE TABLE IF NOT EXISTS wallets (
        user_id TEXT PRIMARY KEY,
        deposit REAL NOT NULL DEFAULT 0,
        earnings REAL NOT NULL DEFAULT 0,
        total_deposited REAL NOT NULL DEFAULT 0,
        total_earned REAL NOT NULL DEFAULT 0,
        total_withdrawn REAL NOT NULL DEFAULT 0
      );
      CREATE TABLE IF NOT EXISTS investments (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id TEXT NOT NULL,
        amount REAL NOT NULL,
        days INTEGER NOT NULL,
        rate REAL NOT NULL,
        created INTEGER NOT NULL,
        credited REAL NOT NULL DEFAULT 0,
        active INTEGER NOT NULL DEFAULT 1
      );
      CREATE TABLE IF NOT EXISTS deposits (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id TEXT NOT NULL,
        amount REAL NOT NULL,
        net TEXT NOT NULL,
        date INTEGER NOT NULL
      );
      CREATE TABLE IF NOT EXISTS withdrawals (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id TEXT NOT NULL,
        amount REAL NOT NULL,
        net TEXT NOT NULL,
        date INTEGER NOT NULL
      );
      CREATE TABLE IF NOT EXISTS settings (
        user_id TEXT PRIMARY KEY,
        withdraw_wallets TEXT NOT NULL DEFAULT '{}',
        notifications INTEGER NOT NULL DEFAULT 1
      );
      CREATE TABLE IF NOT EXISTS withdrawal_wallet_changes (
        user_id TEXT PRIMARY KEY,
        network TEXT NOT NULL,
        address TEXT NOT NULL,
        email TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'pending',
        otp_hash TEXT,
        otp_expires INTEGER,
        attempts INTEGER NOT NULL DEFAULT 0,
        created_at INTEGER NOT NULL,
        updated_at INTEGER NOT NULL
      );
      CREATE TABLE IF NOT EXISTS compute_rentals (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id TEXT NOT NULL,
        product_id TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'pending',
        created_at INTEGER NOT NULL,
        reviewed_at INTEGER
      );
      CREATE INDEX IF NOT EXISTS compute_rentals_user ON compute_rentals (user_id, id);
      CREATE TABLE IF NOT EXISTS account_requests (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id TEXT NOT NULL,
        kind TEXT NOT NULL CHECK (kind IN ('deposit','withdrawal')),
        amount REAL NOT NULL,
        status TEXT NOT NULL DEFAULT 'pending',
        created_at INTEGER NOT NULL,
        reviewed_at INTEGER,
        reviewed_by TEXT,
        review_key TEXT,
        network TEXT,
        tx_hash TEXT,
        withdrawal_address TEXT
      );
      CREATE TABLE IF NOT EXISTS support_messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id TEXT NOT NULL,
        account_id TEXT NOT NULL,
        account_username TEXT NOT NULL,
        sender TEXT NOT NULL CHECK (sender IN ('user','admin')),
        body TEXT NOT NULL,
        created_at INTEGER NOT NULL
      );
      CREATE INDEX IF NOT EXISTS support_messages_account ON support_messages (account_id, id);
      CREATE INDEX IF NOT EXISTS support_messages_unread ON support_messages (account_id, sender, id);
      CREATE TABLE IF NOT EXISTS platform_deposit_addresses (
        network TEXT PRIMARY KEY,
        address TEXT NOT NULL,
        updated_at INTEGER NOT NULL
      );
      CREATE TABLE IF NOT EXISTS compute_purchases (
        id TEXT PRIMARY KEY,
        user_id TEXT NOT NULL,
        product_id TEXT NOT NULL,
        price REAL NOT NULL,
        purchased_at INTEGER NOT NULL
      );
      CREATE INDEX IF NOT EXISTS compute_purchases_user ON compute_purchases (user_id, purchased_at);
      CREATE TABLE IF NOT EXISTS server_instances (
        purchase_id TEXT PRIMARY KEY,
        user_id TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'pending',
        updated_at INTEGER NOT NULL,
        updated_by TEXT,
        yield_total REAL NOT NULL DEFAULT 0,
        active_seconds REAL NOT NULL DEFAULT 0,
        active_since INTEGER,
        credited_yield REAL NOT NULL DEFAULT 0
      );
      CREATE INDEX IF NOT EXISTS server_instances_user ON server_instances (user_id, status);
      CREATE TABLE IF NOT EXISTS server_maintenance_payments (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id TEXT NOT NULL,
        purchase_id TEXT NOT NULL,
        maintenance_started_at INTEGER NOT NULL,
        amount REAL NOT NULL,
        created_at INTEGER NOT NULL,
        UNIQUE(purchase_id, maintenance_started_at)
      );
      CREATE INDEX IF NOT EXISTS server_maintenance_user ON server_maintenance_payments (user_id, created_at);
      CREATE TABLE IF NOT EXISTS admin_balance_adjustments (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id TEXT NOT NULL,
        field TEXT NOT NULL,
        old_value REAL NOT NULL,
        new_value REAL NOT NULL,
        created_at INTEGER NOT NULL,
        admin_id TEXT
      );
      CREATE INDEX IF NOT EXISTS admin_balance_adjustments_user ON admin_balance_adjustments (user_id, created_at);
      CREATE TABLE IF NOT EXISTS earning_credits (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id TEXT NOT NULL,
        investment_id INTEGER NOT NULL,
        amount REAL NOT NULL,
        created_at INTEGER NOT NULL
      );
      CREATE INDEX IF NOT EXISTS earning_credits_user ON earning_credits (user_id, created_at);
      CREATE TABLE IF NOT EXISTS referral_commissions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        referrer_id TEXT NOT NULL,
        referred_user_id TEXT NOT NULL,
        source_kind TEXT NOT NULL,
        source_id TEXT NOT NULL,
        amount REAL NOT NULL,
        applied INTEGER NOT NULL DEFAULT 0,
        created_at INTEGER NOT NULL,
        UNIQUE(referrer_id, source_kind, source_id)
      );
      CREATE INDEX IF NOT EXISTS referral_commissions_user ON referral_commissions (referrer_id, referred_user_id, applied);
      CREATE INDEX IF NOT EXISTS account_requests_user ON account_requests (user_id, id);
      CREATE INDEX IF NOT EXISTS account_requests_status ON account_requests (status, id);
      CREATE TABLE IF NOT EXISTS request_effects (
        request_id INTEGER PRIMARY KEY,
        user_id TEXT NOT NULL,
        kind TEXT NOT NULL,
        amount REAL NOT NULL,
        review_key TEXT NOT NULL,
        applied INTEGER NOT NULL DEFAULT 0
      );
    `);

    const userColumns = (await env.DB.prepare("PRAGMA table_info(users)").all()).results || [];
    if (!userColumns.some(column => column.name === "email_verified")) await env.DB.prepare("ALTER TABLE users ADD COLUMN email_verified INTEGER NOT NULL DEFAULT 0").run();
    const serverColumns = (await env.DB.prepare("PRAGMA table_info(server_instances)").all()).results || [];
    if (!serverColumns.some(column => column.name === "yield_total")) await env.DB.prepare("ALTER TABLE server_instances ADD COLUMN yield_total REAL NOT NULL DEFAULT 0").run();
    if (!serverColumns.some(column => column.name === "active_seconds")) await env.DB.prepare("ALTER TABLE server_instances ADD COLUMN active_seconds REAL NOT NULL DEFAULT 0").run();
    if (!serverColumns.some(column => column.name === "active_since")) await env.DB.prepare("ALTER TABLE server_instances ADD COLUMN active_since INTEGER").run();
    if (!serverColumns.some(column => column.name === "credited_yield")) await env.DB.prepare("ALTER TABLE server_instances ADD COLUMN credited_yield REAL NOT NULL DEFAULT 0").run();
    const earningColumns = (await env.DB.prepare("PRAGMA table_info(earning_credits)").all()).results || [];
    if (!earningColumns.some(column => column.name === "earning_type")) await env.DB.prepare("ALTER TABLE earning_credits ADD COLUMN earning_type TEXT NOT NULL DEFAULT 'investment'").run();
    if (!earningColumns.some(column => column.name === "server_purchase_id")) await env.DB.prepare("ALTER TABLE earning_credits ADD COLUMN server_purchase_id TEXT").run();
    await env.DB.prepare("CREATE INDEX IF NOT EXISTS earning_credits_server ON earning_credits (user_id, server_purchase_id, created_at)").run();
    const rentalColumns = (await env.DB.prepare("PRAGMA table_info(compute_rentals)").all()).results || [];
    if (!rentalColumns.some(column => column.name === "reviewed_at")) await env.DB.prepare("ALTER TABLE compute_rentals ADD COLUMN reviewed_at INTEGER").run();
    const requestColumns = (await env.DB.prepare("PRAGMA table_info(account_requests)").all()).results || [];
    if (!requestColumns.some(column => column.name === "network")) await env.DB.prepare("ALTER TABLE account_requests ADD COLUMN network TEXT").run();
    if (!requestColumns.some(column => column.name === "tx_hash")) await env.DB.prepare("ALTER TABLE account_requests ADD COLUMN tx_hash TEXT").run();
    if (!requestColumns.some(column => column.name === "withdrawal_address")) await env.DB.prepare("ALTER TABLE account_requests ADD COLUMN withdrawal_address TEXT").run();
    await env.DB.prepare("CREATE UNIQUE INDEX IF NOT EXISTS account_requests_tx_hash ON account_requests(tx_hash) WHERE tx_hash IS NOT NULL").run();


    const tokenUser=await sessionUser(request,env);
    if(tokenUser&&!isProjectOwner(request)) {
      const tokenRole=await operatorRoleForUser(env,tokenUser.id);
      const allowedOperatorPath=path.startsWith("/api/admin/")||["/api/me","/api/logout","/api/login"].includes(path)||path.startsWith("/api/password-recovery/");
      if(tokenRole.enabled&&!allowedOperatorPath) return Response.json({error:"Questo account operatore può accedere solo al pannello amministratore."},{status:403});
    }
    try {
      if (method === "POST" && path === "/api/register") return await register(request, env);
      if (method === "POST" && path === "/api/login") return await login(request, env);
      if (method === "POST" && path === "/api/password-recovery/request") return await requestPasswordRecovery(request, env);
      if (method === "POST" && path === "/api/password-recovery/reset") return await resetPasswordWithOtp(request, env);
      if (method === "POST" && path === "/api/logout") return await logout(request, env);
      if (method === "GET" && path === "/api/me") return await me(request, env);
      if (method === "GET" && path === "/api/kyc") return await getKycStatus(request, env);
      if (method === "POST" && path === "/api/kyc") return await submitKyc(request, env);
      if (method === "GET" && path === "/api/admin/kyc") return await getAdminKyc(request, env);
      if (method === "GET" && path === "/api/admin/kyc-image") return await getAdminKycImage(request, env);
      if (method === "POST" && path === "/api/admin/kyc-review") return await reviewKyc(request, env);
      if (method === "GET" && path === "/api/email-verification") return await getEmailVerification(request, env);
      if (method === "POST" && path === "/api/email-verification/request") return await requestEmailVerification(request, env);
      if (method === "POST" && path === "/api/email-verification/verify") return await verifyEmailCode(request, env);
      if (method === "GET" && path === "/api/admin/email-verifications") return await adminEmailVerifications(request, env);
      if (method === "GET" && path === "/api/support/messages") return await getSupportMessages(request, env);
      if (method === "POST" && path === "/api/support/messages") return await postSupportMessage(request, env);
      if (method === "GET" && path === "/api/support/unread") return await getSupportUnread(request, env);
      if (method === "GET" && path === "/api/inbox/updates") return await getInboxUpdates(request, env);
      if (method === "DELETE" && path === "/api/support/conversation") return await closeSupportConversation(request, env);
      if (method === "GET" && path === "/api/admin/support/conversations") return await getSupportConversations(request, env);
      if (method === "GET" && path === "/api/admin/support/messages") return await getAdminSupportMessages(request, env);
      if (method === "POST" && path === "/api/admin/support/reply") return await replySupportMessage(request, env);
      if (method === "POST" && path === "/api/admin/email-verifications/generate") return await generateEmailOtp(request, env);
      if (method === "GET" && path === "/api/team") return await getTeam(request, env);
      if (method === "POST" && path === "/api/invest") return await invest(request, env);
      if (method === "POST" && path === "/api/save-wallet") return await saveWallet(request, env);
      if (method === "POST" && path === "/api/withdrawal-wallet/change-request") return await requestWithdrawalWalletChange(request, env);
      if (method === "POST" && path === "/api/withdrawal-wallet/verify-change") return await verifyWithdrawalWalletChange(request, env);
      if (method === "POST" && path === "/api/admin/withdrawal-wallet-changes/generate") return await generateWithdrawalWalletOtp(request, env);
      if (method === "POST" && path === "/api/admin/delete-user") return await adminDeleteUser(request, env);
      if (method === "POST" && path === "/api/settings") return await updateSettings(request, env);
      if (method === "GET" && path === "/api/admin/operators") return await getAdminOperator(request, env);
      if (method === "POST" && path === "/api/admin/operators") return await saveAdminOperator(request, env);
      if (method === "POST" && path === "/api/admin/login") return await adminLogin(request, env);
      if (method === "POST" && path === "/api/admin/logout") return await adminLogout(request, env);
      if (method === "GET" && path === "/api/admin/sync-users") return await adminSyncUsers(request, env);
      if (method === "GET" && path === "/api/admin/request-counts") return await adminRequestCounts(request, env);
      if (method === "GET" && path === "/api/admin/user-details") return await adminUserDetails(request, env);
      if (method === "POST" && path === "/api/admin/update-user") return await adminUpdateUser(request, env);
      if (method === "GET" && path === "/api/admin/overview") return await adminOverview(request, env);
      if (method === "POST" && path === "/api/change-password") return await changePassword(request, env);
      if (method === "GET" && path === "/api/rentals") return await getRentals(request, env);
      if (method === "POST" && path === "/api/rentals") return await createRental(request, env);
      if (method === "GET" && path === "/api/earning-credits") return await getEarningCredits(request, env);
      if (method === "GET" && path === "/api/server-credits") return await getServerCredits(request, env);
      if (method === "GET" && path === "/api/purchases") return await getPurchases(request, env);
      if (method === "POST" && path === "/api/purchases") return await buyEquipment(request, env);
      if (method === "GET" && path === "/api/account-requests") return await getAccountRequests(request, env);
      if (method === "POST" && path === "/api/account-requests") return await createAccountRequest(request, env);
      if (method === "GET" && path === "/api/deposit-addresses") return await getDepositAddresses(request, env);
      if (method === "GET" && path === "/api/admin/deposit-addresses") return await getAdminDepositAddresses(request, env);
      if (method === "POST" && path === "/api/admin/deposit-addresses") return await saveAdminDepositAddresses(request, env);
      if (method === "POST" && path === "/api/admin/review-rental") return await reviewRental(request, env);
      if (method === "GET" && path === "/api/servers") return await getServers(request, env);
      if (method === "POST" && path === "/api/servers/restore") return await restoreUserServer(request, env);
      if (method === "POST" && path === "/api/admin/server-status") return await setServerStatus(request, env);
      if (method === "POST" && path === "/api/admin/review-account-request") return await reviewAccountRequest(request, env);
    } catch (e) {
      console.error(path, e);
      return Response.json({ error: "Errore del server." }, { status: 500 });
    }
    return new Response("Not found", { status: 404 });
  },
};

async function register(request, env) {
  const body = await request.json();
  const identifier = (body.identifier || "").trim();
  const username = (body.username || "").trim();
  const password = body.password || "";
  const invite = (body.invite || "").trim();
  if (!identifier || !username) return Response.json({ error: "Compila tutti i campi." }, { status: 400 });
  if (!/^[+\-\w.@ ]+$/.test(identifier)) return Response.json({ error: "Inserisci email o numero di telefono valido." }, { status: 400 });
  if (password.length < 6) return Response.json({ error: "Password di almeno 6 caratteri." }, { status: 400 });
  const existing = await env.DB.prepare("SELECT id FROM users WHERE lower(identifier) = lower(?) OR lower(username) = lower(?)").bind(identifier, username).first();
  if (existing) return Response.json({ error: "Email/telefono o nome utente già registrato." }, { status: 400 });
  let referredBy = null;
  if (invite) {
    const ref = await env.DB.prepare("SELECT id FROM users WHERE invite_code = ?").bind(invite).first();
    if (!ref) return Response.json({ error: "Codice invito non valido." }, { status: 400 });
    referredBy = ref.id;
  }
  const id = makeToken();
  const s = genSalt();
  const inviteCode = "H9-" + username.slice(0, 4).toUpperCase() + Math.random().toString(36).slice(2, 6).toUpperCase();
  const ph = await hash(password, s);
  await env.DB.prepare("INSERT INTO users (id, username, identifier, password_hash, salt, invite_code, referred_by, created_at) VALUES (?,?,?,?,?,?,?,?)")
    .bind(id, username, identifier, ph, s, inviteCode, referredBy, Date.now()).run();
  const token = makeToken();
  await env.DB.prepare("INSERT INTO sessions (token, user_id, created_at) VALUES (?,?,?)").bind(token, id, Date.now()).run();
  return Response.json({ ok: true, token });
}

async function login(request, env) {
  const body = await request.json();
  const id = (body.id || "").trim().toLowerCase();
  const pw = body.password || "";
  if (!id || !pw) return Response.json({ error: "Inserisci email/telefono e password." }, { status: 400 });
  const u = await env.DB.prepare("SELECT * FROM users WHERE lower(username) = ? OR lower(identifier) = ?").bind(id, id).first();
  if (!u) return Response.json({ error: "Account non trovato. Registrati prima di accedere." }, { status: 400 });
  const ph = await hash(pw, u.salt);
  if (ph !== u.password_hash) return Response.json({ error: "Password non corretta." }, { status: 400 });
  const token = makeToken();
  await env.DB.prepare("INSERT INTO sessions (token, user_id, created_at) VALUES (?,?,?)").bind(token, u.id, Date.now()).run();
  const role=isProjectOwner(request)?{enabled:false,permissions:[]}:await operatorRoleForUser(env,u.id);
  return Response.json({ ok: true, token, operator:role.enabled, permissions:role.enabled?role.permissions:[] });
}

function recoveryIdentifier(body) {
  const value=typeof body?.identifier==="string"?body.identifier.trim().toLowerCase():"";
  return value.length<=254?value:"";
}
async function findRecoveryAccount(env, identifier) {
  if(!identifier) return null;
  const row=await env.DB.prepare("SELECT u.id,u.username,u.identifier,u.email_verified,e.email AS verification_email,e.status AS verification_status FROM users u LEFT JOIN email_verifications e ON e.user_id=u.id WHERE lower(u.username)=? OR lower(u.identifier)=? LIMIT 1").bind(identifier,identifier).first();
  if(!row) return null;
  const valid=email=>typeof email==="string"&&/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email);
  const email=Number(row.email_verified)===1&&valid(row.verification_email)?row.verification_email.toLowerCase():valid(row.identifier)?row.identifier.toLowerCase():"";
  return {...row,recovery_email:email};
}
async function requestPasswordRecovery(request,env) {
  let body;try{body=await request.json();}catch{return bad("Inserisci il tuo account.");}
  const identifier=recoveryIdentifier(body);
  if(!identifier) return bad("Inserisci un identificativo valido.");
  const account=await findRecoveryAccount(env,identifier);
  if(!account?.recovery_email) return Response.json({ok:true});
  const now=Date.now();
  const previous=await env.DB.prepare("SELECT email,status,otp_expires,updated_at FROM email_verifications WHERE user_id=?").bind(account.id).first();
  if(previous?.email===account.recovery_email&&previous.status==="recovery_code_sent"&&Number(previous.otp_expires||0)>now) return Response.json({ok:true});
  if(previous?.email===account.recovery_email&&previous.status==="recovery_pending"&&now-Number(previous.updated_at||0)<60000) return Response.json({ok:true});
  await env.DB.prepare("INSERT INTO email_verifications (user_id,email,status,created_at,updated_at,otp_hash,otp_expires,attempts) VALUES (?,?, 'recovery_pending',?,?,NULL,NULL,0) ON CONFLICT(user_id) DO UPDATE SET email=excluded.email,status='recovery_pending',created_at=excluded.created_at,updated_at=excluded.updated_at,otp_hash=NULL,otp_expires=NULL,attempts=0").bind(account.id,account.recovery_email,now,now).run();
  return Response.json({ok:true});
}

async function resetPasswordWithOtp(request,env) {
  let body;try{body=await request.json();}catch{return bad("Dati di recupero non validi.");}
  const identifier=recoveryIdentifier(body),code=String(body.code||"").trim(),next=typeof body.password==="string"?body.password:"";
  if(!identifier) return bad("Inserisci un identificativo valido.");
  if(!/^\d{6}$/.test(code)) return bad("Inserisci il codice OTP di 6 cifre.");
  if(next.length<6||next.length>256) return bad("La nuova password deve contenere da 6 a 256 caratteri.");
  const account=await findRecoveryAccount(env,identifier);
  const invalid=()=>Response.json({error:"Codice OTP non valido o scaduto."},{status:400});
  if(!account?.recovery_email) return invalid();
  const row=await env.DB.prepare("SELECT email,status,otp_hash,otp_expires,attempts FROM email_verifications WHERE user_id=?").bind(account.id).first();
  if(!row||row.email!==account.recovery_email||row.status!=="recovery_code_sent"||!row.otp_hash) return invalid();
  const now=Date.now();
  if(Number(row.otp_expires||0)<=now){await env.DB.prepare("UPDATE email_verifications SET status='expired',otp_hash=NULL,updated_at=? WHERE user_id=? AND status='recovery_code_sent'").bind(now,account.id).run();return invalid();}
  const attempts=Number(row.attempts||0);
  if(attempts>=5) return invalid();
  const submitted=await hash(code,String(account.id)+":"+row.email);
  if(submitted!==row.otp_hash){const nextAttempts=attempts+1;await env.DB.prepare("UPDATE email_verifications SET attempts=?,status=?,otp_hash=?,updated_at=? WHERE user_id=? AND status='recovery_code_sent' AND otp_hash=?").bind(nextAttempts,nextAttempts>=5?"expired":"recovery_code_sent",nextAttempts>=5?null:row.otp_hash,now,account.id,row.otp_hash).run();return invalid();}
  const consumed=await env.DB.prepare("UPDATE email_verifications SET status='verified',otp_hash=NULL,otp_expires=NULL,attempts=0,updated_at=? WHERE user_id=? AND status='recovery_code_sent' AND otp_hash=? AND otp_expires>?").bind(now,account.id,row.otp_hash,now).run();
  if(!consumed.meta?.changes) return invalid();
  const salt=genSalt(),passwordHash=await hash(next,salt);
  await env.DB.batch([
    env.DB.prepare("UPDATE users SET password_hash=?,salt=?,email_verified=1 WHERE id=?").bind(passwordHash,salt,account.id),
    env.DB.prepare("DELETE FROM sessions WHERE user_id=?").bind(account.id)
  ]);
  return Response.json({ok:true});
}
async function logout(request, env) {
  const h = (request.headers.get("x-session-token") || "").trim();
  if (h) await env.DB.prepare("DELETE FROM sessions WHERE token = ?").bind(h).run();
  return Response.json({ ok: true });
}

async function me(request, env) {
  const user = await sessionUser(request, env);
  if (!user) return Response.json({ error: "Non autenticato." }, { status: 401 });
  if(!isProjectOwner(request)) {
    const role=await operatorRoleForUser(env,user.id);
    if(role.enabled) return Response.json({operator:true,permissions:role.permissions,user:{id:user.id,username:user.username,identifier:user.identifier}});
  }
  return Response.json(await fullState(env, user));
}

async function authed(request, env) {
  const user = await sessionUser(request, env);
  if (!user) throw Object.assign(new Error("Non autenticato."), { status: 401 });
  return user;
}
function bad(msg) { return Response.json({ error: msg }, { status: 400 }); }

async function deposit(request, env) {
  const user = await authed(request, env);
  const body = await request.json();
  const amount = parseFloat(body.amount);
  const net = (body.net || "USDT").trim();
  if (!(amount > 0)) return bad("Importo non valido.");
  const wallet = await accrue(env, user.id);
  wallet.deposit += amount;
  wallet.total_deposited += amount;
  await env.DB.prepare("UPDATE wallets SET deposit = ?, total_deposited = ? WHERE user_id = ?").bind(wallet.deposit, wallet.total_deposited, user.id).run();
  await env.DB.prepare("INSERT INTO deposits (user_id, amount, net, date) VALUES (?,?,?,?)").bind(user.id, amount, net, Date.now()).run();
  return Response.json({ ok: true, wallet });
}

async function invest(request, env) {
  const user = await authed(request, env);
  const body = await request.json();
  const amount = parseFloat(body.amount);
  const days = parseInt(body.days, 10);
  const plan = PLANS.find(p => p.days === days);
  if (!plan) return bad("Piano non valido.");
  if (!(amount > 0) || amount < plan.min) return bad(`Importo minimo per questo piano: ${plan.min} USDT.`);
  const wallet = await accrue(env, user.id);
  if (amount > wallet.deposit + 1e-9) return bad("Saldo deposito insufficiente.");
  wallet.deposit -= amount;
  await env.DB.prepare("UPDATE wallets SET deposit = ? WHERE user_id = ?").bind(wallet.deposit, user.id).run();
  await env.DB.prepare("INSERT INTO investments (user_id, amount, days, rate, created, credited, active) VALUES (?,?,?,?,?,0,1)")
    .bind(user.id, amount, plan.days, plan.rate, Date.now()).run();
  return Response.json({ ok: true, wallet });
}

async function withdraw(request, env) {
  const user = await authed(request, env);
  const body = await request.json();
  const amount = parseFloat(body.amount);
  const net = (body.net || "USDT").trim();
  if (!(amount > 0)) return bad("Importo non valido.");
  await accrue(env, user.id);
  await accrueServerEarnings(env, user.id);
  await accrueReferralNetwork(env, user.id);
  await accrueUplineCommissions(env, user.id);
  const wallet = await env.DB.prepare("SELECT * FROM wallets WHERE user_id = ?").bind(user.id).first();
  if (amount > wallet.earnings + 1e-9) return bad("Saldo guadagni non sufficiente.");
  wallet.earnings -= amount;
  wallet.total_withdrawn += amount;
  await env.DB.prepare("UPDATE wallets SET earnings = ?, total_withdrawn = ? WHERE user_id = ?").bind(wallet.earnings, wallet.total_withdrawn, user.id).run();
  await env.DB.prepare("INSERT INTO withdrawals (user_id, amount, net, date) VALUES (?,?,?,?)").bind(user.id, amount, net, Date.now()).run();
  return Response.json({ ok: true, wallet });
}

async function upsertSettings(env, userId, { wallets, notifications }) {
  const cur = await env.DB.prepare("SELECT * FROM settings WHERE user_id = ?").bind(userId).first();
  let ww = {}; let notif = true;
  if (cur) { try { ww = JSON.parse(cur.withdraw_wallets || "{}"); } catch { ww = {}; } notif = !!cur.notifications; }
  if (wallets) ww = wallets;
  if (typeof notifications === "boolean") notif = notifications;
  await env.DB.prepare(
    "INSERT INTO settings (user_id, withdraw_wallets, notifications) VALUES (?,?,?) " +
    "ON CONFLICT(user_id) DO UPDATE SET withdraw_wallets=excluded.withdraw_wallets, notifications=excluded.notifications"
  ).bind(userId, JSON.stringify(ww), notif ? 1 : 0).run();
  return { withdraw_wallets: ww, notifications: notif };
}

async function saveWallet(request, env) {
  const user = await authed(request, env);
  const body = await request.json();
  const wallets = body.wallets || {};
  const current = await env.DB.prepare("SELECT withdraw_wallets FROM settings WHERE user_id=?").bind(user.id).first();
  let existing = {}; try { existing = JSON.parse(current?.withdraw_wallets || "{}"); } catch {}
  const clean = { ...existing };
  for (const [net, addr] of Object.entries(wallets)) {
    const a = (typeof addr === "string" ? addr : "").trim();
    if (!["bep20", "trc20"].includes(net)) return bad("Rete USDT non valida.");
    if (!a) continue;
    if (!validWalletAddress(net, a)) return bad(net === "bep20" ? "Indirizzo BEP20 non valido." : "Indirizzo TRC20 non valido.");
    if (existing[net] && existing[net] !== a) return Response.json({error:"Wallet già associato: richiedi la modifica con OTP."},{status:409});
    clean[net] = a;
  }
  const s = await upsertSettings(env, user.id, { wallets: clean });
  return Response.json({ ok: true, acc: s });
}

async function requestWithdrawalWalletChange(request, env) {
  const user = await sessionUser(request, env);
  if (!user) return Response.json({ error: "Non autenticato." }, { status: 401 });
  if (!user.email_verified) return Response.json({ error: "Verifica prima la tua email per modificare il wallet con OTP." }, { status: 403 });
  let body; try { body = await request.json(); } catch { return bad("Richiesta di modifica wallet non valida."); }
  const network = String(body.network || ""), address = String(body.address || "").trim();
  if (!["bep20", "trc20"].includes(network) || !validWalletAddress(network, address)) return bad(network === "trc20" ? "Indirizzo TRC20 non valido." : "Indirizzo BEP20 non valido.");
  const verified = await env.DB.prepare("SELECT email FROM email_verifications WHERE user_id=? AND status='verified'").bind(user.id).first();
  const email = String(verified?.email || (user.identifier.includes("@") ? user.identifier : "")).trim().toLowerCase();
  if (!email || !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) return Response.json({ error: "Indirizzo email verificato non disponibile." }, { status: 400 });
  const existing = await env.DB.prepare("SELECT withdraw_wallets FROM settings WHERE user_id=?").bind(user.id).first();
  let wallets = {}; try { wallets = JSON.parse(existing?.withdraw_wallets || "{}"); } catch {}
  if (!wallets[network]) return Response.json({ error: "Associa prima un indirizzo per questa rete." }, { status: 409 });
  if (wallets[network] === address) return Response.json({ error: "Il nuovo indirizzo coincide con quello attuale." }, { status: 400 });
  const now = Date.now();
  await env.DB.prepare("INSERT INTO withdrawal_wallet_changes (user_id,network,address,email,status,otp_hash,otp_expires,attempts,created_at,updated_at) VALUES (?,?,?,?,'pending',NULL,NULL,0,?,?) ON CONFLICT(user_id) DO UPDATE SET network=excluded.network,address=excluded.address,email=excluded.email,status='pending',otp_hash=NULL,otp_expires=NULL,attempts=0,created_at=excluded.created_at,updated_at=excluded.updated_at").bind(user.id,network,address,email,now,now).run();
  return Response.json({ ok: true, status: "pending" });
}
async function generateWithdrawalWalletOtp(request, env) {
  const access = await requireAdminPermission(request, env, "verification");
  if (!access) return Response.json({ error: "Permesso amministratore mancante." }, { status: 403 });
  let body; try { body = await request.json(); } catch { return bad("Richiesta OTP non valida."); }
  const userId = String(body.id || "");
  const row = await env.DB.prepare("SELECT c.user_id,c.network,c.address,c.email,c.status,u.username FROM withdrawal_wallet_changes c JOIN users u ON u.id=c.user_id WHERE c.user_id=?").bind(userId).first();
  if (!row || !["pending","code_sent"].includes(row.status)) return bad("Richiesta OTP wallet non disponibile.");
  const bytes = new Uint32Array(1); crypto.getRandomValues(bytes);
  const otp = String(bytes[0] % 1000000).padStart(6,"0"), now = Date.now();
  const otpHash = await hash(otp, String(row.user_id) + ":" + row.email + ":" + row.address);
  const updated = await env.DB.prepare("UPDATE withdrawal_wallet_changes SET status='code_sent',otp_hash=?,otp_expires=?,attempts=0,updated_at=? WHERE user_id=? AND status=?").bind(otpHash,now+15*60*1000,now,row.user_id,row.status).run();
  if (!updated.meta?.changes) return Response.json({error:"Richiesta OTP già aggiornata."},{status:409});
  return Response.json({ok:true,email:row.email,username:row.username,network:row.network,address:row.address,otp,expires_at:now+15*60*1000});
}
async function verifyWithdrawalWalletChange(request, env) {
  const user = await sessionUser(request, env);
  if (!user) return Response.json({ error: "Non autenticato." }, { status: 401 });
  let body; try { body = await request.json(); } catch { return bad("Codice OTP non valido."); }
  const code = String(body.code || "").trim();
  if (!/^\d{6}$/.test(code)) return bad("Inserisci un codice OTP di 6 cifre.");
  const row = await env.DB.prepare("SELECT network,address,email,status,otp_hash,otp_expires,attempts FROM withdrawal_wallet_changes WHERE user_id=?").bind(user.id).first();
  if (!row || row.status !== "code_sent" || !row.otp_hash) return bad("Non c’è un codice attivo. Invia prima una richiesta.");
  const now = Date.now();
  if (Number(row.otp_expires || 0) <= now) { await env.DB.prepare("UPDATE withdrawal_wallet_changes SET status='expired',otp_hash=NULL,updated_at=? WHERE user_id=?").bind(now,user.id).run(); return bad("Codice OTP scaduto. Invia una nuova richiesta."); }
  const attempts = Number(row.attempts || 0);
  if (attempts >= 5) return bad("Tentativi esauriti. Invia una nuova richiesta.");
  const submitted = await hash(code,String(user.id)+":"+row.email+":"+row.address);
  if (submitted !== row.otp_hash) { const next=attempts+1; await env.DB.prepare("UPDATE withdrawal_wallet_changes SET attempts=?,status=?,otp_hash=?,updated_at=? WHERE user_id=? AND status='code_sent'").bind(next,next>=5?"expired":"code_sent",next>=5?null:row.otp_hash,now,user.id).run(); return bad(next>=5?"Tentativi esauriti. Invia una nuova richiesta.":"Codice OTP non corretto."); }
  const current = await env.DB.prepare("SELECT withdraw_wallets,notifications FROM settings WHERE user_id=?").bind(user.id).first();
  let wallets={}; try { wallets=JSON.parse(current?.withdraw_wallets||"{}"); } catch {}
  wallets[row.network]=row.address;
  const results=await env.DB.batch([
    env.DB.prepare("UPDATE settings SET withdraw_wallets=? WHERE user_id=? AND EXISTS (SELECT 1 FROM withdrawal_wallet_changes WHERE user_id=? AND status='code_sent' AND otp_hash=? AND otp_expires>?)").bind(JSON.stringify(wallets),user.id,user.id,row.otp_hash,now),
    env.DB.prepare("UPDATE withdrawal_wallet_changes SET status='verified',otp_hash=NULL,otp_expires=NULL,attempts=0,updated_at=? WHERE user_id=? AND status='code_sent' AND otp_hash=? AND otp_expires>?").bind(now,user.id,row.otp_hash,now)
  ]);
  if (!results[0]?.meta?.changes || !results[1]?.meta?.changes) return Response.json({error:"Codice OTP già utilizzato o scaduto."},{status:409});
  return Response.json({ok:true,acc:{withdraw_wallets:wallets,notifications:!!Number(current?.notifications??1)}});
}

async function updateSettings(request, env) {
  const user = await authed(request, env);
  const body = await request.json();
  const notif = typeof body.notifications === "boolean" ? body.notifications : null;
  const s = await upsertSettings(env, user.id, { notifications: notif === null ? undefined : notif });
  return Response.json({ ok: true, acc: s });
}

async function changePassword(request, env) {
  const user = await authed(request, env);
  const body = await request.json();
  const current = body.current || "";
  const next = body.next || "";
  if (next.length < 6) return bad("La nuova password deve avere almeno 6 caratteri.");
  const ph = await hash(current, user.salt);
  if (ph !== user.password_hash) return bad("Password attuale non corretta.");
  const s = genSalt();
  const hash2 = await hash(next, s);
  await env.DB.prepare("UPDATE users SET password_hash = ?, salt = ? WHERE id = ?").bind(hash2, s, user.id).run();
  return Response.json({ ok: true });
}

const COMPUTE_CATALOG = new Map([
  ["basic-20", { price: 20, daily: 0.10 }], ["basic-60", { price: 60, daily: 0.55 }], ["basic-100", { price: 100, daily: 1.10 }], ["basic-200", { price: 200, daily: 2.20 }],
  ["mid-150", { price: 150, daily: 1.65 }], ["mid-350", { price: 350, daily: 3.85 }], ["mid-700", { price: 700, daily: 7.70 }], ["mid-900", { price: 900, daily: 9.90 }],
  ["elite-1000", { price: 1000, daily: 11.00 }], ["elite-2500", { price: 2500, daily: 27.50 }], ["elite-5000", { price: 5000, daily: 55.00 }], ["elite-10000", { price: 10000, daily: 110.00 }]
]);
async function getRentals(request, env) {
  const user = await sessionUser(request, env);
  if (!user) return Response.json({ error: "Non autenticato." }, { status: 401 });
  const rentals = (await env.DB.prepare("SELECT id, product_id, status, created_at FROM compute_rentals WHERE user_id = ? ORDER BY id DESC").bind(user.id).all()).results;
  return Response.json({ rentals });
}
async function createRental(request, env) {
  const user = await sessionUser(request, env);
  if (!user) return Response.json({ error: "Non autenticato." }, { status: 401 });
  const body = await request.json();
  const productId = typeof body.product_id === "string" ? body.product_id : "";
  if (!COMPUTE_CATALOG.has(productId)) return bad("Configurazione non valida.");
  const createdAt = Date.now();
  const result = await env.DB.prepare("INSERT INTO compute_rentals (user_id, product_id, status, created_at) VALUES (?, ?, 'pending', ?)").bind(user.id, productId, createdAt).run();
  return Response.json({ ok: true, rental: { id: result.meta?.last_row_id, product_id: productId, status: "pending", created_at: createdAt } }, { status: 201 });
}


async function getAccountRequests(request, env) {
  const user = await sessionUser(request, env);
  if (!user) return Response.json({ error: "Non autenticato." }, { status: 401 });
  const rows = (await env.DB.prepare("SELECT id, kind, amount, status, created_at, reviewed_at, network, tx_hash, withdrawal_address FROM account_requests WHERE user_id = ? ORDER BY id DESC").bind(user.id).all()).results;
  return Response.json({ requests: rows });
}
async function getDepositAddresses(request, env) {
  const user = await sessionUser(request, env);
  if (!user) return Response.json({ error: "Non autenticato." }, { status: 401 });
  const rows = (await env.DB.prepare("SELECT network,address FROM platform_deposit_addresses").all()).results || [];
  return Response.json({ addresses: Object.fromEntries(rows.map(row => [row.network, row.address])) });
}
async function getAdminDepositAddresses(request, env) {
  const adminAccess = await requireAdminPermission(request, env, "wallets");
  if (!adminAccess) return Response.json({ error: "Permesso amministratore mancante." }, { status: 403 });
  const rows = (await env.DB.prepare("SELECT network,address FROM platform_deposit_addresses").all()).results || [];
  return Response.json({ addresses: Object.fromEntries(rows.map(row => [row.network, row.address])) });
}
function validWalletAddress(network, address) {
  if (typeof address !== 'string') return false;
  if (network === 'bep20') return /^0x[a-fA-F0-9]{40}$/.test(address);
  if (network === 'trc20') return /^T[1-9A-HJ-NP-Za-km-z]{33}$/.test(address);
  return false;
}
async function saveAdminDepositAddresses(request, env) {
  const adminAccess = await requireAdminPermission(request, env, "wallets");
  if (!adminAccess) return Response.json({ error: "Permesso amministratore mancante." }, { status: 403 });
  const body = await request.json();
  const addresses = body.addresses || {};
  const networks = ['bep20', 'trc20'];
  const statements = [];
  for (const network of networks) {
    const address = typeof addresses[network] === 'string' ? addresses[network].trim() : '';
    if (address && !validWalletAddress(network, address)) return bad(network === 'bep20' ? "Indirizzo BEP20 non valido." : "Indirizzo TRC20 non valido.");
    statements.push(address
      ? env.DB.prepare("INSERT INTO platform_deposit_addresses (network,address,updated_at) VALUES (?,?,?) ON CONFLICT(network) DO UPDATE SET address=excluded.address,updated_at=excluded.updated_at").bind(network, address, Date.now())
      : env.DB.prepare("DELETE FROM platform_deposit_addresses WHERE network=?").bind(network));
  }
  await env.DB.batch(statements);
  return Response.json({ ok: true, addresses });
}
async function createAccountRequest(request, env) {
  const user = await sessionUser(request, env);
  if (!user) return Response.json({ error: "Non autenticato." }, { status: 401 });
  const body = await request.json();
  const kind = body.kind;
  const amount = Number(body.amount);
  const network = body.network;
  if (!['deposit', 'withdrawal'].includes(kind)) return bad("Tipo richiesta non valido.");
  if (!Number.isFinite(amount) || amount <= 0 || amount > 1000000) return bad("Importo non valido.");
  if (!['bep20', 'trc20'].includes(network)) return bad("Rete USDT non valida.");
  let txHash = null;
  let withdrawalAddress = null;
  if (kind === 'deposit') {
    txHash = typeof body.tx_hash === 'string' ? body.tx_hash.trim().toLowerCase() : '';
    if (!/^(0x)?[a-f0-9]{64}$/.test(txHash)) return bad("TXHash non valido.");
    const depositAddress = await env.DB.prepare("SELECT address FROM platform_deposit_addresses WHERE network=?").bind(network).first();
    if (!depositAddress?.address) return bad("Indirizzo di deposito non configurato per questa rete.");
  }
  if (kind === 'withdrawal') {
    const settings = await env.DB.prepare("SELECT withdraw_wallets FROM settings WHERE user_id=?").bind(user.id).first();
    let addresses = {}; try { addresses = JSON.parse(settings?.withdraw_wallets || '{}'); } catch {}
    withdrawalAddress = typeof addresses[network] === 'string' ? addresses[network].trim() : '';
    if (!validWalletAddress(network, withdrawalAddress)) return bad("Associa prima un indirizzo USDT valido per la rete selezionata.");
  }
  await env.DB.prepare("INSERT OR IGNORE INTO wallets (user_id, deposit, earnings, total_deposited, total_earned, total_withdrawn) VALUES (?,0,0,0,0,0)").bind(user.id).run();
  if (kind === 'withdrawal') {
    await accrue(env, user.id);
    await accrueServerEarnings(env, user.id);
    await accrueReferralNetwork(env, user.id);
    await accrueUplineCommissions(env, user.id);
    const held = await env.DB.prepare("UPDATE wallets SET earnings=earnings-? WHERE user_id=? AND earnings>=?").bind(amount, user.id, amount).run();
    if (!held.meta?.changes) return bad("Importo superiore al saldo disponibile.");
  }
  const now = Date.now();
  try {
    const result = await env.DB.prepare("INSERT INTO account_requests (user_id, kind, amount, status, created_at, review_key, network, tx_hash, withdrawal_address) VALUES (?, ?, ?, 'pending', ?, ?, ?, ?, ?)").bind(user.id, kind, amount, now, kind === 'withdrawal' ? 'held' : null, network, txHash, withdrawalAddress).run();
    return Response.json({ ok: true, request: { id: result.meta?.last_row_id, kind, amount, status: 'pending', created_at: now, network, tx_hash: txHash } }, { status: 201 });
  } catch (error) {
    if (kind === 'withdrawal') await env.DB.prepare("UPDATE wallets SET earnings=earnings+? WHERE user_id=?").bind(amount, user.id).run();
    if (kind === 'deposit' && String(error).toLowerCase().includes('unique')) return Response.json({ error: "TXHash già associato a una richiesta." }, { status: 409 });
    throw error;
  }
}
async function reviewRental(request, env) {
  const adminAccess = await requireAdminPermission(request, env, "compute-requests");
  if (!adminAccess) return Response.json({ error: "Permesso amministratore mancante." }, { status: 403 });
  const body = await request.json();
  const id = Number(body.id);
  const status = body.status;
  if (!Number.isInteger(id) || id < 1 || !['approved', 'rejected'].includes(status)) return bad("Decisione non valida.");
  const result = await env.DB.prepare("UPDATE compute_rentals SET status = ?, reviewed_at = ? WHERE id = ? AND status = 'pending'").bind(status, Date.now(), id).run();
  if (!result.meta?.changes) return Response.json({ error: "Richiesta non trovata o già gestita." }, { status: 409 });
  return Response.json({ ok: true, id, status });
}
async function reviewAccountRequest(request, env) {
  const adminAccess = await getAdminAccess(request, env);
  if (!adminAccess) return Response.json({ error: "Accesso amministratore richiesto." }, { status: 403 });
  const body = await request.json();
  const id = Number(body.id);
  const decision = body.status;
  if (!Number.isInteger(id) || id < 1 || !['approved', 'rejected'].includes(decision)) return bad("Decisione non valida.");
  const req = await env.DB.prepare("SELECT id, user_id, kind, amount, status, review_key FROM account_requests WHERE id = ?").bind(id).first();
  if (!req || req.status !== 'pending') return Response.json({ error: "Richiesta non trovata o già gestita." }, { status: 409 });
  const neededPermission = req.kind === "deposit" ? "deposit-requests" : "withdrawal-requests";
  if (!adminAccess.owner && !adminAccess.permissions.includes(neededPermission)) return Response.json({ error: "Permesso amministratore mancante." }, { status: 403 });
  const now = Date.now();
  const reviewer = request.headers.get("x-websim-user-id");
  if (decision === 'rejected') {
    const results = await env.DB.batch([
      env.DB.prepare("UPDATE account_requests SET status='rejected', reviewed_at=?, reviewed_by=?, review_key=CASE WHEN review_key='held' THEN 'releasing' ELSE review_key END WHERE id=? AND status='pending'").bind(now, reviewer, id),
      env.DB.prepare("UPDATE wallets SET earnings=earnings+(SELECT amount FROM account_requests WHERE id=? AND status='rejected' AND review_key='releasing') WHERE user_id=(SELECT user_id FROM account_requests WHERE id=? AND status='rejected' AND review_key='releasing') AND EXISTS (SELECT 1 FROM account_requests WHERE id=? AND status='rejected' AND review_key='releasing')").bind(id, id, id),
      env.DB.prepare("UPDATE account_requests SET review_key='released' WHERE id=? AND status='rejected' AND review_key='releasing'").bind(id)
    ]);
    if (!results[0]?.meta?.changes) return Response.json({ error: "La richiesta è già stata gestita." }, { status: 409 });
    return Response.json({ ok: true, id, status: 'rejected' });
  }
  const held = req.kind === 'withdrawal' && req.review_key === 'held';
  const key = makeToken();
  const statements = await env.DB.batch([
    env.DB.prepare("UPDATE account_requests SET status='processing', review_key=?, reviewed_at=?, reviewed_by=? WHERE id=? AND status='pending'").bind(key, now, reviewer, id),
    env.DB.prepare("INSERT INTO request_effects (request_id, user_id, kind, amount, review_key, applied) SELECT r.id, r.user_id, r.kind, r.amount, ?, 0 FROM account_requests r WHERE r.id=? AND r.status='processing' AND r.review_key=? AND (r.kind='deposit' OR ?=1 OR EXISTS (SELECT 1 FROM wallets w WHERE w.user_id=r.user_id AND w.earnings>=r.amount)) ON CONFLICT(request_id) DO NOTHING").bind(key, id, key, held ? 1 : 0),
    env.DB.prepare("UPDATE wallets SET deposit=deposit+CASE WHEN (SELECT kind FROM account_requests WHERE id=? AND review_key=?)='deposit' THEN (SELECT amount FROM account_requests WHERE id=? AND review_key=?) ELSE 0 END, total_deposited=total_deposited+CASE WHEN (SELECT kind FROM account_requests WHERE id=? AND review_key=?)='deposit' THEN (SELECT amount FROM account_requests WHERE id=? AND review_key=?) ELSE 0 END, earnings=earnings-CASE WHEN (SELECT kind FROM account_requests WHERE id=? AND review_key=?)='withdrawal' AND ?=0 THEN (SELECT amount FROM account_requests WHERE id=? AND review_key=?) ELSE 0 END, total_withdrawn=total_withdrawn+CASE WHEN (SELECT kind FROM account_requests WHERE id=? AND review_key=?)='withdrawal' THEN (SELECT amount FROM account_requests WHERE id=? AND review_key=?) ELSE 0 END WHERE user_id=(SELECT user_id FROM account_requests WHERE id=? AND review_key=?) AND EXISTS (SELECT 1 FROM request_effects e WHERE e.request_id=? AND e.review_key=? AND e.applied=0)").bind(id,key,id,key,id,key,id,key,id,key,held?1:0,id,key,id,key,id,key,id,key,id,key),
    env.DB.prepare("UPDATE request_effects SET applied=1 WHERE request_id=? AND review_key=? AND applied=0").bind(id, key),
    env.DB.prepare("INSERT INTO deposits (user_id, amount, net, date) SELECT user_id, amount, CASE network WHEN 'bep20' THEN 'BNB Smart Chain · BEP20' WHEN 'trc20' THEN 'TRON · TRC20' ELSE COALESCE(network,'SIMULATED') END, reviewed_at FROM account_requests WHERE id=? AND status='processing' AND review_key=? AND kind='deposit' AND EXISTS (SELECT 1 FROM request_effects WHERE request_id=? AND review_key=? AND applied=1)").bind(id, key, id, key),
    env.DB.prepare("INSERT INTO withdrawals (user_id, amount, net, date) SELECT user_id, amount, CASE network WHEN 'bep20' THEN 'BNB Smart Chain · BEP20' WHEN 'trc20' THEN 'TRON · TRC20' ELSE COALESCE(network,'SIMULATED') END, reviewed_at FROM account_requests WHERE id=? AND status='processing' AND review_key=? AND kind='withdrawal' AND EXISTS (SELECT 1 FROM request_effects WHERE request_id=? AND review_key=? AND applied=1)").bind(id, key, id, key),
    env.DB.prepare("UPDATE account_requests SET status='approved' WHERE id=? AND status='processing' AND review_key=? AND EXISTS (SELECT 1 FROM request_effects WHERE request_id=? AND review_key=? AND applied=1)").bind(id, key, id, key)
  ]);
  if (!statements[0]?.meta?.changes) return Response.json({ error: "La richiesta è già stata gestita." }, { status: 409 });
  if (!statements[1]?.meta?.changes) {
    await env.DB.prepare("UPDATE account_requests SET status='pending', review_key=NULL, reviewed_at=NULL, reviewed_by=NULL WHERE id=? AND status='processing' AND review_key=?").bind(id, key).run();
    return bad("Saldo disponibile insufficiente per approvare questo prelievo.");
  }
  return Response.json({ ok: true, id, status: 'approved' });
}

async function getServerCredits(request, env) {
  const user = await sessionUser(request, env);
  if (!user) return Response.json({ error: "Non autenticato." }, { status: 401 });
  const purchaseId = new URL(request.url).searchParams.get("purchase_id") || "";
  if (!purchaseId || purchaseId.length > 128) return bad("Server non valido.");
  const owned = await env.DB.prepare("SELECT p.product_id FROM compute_purchases p JOIN server_instances s ON s.purchase_id=p.id WHERE p.id=? AND p.user_id=?").bind(purchaseId, user.id).first();
  if (!owned) return Response.json({ error: "Server non trovato." }, { status: 404 });
  await accrueServerEarnings(env, user.id);
  const [server, rows, sum] = await Promise.all([
    env.DB.prepare("SELECT credited_yield FROM server_instances WHERE purchase_id=? AND user_id=?").bind(purchaseId, user.id).first(),
    env.DB.prepare("SELECT amount,created_at FROM earning_credits WHERE user_id=? AND earning_type='server' AND server_purchase_id=? ORDER BY created_at DESC LIMIT 100").bind(user.id, purchaseId).all(),
    env.DB.prepare("SELECT COALESCE(SUM(amount),0) AS total FROM earning_credits WHERE user_id=? AND earning_type='server' AND server_purchase_id=?").bind(user.id, purchaseId).first("total")
  ]);
  const creditedTotal = Number(server?.credited_yield || 0);
  const attributedTotal = Number(sum || 0);
  return Response.json({ product_id: owned.product_id, credits: rows.results || [], credited_total: creditedTotal, unassigned_legacy: Math.max(0, creditedTotal - attributedTotal) });
}
async function getEarningCredits(request, env) {
  const user = await sessionUser(request, env);
  if (!user) return Response.json({ error: "Non autenticato." }, { status: 401 });
  await accrue(env, user.id);
  await accrueServerEarnings(env, user.id);
  await accrueReferralNetwork(env, user.id);
  await accrueUplineCommissions(env, user.id);
  const credits = (await env.DB.prepare("SELECT amount, created_at, earning_type FROM earning_credits WHERE user_id=? AND earning_type!='server' AND server_purchase_id IS NULL ORDER BY created_at DESC").bind(user.id).all()).results;
  return Response.json({ credits });
}
async function getPurchases(request, env) {
  const user = await sessionUser(request, env);
  if (!user) return Response.json({ error: "Non autenticato." }, { status: 401 });
  const purchases = (await env.DB.prepare("SELECT id, product_id, price, purchased_at FROM compute_purchases WHERE user_id = ? ORDER BY purchased_at DESC").bind(user.id).all()).results;
  return Response.json({ purchases });
}
async function buyEquipment(request, env) {
  const user = await sessionUser(request, env);
  if (!user) return Response.json({ error: "Non autenticato." }, { status: 401 });
  const body = await request.json();
  const productId = typeof body.product_id === "string" ? body.product_id : "";
  const product = COMPUTE_CATALOG.get(productId);
  if (!product) return bad("Modello non valido.");
  await env.DB.prepare("INSERT OR IGNORE INTO wallets (user_id, deposit, earnings, total_deposited, total_earned, total_withdrawn) VALUES (?,0,0,0,0,0)").bind(user.id).run();
  const id = makeToken();
  const now = Date.now();
  const result = await env.DB.batch([
    env.DB.prepare("INSERT INTO compute_purchases (id, user_id, product_id, price, purchased_at) SELECT ?, ?, ?, ?, ? WHERE EXISTS (SELECT 1 FROM wallets WHERE user_id = ? AND deposit >= ?)").bind(id, user.id, productId, product.price, now, user.id, product.price),
    env.DB.prepare("UPDATE wallets SET deposit = deposit - ? WHERE user_id = ? AND deposit >= ? AND EXISTS (SELECT 1 FROM compute_purchases WHERE id = ? AND user_id = ?)").bind(product.price, user.id, product.price, id, user.id),
    env.DB.prepare("INSERT INTO server_instances (purchase_id, user_id, status, updated_at) SELECT id, user_id, 'pending', ? FROM compute_purchases WHERE id = ? AND user_id = ?").bind(now, id, user.id)
  ]);
  if (!result[0]?.meta?.changes) return bad("Saldo deposito insufficiente per questo modello.");
  const wallet = await env.DB.prepare("SELECT deposit FROM wallets WHERE user_id = ?").bind(user.id).first();
  return Response.json({ ok: true, purchase: { id, product_id: productId, price: product.price, purchased_at: now }, deposit_balance: wallet?.deposit ?? 0 }, { status: 201 });
}

async function getServers(request, env) {
  const user = await sessionUser(request, env);
  if (!user) return Response.json({ error: "Non autenticato." }, { status: 401 });
  await accrueServerEarnings(env, user.id);
  const rows = (await env.DB.prepare("SELECT p.id AS purchase_id, p.product_id, p.price, p.purchased_at, s.status, s.updated_at, s.yield_total, s.active_seconds, s.active_since FROM compute_purchases p JOIN server_instances s ON s.purchase_id=p.id WHERE p.user_id=? ORDER BY p.purchased_at DESC").bind(user.id).all()).results || [];
  const now = Date.now();
  const servers = rows.map(row => {
    const model = COMPUTE_CATALOG.get(row.product_id) || { daily: 0 };
    const activeSince = row.status === "active" ? Number(row.active_since || row.updated_at) : null;
    const runningSeconds = activeSince ? Math.max(0, (now - activeSince) / 1000) : 0;
    const activeSeconds = Number(row.active_seconds || 0) + runningSeconds;
    const totalYield = Number(row.yield_total || 0) + model.daily * activeSeconds / 86400;
    return { ...row, total_yield: totalYield, active_seconds: activeSeconds, maintenance_fee: Math.round(Number(row.price || 0) * 5) / 100 };
  });
  const payments = (await env.DB.prepare("SELECT id, purchase_id, amount, created_at, maintenance_started_at FROM server_maintenance_payments WHERE user_id=? ORDER BY created_at DESC").bind(user.id).all()).results || [];
  return Response.json({ servers, maintenance_payments: payments });
}
async function restoreMaintenanceServer(env, userId, purchaseId, actorId) {
  const current = await env.DB.prepare("SELECT s.status, s.updated_at, p.price FROM server_instances s JOIN compute_purchases p ON p.id=s.purchase_id WHERE s.purchase_id=? AND s.user_id=?").bind(purchaseId, userId).first();
  if (!current) return { error: "Server non trovato.", status: 404 };
  if (current.status !== "maintenance") return { error: "Il server non è in manutenzione.", status: 409 };
  const fee = Math.round(Number(current.price || 0) * 5) / 100;
  const wallet = await env.DB.prepare("SELECT deposit FROM wallets WHERE user_id=?").bind(userId).first();
  if (Number(wallet?.deposit || 0) < fee) return { error: "Saldo deposito insufficiente per ripristinare il server. Deposita almeno " + fee.toFixed(2) + " USDT.", status: 400, fee };
  const cycleAt = Number(current.updated_at || 0), now = Date.now();
  const result = await env.DB.batch([
    env.DB.prepare("INSERT INTO server_maintenance_payments (user_id,purchase_id,maintenance_started_at,amount,created_at) SELECT ?,?,?,?,? WHERE EXISTS (SELECT 1 FROM server_instances WHERE purchase_id=? AND user_id=? AND status='maintenance' AND updated_at=?) AND EXISTS (SELECT 1 FROM wallets WHERE user_id=? AND deposit>=?) AND NOT EXISTS (SELECT 1 FROM server_maintenance_payments WHERE purchase_id=? AND maintenance_started_at=?)").bind(userId,purchaseId,cycleAt,fee,now,purchaseId,userId,cycleAt,userId,fee,purchaseId,cycleAt),
    env.DB.prepare("UPDATE wallets SET deposit=deposit-? WHERE user_id=? AND deposit>=? AND EXISTS (SELECT 1 FROM server_maintenance_payments WHERE user_id=? AND purchase_id=? AND maintenance_started_at=? AND created_at=?) AND EXISTS (SELECT 1 FROM server_instances WHERE purchase_id=? AND user_id=? AND status='maintenance' AND updated_at=?)").bind(fee,userId,fee,userId,purchaseId,cycleAt,now,purchaseId,userId,cycleAt),
    env.DB.prepare("UPDATE server_instances SET status='active',updated_at=?,updated_by=?,active_since=? WHERE purchase_id=? AND user_id=? AND status='maintenance' AND updated_at=? AND EXISTS (SELECT 1 FROM server_maintenance_payments WHERE user_id=? AND purchase_id=? AND maintenance_started_at=? AND created_at=?)").bind(now,actorId,now,purchaseId,userId,cycleAt,userId,purchaseId,cycleAt,now)
  ]);
  if (!result[0]?.meta?.changes) return { error: "Il server è già stato ripristinato oppure il saldo è insufficiente.", status: 409, fee };
  if (!result[1]?.meta?.changes || !result[2]?.meta?.changes) return { error: "Impossibile completare il pagamento di manutenzione. Aggiorna la pagina e riprova.", status: 409, fee };
  const updatedWallet = await env.DB.prepare("SELECT deposit FROM wallets WHERE user_id=?").bind(userId).first();
  return { ok: true, purchase_id: purchaseId, fee, deposit_balance: Number(updatedWallet?.deposit || 0), updated_at: now };
}
async function restoreUserServer(request, env) {
  const user = await sessionUser(request, env);
  if (!user) return Response.json({ error: "Non autenticato." }, { status: 401 });
  let body;
  try { body = await request.json(); } catch { return bad("Server non valido."); }
  const purchaseId = typeof body.purchase_id === "string" ? body.purchase_id.trim() : "";
  if (!purchaseId || purchaseId.length > 128) return bad("Server non valido.");
  const result = await restoreMaintenanceServer(env, user.id, purchaseId, user.id);
  if (!result.ok) return Response.json({ error: result.error }, { status: result.status });
  return Response.json(result);
}
async function setServerStatus(request, env) {
  const adminAccess = await requireAdminPermission(request, env, "servers");
  if (!adminAccess) return Response.json({ error: "Permesso amministratore mancante." }, { status: 403 });
  let body;
  try { body = await request.json(); } catch { return bad("Stato server non valido."); }
  const purchaseId = typeof body.purchase_id === "string" ? body.purchase_id.trim() : "";
  const status = body.status;
  if (!purchaseId || !["active", "offline", "pending", "maintenance"].includes(status)) return bad("Stato server non valido.");
  const current = await env.DB.prepare("SELECT s.user_id,s.status,s.updated_at,s.active_since,s.yield_total,s.active_seconds,s.credited_yield,p.product_id,p.price FROM server_instances s JOIN compute_purchases p ON p.id=s.purchase_id WHERE s.purchase_id=?").bind(purchaseId).first();
  if (!current) return Response.json({ error: "Acquisto server non trovato." }, { status: 404 });
  const nextProduct = body.product_id === undefined ? current.product_id : String(body.product_id);
  if (!COMPUTE_CATALOG.has(nextProduct)) return bad("Modello non valido.");
  const nextPrice = body.price === undefined ? Number(current.price) : Number(body.price);
  if (!Number.isFinite(nextPrice) || nextPrice <= 0 || nextPrice > 1e12) return bad("Prezzo server non valido.");
  const values = {};
  for (const key of ["yield_total", "active_seconds", "credited_yield"]) {
    const raw = body[key] === undefined ? Number(current[key] || 0) : Number(body[key]);
    if (!Number.isFinite(raw) || raw < 0 || raw > 1e15) return bad("Dati server non validi.");
    values[key] = raw;
  }
  if (current.status === "maintenance" && status === "active") {
    const restored = await restoreMaintenanceServer(env, current.user_id, purchaseId, request.headers.get("x-websim-user-id"));
    if (!restored.ok) return Response.json({ error: restored.error }, { status: restored.status });
  }
  const now = Date.now();
  let yieldTotal = Number(current.yield_total || 0), activeSeconds = Number(current.active_seconds || 0);
  let activeSince = current.status === "active" ? Number(current.active_since || current.updated_at || now) : null;
  if (current.status === "active" && status !== "active") {
    const elapsed = Math.max(0, (now - activeSince) / 1000);
    activeSeconds += elapsed;
    yieldTotal += (COMPUTE_CATALOG.get(current.product_id)?.daily || 0) * elapsed / 86400;
    activeSince = null;
  } else if (current.status !== "active" && status === "active") activeSince = now;
  const yieldEdited = body.yield_total !== undefined && values.yield_total !== Number(current.yield_total || 0);
  const secondsEdited = body.active_seconds !== undefined && values.active_seconds !== Number(current.active_seconds || 0);
  const finalYield = yieldEdited ? values.yield_total : yieldTotal;
  const finalSeconds = secondsEdited ? values.active_seconds : activeSeconds;
  if (current.status === "active" && status === "active" && (yieldEdited || secondsEdited)) activeSince = now;
  const finalCredited = values.credited_yield;
  const result = await env.DB.batch([
    env.DB.prepare("UPDATE server_instances SET status=?,updated_at=?,updated_by=?,yield_total=?,active_seconds=?,active_since=?,credited_yield=? WHERE purchase_id=?").bind(status,now,request.headers.get("x-websim-user-id"),finalYield,finalSeconds,activeSince,finalCredited,purchaseId),
    env.DB.prepare("UPDATE compute_purchases SET product_id=?,price=? WHERE id=?").bind(nextProduct,nextPrice,purchaseId)
  ]);
  if (!result[0]?.meta?.changes) return Response.json({ error: "Server non aggiornato." }, { status: 409 });
  return Response.json({ ok: true, purchase_id: purchaseId, status, product_id: nextProduct, price: nextPrice });
}

async function getEmailVerification(request, env) {
  const user = await sessionUser(request, env);
  if (!user) return Response.json({ error: "Non autenticato." }, { status: 401 });
  const row = await env.DB.prepare("SELECT email,status,created_at,otp_expires FROM email_verifications WHERE user_id=?").bind(user.id).first();
  return Response.json({ verified: !!user.email_verified, email: row?.email || (/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(user.identifier) ? user.identifier : ""), status: row?.status || "none", created_at: row?.created_at || null, expires_at: row?.otp_expires || null });
}
async function requestEmailVerification(request, env) {
  const user=await sessionUser(request,env);
  if(!user) return Response.json({error:"Non autenticato."},{status:401});
  if(user.email_verified) return Response.json({error:"Questo indirizzo email è già verificato."},{status:400});
  let body;try{body=await request.json();}catch{return bad("Indirizzo email non valido.");}
  const email=String(body.email||"").trim().toLowerCase();
  if(!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)||email.length>254) return bad("Inserisci un indirizzo email valido.");
  const now=Date.now(),previous=await env.DB.prepare("SELECT status,otp_expires,updated_at FROM email_verifications WHERE user_id=?").bind(user.id).first();
  if(previous?.status==="code_sent"&&Number(previous.otp_expires||0)>now) return Response.json({ok:true,status:"code_sent"});
  if(previous?.status==="pending"&&now-Number(previous.updated_at||0)<60000) return Response.json({ok:true,status:"pending"});
  await env.DB.prepare("INSERT INTO email_verifications (user_id,email,status,created_at,updated_at,otp_hash,otp_expires,attempts) VALUES (?,?, 'pending',?,?,NULL,NULL,0) ON CONFLICT(user_id) DO UPDATE SET email=excluded.email,status='pending',created_at=excluded.created_at,updated_at=excluded.updated_at,otp_hash=NULL,otp_expires=NULL,attempts=0").bind(user.id,email,now,now).run();
  return Response.json({ok:true,status:"pending"});
}

async function verifyEmailCode(request, env) {
  const user = await sessionUser(request, env);
  if (!user) return Response.json({ error: "Non autenticato." }, { status: 401 });
  if (user.email_verified) return Response.json({ ok: true, verified: true });
  const body = await request.json();
  const code = String(body.code || "").trim();
  if (!/^\d{6}$/.test(code)) return Response.json({ error: "Inserisci un codice OTP di 6 cifre." }, { status: 400 });
  const row = await env.DB.prepare("SELECT email,status,otp_hash,otp_expires,attempts FROM email_verifications WHERE user_id=?").bind(user.id).first();
  if (!row || row.status !== "code_sent" || !row.otp_hash) return Response.json({ error: "Non c’è un codice attivo. Invia prima una richiesta." }, { status: 400 });
  const now = Date.now();
  if (Number(row.otp_expires || 0) <= now) {
    await env.DB.prepare("UPDATE email_verifications SET status='expired',otp_hash=NULL,updated_at=? WHERE user_id=?").bind(now,user.id).run();
    return Response.json({ error: "Codice OTP scaduto. Invia una nuova richiesta." }, { status: 400 });
  }
  if (Number(row.attempts || 0) >= 5) return Response.json({ error: "Tentativi esauriti. Invia una nuova richiesta." }, { status: 400 });
  const submittedHash = await hash(code, String(user.id) + ":" + row.email);
  if (submittedHash !== row.otp_hash) {
    const attempts = Number(row.attempts || 0) + 1;
    await env.DB.prepare("UPDATE email_verifications SET attempts=?,status=?,otp_hash=?,updated_at=? WHERE user_id=?").bind(attempts, attempts >= 5 ? "expired" : "code_sent", attempts >= 5 ? null : row.otp_hash, now, user.id).run();
    return Response.json({ error: attempts >= 5 ? "Tentativi esauriti. Invia una nuova richiesta." : "Codice OTP non corretto." }, { status: 400 });
  }
  await env.DB.batch([
    env.DB.prepare("UPDATE users SET email_verified=1 WHERE id=?").bind(user.id),
    env.DB.prepare("UPDATE email_verifications SET status='verified',otp_hash=NULL,updated_at=? WHERE user_id=?").bind(now,user.id)
  ]);
  return Response.json({ ok: true, verified: true });
}
function encodeKycKey(bytes) { let raw=""; for (const byte of bytes) raw+=String.fromCharCode(byte); return btoa(raw); }
async function getKycCryptoKey(env) {
  let row=await env.DB.prepare("SELECT secret_b64 FROM kyc_keyring WHERE key_id='documents-v1'").first();
  if(!row){const raw=crypto.getRandomValues(new Uint8Array(32));await env.DB.prepare("INSERT OR IGNORE INTO kyc_keyring (key_id,secret_b64) VALUES ('documents-v1',?)").bind(encodeKycKey(raw)).run();row=await env.DB.prepare("SELECT secret_b64 FROM kyc_keyring WHERE key_id='documents-v1'").first();}
  if(!row?.secret_b64)throw new Error("Chiave di protezione KYC non disponibile.");
  const decoded=atob(row.secret_b64),raw=Uint8Array.from(decoded,char=>char.charCodeAt(0));
  if(raw.length!==32)throw new Error("Chiave di protezione KYC non valida.");
  return crypto.subtle.importKey("raw",raw,{name:"AES-GCM"},false,["encrypt","decrypt"]);
}
function kycImageType(bytes) {
  if(bytes.length>=3&&bytes[0]===255&&bytes[1]===216&&bytes[2]===255)return "image/jpeg";
  if(bytes.length>=8&&bytes[0]===137&&bytes[1]===80&&bytes[2]===78&&bytes[3]===71&&bytes[4]===13&&bytes[5]===10&&bytes[6]===26&&bytes[7]===10)return "image/png";
  if(bytes.length>=12&&String.fromCharCode(...bytes.slice(0,4))==="RIFF"&&String.fromCharCode(...bytes.slice(8,12))==="WEBP")return "image/webp";
  return "";
}
async function storeEncryptedKycImage(env,file,key) {
  const bytes=new Uint8Array(await file.arrayBuffer()),mime=kycImageType(bytes);
  if(!mime||mime!==file.type)return {error:"Carica immagini JPG, PNG o WebP valide."};
  const iv=crypto.getRandomValues(new Uint8Array(12)),encrypted=new Uint8Array(await crypto.subtle.encrypt({name:"AES-GCM",iv},key,bytes));
  const packed=new Uint8Array(iv.length+encrypted.length);packed.set(iv);packed.set(encrypted,iv.length);
  const stored=await env.BLOB.put("kyc-"+crypto.randomUUID()+".enc",packed,{contentType:"application/octet-stream"});
  return {url:stored.url,mime};
}
async function getKycStatus(request,env) {
  const user=await sessionUser(request,env);if(!user)return Response.json({error:"Accedi per richiedere la verifica KYC."},{status:401});
  const row=await env.DB.prepare("SELECT status,created_at,reviewed_at,admin_note FROM kyc_submissions WHERE account_id=?").bind(user.id).first();
  return Response.json({request:row||null});
}
async function submitKyc(request,env) {
  const user=await sessionUser(request,env);if(!user)return Response.json({error:"Accedi per richiedere la verifica KYC."},{status:401});
  if(!env.BLOB?.put)return Response.json({error:"Archivio documenti protetto non disponibile."},{status:503});
  const contentLength=Number(request.headers.get("content-length")||0);if(contentLength>11*1024*1024)return Response.json({error:"Richiesta KYC troppo grande."},{status:413});
  const existing=await env.DB.prepare("SELECT status FROM kyc_submissions WHERE account_id=?").bind(user.id).first();
  if(existing&&existing.status!=="rejected")return Response.json({error:existing.status==="approved"?"La tua identità è già verificata.":"Hai già una richiesta KYC in attesa."},{status:409});
  let form;try{form=await request.formData();}catch{return bad("Modulo KYC non valido.");}
  const fullName=String(form.get("full_name")||"").trim(),birthDate=String(form.get("birth_date")||""),nationality=String(form.get("nationality")||"").trim(),docType=String(form.get("document_type")||""),docNumber=String(form.get("document_number")||"").trim(),expiry=String(form.get("document_expiry")||"");
  const validDate=value=>/^\d{4}-\d{2}-\d{2}$/.test(value)&&!Number.isNaN(Date.parse(value+"T00:00:00.000Z"))&&new Date(value+"T00:00:00.000Z").toISOString().slice(0,10)===value;
  const today=new Date().toISOString().slice(0,10);
  if(fullName.length<2||fullName.length>120||nationality.length<2||nationality.length>80||!validDate(birthDate)||birthDate>=today||!validDate(expiry)||expiry<today||!/^[A-Za-z0-9][A-Za-z0-9 -]{1,39}$/.test(docNumber))return bad("Controlla i dati anagrafici e del documento.");
  if(!["passport","id_card","driving_license"].includes(docType))return bad("Tipo di documento non valido.");
  const front=form.get("front_image"),back=form.get("back_image");
  if(!front||typeof front.arrayBuffer!=="function"||!back||typeof back.arrayBuffer!=="function")return bad("Allega fronte e retro del documento.");
  if(front.size<1||back.size<1||front.size>5*1024*1024||back.size>5*1024*1024)return bad("Ogni immagine deve pesare al massimo 5 MB.");
  const key=await getKycCryptoKey(env),frontImage=await storeEncryptedKycImage(env,front,key),backImage=await storeEncryptedKycImage(env,back,key);
  if(frontImage.error||backImage.error)return bad(frontImage.error||backImage.error);
  const now=Date.now(),platformUserId=request.headers.get("x-websim-user-id")||user.id;
  const saved=await env.DB.prepare("INSERT INTO kyc_submissions (account_id,user_id,username,full_name,birth_date,nationality,document_type,document_number,document_expiry,front_blob_url,front_mime,back_blob_url,back_mime,status,created_at,updated_at,reviewed_at,reviewer_id,admin_note) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,'pending',?,?,NULL,NULL,'') ON CONFLICT(account_id) DO UPDATE SET user_id=excluded.user_id,username=excluded.username,full_name=excluded.full_name,birth_date=excluded.birth_date,nationality=excluded.nationality,document_type=excluded.document_type,document_number=excluded.document_number,document_expiry=excluded.document_expiry,front_blob_url=excluded.front_blob_url,front_mime=excluded.front_mime,back_blob_url=excluded.back_blob_url,back_mime=excluded.back_mime,status='pending',created_at=excluded.created_at,updated_at=excluded.updated_at,reviewed_at=NULL,reviewer_id=NULL,admin_note='' WHERE kyc_submissions.status='rejected'").bind(user.id,platformUserId,user.username,fullName,birthDate,nationality,docType,docNumber,expiry,frontImage.url,frontImage.mime,backImage.url,backImage.mime,now,now).run();
  if(!Number(saved.meta?.changes||0))return Response.json({error:"La richiesta KYC è già stata inviata."},{status:409});
  return Response.json({ok:true,status:"pending"});
}
async function getAdminKyc(request,env) {
  const access=await requireAdminPermission(request,env,"kyc");if(!access)return Response.json({error:"Permesso KYC mancante."},{status:403});
  const rows=await env.DB.prepare("SELECT account_id,username,full_name,birth_date,nationality,document_type,document_number,document_expiry,status,created_at,reviewed_at,admin_note FROM kyc_submissions ORDER BY CASE status WHEN 'pending' THEN 0 ELSE 1 END,created_at DESC LIMIT 300").all();
  return Response.json({requests:rows.results||[]});
}
async function getAdminKycImage(request,env) {
  const access=await requireAdminPermission(request,env,"kyc");if(!access)return Response.json({error:"Permesso KYC mancante."},{status:403});
  const url=new URL(request.url),accountId=url.searchParams.get("account_id")||"",side=url.searchParams.get("side");if(!accountId||accountId.length>128||!["front","back"].includes(side))return bad("Immagine KYC non valida.");
  const row=await env.DB.prepare("SELECT front_blob_url,front_mime,back_blob_url,back_mime FROM kyc_submissions WHERE account_id=?").bind(accountId).first();if(!row)return Response.json({error:"Richiesta KYC non trovata."},{status:404});
  const blobUrl=side==="front"?row.front_blob_url:row.back_blob_url,mime=side==="front"?row.front_mime:row.back_mime,stored=env.BLOB?.get?await env.BLOB.get(blobUrl):await fetch(blobUrl);if(!stored.ok)return Response.json({error:"Immagine documento non disponibile."},{status:502});
  const packed=new Uint8Array(await stored.arrayBuffer());if(packed.length<29)return Response.json({error:"Immagine documento non valida."},{status:502});
  const key=await getKycCryptoKey(env),plain=await crypto.subtle.decrypt({name:"AES-GCM",iv:packed.slice(0,12)},key,packed.slice(12));
  return new Response(plain,{headers:{"Content-Type":mime,"Cache-Control":"private, no-store","X-Content-Type-Options":"nosniff","Content-Disposition":"inline"}});
}
async function reviewKyc(request,env) {
  const access=await requireAdminPermission(request,env,"kyc");if(!access)return Response.json({error:"Permesso KYC mancante."},{status:403});
  let body;try{body=await request.json();}catch{return bad("Decisione KYC non valida.");}
  const accountId=String(body.account_id||""),status=String(body.status||""),note=String(body.note||"").trim();if(!accountId||accountId.length>128||!["approved","rejected"].includes(status)||note.length>500)return bad("Decisione KYC non valida.");
  const now=Date.now(),result=await env.DB.prepare("UPDATE kyc_submissions SET status=?,updated_at=?,reviewed_at=?,reviewer_id=?,admin_note=? WHERE account_id=? AND status='pending'").bind(status,now,now,access.userId,note,accountId).run();
  if(!Number(result.meta?.changes||0))return Response.json({error:"Richiesta KYC non trovata o già esaminata."},{status:409});
  return Response.json({ok:true,status});
}
async function adminEmailVerifications(request, env) {
  const adminAccess = await requireAdminPermission(request, env, "verification");
  if (!adminAccess) return Response.json({ error: "Permesso amministratore mancante." }, { status: 403 });
  const [rows,walletChanges] = await Promise.all([
    env.DB.prepare("SELECT e.user_id AS id,e.email,e.status,e.created_at,e.otp_expires,u.username FROM email_verifications e JOIN users u ON u.id=e.user_id ORDER BY e.created_at DESC LIMIT 500").all(),
    env.DB.prepare("SELECT c.user_id AS id,c.email,c.network,c.address,c.status,c.created_at,u.username FROM withdrawal_wallet_changes c JOIN users u ON u.id=c.user_id WHERE c.status IN ('pending','code_sent') ORDER BY c.created_at DESC LIMIT 500").all()
  ]);
  return Response.json({ requests: rows.results || [], walletChanges: walletChanges.results || [] });
}
async function generateEmailOtp(request, env) {
  const access=await requireAdminPermission(request,env,"verification");
  if(!access) return Response.json({error:"Permesso amministratore mancante."},{status:403});
  const body=await request.json(),userId=String(body.id||"");
  const row=await env.DB.prepare("SELECT e.user_id,e.email,e.status,u.username FROM email_verifications e JOIN users u ON u.id=e.user_id WHERE e.user_id=?").bind(userId).first();
  const allowed=["pending","code_sent","recovery_pending","recovery_code_sent"];
  if(!row||!allowed.includes(row.status)) return bad("Richiesta OTP non disponibile.");
  const recovery=row.status.startsWith("recovery_"),status=recovery?"recovery_code_sent":"code_sent";
  const bytes=new Uint32Array(1);crypto.getRandomValues(bytes);
  const otp=String(bytes[0]%1000000).padStart(6,"0"),now=Date.now();
  const otpHash=await hash(otp,String(row.user_id)+":"+row.email);
  const updated=await env.DB.prepare("UPDATE email_verifications SET status=?,otp_hash=?,otp_expires=?,attempts=0,updated_at=? WHERE user_id=? AND status=?").bind(status,otpHash,now+15*60*1000,now,row.user_id,row.status).run();
  if(!updated.meta?.changes) return Response.json({error:"Richiesta OTP già aggiornata."},{status:409});
  return Response.json({ok:true,email:row.email,username:row.username,otp,expires_at:now+15*60*1000,recovery});
}

function isProjectOwner(request) {
  const userId = request.headers.get("x-websim-user-id");
  const ownerId = request.headers.get("x-websim-project-owner-id");
  return !!userId && !!ownerId && userId === ownerId;
}
async function getSupportMessages(request, env) {
  const user = await sessionUser(request, env);
  if (!user) return Response.json({ error: "Accedi per usare la chat." }, { status: 401 });
  const rows = await env.DB.prepare("SELECT id, sender, body, created_at FROM support_messages WHERE account_id=? ORDER BY id DESC LIMIT 100").bind(user.id).all();
  return Response.json({ messages: (rows.results || []).reverse() });
}
async function postSupportMessage(request, env) {
  const user = await sessionUser(request, env);
  if (!user) return Response.json({ error: "Accedi per inviare un messaggio." }, { status: 401 });
  const platformUserId = request.headers.get("x-websim-user-id") || user.id;
  let body;
  try { body = await request.json(); } catch { return bad("Messaggio non valido."); }
  const message = typeof body.message === "string" ? body.message.trim() : "";
  if (!message || message.length > 2000) return bad("Il messaggio deve contenere da 1 a 2000 caratteri.");
  const result = await env.DB.prepare("INSERT INTO support_messages (user_id, account_id, account_username, sender, body, created_at) VALUES (?, ?, ?, 'user', ?, ?)").bind(platformUserId, user.id, user.username, message, Date.now()).run();
  return Response.json({ ok: true, id: result.meta?.last_row_id });
}
async function getInboxUpdates(request, env) {
  const user = await sessionUser(request, env);
  if (!user) return Response.json({ error: "Accedi per controllare la posta in arrivo." }, { status: 401 });
  const [accounts, rentals, chats, verification, servers] = await Promise.all([
    env.DB.prepare("SELECT id, kind, amount, network, status, created_at, reviewed_at FROM account_requests WHERE user_id=? ORDER BY id DESC LIMIT 200").bind(user.id).all(),
    env.DB.prepare("SELECT id, product_id, status, created_at, reviewed_at FROM compute_rentals WHERE user_id=? ORDER BY id DESC LIMIT 200").bind(user.id).all(),
    env.DB.prepare("SELECT id, body, created_at FROM support_messages WHERE account_id=? AND sender='admin' ORDER BY id DESC LIMIT 200").bind(user.id).all(),
    env.DB.prepare("SELECT status, updated_at FROM email_verifications WHERE user_id=?").bind(user.id).first(),
    env.DB.prepare("SELECT s.purchase_id, s.status, s.updated_at, p.product_id FROM server_instances s JOIN compute_purchases p ON p.id=s.purchase_id WHERE s.user_id=? ORDER BY s.updated_at DESC LIMIT 100").bind(user.id).all()
  ]);
  const items = [];
  for (const row of accounts.results || []) items.push({ key: 'account:' + row.id, type: 'account', id: row.id, kind: row.kind, amount: row.amount, network: row.network, status: row.status, version: row.status + ':' + (row.reviewed_at || row.created_at), occurred_at: row.reviewed_at || row.created_at });
  for (const row of rentals.results || []) items.push({ key: 'rental:' + row.id, type: 'rental', id: row.id, product_id: row.product_id, status: row.status, version: row.status + ':' + (row.reviewed_at || row.created_at), occurred_at: row.reviewed_at || row.created_at });
  for (const row of chats.results || []) items.push({ key: 'chat:' + row.id, type: 'chat', version: String(row.id), id: row.id, body: row.body, occurred_at: row.created_at });
  if (verification) items.push({ key: 'verification', type: 'verification', status: verification.status, version: verification.status + ':' + verification.updated_at, occurred_at: verification.updated_at });
  for (const row of servers.results || []) items.push({ key: 'server:' + row.purchase_id, type: 'server', product_id: row.product_id, status: row.status, version: row.status + ':' + row.updated_at, occurred_at: row.updated_at });
  return Response.json({ items });
}
async function getSupportUnread(request, env) {
  const user = await sessionUser(request, env);
  if (!user) return Response.json({ error: "Accedi per controllare i nuovi messaggi." }, { status: 401 });
  const raw = Number(new URL(request.url).searchParams.get("after_id") || 0);
  const afterId = Number.isSafeInteger(raw) && raw > 0 ? raw : 0;
  const unread = await env.DB.prepare("SELECT COUNT(*) AS count FROM support_messages WHERE account_id=? AND sender='admin' AND id>?").bind(user.id, afterId).first("count");
  return Response.json({ unread: Number(unread || 0) });
}
async function closeSupportConversation(request, env) {
  const user = await sessionUser(request, env);
  if (!user) return Response.json({ error: "Accedi per chiudere la conversazione." }, { status: 401 });
  await env.DB.prepare("DELETE FROM support_messages WHERE account_id=?").bind(user.id).run();
  return Response.json({ ok: true });
}
async function getSupportConversations(request, env) {
  const adminAccess = await requireAdminPermission(request, env, "support");
  if (!adminAccess) return Response.json({ error: "Permesso amministratore mancante." }, { status: 403 });
  const rows = await env.DB.prepare("SELECT account_id, account_username, MAX(id) AS last_id, MAX(created_at) AS updated_at FROM support_messages GROUP BY account_id, account_username ORDER BY last_id DESC LIMIT 100").all();
  return Response.json({ conversations: rows.results || [] });
}
async function getAdminSupportMessages(request, env) {
  const adminAccess = await requireAdminPermission(request, env, "support");
  if (!adminAccess) return Response.json({ error: "Permesso amministratore mancante." }, { status: 403 });
  const accountId = new URL(request.url).searchParams.get("account_id") || "";
  if (!accountId || accountId.length > 128) return bad("Conversazione non valida.");
  const rows = await env.DB.prepare("SELECT id, sender, body, created_at FROM support_messages WHERE account_id=? ORDER BY id DESC LIMIT 100").bind(accountId).all();
  return Response.json({ messages: (rows.results || []).reverse() });
}
async function replySupportMessage(request, env) {
  const adminAccess = await requireAdminPermission(request, env, "support");
  if (!adminAccess) return Response.json({ error: "Permesso amministratore mancante." }, { status: 403 });
  const ownerId = request.headers.get("x-websim-user-id");
  const body = await request.json();
  const accountId = typeof body.account_id === "string" ? body.account_id : "";
  const message = typeof body.message === "string" ? body.message.trim() : "";
  if (!accountId || accountId.length > 128) return bad("Conversazione non valida.");
  if (!message || message.length > 2000) return bad("Il messaggio deve contenere da 1 a 2000 caratteri.");
  const conversation = await env.DB.prepare("SELECT account_username FROM support_messages WHERE account_id=? ORDER BY id DESC LIMIT 1").bind(accountId).first();
  if (!conversation) return Response.json({ error: "Conversazione non trovata." }, { status: 404 });
  await env.DB.prepare("INSERT INTO support_messages (user_id, account_id, account_username, sender, body, created_at) VALUES (?, ?, ?, 'admin', ?, ?)").bind(ownerId, accountId, conversation.account_username, message, Date.now()).run();
  return Response.json({ ok: true });
}
const ADMIN_PERMISSIONS = ["overview","support","users","deposits","withdrawals","investments","wallets","verification","kyc","deposit-requests","withdrawal-requests","servers","compute-requests"];
async function getAdminAccess(request, env) {
  if (isProjectOwner(request)) return { owner: true, userId: request.headers.get("x-websim-user-id"), permissions: ADMIN_PERMISSIONS.slice() };
  const user = await sessionUser(request, env);
  if (!user) return null;
  const row = await env.DB.prepare("SELECT permissions,enabled FROM admin_operators WHERE user_id=?").bind(user.id).first();
  if (!row || Number(row.enabled) !== 1) return null;
  let permissions=[];
  try { const parsed=JSON.parse(row.permissions || "[]"); if (Array.isArray(parsed)) permissions=parsed.filter(value=>ADMIN_PERMISSIONS.includes(value)); } catch {}
  return { owner:false, userId:user.id, permissions };
}
async function requireAdminPermission(request, env, permission) {
  const access=await getAdminAccess(request,env);
  return access && (access.owner || access.permissions.includes(permission)) ? access : null;
}
async function getAdminOperator(request, env) {
  if (!isProjectOwner(request)) return Response.json({ error: "Accesso riservato al proprietario del progetto." }, { status:403 });
  const userId=new URL(request.url).searchParams.get("user_id") || "";
  if (!userId || userId.length>128) return bad("Utente non valido.");
  const user=await env.DB.prepare("SELECT id,username FROM users WHERE id=?").bind(userId).first();
  if (!user) return Response.json({ error:"Utente non trovato." },{status:404});
  const row=await env.DB.prepare("SELECT permissions,enabled FROM admin_operators WHERE user_id=?").bind(userId).first();
  let permissions=[]; try { const parsed=JSON.parse(row?.permissions||"[]"); if(Array.isArray(parsed)) permissions=parsed.filter(value=>ADMIN_PERMISSIONS.includes(value)); } catch {}
  return Response.json({ user, enabled:Number(row?.enabled||0)===1, permissions });
}
async function saveAdminOperator(request, env) {
  if (!isProjectOwner(request)) return Response.json({ error: "Accesso riservato al proprietario del progetto." }, { status:403 });
  let body; try { body=await request.json(); } catch { return bad("Permessi operatore non validi."); }
  const userId=typeof body.user_id==="string"?body.user_id.trim():"";
  if(!userId || userId.length>128) return bad("Utente non valido.");
  const user=await env.DB.prepare("SELECT id FROM users WHERE id=?").bind(userId).first();
  if(!user) return Response.json({error:"Utente non trovato."},{status:404});
  const enabled=body.enabled===true, raw=Array.isArray(body.permissions)?body.permissions:[];
  const permissions=[...new Set(raw.filter(value=>ADMIN_PERMISSIONS.includes(value)))];
  if(enabled && !permissions.length) return bad("Concedi almeno un permesso.");
  if(!enabled) await env.DB.prepare("DELETE FROM admin_operators WHERE user_id=?").bind(userId).run();
  else await env.DB.prepare("INSERT INTO admin_operators (user_id,permissions,enabled,updated_at) VALUES (?,?,1,?) ON CONFLICT(user_id) DO UPDATE SET permissions=excluded.permissions,enabled=1,updated_at=excluded.updated_at").bind(userId,JSON.stringify(permissions),Date.now()).run();
  return Response.json({ok:true,enabled,permissions});
}
async function adminLogin(request, env) {
  const access=await getAdminAccess(request,env);
  if (!access) return Response.json({ error: "Accedi con un account operatore autorizzato o con l’account proprietario." }, { status:403 });
  return Response.json({ ok:true, owner:access.owner, permissions:access.permissions });
}
async function adminLogout() { return Response.json({ ok: true }); }
async function adminRequestCounts(request, env) {
  const adminAccess = await getAdminAccess(request, env);
  if (!adminAccess || (!adminAccess.owner && !adminAccess.permissions.length)) return Response.json({ error: "Permesso amministratore mancante." }, { status: 403 });
  const row = await env.DB.prepare(
    "SELECT " +
    "(SELECT COUNT(*) FROM account_requests WHERE kind='deposit' AND status='pending') AS deposit_requests, " +
    "(SELECT COUNT(*) FROM account_requests WHERE kind='withdrawal' AND status='pending') AS withdrawal_requests, " +
    "(SELECT COUNT(*) FROM compute_rentals WHERE status='pending') AS compute_requests, " +
    "(SELECT COUNT(*) FROM email_verifications WHERE status IN ('pending','code_sent')) + (SELECT COUNT(*) FROM withdrawal_wallet_changes WHERE status IN ('pending','code_sent')) AS email_verifications, " +
    "(SELECT COUNT(*) FROM kyc_submissions WHERE status='pending') AS kyc_requests, " +
    "(SELECT COUNT(*) FROM server_instances WHERE status='pending') AS servers, " +
    "(SELECT COUNT(*) FROM (SELECT account_id, MAX(id) AS latest_id FROM support_messages GROUP BY account_id) latest " +
      "JOIN support_messages m ON m.id=latest.latest_id WHERE m.sender='user') AS support_messages"
  ).first();
  const allowed=permission=>adminAccess.owner||adminAccess.permissions.includes(permission);
  return Response.json({ counts: {
    depositRequests: allowed("deposit-requests") ? Number(row?.deposit_requests || 0) : 0,
    withdrawalRequests: allowed("withdrawal-requests") ? Number(row?.withdrawal_requests || 0) : 0,
    computeRequests: allowed("compute-requests") ? Number(row?.compute_requests || 0) : 0,
    emailVerifications: allowed("verification") ? Number(row?.email_verifications || 0) : 0,
    kyc: allowed("kyc") ? Number(row?.kyc_requests || 0) : 0,
    servers: allowed("servers") ? Number(row?.servers || 0) : 0,
    support: allowed("support") ? Number(row?.support_messages || 0) : 0
  }});
}

async function adminSyncUsers(request, env) {
  if (!isProjectOwner(request)) return Response.json({ error: "Accesso riservato al proprietario del progetto." }, { status: 403 });
  const rawCursor = new URL(request.url).searchParams.get("cursor") || "";
  let cursorCreated = 0, cursorId = "";
  if (rawCursor) {
    const separator = rawCursor.indexOf("|");
    cursorCreated = Number(rawCursor.slice(0, separator));
    cursorId = separator >= 0 ? rawCursor.slice(separator + 1) : "";
    if (separator < 0 || !Number.isSafeInteger(cursorCreated) || cursorCreated < 0 || cursorId.length > 128) return bad("Cursore di sincronizzazione non valido.");
  }
  const rows = (await env.DB.prepare("SELECT id,created_at FROM users WHERE created_at>? OR (created_at=? AND id>?) ORDER BY created_at,id LIMIT 4").bind(cursorCreated, cursorCreated, cursorId).all()).results || [];
  for (const user of rows) {
    await accrue(env, user.id);
    await accrueServerEarnings(env, user.id);
    await accrueReferralNetwork(env, user.id);
  }
  const last = rows[rows.length - 1];
  const nextCursor = last ? String(last.created_at) + "|" + last.id : rawCursor;
  return Response.json({ next_cursor: nextCursor, done: rows.length < 4, processed: rows.length });
}

async function adminOverview(request, env) {
  const adminAccess = await getAdminAccess(request, env);
  if (!adminAccess || (!adminAccess.owner && !adminAccess.permissions.length)) return Response.json({ error: "Permesso amministratore mancante." }, { status: 403 });
  const [summary, users, deposits, withdrawals, investments, rentals, accountRequests, purchases] = await Promise.all([
    env.DB.prepare("SELECT COUNT(*) AS users, COALESCE(SUM(w.deposit),0) AS deposits_balance, COALESCE(SUM(w.earnings),0) AS earnings_balance, COALESCE(SUM(w.total_withdrawn),0) AS withdrawn FROM users u LEFT JOIN wallets w ON w.user_id = u.id").first(),
    env.DB.prepare("SELECT u.id, u.username, u.identifier, u.created_at, COALESCE(w.deposit,0) AS deposit, COALESCE(w.earnings,0) AS earnings, COALESCE(w.total_deposited,0) AS total_deposited, COALESCE(w.total_earned,0) AS total_earned, COALESCE(w.total_withdrawn,0) AS total_withdrawn FROM users u LEFT JOIN wallets w ON w.user_id = u.id ORDER BY u.created_at DESC").all(),
    env.DB.prepare("SELECT u.username, d.amount, d.net, d.date FROM deposits d JOIN users u ON u.id = d.user_id ORDER BY d.date DESC").all(),
    env.DB.prepare("SELECT u.username, w.amount, w.net, w.date FROM withdrawals w JOIN users u ON u.id = w.user_id ORDER BY w.date DESC").all(),
    env.DB.prepare("SELECT u.username, i.amount, i.days, i.rate, i.created, i.credited, i.active FROM investments i JOIN users u ON u.id = i.user_id ORDER BY i.created DESC").all(),
    env.DB.prepare("SELECT r.id, u.username, r.product_id, r.status, r.created_at FROM compute_rentals r JOIN users u ON u.id = r.user_id ORDER BY r.created_at DESC").all(),
    env.DB.prepare("SELECT r.id, u.username, r.kind, r.amount, r.status, r.created_at, r.reviewed_at, r.network, r.tx_hash, r.withdrawal_address FROM account_requests r JOIN users u ON u.id = r.user_id ORDER BY r.created_at DESC").all(),
    env.DB.prepare("SELECT p.id AS purchase_id, p.product_id, p.price, p.purchased_at, u.username, s.status AS server_status FROM compute_purchases p JOIN users u ON u.id = p.user_id JOIN server_instances s ON s.purchase_id=p.id ORDER BY p.purchased_at DESC").all()
  ]);
  if (!adminAccess.owner) {
    const has=permission=>adminAccess.permissions.includes(permission);
    return Response.json({ summary:has("overview")?summary:null, users:has("users")?users.results:[], deposits:has("deposits")?deposits.results:[], withdrawals:has("withdrawals")?withdrawals.results:[], investments:has("investments")?investments.results:[], rentals:has("compute-requests")?rentals.results:[], accountRequests:(accountRequests.results||[]).filter(row=>has(row.kind==="deposit"?"deposit-requests":"withdrawal-requests")), purchases:has("servers")?purchases.results:[] });
  }
  return Response.json({ summary, users: users.results, deposits: deposits.results, withdrawals: withdrawals.results, investments: investments.results, rentals: rentals.results, accountRequests: accountRequests.results, purchases: purchases.results });
}



async function adminUserDetails(request, env) {
  const adminAccess = await requireAdminPermission(request, env, "users");
  if (!adminAccess) return Response.json({ error: "Permesso amministratore mancante." }, { status: 403 });
  const id = new URL(request.url).searchParams.get("id") || "";
  if (!id || id.length > 128) return bad("Utente non valido.");
  const user = await env.DB.prepare("SELECT id,username,identifier,invite_code,referred_by,created_at,email_verified FROM users WHERE id=?").bind(id).first();
  if (!user) return Response.json({ error: "Utente non trovato." }, { status: 404 });
  const [wallet,settings,verification,investments,deposits,withdrawals,accountRequests,rentals,purchases,earningCredits,referralCommissions,referrals,supportMessages,serverMaintenancePayments,balanceAdjustments] = await Promise.all([
    env.DB.prepare("SELECT deposit,earnings,total_deposited,total_earned,total_withdrawn FROM wallets WHERE user_id=?").bind(id).first(),
    env.DB.prepare("SELECT withdraw_wallets,notifications FROM settings WHERE user_id=?").bind(id).first(),
    env.DB.prepare("SELECT email,status,created_at,updated_at,otp_expires,attempts FROM email_verifications WHERE user_id=?").bind(id).first(),
    env.DB.prepare("SELECT id,amount,days,rate,created,credited,active FROM investments WHERE user_id=? ORDER BY created DESC,id DESC").bind(id).all(),
    env.DB.prepare("SELECT id,amount,net,date FROM deposits WHERE user_id=? ORDER BY date DESC,id DESC").bind(id).all(),
    env.DB.prepare("SELECT id,amount,net,date FROM withdrawals WHERE user_id=? ORDER BY date DESC,id DESC").bind(id).all(),
    env.DB.prepare("SELECT id,kind,amount,status,created_at,reviewed_at,network,tx_hash,withdrawal_address FROM account_requests WHERE user_id=? ORDER BY created_at DESC,id DESC").bind(id).all(),
    env.DB.prepare("SELECT id,product_id,status,created_at,reviewed_at FROM compute_rentals WHERE user_id=? ORDER BY created_at DESC,id DESC").bind(id).all(),
    env.DB.prepare("SELECT p.id AS purchase_id,p.product_id,p.price,p.purchased_at,s.status,s.updated_at,s.updated_by,s.yield_total,s.active_seconds,s.active_since,s.credited_yield FROM compute_purchases p JOIN server_instances s ON s.purchase_id=p.id WHERE p.user_id=? ORDER BY p.purchased_at DESC").bind(id).all(),
    env.DB.prepare("SELECT id,investment_id,amount,created_at,earning_type,server_purchase_id FROM earning_credits WHERE user_id=? ORDER BY created_at DESC,id DESC").bind(id).all(),
    env.DB.prepare("SELECT id,referrer_id,referred_user_id,source_kind,source_id,amount,applied,created_at FROM referral_commissions WHERE referrer_id=? OR referred_user_id=? ORDER BY created_at DESC,id DESC").bind(id,id).all(),
    env.DB.prepare("SELECT id,username,identifier,created_at FROM users WHERE referred_by=? ORDER BY created_at DESC").bind(id).all(),
    env.DB.prepare("SELECT id,sender,body,created_at FROM support_messages WHERE account_id=? ORDER BY id DESC").bind(id).all(),
    env.DB.prepare("SELECT id,purchase_id,maintenance_started_at,amount,created_at FROM server_maintenance_payments WHERE user_id=? ORDER BY created_at DESC,id DESC").bind(id).all(),
    env.DB.prepare("SELECT id,field,old_value,new_value,created_at,admin_id FROM admin_balance_adjustments WHERE user_id=? ORDER BY created_at DESC,id DESC").bind(id).all()
  ]);
  return Response.json({ user, wallet: wallet || { deposit:0,earnings:0,total_deposited:0,total_earned:0,total_withdrawn:0 }, settings: settings || { withdraw_wallets:"{}",notifications:1 }, verification: verification || null, investments:investments.results||[], deposits:deposits.results||[], withdrawals:withdrawals.results||[], accountRequests:accountRequests.results||[], rentals:rentals.results||[], purchases:purchases.results||[], earningCredits:earningCredits.results||[], referralCommissions:referralCommissions.results||[], referrals:referrals.results||[], supportMessages:supportMessages.results||[], serverMaintenancePayments:serverMaintenancePayments.results||[], balanceAdjustments:balanceAdjustments.results||[] });
}
async function adminDeleteUser(request, env) {
  if (!isProjectOwner(request)) return Response.json({error:"Accesso riservato al proprietario del progetto."},{status:403});
  let body; try { body=await request.json(); } catch { return bad("Utente non valido."); }
  const id=String(body.id||"").trim();
  if (!id || id.length>128) return bad("Utente non valido.");
  const current=await sessionUser(request,env);
  if (current?.id===id || request.headers.get("x-websim-user-id")===id) return Response.json({error:"Non puoi eliminare il tuo account amministratore."},{status:409});
  const exists=await env.DB.prepare("SELECT id FROM users WHERE id=?").bind(id).first();
  if (!exists) return Response.json({error:"Utente non trovato."},{status:404});
  await env.DB.batch([
    env.DB.prepare("UPDATE users SET referred_by=NULL WHERE referred_by=?").bind(id),
    env.DB.prepare("DELETE FROM sessions WHERE user_id=?").bind(id),
    env.DB.prepare("DELETE FROM admin_operators WHERE user_id=?").bind(id),
    env.DB.prepare("DELETE FROM email_verifications WHERE user_id=?").bind(id),
    env.DB.prepare("DELETE FROM withdrawal_wallet_changes WHERE user_id=?").bind(id),
    env.DB.prepare("DELETE FROM kyc_submissions WHERE user_id=? OR account_id=?").bind(id,id),
    env.DB.prepare("DELETE FROM investments WHERE user_id=?").bind(id),
    env.DB.prepare("DELETE FROM deposits WHERE user_id=?").bind(id),
    env.DB.prepare("DELETE FROM withdrawals WHERE user_id=?").bind(id),
    env.DB.prepare("DELETE FROM settings WHERE user_id=?").bind(id),
    env.DB.prepare("DELETE FROM compute_rentals WHERE user_id=?").bind(id),
    env.DB.prepare("DELETE FROM account_requests WHERE user_id=?").bind(id),
    env.DB.prepare("DELETE FROM support_messages WHERE user_id=? OR account_id=?").bind(id,id),
    env.DB.prepare("DELETE FROM server_maintenance_payments WHERE user_id=?").bind(id),
    env.DB.prepare("DELETE FROM earning_credits WHERE user_id=?").bind(id),
    env.DB.prepare("DELETE FROM referral_commissions WHERE referrer_id=? OR referred_user_id=?").bind(id,id),
    env.DB.prepare("DELETE FROM admin_balance_adjustments WHERE user_id=?").bind(id),
    env.DB.prepare("DELETE FROM request_effects WHERE user_id=?").bind(id),
    env.DB.prepare("DELETE FROM server_instances WHERE user_id=? OR purchase_id IN (SELECT id FROM compute_purchases WHERE user_id=?)").bind(id,id),
    env.DB.prepare("DELETE FROM compute_purchases WHERE user_id=?").bind(id),
    env.DB.prepare("DELETE FROM users WHERE id=?").bind(id)
  ]);
  return Response.json({ok:true});
}

async function adminUpdateUser(request, env) {
  const adminAccess = await requireAdminPermission(request, env, "users");
  if (!adminAccess) return Response.json({ error: "Permesso amministratore mancante." }, { status: 403 });
  let body;
  try { body = await request.json(); } catch { return bad("Dati utente non validi."); }
  const id = typeof body.id === "string" ? body.id.trim() : "";
  const profile = body.profile || {}, rawWallet = body.wallet || {}, settings = body.settings || {};
  if (!id || id.length > 128) return bad("Utente non valido.");
  const current = await env.DB.prepare("SELECT id,email_verified FROM users WHERE id=?").bind(id).first();
  if (!current) return Response.json({ error: "Utente non trovato." }, { status: 404 });
  const username = String(profile.username || "").trim();
  const identifier = String(profile.identifier || "").trim();
  if (!username || username.length > 80 || !identifier || identifier.length > 254 || !/^[+\-\w.@ ]+$/.test(identifier)) return bad("Nome utente o email/telefono non valido.");
  const duplicate = await env.DB.prepare("SELECT id FROM users WHERE id<>? AND (lower(username)=lower(?) OR lower(identifier)=lower(?)) LIMIT 1").bind(id,username,identifier).first();
  if (duplicate) return Response.json({ error: "Email/telefono o nome utente già registrato." }, { status: 409 });
  if (typeof profile.email_verified !== "boolean") return bad("Stato verifica email non valido.");
  const fields = ["deposit","earnings","total_deposited","total_earned","total_withdrawn"];
  const wallet = {};
  for (const field of fields) {
    const value = Number(rawWallet[field]);
    if (!Number.isFinite(value) || value < 0 || value > 1e15) return bad("Saldo utente non valido.");
    wallet[field] = value;
  }
  const savedSettings = await env.DB.prepare("SELECT withdraw_wallets FROM settings WHERE user_id=?").bind(id).first();
  const lockedWithdrawalWallets = savedSettings?.withdraw_wallets || "{}";
  if (typeof settings.notifications !== "boolean") return bad("Preferenze notifiche non valide.");
  const oldWallet = await env.DB.prepare("SELECT deposit,earnings,total_deposited,total_earned,total_withdrawn FROM wallets WHERE user_id=?").bind(id).first() || { deposit:0,earnings:0,total_deposited:0,total_earned:0,total_withdrawn:0 };
  const now = Date.now(), adminId = request.headers.get("x-websim-user-id") || null;
  const statements = [
    env.DB.prepare("UPDATE users SET username=?,identifier=?,email_verified=? WHERE id=?").bind(username,identifier,profile.email_verified?1:0,id),
    env.DB.prepare("UPDATE support_messages SET account_username=? WHERE account_id=?").bind(username,id),
    env.DB.prepare("INSERT INTO wallets (user_id,deposit,earnings,total_deposited,total_earned,total_withdrawn) VALUES (?,?,?,?,?,?) ON CONFLICT(user_id) DO UPDATE SET deposit=excluded.deposit,earnings=excluded.earnings,total_deposited=excluded.total_deposited,total_earned=excluded.total_earned,total_withdrawn=excluded.total_withdrawn").bind(id,wallet.deposit,wallet.earnings,wallet.total_deposited,wallet.total_earned,wallet.total_withdrawn),
    env.DB.prepare("INSERT INTO settings (user_id,withdraw_wallets,notifications) VALUES (?,?,?) ON CONFLICT(user_id) DO UPDATE SET withdraw_wallets=excluded.withdraw_wallets,notifications=excluded.notifications").bind(id,lockedWithdrawalWallets,settings.notifications?1:0)
  ];
  if (Number(current.email_verified || 0) !== (profile.email_verified ? 1 : 0)) statements.push(env.DB.prepare("UPDATE email_verifications SET status=?,otp_hash=NULL,otp_expires=NULL,attempts=0,updated_at=? WHERE user_id=?").bind(profile.email_verified ? "verified" : "pending",now,id));
  for (const field of fields) {
    const oldValue = Number(oldWallet[field] || 0);
    if (oldValue !== wallet[field]) statements.push(env.DB.prepare("INSERT INTO admin_balance_adjustments (user_id,field,old_value,new_value,created_at,admin_id) VALUES (?,?,?,?,?,?)").bind(id,field,oldValue,wallet[field],now,adminId));
  }
  await env.DB.batch(statements);
  return Response.json({ ok:true });
}

export default websimWorker;

// Render runs this module as a Node process; Websim continues using the worker export above.
async function startRenderServer() {
  const [{ createServer }, { readFile, stat }, pathModule] = await Promise.all([
    import("node:http"),
    import("node:fs/promises"),
    import("node:path")
  ]);
  const root = process.cwd();
  const mimeTypes = {
    ".html": "text/html; charset=utf-8", ".css": "text/css; charset=utf-8",
    ".js": "text/javascript; charset=utf-8", ".json": "application/json; charset=utf-8",
    ".webmanifest": "application/manifest+json; charset=utf-8", ".svg": "image/svg+xml",
    ".png": "image/png", ".jpg": "image/jpeg", ".jpeg": "image/jpeg",
    ".webp": "image/webp", ".avif": "image/avif", ".ico": "image/x-icon",
    ".woff2": "font/woff2"
  };
  const sendFile = async (file, response, method) => {
    const content = await readFile(file);
    response.writeHead(200, {
      "Content-Type": mimeTypes[pathModule.extname(file).toLowerCase()] || "application/octet-stream",
      "Content-Length": content.length,
      "Cache-Control": "no-cache",
      "X-Content-Type-Options": "nosniff"
    });
    response.end(method === "HEAD" ? undefined : content);
  };
  const server = createServer(async (incoming, outgoing) => {
    try {
      const url = new URL(incoming.url || "/", "http://localhost");
      if (url.pathname.startsWith("/api/")) {
        const headers = new Headers();
        for (const [name, raw] of Object.entries(incoming.headers)) {
          if (raw == null || ["host", "connection", "transfer-encoding", "content-length", "x-websim-user-id", "x-websim-project-owner-id"].includes(name.toLowerCase())) continue;
          headers.set(name, Array.isArray(raw) ? raw.join(", ") : String(raw));
        }
        const method = incoming.method || "GET";
        let body;
        if (method !== "GET" && method !== "HEAD") {
          const chunks = []; let size = 0;
          for await (const chunk of incoming) {
            size += chunk.length;
            if (size > 12 * 1024 * 1024) {
              outgoing.writeHead(413, { "Content-Type": "application/json; charset=utf-8" });
              outgoing.end(JSON.stringify({ error: "Richiesta troppo grande." }));
              return;
            }
            chunks.push(chunk);
          }
          body = Buffer.concat(chunks);
        }
        const requestInit = { method, headers };
        if (body) { requestInit.body = body; requestInit.duplex = "half"; }
        const request = new Request(new URL(incoming.url || "/", "http://localhost"), requestInit);
        const runtimeEnv = {
          TURSO_DATABASE_URL: process.env.TURSO_DATABASE_URL,
          TURSO_AUTH_TOKEN: process.env.TURSO_AUTH_TOKEN
        };
        const result = await websimWorker.fetch(request, runtimeEnv);
        const responseHeaders = {};
        for (const [name, value] of result.headers) responseHeaders[name] = value;
        outgoing.writeHead(result.status, responseHeaders);
        outgoing.end(Buffer.from(await result.arrayBuffer()));
        return;
      }

      let pathname;
      try { pathname = decodeURIComponent(url.pathname); }
      catch { outgoing.writeHead(400); outgoing.end("Bad request"); return; }
      const requested = pathname === "/" ? "/index.html" : pathname;
      const file = pathModule.resolve(root, "." + requested);
      if (file !== root && !file.startsWith(root + pathModule.sep)) {
        outgoing.writeHead(403); outgoing.end("Forbidden"); return;
      }
      try {
        if ((await stat(file)).isFile()) { await sendFile(file, outgoing, incoming.method || "GET"); return; }
      } catch {}
      if (!pathModule.extname(pathname)) { await sendFile(pathModule.join(root, "index.html"), outgoing, incoming.method || "GET"); return; }
      outgoing.writeHead(404); outgoing.end("Not found");
    } catch (error) {
      console.error("Render request failed:", error);
      if (!outgoing.headersSent) outgoing.writeHead(500, { "Content-Type": "text/plain; charset=utf-8" });
      outgoing.end("Internal server error");
    }
  });
  const port = Number.parseInt(process.env.PORT || "10000", 10);
  server.listen(port, "0.0.0.0", () => console.log("Render server listening on port " + port));
}
if (typeof process !== "undefined" && process.versions?.node) startRenderServer().catch(error => { console.error("Unable to start Render server:", error); process.exitCode = 1; });
