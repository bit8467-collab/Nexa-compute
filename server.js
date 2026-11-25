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
      CREATE TABLE IF NOT EXISTS sessions (
        token TEXT PRIMARY KEY,
        user_id TEXT NOT NULL,
        created_at INTEGER NOT NULL
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

    const url = new URL(request.url);
    const path = url.pathname;
    const method = request.method;

    try {
      if (method === "POST" && path === "/api/register") return await register(request, env);
      if (method === "POST" && path === "/api/login") return await login(request, env);
      if (method === "POST" && path === "/api/logout") return await logout(request, env);
      if (method === "GET" && path === "/api/me") return await me(request, env);
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
      if (method === "POST" && path === "/api/settings") return await updateSettings(request, env);
      if (method === "POST" && path === "/api/admin/login") return await adminLogin(request, env);
      if (method === "POST" && path === "/api/admin/logout") return await adminLogout(request, env);
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
  return Response.json({ ok: true, token });
}

async function logout(request, env) {
  const h = (request.headers.get("x-session-token") || "").trim();
  if (h) await env.DB.prepare("DELETE FROM sessions WHERE token = ?").bind(h).run();
  return Response.json({ ok: true });
}

async function me(request, env) {
  const user = await sessionUser(request, env);
  if (!user) return Response.json({ error: "Non autenticato." }, { status: 401 });
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
  const clean = {};
  for (const [net, addr] of Object.entries(wallets)) {
    const a = (typeof addr === "string" ? addr : "").trim();
    if (a && ["bep20", "trc20"].includes(net) && !validWalletAddress(net, a)) return bad(net === "bep20" ? "Indirizzo BEP20 non valido." : "Indirizzo TRC20 non valido.");
    if (a) clean[net] = a;
  }
  const s = await upsertSettings(env, user.id, { wallets: clean });
  return Response.json({ ok: true, acc: s });
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
  if (!isProjectOwner(request)) return Response.json({ error: "Accesso riservato al proprietario del progetto." }, { status: 403 });
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
  if (!isProjectOwner(request)) return Response.json({ error: "Accesso riservato al proprietario del progetto." }, { status: 403 });
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
  if (!isProjectOwner(request)) return Response.json({ error: "Accesso riservato al proprietario del progetto." }, { status: 403 });
  const body = await request.json();
  const id = Number(body.id);
  const status = body.status;
  if (!Number.isInteger(id) || id < 1 || !['approved', 'rejected'].includes(status)) return bad("Decisione non valida.");
  const result = await env.DB.prepare("UPDATE compute_rentals SET status = ?, reviewed_at = ? WHERE id = ? AND status = 'pending'").bind(status, Date.now(), id).run();
  if (!result.meta?.changes) return Response.json({ error: "Richiesta non trovata o già gestita." }, { status: 409 });
  return Response.json({ ok: true, id, status });
}
async function reviewAccountRequest(request, env) {
  if (!isProjectOwner(request)) return Response.json({ error: "Accesso riservato al proprietario del progetto." }, { status: 403 });
  const body = await request.json();
  const id = Number(body.id);
  const decision = body.status;
  if (!Number.isInteger(id) || id < 1 || !['approved', 'rejected'].includes(decision)) return bad("Decisione non valida.");
  const req = await env.DB.prepare("SELECT id, user_id, kind, amount, status, review_key FROM account_requests WHERE id = ?").bind(id).first();
  if (!req || req.status !== 'pending') return Response.json({ error: "Richiesta non trovata o già gestita." }, { status: 409 });
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
    return { ...row, total_yield: totalYield, active_seconds: activeSeconds };
  });
  return Response.json({ servers });
}
async function setServerStatus(request, env) {
  if (!isProjectOwner(request)) return Response.json({ error: "Accesso riservato al proprietario del progetto." }, { status: 403 });
  const body = await request.json();
  const purchaseId = typeof body.purchase_id === "string" ? body.purchase_id : "";
  const status = body.status;
  if (!purchaseId || !["active", "offline", "pending"].includes(status)) return bad("Stato server non valido.");
  const current = await env.DB.prepare("SELECT s.status, s.updated_at, s.active_since, s.yield_total, s.active_seconds, p.product_id FROM server_instances s JOIN compute_purchases p ON p.id=s.purchase_id WHERE s.purchase_id=?").bind(purchaseId).first();
  if (!current) return Response.json({ error: "Acquisto server non trovato." }, { status: 404 });
  const now = Date.now();
  let yieldTotal = Number(current.yield_total || 0);
  let activeSeconds = Number(current.active_seconds || 0);
  let activeSince = current.status === "active" ? Number(current.active_since || current.updated_at || now) : null;
  if (current.status === "active" && status !== "active") {
    const elapsed = Math.max(0, (now - activeSince) / 1000);
    activeSeconds += elapsed;
    yieldTotal += (COMPUTE_CATALOG.get(current.product_id)?.daily || 0) * elapsed / 86400;
    activeSince = null;
  } else if (current.status !== "active" && status === "active") {
    activeSince = now;
  }
  await env.DB.prepare("UPDATE server_instances SET status=?, updated_at=?, updated_by=?, yield_total=?, active_seconds=?, active_since=? WHERE purchase_id=?").bind(status, now, request.headers.get("x-websim-user-id"), yieldTotal, activeSeconds, activeSince, purchaseId).run();
  return Response.json({ ok: true, purchase_id: purchaseId, status });
}

async function getEmailVerification(request, env) {
  const user = await sessionUser(request, env);
  if (!user) return Response.json({ error: "Non autenticato." }, { status: 401 });
  const row = await env.DB.prepare("SELECT email,status,created_at,otp_expires FROM email_verifications WHERE user_id=?").bind(user.id).first();
  return Response.json({ verified: !!user.email_verified, email: row?.email || (/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(user.identifier) ? user.identifier : ""), status: row?.status || "none", created_at: row?.created_at || null, expires_at: row?.otp_expires || null });
}
async function requestEmailVerification(request, env) {
  const user = await sessionUser(request, env);
  if (!user) return Response.json({ error: "Non autenticato." }, { status: 401 });
  if (user.email_verified) return Response.json({ error: "Questo indirizzo email è già verificato." }, { status: 400 });
  const body = await request.json();
  const email = String(body.email || "").trim().toLowerCase();
  if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email) || email.length > 254) return Response.json({ error: "Inserisci un indirizzo email valido." }, { status: 400 });
  const now = Date.now();
  const previous = await env.DB.prepare("SELECT status,otp_expires FROM email_verifications WHERE user_id=?").bind(user.id).first();
  if (previous?.status === "pending") return Response.json({ ok: true, status: "pending" });
  if (previous?.status === "code_sent" && Number(previous.otp_expires || 0) > now) return Response.json({ ok: true, status: "code_sent" });
  await env.DB.prepare("INSERT INTO email_verifications (user_id,email,status,created_at,updated_at,otp_hash,otp_expires,attempts) VALUES (?,?, 'pending',?,?,NULL,NULL,0) ON CONFLICT(user_id) DO UPDATE SET email=excluded.email,status='pending',created_at=excluded.created_at,updated_at=excluded.updated_at,otp_hash=NULL,otp_expires=NULL,attempts=0").bind(user.id, email, now, now).run();
  return Response.json({ ok: true, status: "pending" });
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
async function adminEmailVerifications(request, env) {
  if (!isProjectOwner(request)) return Response.json({ error: "Accesso riservato al proprietario del progetto." }, { status: 403 });
  const rows = await env.DB.prepare("SELECT e.user_id AS id,e.email,e.status,e.created_at,e.otp_expires,u.username FROM email_verifications e JOIN users u ON u.id=e.user_id ORDER BY e.created_at DESC LIMIT 500").all();
  return Response.json({ requests: rows.results || [] });
}
async function generateEmailOtp(request, env) {
  if (!isProjectOwner(request)) return Response.json({ error: "Accesso riservato al proprietario del progetto." }, { status: 403 });
  const body = await request.json();
  const userId = String(body.id || "");
  const row = await env.DB.prepare("SELECT e.user_id,e.email,e.status,u.username FROM email_verifications e JOIN users u ON u.id=e.user_id WHERE e.user_id=?").bind(userId).first();
  if (!row || !["pending","code_sent"].includes(row.status)) return Response.json({ error: "Richiesta di verifica non disponibile." }, { status: 400 });
  const bytes = new Uint32Array(1);
  crypto.getRandomValues(bytes);
  const otp = String(bytes[0] % 1000000).padStart(6, "0");
  const now = Date.now();
  const otpHash = await hash(otp, String(row.user_id) + ":" + row.email);
  await env.DB.prepare("UPDATE email_verifications SET status='code_sent',otp_hash=?,otp_expires=?,attempts=0,updated_at=? WHERE user_id=?").bind(otpHash, now + 15 * 60 * 1000, now, row.user_id).run();
  return Response.json({ ok: true, email: row.email, username: row.username, otp, expires_at: now + 15 * 60 * 1000, sender: "info.cup9@yahoo.com" });
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
  if (!isProjectOwner(request)) return Response.json({ error: "Accesso riservato al proprietario del progetto." }, { status: 403 });
  const rows = await env.DB.prepare("SELECT account_id, account_username, MAX(id) AS last_id, MAX(created_at) AS updated_at FROM support_messages GROUP BY account_id, account_username ORDER BY last_id DESC LIMIT 100").all();
  return Response.json({ conversations: rows.results || [] });
}
async function getAdminSupportMessages(request, env) {
  if (!isProjectOwner(request)) return Response.json({ error: "Accesso riservato al proprietario del progetto." }, { status: 403 });
  const accountId = new URL(request.url).searchParams.get("account_id") || "";
  if (!accountId || accountId.length > 128) return bad("Conversazione non valida.");
  const rows = await env.DB.prepare("SELECT id, sender, body, created_at FROM support_messages WHERE account_id=? ORDER BY id DESC LIMIT 100").bind(accountId).all();
  return Response.json({ messages: (rows.results || []).reverse() });
}
async function replySupportMessage(request, env) {
  if (!isProjectOwner(request)) return Response.json({ error: "Accesso riservato al proprietario del progetto." }, { status: 403 });
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
async function adminLogin(request) {
  if (!isProjectOwner(request)) return Response.json({ error: "Accedi come proprietario del progetto Websim per aprire il pannello." }, { status: 403 });
  return Response.json({ ok: true });
}
async function adminLogout() { return Response.json({ ok: true }); }
async function adminOverview(request, env) {
  if (!isProjectOwner(request)) return Response.json({ error: "Accesso riservato al proprietario del progetto." }, { status: 403 });
  const [summary, users, deposits, withdrawals, investments, rentals, accountRequests, purchases] = await Promise.all([
    env.DB.prepare("SELECT COUNT(*) AS users, COALESCE(SUM(w.deposit),0) AS deposits_balance, COALESCE(SUM(w.earnings),0) AS earnings_balance, COALESCE(SUM(w.total_withdrawn),0) AS withdrawn FROM users u LEFT JOIN wallets w ON w.user_id = u.id").first(),
    env.DB.prepare("SELECT u.id, u.username, u.identifier, u.created_at, COALESCE(w.deposit,0) AS deposit, COALESCE(w.earnings,0) AS earnings, COALESCE(w.total_deposited,0) AS total_deposited, COALESCE(w.total_earned,0) AS total_earned, COALESCE(w.total_withdrawn,0) AS total_withdrawn FROM users u LEFT JOIN wallets w ON w.user_id = u.id ORDER BY u.created_at DESC").all(),
    env.DB.prepare("SELECT u.username, d.amount, d.net, d.date FROM deposits d JOIN users u ON u.id = d.user_id ORDER BY d.date DESC LIMIT 1000").all(),
    env.DB.prepare("SELECT u.username, w.amount, w.net, w.date FROM withdrawals w JOIN users u ON u.id = w.user_id ORDER BY w.date DESC LIMIT 1000").all(),
    env.DB.prepare("SELECT u.username, i.amount, i.days, i.rate, i.created, i.credited, i.active FROM investments i JOIN users u ON u.id = i.user_id ORDER BY i.created DESC LIMIT 1000").all(),
    env.DB.prepare("SELECT r.id, u.username, r.product_id, r.status, r.created_at FROM compute_rentals r JOIN users u ON u.id = r.user_id ORDER BY r.created_at DESC LIMIT 1000").all(),
    env.DB.prepare("SELECT r.id, u.username, r.kind, r.amount, r.status, r.created_at, r.reviewed_at, r.network, r.tx_hash, r.withdrawal_address FROM account_requests r JOIN users u ON u.id = r.user_id ORDER BY r.created_at DESC LIMIT 1000").all(),
    env.DB.prepare("SELECT p.id AS purchase_id, p.product_id, p.price, p.purchased_at, u.username, s.status AS server_status FROM compute_purchases p JOIN users u ON u.id = p.user_id JOIN server_instances s ON s.purchase_id=p.id ORDER BY p.purchased_at DESC LIMIT 1000").all()
  ]);
  return Response.json({ summary, users: users.results, deposits: deposits.results, withdrawals: withdrawals.results, investments: investments.results, rentals: rentals.results, accountRequests: accountRequests.results, purchases: purchases.results, records_limited: 1000 });
}



// Render / Express adapter. Worker-style route handlers above are retained,
// backed by Turso in production and a local libSQL file for development.
import express from 'express';
import { createClient } from '@libsql/client';
import { createHmac, timingSafeEqual, webcrypto } from 'node:crypto';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
if (!globalThis.crypto) globalThis.crypto = webcrypto;
const __dirname = path.dirname(fileURLToPath(import.meta.url));
const databaseUrl = process.env.TURSO_DATABASE_URL || 'file:./nexa-render.db';
const databaseToken = process.env.TURSO_AUTH_TOKEN || process.env.TURSO_DATABASE_AUTH_TOKEN;
const libsql = createClient({ url: databaseUrl, ...(databaseToken ? { authToken: databaseToken } : {}), intMode: 'number' });
function resultRows(result) {
  const columns = result.columns || [];
  return (result.rows || []).map(row => Object.fromEntries(columns.map((name, index) => [name, row[name] ?? row[index] ?? null])));
}
function resultMeta(result) {
  return { changes: Number(result.rowsAffected || 0), last_row_id: result.lastInsertRowid == null ? null : Number(result.lastInsertRowid) };
}
function makeD1Adapter(client) {
  return {
    async exec(sql) { await client.executeMultiple(sql); },
    prepare(sql) {
      const state = { sql, args: [] };
      const statement = {
        bind(...args) { state.args = args.map(value => value === undefined ? null : value); return statement; },
        async run() { const result = await client.execute({ sql: state.sql, args: state.args }); return { meta: resultMeta(result), results: resultRows(result) }; },
        async all() { const result = await client.execute({ sql: state.sql, args: state.args }); return { meta: resultMeta(result), results: resultRows(result) }; },
        async first(column) { const result = await client.execute({ sql: state.sql, args: state.args }); const row = resultRows(result)[0] || null; return column && row ? row[column] : row; },
        _statement: state
      };
      return statement;
    },
    async batch(statements) {
      const results = await client.batch(statements.map(statement => ({ sql: statement._statement.sql, args: statement._statement.args })), 'write');
      return results.map(result => ({ meta: resultMeta(result), results: resultRows(result) }));
    }
  };
}
const env = { DB: makeD1Adapter(libsql) };
const app = express();
app.disable('x-powered-by');
app.set('trust proxy', 1);
app.use(express.json({ limit: '1mb' }));
const ADMIN_COOKIE = 'nexa_render_admin';
const SESSION_SECRET = process.env.SESSION_SECRET || process.env.ADMIN_PASSWORD || 'configure-SESSION_SECRET-before-production';
function signAdmin(payload) { return createHmac('sha256', SESSION_SECRET).update(payload).digest('base64url'); }
function constantTimeEqual(a, b) {
  const left = Buffer.from(String(a || ''));
  const right = Buffer.from(String(b || ''));
  return left.length === right.length && timingSafeEqual(left, right);
}
function cookieValue(header, name) {
  for (const part of String(header || '').split(';')) {
    const index = part.indexOf('=');
    if (index > 0 && part.slice(0, index).trim() === name) return part.slice(index + 1).trim();
  }
  return '';
}
function isRenderAdmin(req) {
  const value = cookieValue(req.headers.cookie, ADMIN_COOKIE);
  const split = value.lastIndexOf('.');
  if (split < 1) return false;
  const payload = value.slice(0, split), signature = value.slice(split + 1);
  if (!constantTimeEqual(signature, signAdmin(payload))) return false;
  try {
    const session = JSON.parse(Buffer.from(payload, 'base64url').toString('utf8'));
    return session.user === process.env.ADMIN_USERNAME && Number(session.exp) > Date.now();
  } catch { return false; }
}
function setAdminCookie(res, value, maxAge) {
  const secure = process.env.COOKIE_SECURE === 'false' ? '' : '; Secure';
  res.setHeader('Set-Cookie', ADMIN_COOKIE + '=' + value + '; Path=/; HttpOnly; SameSite=Strict; Max-Age=' + maxAge + secure);
}
app.post('/api/admin/login', (req, res) => {
  const expectedUser = process.env.ADMIN_USERNAME || '';
  const expectedPassword = process.env.ADMIN_PASSWORD || '';
  if (!expectedUser || !expectedPassword) return res.status(503).json({ error: 'Configura ADMIN_USERNAME e ADMIN_PASSWORD nelle variabili ambiente Render.' });
  if (!constantTimeEqual(req.body?.username, expectedUser) || !constantTimeEqual(req.body?.password, expectedPassword)) return res.status(401).json({ error: 'Credenziali amministratore non valide.' });
  const payload = Buffer.from(JSON.stringify({ user: expectedUser, exp: Date.now() + 12 * 60 * 60 * 1000 })).toString('base64url');
  setAdminCookie(res, payload + '.' + signAdmin(payload), 12 * 60 * 60);
  return res.json({ ok: true });
});
app.post('/api/admin/logout', (req, res) => { setAdminCookie(res, '', 0); return res.json({ ok: true }); });
app.all('/api/*', async (req, res) => {
  const admin = isRenderAdmin(req);
  if (req.path.startsWith('/api/admin/') && !['/api/admin/login', '/api/admin/logout'].includes(req.path) && !admin) return res.status(401).json({ error: 'Accesso riservato al proprietario del progetto.' });
  try {
    const headers = new Headers();
    for (const [name, value] of Object.entries(req.headers)) {
      if (name === 'host' || name === 'content-length' || value == null) continue;
      headers.set(name, Array.isArray(value) ? value.join(', ') : String(value));
    }
    if (admin) {
      headers.set('x-websim-user-id', 'render-admin');
      headers.set('x-websim-project-owner-id', 'render-admin');
      headers.set('x-websim-username', process.env.ADMIN_USERNAME || 'admin');
    }
    const method = req.method.toUpperCase();
    const hasBody = !['GET', 'HEAD'].includes(method) && req.body !== undefined;
    if (hasBody && !headers.has('content-type')) headers.set('content-type', 'application/json');
    const origin = req.protocol + '://' + req.get('host');
    const request = new Request(origin + req.originalUrl, { method, headers, ...(hasBody ? { body: JSON.stringify(req.body) } : {}) });
    const response = await websimWorker.fetch(request, env);
    res.status(response.status);
    response.headers.forEach((value, name) => res.setHeader(name, value));
    return res.send(Buffer.from(await response.arrayBuffer()));
  } catch (error) {
    console.error('Render API error:', req.originalUrl, error);
    return res.status(500).json({ error: 'Errore del server.' });
  }
});
app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'index.html')));
app.get('/index.html', (req, res) => res.sendFile(path.join(__dirname, 'index.html')));
app.get('/script.js', (req, res) => res.sendFile(path.join(__dirname, 'script.js')));
app.get('/style.css', (req, res) => res.sendFile(path.join(__dirname, 'style.css')));
app.get('/favicon.ico', (req, res) => res.status(204).end());
app.use((req, res) => res.status(404).send('Not found'));
const port = Number(process.env.PORT) || 10000;
app.listen(port, '0.0.0.0', () => {
  console.log('Nexa Compute listening on port ' + port);
  if (databaseUrl.startsWith('file:')) console.warn('Using local SQLite storage. Configure TURSO_DATABASE_URL and TURSO_AUTH_TOKEN for durable production storage.');
  if (!process.env.ADMIN_USERNAME || !process.env.ADMIN_PASSWORD) console.warn('Set ADMIN_USERNAME and ADMIN_PASSWORD in Render environment variables to enable admin login.');
});
