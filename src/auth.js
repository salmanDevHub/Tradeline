const express = require('express');
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');
const crypto = require('crypto');
const pool = require('./db');

const SECRET = process.env.JWT_SECRET || 'dev-only-change-me';
const ACCESS_TTL = 15 * 60;            // 15 minutes
const REFRESH_TTL = 7 * 24 * 3600;     // 7 days
const secure = process.env.COOKIE_SECURE !== 'false';
const cookie = (maxAge, path = '/') => ({ httpOnly: true, secure, sameSite: 'lax', maxAge: maxAge * 1000, path });
const sha = (s) => crypto.createHash('sha256').update(s).digest('hex');
const h = (fn) => (req, res, next) => fn(req, res, next).catch(next);

// Access token (JWT) + refresh token (random, DB mein sirf hash)
async function issue(res, user, family) {
  const access = jwt.sign({ sub: user.id, role: user.role, tenant: user.tenant_id, email: user.email }, SECRET, { expiresIn: ACCESS_TTL });
  const refresh = crypto.randomBytes(48).toString('hex');
  await pool.query(`INSERT INTO refresh_tokens(token_hash,user_id,family_id,expires_at) VALUES($1,$2,$3,now()+interval '7 days')`,
    [sha(refresh), user.id, family || crypto.randomUUID()]);
  res.cookie('access_token', access, cookie(ACCESS_TTL));
  res.cookie('refresh_token', refresh, cookie(REFRESH_TTL, '/auth'));
}
const clear = (res) => { res.clearCookie('access_token', { path: '/' }); res.clearCookie('refresh_token', { path: '/auth' }); };

function authenticate(req, res, next) {
  try { req.user = jwt.verify(req.cookies.access_token || '', SECRET); next(); }
  catch { res.status(401).json({ error: 'not authenticated' }); }
}
const authorize = (...roles) => (req, res, next) =>
  roles.includes(req.user.role) ? next() : res.status(403).json({ error: 'forbidden for role ' + req.user.role });

// Simple login rate limit (in-memory)
const tries = new Map();
function limited(key) {
  const now = Date.now(), a = (tries.get(key) || []).filter((t) => now - t < 15 * 60e3);
  a.push(now); tries.set(key, a); return a.length > 10;
}

const r = express.Router();
const DUMMY = bcrypt.hashSync('dummy-password', 12);

// Company registration: naya tenant + uska Manager + starter products
r.post('/register', h(async (req, res) => {
  const { company = '', name = '', email = '', password = '' } = req.body || {};
  const em = email.toLowerCase();
  if (company.trim().length < 2 || !/^\S+@\S+\.\S+$/.test(em) || password.length < 8)
    return res.status(400).json({ error: 'company name, valid email and 8+ character password required' });
  if ((await pool.query('SELECT 1 FROM users WHERE email=$1', [em])).rowCount) return res.status(409).json({ error: 'email already registered' });
  let slug = company.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '').slice(0, 30) || 'company';
  if ((await pool.query('SELECT 1 FROM tenants WHERE id=$1', [slug])).rowCount) slug += '-' + crypto.randomBytes(2).toString('hex');
  const c = await pool.connect();
  try {
    await c.query('BEGIN');
    await c.query('INSERT INTO tenants(id,name) VALUES($1,$2)', [slug, company.trim()]);
    await c.query(`INSERT INTO products(tenant_id,name,price,stock) SELECT $1::text, n, p, 300 FROM (VALUES ('Starter Kit',25),('Standard Pack',40),('Premium Pack',75)) v(n,p)`, [slug]);
    const u = (await c.query(`INSERT INTO users(email,name,password_hash,role,tenant_id) VALUES($1,$2,$3,'manager',$4) RETURNING *`,
      [em, name || em.split('@')[0], await bcrypt.hash(password, 12), slug])).rows[0];
    await c.query('COMMIT');
    await issue(res, u);
    res.status(201).json({ id: u.id, email: u.email, role: u.role, tenant: slug });
  } catch (e) { await c.query('ROLLBACK'); throw e; } finally { c.release(); }
}));

r.post('/login', h(async (req, res) => {
  const { email = '', password = '' } = req.body || {};
  if (limited(req.ip + email.toLowerCase())) return res.status(429).json({ error: 'too many attempts, try later' });
  const u = (await pool.query('SELECT * FROM users WHERE email=$1', [email.toLowerCase()])).rows[0];
  const ok = await bcrypt.compare(password, (u && u.password_hash) || DUMMY); // timing barabar rakhne ke liye
  if (!u || !u.password_hash || !ok) return res.status(401).json({ error: 'invalid email or password' });
  await issue(res, u);
  res.json({ id: u.id, email: u.email, role: u.role, tenant: u.tenant_id });
}));

// Refresh token rotation: har refresh par naya token, purana "used". Used token dobara aaye = chori, poori family revoke.
r.post('/refresh', h(async (req, res) => {
  const t = req.cookies.refresh_token;
  const row = t && (await pool.query('SELECT * FROM refresh_tokens WHERE token_hash=$1', [sha(t)])).rows[0];
  if (!row) { clear(res); return res.status(401).json({ error: 'invalid refresh token' }); }
  if (row.used) { await pool.query('DELETE FROM refresh_tokens WHERE family_id=$1', [row.family_id]); clear(res); return res.status(401).json({ error: 'token reuse detected' }); }
  if (new Date(row.expires_at) < new Date()) { clear(res); return res.status(401).json({ error: 'expired' }); }
  const claimed = await pool.query('UPDATE refresh_tokens SET used=true WHERE id=$1 AND used=false', [row.id]);
  if (!claimed.rowCount) { clear(res); return res.status(401).json({ error: 'invalid refresh token' }); }
  const u = (await pool.query('SELECT * FROM users WHERE id=$1', [row.user_id])).rows[0];
  await issue(res, u, row.family_id);
  res.json({ ok: true });
}));

r.post('/logout', h(async (req, res) => {
  const t = req.cookies.refresh_token;
  if (t) await pool.query('DELETE FROM refresh_tokens WHERE family_id=(SELECT family_id FROM refresh_tokens WHERE token_hash=$1)', [sha(t)]);
  clear(res); res.json({ ok: true });
}));

r.get('/me', authenticate, (req, res) => res.json({ id: req.user.sub, email: req.user.email, role: req.user.role, tenant: req.user.tenant }));

// ---- Google OAuth 2.0 / OpenID Connect (authorization code flow) ----
const G = { id: () => process.env.GOOGLE_CLIENT_ID, secret: () => process.env.GOOGLE_CLIENT_SECRET,
  redirect: () => process.env.GOOGLE_REDIRECT_URI || 'http://localhost:3000/auth/google/callback' };

r.get('/google', (req, res) => {
  if (!G.id()) return res.redirect('/?error=google_not_configured');
  const state = crypto.randomBytes(16).toString('hex'), nonce = crypto.randomBytes(16).toString('hex');
  res.cookie('oauth_tmp', state + '.' + nonce, cookie(600, '/auth'));
  const q = new URLSearchParams({ client_id: G.id(), redirect_uri: G.redirect(), response_type: 'code',
    scope: 'openid email profile', state, nonce });
  res.redirect('https://accounts.google.com/o/oauth2/v2/auth?' + q);
});

r.get('/google/callback', h(async (req, res) => {
  const [state, nonce] = (req.cookies.oauth_tmp || '.').split('.');
  res.clearCookie('oauth_tmp', { path: '/auth' });
  if (!req.query.code || !state || req.query.state !== state) return res.redirect('/?error=oauth_state');
  const tok = await (await fetch('https://oauth2.googleapis.com/token', { method: 'POST',
    headers: { 'content-type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({ code: req.query.code, client_id: G.id(), client_secret: G.secret(),
      redirect_uri: G.redirect(), grant_type: 'authorization_code' }) })).json();
  // id_token seedha Google ke token endpoint se TLS par aaya hai (OIDC Core 3.1.3.7), isliye claims check karte hain
  const c = tok.id_token && jwt.decode(tok.id_token);
  if (!c || c.aud !== G.id() || !['https://accounts.google.com', 'accounts.google.com'].includes(c.iss)
      || c.exp * 1000 < Date.now() || c.nonce !== nonce || !c.email_verified) return res.redirect('/?error=oauth_invalid');
  const email = c.email.toLowerCase();
  let u = (await pool.query('SELECT * FROM users WHERE google_sub=$1 OR email=$2', [c.sub, email])).rows[0];
  if (!u) u = (await pool.query(`INSERT INTO users(email,name,role,tenant_id,google_sub) VALUES($1,$2,'employee',$3,$4) RETURNING *`,
    [email, c.name, process.env.GOOGLE_DEFAULT_TENANT || 'acme', c.sub])).rows[0];
  else if (!u.google_sub) await pool.query('UPDATE users SET google_sub=$1 WHERE id=$2', [c.sub, u.id]);
  await issue(res, u);
  res.redirect('/');
}));

module.exports = { router: r, authenticate, authorize };
