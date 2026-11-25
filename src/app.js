require('dotenv').config();
const express = require('express');
const path = require('path');
const crypto = require('crypto');
const cookieParser = require('cookie-parser');
const pool = require('./db');
const { router: authRouter, authenticate, authorize } = require('./auth');
const bcrypt = require('bcryptjs');
const { kafka, TOPIC } = require('./kafka');

const producer = kafka.producer();
let producerConnectPromise;

async function ensureProducer() {
  if (!producerConnectPromise) {
    producerConnectPromise = producer.connect().catch((err) => {
      producerConnectPromise = null;
      throw err;
    });
  }
  await producerConnectPromise;
}
const app = express();
app.use(express.json());
app.use(cookieParser());
app.use('/auth', authRouter);
app.use(express.static(path.join(__dirname, '../public')));
app.post('/api/invoice', require('../api/invoice')); // local copy of the serverless function

const h = (fn) => (req, res, next) => fn(req, res, next).catch(next);
const v1 = express.Router();

// SuperAdmin sab tenants dekhta hai, baaqi sirf apna
app.get('/api/tenants', authenticate, h(async (req, res) => {
  const q = req.user.role === 'superadmin'
    ? await pool.query('SELECT id, name FROM tenants ORDER BY id')
    : await pool.query('SELECT id, name FROM tenants WHERE id=$1', [req.user.tenant]);
  res.json(q.rows);
}));

// Tenant resolution: JWT claim se. Sirf SuperAdmin x-tenant-id header se tenant badal sakta hai.
v1.use(authenticate, h(async (req, res, next) => {
  const hdr = String(req.headers['x-tenant-id'] || req.query.tenant || '').toLowerCase();
  let id;
  if (req.user.role === 'superadmin') id = hdr || req.user.tenant;
  else { if (hdr && hdr !== req.user.tenant) return res.status(403).json({ error: 'cross-tenant access denied' }); id = req.user.tenant; }
  if (!id) return res.status(400).json({ error: 'x-tenant-id required' });
  const r = await pool.query('SELECT id, name, plan FROM tenants WHERE id=$1', [id]);
  if (!r.rowCount) return res.status(404).json({ error: 'unknown tenant' });
  req.tenant = r.rows[0];
  next();
}));

// Har query mein tenant_id filter hai -> Tenant A ko Tenant B ka data kabhi nahi milta
v1.get('/products', h(async (req, res) => {
  res.json((await pool.query(
    'SELECT id, name, price, stock FROM products WHERE tenant_id=$1 ORDER BY id', [req.tenant.id])).rows);
}));

v1.post('/orders', h(async (req, res) => {
  const { customer, items } = req.body || {};
  if (!customer || !Array.isArray(items) || !items.length)
    return res.status(400).json({ error: 'customer and items required' });
  const ids = items.map((i) => Number(i.productId));
  const p = await pool.query('SELECT id, price FROM products WHERE tenant_id=$1 AND id=ANY($2)', [req.tenant.id, ids]);
  const price = Object.fromEntries(p.rows.map((r) => [r.id, Number(r.price)]));
  if (items.some((i) => price[i.productId] === undefined || !(Number(i.qty) > 0)))
    return res.status(400).json({ error: 'invalid product or qty for this tenant' });
  const event = {
    type: 'OrderCreated', orderId: crypto.randomUUID(), tenantId: req.tenant.id, customer,
    items: items.map((i) => ({ productId: Number(i.productId), qty: Number(i.qty), price: price[i.productId] })),
    createdAt: new Date().toISOString(),
  };
  // key = tenant_id -> same tenant ke events ek hi partition mein, order maintain
  await ensureProducer();
  await producer.send({ topic: TOPIC, messages: [{ key: req.tenant.id, value: JSON.stringify(event) }] });
  pool.query('SELECT pg_notify($1,$2)', ['orders', JSON.stringify({ type: 'order.queued', tenantId: req.tenant.id, orderId: event.orderId,
    customer, total: event.items.reduce((s, i) => s + i.qty * i.price, 0) })]).catch(() => {});
  res.status(202).json({ status: 'accepted', orderId: event.orderId });
}));

// mode=slow -> N+1 problem, mode=fast -> single JOIN
v1.get('/orders', h(async (req, res) => {
  const t = req.tenant.id;
  const mode = req.query.mode === 'slow' ? 'slow' : 'fast';
  const limit = Math.min(Number(req.query.limit) || 20, 200);
  const start = process.hrtime.bigint();
  let rows, queries = 1;
  if (mode === 'slow') {
    rows = (await pool.query('SELECT * FROM orders WHERE tenant_id=$1 ORDER BY created_at DESC LIMIT $2', [t, limit])).rows;
    for (const o of rows) {
      o.items = (await pool.query('SELECT product_id, qty, price FROM order_items WHERE order_id=$1 AND tenant_id=$2', [o.id, t])).rows;
      queries++;
    }
  } else {
    rows = (await pool.query(`
      SELECT o.id, o.customer, o.total, o.status, o.created_at,
        COALESCE(json_agg(json_build_object('product_id', i.product_id, 'qty', i.qty, 'price', i.price))
                 FILTER (WHERE i.id IS NOT NULL), '[]') AS items
      FROM (SELECT * FROM orders WHERE tenant_id=$1 ORDER BY created_at DESC LIMIT $2) o
      LEFT JOIN order_items i ON i.order_id=o.id AND i.tenant_id=o.tenant_id
      GROUP BY o.id, o.customer, o.total, o.status, o.created_at
      ORDER BY o.created_at DESC`, [t, limit])).rows;
  }
  res.json({ mode, queries, ms: Number(process.hrtime.bigint() - start) / 1e6, orders: rows });
}));

v1.get('/orders/:id', h(async (req, res) => {
  if (!/^[0-9a-f-]{36}$/i.test(req.params.id)) return res.status(404).json({ error: 'not found' });
  const r = await pool.query('SELECT * FROM orders WHERE id=$1 AND tenant_id=$2', [req.params.id, req.tenant.id]);
  r.rowCount ? res.json(r.rows[0]) : res.status(404).json({ error: 'not found' });
}));

v1.get('/pipeline', authorize('manager', 'superadmin'), h(async (req, res) => {
  const t = req.tenant.id;
  const [offsets, invoices, avg] = await Promise.all([
    pool.query('SELECT partition, "offset", order_id, processed_at FROM processed_offsets WHERE tenant_id=$1 ORDER BY processed_at DESC LIMIT 8', [t]),
    pool.query('SELECT order_id, cold_start, latency_ms, created_at FROM invoice_log WHERE tenant_id=$1 ORDER BY id DESC LIMIT 8', [t]),
    pool.query('SELECT cold_start, count(*)::int n, round(avg(latency_ms))::int avg_ms FROM invoice_log WHERE tenant_id=$1 GROUP BY cold_start', [t]),
  ]);
  res.json({ offsets: offsets.rows, invoices: invoices.rows, invoiceAvg: avg.rows });
}));

// Users: Manager apne tenant mein Employee bana sakta hai; SuperAdmin Manager bhi bana sakta hai
v1.get('/users', authorize('manager', 'superadmin'), h(async (req, res) => {
  res.json((await pool.query('SELECT id, email, name, role FROM users WHERE tenant_id=$1 ORDER BY id', [req.tenant.id])).rows);
}));
v1.post('/users', authorize('manager', 'superadmin'), h(async (req, res) => {
  const { email, password, name, role = 'employee' } = req.body || {};
  const allowed = req.user.role === 'superadmin' ? ['employee', 'manager'] : ['employee'];
  if (!/^\S+@\S+\.\S+$/.test(email || '') || (password || '').length < 8) return res.status(400).json({ error: 'valid email and 8+ char password required' });
  if (!allowed.includes(role)) return res.status(403).json({ error: 'you cannot create role ' + role });
  try {
    const u = await pool.query('INSERT INTO users(email,name,password_hash,role,tenant_id) VALUES($1,$2,$3,$4,$5) RETURNING id,email,role',
      [email.toLowerCase(), name || email.split('@')[0], await bcrypt.hash(password, 12), role, req.tenant.id]);
    res.status(201).json(u.rows[0]);
  } catch (e) { if (e.code === '23505') return res.status(409).json({ error: 'email already exists' }); throw e; }
}));

v1.get('/stats', h(async (req, res) => {
  const t = req.tenant.id;
  const [a, m, tp, u, ls] = await Promise.all([
    pool.query(`SELECT count(*)::int orders, coalesce(sum(total),0)::float revenue,
      count(*) FILTER (WHERE created_at>now()-interval '30 days')::int orders30,
      coalesce(sum(total) FILTER (WHERE created_at>now()-interval '30 days'),0)::float revenue30,
      count(*) FILTER (WHERE created_at>=date_trunc('day',now()))::int "ordersToday",
      coalesce(sum(total) FILTER (WHERE created_at>=date_trunc('day',now())),0)::float "revenueToday" FROM orders WHERE tenant_id=$1`, [t]),
    pool.query(`SELECT date_trunc('month',created_at) d, sum(total)::float v FROM orders
      WHERE tenant_id=$1 AND created_at>date_trunc('month',now())-interval '5 months' GROUP BY d ORDER BY d`, [t]),
    pool.query(`SELECT p.name, sum(i.qty)::int sold FROM order_items i JOIN products p ON p.id=i.product_id
      WHERE i.tenant_id=$1 AND p.tenant_id=$1 GROUP BY p.name ORDER BY sold DESC LIMIT 5`, [t]),
    pool.query('SELECT count(*)::int n FROM users WHERE tenant_id=$1', [t]),
    pool.query('SELECT count(*)::int n FROM products WHERE tenant_id=$1 AND stock<100', [t]),
  ]);
  res.json({ ...a.rows[0], monthly: m.rows, top: tp.rows, users: u.rows[0].n, lowStock: ls.rows[0].n, plan: req.tenant.plan, name: req.tenant.name });
}));
v1.patch('/products/:id', authorize('manager', 'superadmin'), h(async (req, res) => {
  const stock = Number(req.body && req.body.stock);
  if (!Number.isInteger(stock) || stock < 0) return res.status(400).json({ error: 'stock must be 0 or more' });
  const r = await pool.query('UPDATE products SET stock=$1 WHERE id=$2 AND tenant_id=$3', [stock, Number(req.params.id) || 0, req.tenant.id]);
  r.rowCount ? res.json({ ok: true }) : res.status(404).json({ error: 'not found' });
}));
v1.get('/invoices', authorize('manager', 'superadmin'), h(async (req, res) => {
  res.json((await pool.query(`SELECT l.order_id, l.cold_start, l.latency_ms, l.created_at, o.customer, o.total
    FROM invoice_log l JOIN orders o ON o.id=l.order_id WHERE l.tenant_id=$1 ORDER BY l.id DESC LIMIT 30`, [req.tenant.id])).rows);
}));

// Real-time: Server-Sent Events. Consumer pg_notify bhejta hai, API sirf us tenant ke browsers ko forward karta hai.
const clients = new Set();
v1.get('/events', (req, res) => {
  res.set({ 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache', Connection: 'keep-alive' });
  res.flushHeaders(); res.write('retry: 3000\n\n');
  const c = { tenant: req.tenant.id, res }; clients.add(c);
  req.on('close', () => clients.delete(c));
});


// SuperAdmin panel: sab tenants ka overview aur plan change
app.get('/api/admin/overview', authenticate, authorize('superadmin'), h(async (_q, res) => {
  res.json((await pool.query(`SELECT t.id, t.name, t.plan,
    (SELECT count(*) FROM users u WHERE u.tenant_id=t.id)::int users,
    (SELECT count(*) FROM orders o WHERE o.tenant_id=t.id)::int orders,
    (SELECT coalesce(sum(total),0) FROM orders o WHERE o.tenant_id=t.id)::float revenue,
    (SELECT max(created_at) FROM orders o WHERE o.tenant_id=t.id) last_order FROM tenants t ORDER BY t.name`)).rows);
}));
app.put('/api/admin/tenants/:id/plan', authenticate, authorize('superadmin'), h(async (req, res) => {
  if (!['starter', 'growth', 'enterprise'].includes(req.body && req.body.plan)) return res.status(400).json({ error: 'bad plan' });
  await pool.query('UPDATE tenants SET plan=$1 WHERE id=$2', [req.body.plan, req.params.id]);
  res.json({ ok: true });
}));

app.use('/api/v1', v1);
app.use((err, _req, res, _next) => { console.error(err); res.status(500).json({ error: 'server error' }); });


module.exports = app;

