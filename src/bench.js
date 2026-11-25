require('dotenv').config();
const fs = require('fs');
const pool = require('./db');
const T = 'acme';
const Q = `SELECT * FROM orders WHERE tenant_id='${T}' AND created_at > now() - interval '30 days' ORDER BY created_at DESC LIMIT 50`;
const median = async (fn, n = 7) => {
  const a = [];
  for (let i = 0; i < n; i++) { const s = process.hrtime.bigint(); await fn(); a.push(Number(process.hrtime.bigint() - s) / 1e6); }
  return a.sort((x, y) => x - y)[Math.floor(n / 2)];
};
const explain = async () => (await pool.query('EXPLAIN (ANALYZE, BUFFERS) ' + Q)).rows.map((r) => r['QUERY PLAN']).join('\n');

(async () => {
  let out = '';
  const log = (s) => { console.log(s); out += s + '\n'; };

  const n1 = await median(async () => {
    const o = (await pool.query('SELECT id FROM orders WHERE tenant_id=$1 ORDER BY created_at DESC LIMIT 200', [T])).rows;
    for (const r of o) await pool.query('SELECT * FROM order_items WHERE order_id=$1', [r.id]);
  });
  const join = await median(() => pool.query(`SELECT o.id, i.* FROM (SELECT id FROM orders WHERE tenant_id=$1 ORDER BY created_at DESC LIMIT 200) o
    LEFT JOIN order_items i ON i.order_id=o.id`, [T]));
  log(`N+1 (201 queries): ${n1.toFixed(1)} ms\nJOIN (1 query):    ${join.toFixed(1)} ms\n`);

  await pool.query('DROP INDEX IF EXISTS idx_orders_tenant_created'); await pool.query('ANALYZE orders');
  const before = await median(() => pool.query(Q));
  log('=== EXPLAIN ANALYZE (BEFORE index) ===\n' + await explain() + `\nmedian latency: ${before.toFixed(2)} ms\n`);

  await pool.query('CREATE INDEX idx_orders_tenant_created ON orders(tenant_id, created_at DESC)'); await pool.query('ANALYZE orders');
  const after = await median(() => pool.query(Q));
  log('=== EXPLAIN ANALYZE (AFTER index) ===\n' + await explain() + `\nmedian latency: ${after.toFixed(2)} ms\n`);
  log(`Speedup: ${(before / after).toFixed(1)}x`);
  fs.writeFileSync('bench-results.txt', out);
  await pool.end();
})();
