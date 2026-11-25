require('dotenv').config();
const fs = require('fs'), path = require('path');
const pool = require('./db');
const bcrypt = require('bcryptjs');
const N = Number(process.argv[2]) || 30000; // orders per tenant
(async () => {
  await pool.query(fs.readFileSync(path.join(__dirname, '../db/schema.sql'), 'utf8'));
  await pool.query('TRUNCATE refresh_tokens, users, order_items, orders, products, tenants RESTART IDENTITY CASCADE');
  await pool.query(`INSERT INTO tenants(id,name,plan) VALUES ('acme','Acme Traders','growth'),('globex','Globex Supplies','enterprise')`);
  const names = ['Steel Bolt Box', 'Packing Tape', 'Pallet Wrap', 'Label Roll', 'Safety Gloves', 'Cardboard Carton'];
  for (const t of ['acme', 'globex'])
    for (const [i, n] of names.entries())
      await pool.query('INSERT INTO products(tenant_id,name,price,stock) VALUES($1,$2,$3,500)', [t, n, 5 + i * 4]);
  await pool.query(`INSERT INTO orders(id,tenant_id,customer,created_at)
    SELECT gen_random_uuid(), t, 'Customer '||g, now()-(random()*interval '365 days')
    FROM unnest(array['acme','globex']) t, generate_series(1,$1) g`, [N]);
  await pool.query(`INSERT INTO order_items(order_id,tenant_id,product_id,qty,price)
    SELECT o.id, o.tenant_id, p.id, 1+(random()*3)::int, p.price FROM orders o
    JOIN LATERAL (SELECT id, price FROM products WHERE tenant_id=o.tenant_id ORDER BY random() LIMIT 3) p ON true`);
  await pool.query(`UPDATE orders o SET total=s.t FROM
    (SELECT order_id, sum(qty*price) t FROM order_items GROUP BY order_id) s WHERE s.order_id=o.id`);
  const hash = await bcrypt.hash(process.env.SEED_PASSWORD || 'Passw0rd!23', 12);
  for (const [email, role, t] of [['superadmin@saas.test','superadmin',null],['manager@acme.test','manager','acme'],
    ['employee@acme.test','employee','acme'],['manager@globex.test','manager','globex'],['employee@globex.test','employee','globex']])
    await pool.query('INSERT INTO users(email,name,password_hash,role,tenant_id) VALUES($1,$2,$3,$4,$5)', [email, email.split('@')[0], hash, role, t]);
  await pool.query('ANALYZE');
  console.log(`Seeded 2 tenants x ${N} orders`);
  await pool.end();
})();
