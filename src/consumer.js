require('dotenv').config();
const pool = require('./db');
const { kafka, TOPIC } = require('./kafka');

async function callInvoice(e) {
  const t0 = Date.now();
  try {
    const r = await fetch(process.env.INVOICE_URL || 'http://localhost:3000/api/invoice', {
      method: 'POST', headers: { 'content-type': 'application/json', 'x-internal-key': process.env.INVOICE_SECRET || '' }, body: JSON.stringify(e) });
    const j = await r.json();
    await pool.query('INSERT INTO invoice_log(tenant_id, order_id, cold_start, latency_ms) VALUES($1,$2,$3,$4)',
      [e.tenantId, e.orderId, !!j.coldStart, Date.now() - t0]);
    return { cold: !!j.coldStart, ms: Date.now() - t0 };
  } catch (err) { console.error('invoice failed:', err.message); }
}

(async () => {
  const consumer = kafka.consumer({ groupId: 'order-processors' });
  await consumer.connect();
  await consumer.subscribe({ topic: TOPIC, fromBeginning: true });
  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const e = JSON.parse(message.value.toString());
      const c = await pool.connect();
      let fresh = false;
      const stock = [];
      try {
        await c.query('BEGIN');
        // offset log = idempotency: same message dobara aaye to dobara process nahi hoga
        const log = await c.query(
          'INSERT INTO processed_offsets(topic,partition,"offset",order_id,tenant_id) VALUES($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING',
          [topic, partition, message.offset, e.orderId, e.tenantId]);
        if (log.rowCount) {
          fresh = true;
          const total = e.items.reduce((s, i) => s + i.qty * i.price, 0);
          await c.query('INSERT INTO orders(id,tenant_id,customer,total,created_at) VALUES($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING',
            [e.orderId, e.tenantId, e.customer, total, e.createdAt]);
          for (const i of e.items) {
            await c.query('INSERT INTO order_items(order_id,tenant_id,product_id,qty,price) VALUES($1,$2,$3,$4,$5)',
              [e.orderId, e.tenantId, i.productId, i.qty, i.price]);
            const up = await c.query('UPDATE products SET stock=GREATEST(stock-$1,0) WHERE id=$2 AND tenant_id=$3 RETURNING id, stock',
              [i.qty, i.productId, e.tenantId]);
            if (up.rows[0]) stock.push(up.rows[0]);
          }
        }
        await c.query('COMMIT');
      } catch (err) { await c.query('ROLLBACK'); throw err; } finally { c.release(); }
      console.log(`[p${partition} @${message.offset}] tenant=${e.tenantId} order=${e.orderId.slice(0, 8)}`);
      if (fresh) {
        // Real-time events: Postgres NOTIFY -> API -> SSE -> browser
        const notify = (o) => pool.query('SELECT pg_notify($1,$2)', ['orders', JSON.stringify({ tenantId: e.tenantId, ...o })]).catch(() => {});
        const total = e.items.reduce((s, i) => s + i.qty * i.price, 0);
        notify({ type: 'order.processed', orderId: e.orderId, customer: e.customer, total, createdAt: e.createdAt });
        notify({ type: 'inventory.updated', stock });
        callInvoice(e).then((r) => r && notify({ type: 'invoice.ready', orderId: e.orderId, cold: r.cold, ms: r.ms }));
      }
    },
  });
})();
