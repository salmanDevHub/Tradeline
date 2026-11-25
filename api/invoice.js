// Serverless function (Vercel: /api/invoice). Module-level variable cold start pakadta hai:
// naya container = pehli request cold, uske baad warm.
let isCold = true;
module.exports = (req, res) => {
  if (process.env.INVOICE_SECRET && req.headers['x-internal-key'] !== process.env.INVOICE_SECRET)
    return res.status(401).json({ error: 'unauthorized' });
  const t0 = Date.now();
  const coldStart = isCold;
  isCold = false;
  // Local par asli cold start nahi hota, isliye pehli call par init delay simulate hota hai.
  const delay = coldStart && !process.env.VERCEL ? 350 : 0;
  setTimeout(() => {
    const e = req.body || {};
    const items = e.items || [];
    const total = items.reduce((s, i) => s + i.qty * i.price, 0);
    res.status(200).json({
      coldStart,
      serverMs: Date.now() - t0,
      invoiceNo: 'INV-' + String(e.orderId || '').slice(0, 8).toUpperCase(),
      total,
      invoiceText: [`Invoice for ${e.customer}`, ...items.map((i) => `product #${i.productId} x${i.qty} @ ${i.price}`), `TOTAL ${total}`].join('\n'),
    });
  }, delay);
};
