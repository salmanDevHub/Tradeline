const { Pool } = require('pg');
// Connection pooling: max connections limit, idle cleanup, connect timeout
module.exports = new Pool({
  connectionString: process.env.DATABASE_URL || 'postgres://saas:saas@localhost:5432/saas',
  max: Number(process.env.PG_POOL_MAX) || 10,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 5000,
});
