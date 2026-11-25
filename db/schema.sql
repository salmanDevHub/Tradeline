CREATE TABLE IF NOT EXISTS tenants(id TEXT PRIMARY KEY, name TEXT NOT NULL, plan TEXT NOT NULL DEFAULT 'starter');
ALTER TABLE tenants ADD COLUMN IF NOT EXISTS plan TEXT NOT NULL DEFAULT 'starter';
CREATE TABLE IF NOT EXISTS products(
  id SERIAL PRIMARY KEY, tenant_id TEXT NOT NULL REFERENCES tenants(id),
  name TEXT NOT NULL, price NUMERIC(10,2) NOT NULL, stock INT NOT NULL);
CREATE TABLE IF NOT EXISTS orders(
  id UUID PRIMARY KEY, tenant_id TEXT NOT NULL REFERENCES tenants(id),
  customer TEXT NOT NULL, total NUMERIC(12,2) NOT NULL DEFAULT 0,
  status TEXT NOT NULL DEFAULT 'processed', created_at TIMESTAMPTZ NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS order_items(
  id SERIAL PRIMARY KEY, order_id UUID NOT NULL REFERENCES orders(id),
  tenant_id TEXT NOT NULL, product_id INT NOT NULL, qty INT NOT NULL, price NUMERIC(10,2) NOT NULL);
CREATE INDEX IF NOT EXISTS idx_items_order ON order_items(order_id);
CREATE INDEX IF NOT EXISTS idx_orders_tenant_created ON orders(tenant_id, created_at DESC);
CREATE TABLE IF NOT EXISTS processed_offsets(
  topic TEXT, partition INT, "offset" BIGINT, order_id UUID, tenant_id TEXT,
  processed_at TIMESTAMPTZ DEFAULT now(), PRIMARY KEY(topic, partition, "offset"));
CREATE TABLE IF NOT EXISTS invoice_log(
  id SERIAL PRIMARY KEY, tenant_id TEXT, order_id UUID, cold_start BOOLEAN,
  latency_ms INT, created_at TIMESTAMPTZ DEFAULT now());
CREATE TABLE IF NOT EXISTS users(
  id SERIAL PRIMARY KEY, email TEXT UNIQUE NOT NULL, name TEXT, password_hash TEXT,
  role TEXT NOT NULL CHECK (role IN ('superadmin','manager','employee')),
  tenant_id TEXT REFERENCES tenants(id), google_sub TEXT UNIQUE, created_at TIMESTAMPTZ DEFAULT now());
CREATE TABLE IF NOT EXISTS refresh_tokens(
  id SERIAL PRIMARY KEY, token_hash TEXT UNIQUE NOT NULL, user_id INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  family_id UUID NOT NULL, used BOOLEAN NOT NULL DEFAULT false, expires_at TIMESTAMPTZ NOT NULL);
