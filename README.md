# Multi-Tenant B2B SaaS — Kafka, Postgres tuning, Serverless

Stack: Node.js + Express, PostgreSQL (`pg` Pool), Apache Kafka (KafkaJS, Redpanda locally), Vercel serverless function.

## Run locally
```bash
cp .env.example .env
docker compose up -d          # Postgres + Redpanda (Kafka)
npm install
npm run seed                  # 2 tenants x 30,000 orders
npm run api                   # terminal 1  -> http://localhost:3000
npm run consumer              # terminal 2  (Kafka consumer group)
npm run bench                 # N+1 vs JOIN + EXPLAIN ANALYZE before/after index
```

## How each requirement is covered
| Requirement | Where |
|---|---|
| Tenant isolation (shared DB + `tenant_id`) | every query in `src/server.js` and `src/consumer.js` filters by `tenant_id`; dashboard has an "isolation check" button |
| Tenant resolution | middleware in `src/server.js`: `x-tenant-id` header or subdomain |
| N+1 vs JOIN | `GET /api/v1/orders?mode=slow` (201 queries) vs `?mode=fast` (1 query); also in `src/bench.js` |
| EXPLAIN ANALYZE + composite index | `src/bench.js` drops/creates `idx_orders_tenant_created (tenant_id, created_at DESC)`, writes `bench-results.txt` |
| Connection pooling | `src/db.js` (`max`, `idleTimeoutMillis`, `connectionTimeoutMillis`) |
| Kafka producer, 202 Accepted | `POST /api/v1/orders`, topic `tenant.orders`, message key = `tenant_id` |
| Consumer group | `src/consumer.js` (`order-processors`): saves order, reduces stock, stores offsets in `processed_offsets` (idempotent) |
| Serverless worker + cold start | `api/invoice.js`; latency saved in `invoice_log`, shown in dashboard |

## For the report (you must add your own results)
1. Run `npm run bench`, copy numbers from `bench-results.txt` and take screenshots of the plans (Seq Scan + Sort before, Index Scan after).
2. Deploy `api/invoice.js` to Vercel (`vercel --prod`), set `INVOICE_URL=https://<your-app>.vercel.app/api/invoice`, place a few orders, and compare **Cold** vs **Warm** averages shown in the dashboard. Locally the first call is only a *simulated* cold start (350 ms), so use the Vercel numbers in the report.
3. Cloud Kafka (Upstash/Aiven): set `KAFKA_BROKERS`, `KAFKA_SSL=true`, `KAFKA_USER`, `KAFKA_PASS`, `KAFKA_MECHANISM`, and create topic `tenant.orders` with 3 partitions.

## Auth & roles (assignment checklist)
| Requirement | Status / where |
|---|---|
| Passwords hashed (Bcrypt) | `bcryptjs`, 12 rounds, `src/auth.js` (no plain-text anywhere) |
| OAuth 2.0 / OIDC login | Google authorization-code flow with `state` + `nonce` + id_token claim checks (`/auth/google`). **Needs your Google keys in `.env`** |
| Access + Refresh rotation, httpOnly Secure cookies | `src/auth.js`: 15-min access JWT, 7-day refresh token stored as SHA-256 hash, rotated on every `/auth/refresh`, reuse = whole family revoked |
| RBAC | `superadmin` (all tenants, plans), `manager` (own tenant, team, invoices, system), `employee` (orders, inventory view); enforced by `authorize()` on the server, not only in the UI |
| Registration | "Create company" makes a new tenant + its Manager |

Demo users (after `npm run seed`, password `Passw0rd!23`): superadmin@saas.test, manager@acme.test, employee@acme.test, manager@globex.test, employee@globex.test.
Real-time updates: consumer -> Postgres `NOTIFY` -> API -> Server-Sent Events -> dashboard.
