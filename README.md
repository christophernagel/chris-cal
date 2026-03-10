# chriscal

Personal event calendar pipeline — UCLA/Westside/canyon ecosystem.
Aggregates event sources into a unified calendar with iCal feed and health monitoring.

## Stack
- Python 3.13 + FastAPI + APScheduler
- PostgreSQL 17
- nginx + certbot (TLS)
- systemd (process management)

## Setup sequence

```bash
# 1. Database
sudo -u postgres createuser chriscal
sudo -u postgres createdb chriscal -O chriscal
sudo -u postgres psql -c "ALTER USER chriscal WITH PASSWORD 'yourpassword';"
psql postgresql://chriscal:yourpassword@localhost/chriscal < schema.sql
psql postgresql://chriscal:yourpassword@localhost/chriscal < seed.sql

# 2. Python env
python3 -m venv venv
venv/bin/pip install -r requirements.txt

# 3. Secrets
cp .env.example /etc/chriscal.env
# edit /etc/chriscal.env with real credentials
chmod 600 /etc/chriscal.env

# 4. Smoke test
venv/bin/python smoke_test.py

# 5. Systemd
cp chriscal.service /etc/systemd/system/
systemctl daemon-reload
systemctl enable chriscal
systemctl start chriscal
systemctl status chriscal
```

## Endpoints
- `GET /api/events` — JSON event list (filterable)
- `GET /feed.ics` — iCal feed (subscribable)
- `GET /health` — source health dashboard (Basic Auth)

## Adding a new fetcher
1. Create `fetchers/yourname.py` implementing `async def fetch() -> FetchResult`
2. Add source row to `sources` table (or seed.sql)
3. Register in scheduler.py sources list
4. Add venue/alias rows to seed.sql if needed

