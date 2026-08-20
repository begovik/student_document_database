# VPS Setup: PostgreSQL for Harvester

This guide is for the **agent or operator on the VPS** that will host the
remote PostgreSQL database for the *Harvester* project (Ukrainian scientific
PDF sources collector). The Harvester node itself runs elsewhere and connects
to this PostgreSQL over the network, with automatic fallback to its local
SQLite if this server is unreachable.

Repository: `git@github.com:begovik/student_document_database.git`

---

## 1. Prerequisites

- A Linux VPS (Debian/Ubuntu recommended), with root or sudo access.
- PostgreSQL 14+ (any recent version works).
- The Harvester node's IP address (to restrict access via firewall).

---

## 2. Install PostgreSQL

Debian/Ubuntu:

```bash
sudo apt update
sudo apt install -y postgresql postgresql-contrib
```

Verify:

```bash
sudo systemctl enable --now postgresql
sudo -u postgres psql -c "SELECT version();"
# expect: PostgreSQL 14.x (or newer) — older 12/13 also acceptable
```

---

## 3. Create the database and user

Run as the `postgres` OS user:

```bash
sudo -u postgres psql
```

Inside `psql`:

```sql
CREATE USER harvester WITH PASSWORD 'CHANGE_ME_STRONG_PASSWORD';
CREATE DATABASE harvester OWNER harvester;
GRANT ALL PRIVILEGES ON DATABASE harvester TO harvester;
\q
```

Notes:

- The password is passed to Harvester via environment variable `HARVESTER_PG_PASSWORD`
  on the Harvester node — it never goes into the git repo.
- Keep the password in your secret store / `.env` on the Harvester node,
  NOT in `config.yaml` committed to a repository.

---

## 4. Listen on the network (default is localhost only)

Edit `/etc/postgresql/<version>/main/postgresql.conf`:

```ini
listen_addresses = '*'          # or your specific interface
```

Allow the Harvester node over TCP, edit `/etc/postgresql/<version>/main/pg_hba.conf`
(append — put the *Harvester node's* IP here, NOT 0.0.0.0/0):

```
# harvester node:
host    harvester    harvester    <HARVESTER_NODE_IP>/32    scram-sha-256
```

Then restart:

```bash
sudo systemctl restart postgresql
```

Check the listening socket:

```bash
sudo ss -tlnp | grep 5432
# tcp  LISTEN 0  128  0.0.0.0:5432  ...  postgres
```

---

## 5. Firewall

Open port 5432 **only** for the Harvester node's IP:

```bash
sudo ufw allow from <HARVESTER_NODE_IP> to any port 5432 proto tcp
sudo ufw enable          # if not enabled yet
```

Alternative (if you use cloud provider security groups / firewall — e.g. Oracle
OCI, Hetzner): add an ingress rule for TCP/5432 restricted to the Harvester node IP.

---

## 6. Verify connectivity from the Harvester node

From the Harvester node (the machine running the service):

```bash
psql "postgresql://harvester:PASSWORD@VPS_IP:5432/harvester" -c "SELECT 1;"
# or with a generic client:
PGPASSWORD=PASSWORD psql -h VPS_IP -U harvester -d harvester -c "SELECT 1;"
```

Expect: `?column?  1`.

---

## 7. Harvester-side configuration (performed on the Harvester node)

In `config.yaml` on the Harvester node:

```yaml
database:
  mode: auto                        # auto | remote | local
  host: "<VPS_IP_OR_HOSTNAME>"
  port: 5432
  name: "harvester"
  user: "harvester"
  connect_timeout_s: 5
  retries: 3
  retry_delay_s: 2
  restore_probe_interval_s: 30
  local_db_path: "data/harvester.db"
```

And in the Harvester node's `.env`:

```
HARVESTER_PG_PASSWORD=CHANGE_ME_STRONG_PASSWORD
```

### One-time data transfer (run once, on the Harvester node):

```bash
harvester db-seed
harvester db-status    # should show: remote (PostgreSQL)
harvester doctor       # all green
```

After the first successful connection, Harvester runs on PostgreSQL; its
`data/harvester.db` acts as a local fallback (outbox). If they are later
disconnected, everything collected during the outage is merged back
automatically (see TECHNICAL_DESIGN.md §12.3 for details).

---

## 8. Backup & maintenance on the VPS (optional, recommended)

Add a cron job on the VPS to dump the DB daily:

```bash
sudo -u postgres bash -c 'mkdir -p /var/backups/harvester'
```

`/etc/cron.d/harvester-pg-backup`:

```
# m h dom mon dow user  command
30 3 * * * postgres pg_dump -U harvester harvester | gzip > /var/backups/harvester/harvester-$(date +\%Y\%m\%d).sql.gz
```

Keep ~14 daily dumps (add a small cleanup command if desired).

---

## 9. Troubleshooting

| Symptom | Check |
|---|---|
| `harvester db-status` shows `local (SQLite)` and `Remote достуна: ні` | network/firewall: `sudo ss -tlnp \| grep 5432` on VPS; `nc -zv VPS_IP 5432` from Harvester node |
| Connection refused | PostgreSQL not listening on `listen_addresses='*'` or service not running |
| `password authentication failed` | wrong password in `HARVESTER_PG_PASSWORD` on Harvester node; `pg_hba.conf` row (must be `scram-sha-256`, not `trust`) |
| `no pg_hba.conf entry for host ...` | your node IP is missing in `pg_hba.conf` (step 4) |
| Timeouts during heavy load | increase `pool_max_size` / `connect_timeout_s` in `database:` block; restart Harvester |
| After restore, `db-status` still shows outbox>0 | temporary; the outbox is drained on restore. If stuck, check `journalctl -u harvester` for `db_restore_merge_error`. |