# Налаштування Harvester на хостингу (VPS)

Інструкція для запуску Harvester на власному сервері (VPS/VDS) у режимі
failover: локальна база SQLite + віддалена PostgreSQL. Розрахована на
Ubuntu 22.04/24.04.

---

## 1. Що вийде в результаті

| Компонент | Роль |
|---|---|
| PostgreSQL на VPS | **джерело істини**: усі дані зберігаються віддалено |
| SQLite на машині додатка | локальне **дзеркало** у реальному часі (dual-write) |
| `failover_outbox` | черга операцій, накопичених, поки додаток працював без звʼязку з PostgreSQL |

Якщо додаток запускається на **тому самому** VPS, де стоїть PostgreSQL —
логіка та сама: SQLite лишається локальним дзеркалом, PostgreSQL вважається
«віддаленою» базою (host = `127.0.0.1`).

## 2. Як працює синхронізація (коротко)

- Додаток запущено і PostgreSQL доступна → **кожна операція запису**
  виконується у PostgreSQL, а потім дублюється у локальний SQLite
  (з тими самими id, тож FK-ланцюжки валідні в обох копіях).
- **При старті додатка** виконується синхронізація: спершу зливається
  накопичена черга `failover_outbox` у PostgreSQL, потім локальне дзеркало
  **повністю перебудовується з PostgreSQL** — дані завжди свіжі.
- Поки додаток працює, кожні `restore_probe_interval_s` (30 с) фоновий цикл
  звіряє кількість рядків; якщо дзеркало розійшлося — воно автоматично
  відновлюється з PostgreSQL.
- PostgreSQL недоступна (обрив мережі тощо) → додаток автоматично працює
  на локальному SQLite, накопичуючи операції в outbox; щойно звʼязок
  відновлюється — черга зливається у PostgreSQL, дзеркало оновлюється,
  режим повертається на remote.

Тобто: **дані завжди в обох місцях**, PostgreSQL — основна копія, локальна
SQLite — свіжа резервна (аварійна) копія.

## 3. Передумови на сервері

```bash
sudo apt update && sudo apt upgrade -y
sudo apt install -y python3 python3-venv python3-pip git postgresql postgresql-contrib nginx
python3 --version   # потрібно >= 3.12
```

Системний користувач для додатка:

```bash
sudo useradd -r -s /usr/sbin/nologin -m harvester
```

## 4. PostgreSQL: створення бази та користувача

```bash
sudo -u postgres psql
```

Виконати (пароль замінити на надійний):

```sql
CREATE USER harvester WITH PASSWORD 'ВАШ_НАДІЙНИЙ_ПАРОЛЬ';
CREATE DATABASE harvester OWNER harvester;
\q
```

Дозволити вхід по паролю з локальної мережі (той самий VPS) або з IP машини,
де запускається додаток. У `/etc/postgresql/*/main/pg_hba.conf` додати рядок:

```
host    harvester    harvester    127.0.0.1/32    scram-sha-256
```

та, якщо додаток на іншій машині, рядок з її IP (наприклад):

```
host    harvester    harvester    203.0.113.10/32  scram-sha-256
```

У `/etc/postgresql/*/main/postgresql.conf`:

```
listen_addresses = '*'        # якщо підключаються ззовні
```

Перезапуск:

```bash
sudo systemctl restart postgresql
```

Якщо додаток підключається ззовні — у файрволі відкрити порт 5432 лише
для IP машини додатка:

```bash
sudo ufw allow from <IP_МАШИНИ_ДОДАТКА> to any port 5432 proto tcp
```

## 5. Розгортання додатка

```bash
cd /opt
sudo git clone git@github.com:begovik/student_document_database.git harvester
sudo chown -R harvester:harvester /opt/harvester
sudo -u harvester bash -c '
  cd /opt/harvester
  python3 -m venv .venv
  .venv/bin/pip install --upgrade pip
  .venv/bin/pip install -e .
  .venv/bin/pip install -e ".[dev]"   # опційно: для тестів
  cp config.example.yaml config.yaml
'
```

## 6. Конфіг `config.yaml` та пароль

`config.yaml` (мінімум для remote-режиму — блок `database`):

```yaml
database:
  mode: auto              # auto | remote | local
  host: "127.0.0.1"       # або IP/DNS сервера PostgreSQL
  port: 5432
  name: harvester
  user: harvester
  connect_timeout_s: 5
  retries: 3
  retry_delay_s: 2
  restore_probe_interval_s: 30   # інтервал фонових звірок дзеркала
  merge_on_restore: true
  local_db_path: data/harvester.db

paths:
  db_path: "data/harvester.db"
  tmp_dir: "data/tmp"
  backup_dir: "backups"
```

**Пароль PostgreSQL не зберігається у `config.yaml`** — через змінну
середовища у системному юніті (розділ 8):

```
HARVESTER_PG_PASSWORD=ВАШ_НАДІЙНИЙ_ПАРОЛЬ
```

## 7. Ініціалізація

```bash
sudo -u harvester bash -c '
  cd /opt/harvester
  set -a; . /etc/harvester.env; set +a   # див. розділ 8

  # 1) Створити схему у PostgreSQL (таблиці + міграції):
  .venv/bin/harvester init-db --config config.yaml

  # 2) Статус: має бути "Активна БД: віддалена (PostgreSQL)", дзеркало — синхронно:
  .venv/bin/harvester db-status
'
```

> Перший запуск з **порожньою** PostgreSQL: локальна SQLite не перезаписується
> (захист), дані переносяться вручну один раз командою
> `.venv/bin/harvester db-seed --config config.yaml`.

Перевірка стану (періодично):

| Команда | Що показує |
|---|---|
| `harvester status` | режим БД, heartbeat, кількість завдань |
| `harvester db-status` | активна БД, outbox, стан дзеркала |
| `harvester doctor` | перевірки + стан дзеркала (зелений/жовтий/червоний) |

## 8. Автозапуск (systemd)

Змінні середовища — у `/etc/harvester.env`:

```bash
sudo tee /etc/harvester.env >/dev/null <<'EOF'
HARVESTER_PG_PASSWORD=ВАШ_НАДІЙНИЙ_ПАРОЛЬ
EOF
sudo chmod 600 /etc/harvester.env
```

Готовий юніт — у репозиторії (`systemd/harvester.service`):

```ini
[Unit]
Description=Harvester — PDF sources collector
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=harvester
WorkingDirectory=/opt/harvester
EnvironmentFile=/etc/harvester.env
ExecStart=/opt/harvester/.venv/bin/harvester start --config /opt/harvester/config.yaml
Restart=always
RestartSec=10
TimeoutStopSec=30
MemoryMax=1G
Nice=10
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=full
ReadWritePaths=/opt/harvester/data /opt/harvester/backups

[Install]
WantedBy=multi-user.target
```

Встановити та запустити:

```bash
sudo cp systemd/harvester.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now harvester
systemctl status harvester
sudo journalctl -u harvester -f
```

У логах при старті мають зʼявитися рядки `db_mode_remote` та
`db_local_resynced` (синхронізація при старті виконана).

## 9. Експлуатаційні команди

```bash
sudo -u harvester bash -c 'cd /opt/harvester && .venv/bin/harvester <команда> --config config.yaml'
```

| Команда | Призначення |
|---|---|
| `status` | стан додатка і сервісів |
| `db-status` | активна БД, кількість у outbox, стан дзеркала; read-only, без resync |
| `doctor` | самодіагностика (БД, мережа, дзеркало) |
| `db-resync` | примусове відновлення локального дзеркала з PostgreSQL; перед запуском зупинити сервіс |
| `db-seed` | однократне перенесення локальної SQLite у PostgreSQL (лише при першому підключенні, remote має бути порожнім) |
| `init-db` | створення схеми таблиць (безпечно повторювати) |
| `statistics` | лічильники/статистика збору |
| `events` | події системи |
| `queries` | черга пошукових запитів |
| `vacuum` | стиснення локальної SQLite |
| `export` | експорт даних |
| `extract run` | витяг цитат і сумаризацій з PDF-документів (викликає LLM) |
| `curator prepare` | підготовка каталогу документів для теми |
| `curator verify` | перевірка і виправлення каталогу |

## 10. Резервне копіювання

- PostgreSQL: `pg_dump` за cron:

```bash
0 3 * * * /usr/bin/pg_dump -U harvester harvester | gzip > /opt/backups/pg_$(date +\%F).sql.gz
```

- Локальна SQLite (дзеркало) — файл `data/harvester.db`; копіювати через
  `harvester vacuum` або VACUUM INTO для консистентної копії.

Зберігати щонайменше 14 щоденних копій.

## 11. Помилки та як діяти

| Ознака | Причина | Дія |
|---|---|---|
| `db_mode_local` у логах | PostgreSQL недоступна при старті | перевірити мережу/файрвол/сервіс; додаток працює на local і зіллється сам |
| `db_startup_merge_failed` | збій злиття outbox при старті | глянути `journalctl`; повторити старт (злиття ідемпотентне: дублікати пропускаються) |
| `doctor` показує червоний стан дзеркала | дзеркало розійшлося з PostgreSQL | зупинити сервіс, виконати `harvester db-resync`, потім запустити сервіс; `db-status` можна виконувати без зупинки |
| додаток не стартує, помилка зʼєднання | невірний пароль/хост у `config.yaml`/`/etc/harvester.env` | перевірити значення, обидва файли чутливі до пароля |
| `init-db` виконує міграції повторно | так задумано | команда ідемпотентна, версія у `schema_version` |

## 12. Безпека (обовʼязково)

- Пароль PostgreSQL — **тільки** в `/etc/harvester.env` (права 600), ніколи
  у `config.yaml`/git.
- Застосовувати окремого користувача БД без `SUPERUSER`.
- Порт 5432 у файрволі відкривати лише для IP машини додатка (або
  використовувати SSH-тунель).
- Оновлювати систему: `sudo apt upgrade` регулярно, перезапускати додаток
  після оновлень.
