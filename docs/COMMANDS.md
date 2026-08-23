# Список CLI-команд Harvester

## 1. Основний CLI — `harvester`

Запуск додатка: `harvester start`

### Service commands

| Команда | Опис |
|---|---|
| `harvester start` | Запустити сервіс у безперервному режимі (discovery + verify + classify) |
| `harvester status` | Показати стан сервісу (heartbeat, завдання, канали) |
| `harvester doctor` | Самодіагностика (БД, мережа, дзеркало) |
| `harvester db-status` | Активна БД, кількість у outbox, стан дзеркала |
| `harvester db-resync` | Примусове відновлення локального дзеркала з PostgreSQL |
| `harvester db-seed` | Однократне перенесення локальної SQLite у PostgreSQL |
| `harvester init-db` | Створити схему таблиць (міграції) |
| `harvester vacuum` | Стиснення локальної SQLite |
| `harvester statistics` | Лічильники/статистика збору |
| `harvester events` | Події системи |
| `harvester queries` | Черга пошукових запитів |
| `harvester export` | Експорт даних |

### Discovery commands

| Команда | Опис |
|---|---|
| `harvester db-status` | Активна БД, кількість у outbox, стан дзеркала |
| `harvester init-db` | Створити схему таблиць (міграції) |
| `harvester db-seed` | Однократне перенесення локальної SQLite у PostgreSQL |
| `harvester status` | Показати стан сервісу |
| `harvester doctor` | Самодіагностика |

---

## 2. Витяг цитат і сумаризацій — `harvester extract`

```bash
harvester extract run [OPTIONS]
```

| Опція | Дефолт | Опис |
|---|---|---|
| `--topic`, `-t` | — | Фільтр по назві теми (часткова підстрока) |
| `--topic-code`, `-c` | — | Фільтр по коду теми (trade, 076, econ, ...) |
| `--limit`, `-n` | 30 | Максимальна кількість документів для обробки |
| `--batch`, `-b` | 5 | Кількість одночасних завдань |
| `--dry-run`, `-d` | False | Не зберігати результати, лише показати |
| `--retry-failed`, `-r` | False | Обробляти тільки документи з попередніми помилками |
| `--skip-extracted`, `--no-skip-extracted` | True (пропускати) | Пропускати вже оброблені |
| `--catalog-dir`, `-C` | — | Шлях до каталогу з resources/ (для використання локальних PDF замість завантаження) |

### Приклади

```bash
# Витяг для всіх документів теми "Підприємництво"
harvester extract run --topic "Підприємництво"

# Витяг для теми з кодом 076
harvester extract run --topic-code 076

# Витяг з використанням локальних PDF з каталогу
harvester extract run --catalog-dir catalogs/catalog_20260823_015827 --limit 10

# Dry-run
harvester extract run --topic "Економіка" --dry-run
```

---

## 3. Підготовка каталогів — `harvester curator`

### `harvester curator prepare`

Підготувати каталог документів для теми: відбір, перевірка доступності, завантаження PDF, запис каталогу.

```bash
harvester curator prepare TOPIC [OPTIONS]
```

| Опція | Дефолт | Опис |
|---|---|---|
| `TOPIC` | — | Назва теми (наприклад, "Підприємництво, торгівля та біржова діяльність") |
| `--output-dir`, `-o` | catalogs | Директорія для збереження каталогу |
| `--limit`, `-n` | — | Максимальна кількість документів (LLM може обрати менше) |
| `--dry-run`, `-d` | False | Не зберігати результат, лише показати що було б зроблено |

### Приклади

```bash
# Підготувати каталог для теми "Підприємництво, торгівля та біржова діяльність"
harvester curator prepare "Підприємництво, торгівля та біржова діяльність"

# З обмеженням кількості
harvester curator prepare "Економіка" --limit 50

# Dry-run
harvester curator prepare "Інформатика" --dry-run
```

Результат: створюється папка `catalogs/catalog_YYYYMMDD_HHMMSS/` з:
- `catalog_YYYYMMDD_HHMMSS.json` — метаданими каталогу
- `resources/` — завантаженими PDF-файлами

### `harvester curator verify`

Перевірити каталог: знайти помилки, вирішити що робити, виправити.

```bash
harvester curator verify CATALOG_PATH [OPTIONS]
```

| Опція | Дефолт | Опис |
|---|---|---|
| `CATALOG_PATH` | — | Шлях до каталогу (файл або папка) |
| `--dry-run`, `-d` | False | Не зберігати результат, лише показати |

### Приклади

```bash
# Перевірити каталог
harvester curator verify catalogs/catalog_20260823_015827

# Dry-run
harvester curator verify catalogs/catalog_20260823_015827 --dry-run
```

---

## 4. Скрипт extract_from_catalog.py

Скрипт для витягу цитат і сумаризацій з каталогу та запис результатів назад у каталог.

```bash
python scripts/extract_from_catalog.py CATALOG_PATH [OPTIONS]
```

| Опція | Дефолт | Опис |
|---|---|---|
| `CATALOG_PATH` | — | Шлях до каталогу (файл або папка) |
| `--limit`, `-n` | — | Максимальна кількість документів для витягу |
| `--dry-run`, `-d` | False | Не зберігати результат |
| `--force`, `-f` | False | Ігнорувати наявні витяги в БД |

### Приклади

```bash
# Витяг з каталогу (файл)
python scripts/extract_from_catalog.py catalogs/catalog_076.json

# Витяг з каталогу (папка)
python scripts/extract_from_catalog.py catalogs/catalog_20260823_015827/

# З обмеженням
python scripts/extract_from_catalog.py catalogs/catalog_076.json --limit 5

# Dry-run
python scripts/extract_from_catalog.py catalogs/catalog_076.json --dry-run
```

---

## 5. Інші команди

| Команда | Опис |
|---|---|
| `harvester status` | Показати стан сервісу |
| `harvester db-status` | Показати стан БД |
| `harvester doctor` | Самодіагностика |
| `harvester init-db` | Ініціалізація БД |

---

## 6. Структура каталогу

Каталог (папка) має таку структуру:

```
catalogs/
└── catalog_YYYYMMDD_HHMMSS/
    ├── catalog_YYYYMMDD_HHMMSS.json   # Метадані каталогу
    └── resources/                      # Завантажені PDF
        ├── 10678.pdf
        ├── 15901.pdf
        └── ...
```

Кожен `catalog_YYYYMMDD_HHMMSS.json` містить:

```json
{
  "topic": "Підприємництво, торгівля та біржова діяльність",
  "created_at": "2026-08-23T01:58:27.788846",
  "total_documents": 30,
  "replaced_count": 0,
  "documents": [
    {
      "id": 47879,
      "title": "ПІДПРИЄМНИЦТВО, ТОРГІВЛЯ ТА БІРЖОВА ДІЯЛЬНІСТЬ",
      "authors": ["Iryna Khoma", "Юліана Мисько"],
      "year": 2023,
      "publisher": null,
      "doc_type": "article",
      "canonical_url": "https://economyandsociety.in.ua/...",
      "language": "uk",
      "udc": "336.76",
      "page_count": 10,
      "size_bytes": 417127,
      "sha256": "...",
      "has_text_layer": 1,
      "verified_at": "...",
      "first_seen_at": "...",
      "pdf_path": "resources/47879.pdf",
      "topics": [{"topic_id": 3, "topic_name": "Економіка", "score": 0.83}]
    }
  ]
}
```

---

## 7. Робочий процес

1. **Підготовка каталогу** (опціонально при першому запуску):
   ```bash
   harvester curator prepare "Тема"
   ```

2. **Витяг цитат і сумаризацій**:
   ```bash
   harvester extract run --catalog-dir catalogs/catalog_YYYYMMDD_HHMMSS
   ```

3. **Перевірка каталогу** (при виявленні помилок):
   ```bash
   harvester curator verify catalogs/catalog_YYYYMMDD_HHMMSS
   ```

4. **Витяг з каталогу** (заповнення цитатами та сумаризаціями в JSON):
   ```bash
   python scripts/extract_from_catalog.py catalogs/catalog_YYYYMMDD_HHMMSS
   ```

---

## 8. Моніторинг хостингу (фонова робота)

Команди для перевірки стану сервісу на VPS, де Harvester працює як systemd-сервіс.

### Керування сервісом

```bash
# Статус
sudo systemctl status harvester

# Зупинити / запустити / перезапустити
sudo systemctl stop harvester
sudo systemctl start harvester
sudo systemctl restart harvester

# Live-логи
sudo journalctl -u harvester -f

# Логи за період
sudo journalctl -u harvester --since "1 hour ago"

# Тільки помилки
sudo journalctl -u harvester -p err
```

### Стан БД

```bash
# Статус (режим, outbox, дзеркало)
sudo -u harvester bash -c 'cd /opt/harvester && .venv/bin/harvester db-status'

# Діагностика
sudo -u harvester bash -c 'cd /opt/harvester && .venv/bin/harvester doctor'

# Кількість в outbox (дані, що очікують злиття в PG)
sudo -u harvester python3 -c "
import sqlite3
c = sqlite3.connect('/opt/harvester/data/harvester.db').cursor()
c.execute('SELECT count(*) FROM failover_outbox')
print(f'Outbox: {c.fetchone()[0]}')
"

# Кількість документів у PG
PGPASSWORD=<пароль> psql -h <VPS_IP> -U harvester -d harvester \
  -c "SELECT count(*) FROM documents;"

# Перевірка FK-порушень (має бути 0)
PGPASSWORD=<пароль> psql -h <VPS_IP> -U harvester -d harvester \
  -c "SELECT count(*) FROM document_refs WHERE document_id NOT IN (SELECT id FROM documents);"
```

### Що означають показники

| Показник | Норма | Що означає |
|---|---|---|
| `Активна БД: remote (PostgreSQL)` | ОК | З'єднання з PG працює |
| `Активна БД: local (SQLite)` | ⚠️ | PG недоступна, працює в offline |
| `Дзеркало: синхронно` | ОК | Дані збігаються |
| `Дзеркало: розбіжність` | ⚠️ | Потрібен `db-resync` |
| `Outbox: 0` | ОК | Усі дані злиті в PG |
| `Outbox: >0` | ⚠️ | Дані очікують злиття (нормально при старті) |
| `FK-порушення: 0` | ОК | Цілісність даних |
| `FK-порушення: >0` | 🔴 | Потрібне втручання |
