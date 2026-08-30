# Повний перелік CLI-команд Harvester

Усі CLI-команди розподілені за сервісами та напрямками діяльності системи.

## 0. Загальні команди та середовище

### Активація віртуального середовища
```bash
# Перехід у директорію проєкту та активація venv
cd /opt/harvester
source .venv/bin/activate
```
Для виходу з віртуального середовища виконайте `deactivate`.

---

## 1. Сервіс збору та моніторингу (`harvester`)

Основні команди для керування фоновим демоном, діагностики та перегляду стану.

| Команда | Опис |
|---|---|
| `harvester start [--config PATH]` | Запустити сервіс у безперервному режимі 24/7 (supervisor, workers, discovery) |
| `harvester status` | Показати поточний стан сервісу (heartbeat, живі воркери, лічильники) |
| `harvester doctor` | Самодіагностика системи (БД, outbox, дзеркало, конфігурація, email) |
| `harvester stats [--period 24h\|7d\|30d] [--json]` | Статистика по каналах пошуку (запити, успіхи, помилки, нові документи) |
| `harvester export --output FILE [--format csv\|jsonl] [--lang LANG] [--status STATUS]` | Експорт верифікованих документів з бази даних |
| `harvester events [--limit N] [--level LEVEL]` | Перегляд системних подій (WARN, ERROR, CRITICAL) |
| `harvester queries [--top N]` | Аналіз ефективності пошукових запитів та їх yield |
| `harvester find --topic TOPIC [--limit N] [--lang LANG] [--type TYPE]` | Пошук джерел та літератури в існуючій базі за темою/УДК |

---

## 2. Управління базою даних (`harvester db-*`)

Команди для керування SQLite, синхронізації з віддаленою PostgreSQL та оптимізації.

| Команда | Опис |
|---|---|
| `harvester init-db` | Ініціалізувати схему бази даних (застосування міграцій) |
| `harvester db-status` | Стан підключення (active DB mode, remote/local, outbox, mirror status) |
| `harvester db-size` | Показати розміри та кількість рядків у локальній SQLite та віддаленій PostgreSQL |
| `harvester db-resync` | Відновити локальне дзеркало SQLite з віддаленої PostgreSQL |
| `harvester db-seed` | Однократне перенесення даних з локальної SQLite у віддалену PostgreSQL |
| `harvester vacuum` | Оптимізація та стиснення локальної бази даних SQLite |

---

## 3. Сервіс кураторства каталогів (`harvester curator`)

Формування та верифікація тематичних каталогів літератури з автозавантаженням PDF.

### `harvester curator prepare`
Підготувати каталог документів для заданої теми.

```bash
harvester curator prepare TOPIC [OPTIONS]
```
- `--output-dir`, `-o` (дефолт: `catalogs`): Директорія для збереження каталогу
- `--limit`, `-n`: Максимальна кількість кандидатів
- `--dry-run`, `-d`: Режим без збереження файлів

### `harvester curator verify`
Верифікувати каталог: перевірка доступності, виявлення помилок та автоматична заміна недоступних джерел.

```bash
harvester curator verify CATALOG_PATH [OPTIONS]
```
- `--dry-run`, `-d`: Показати план виправлення без внесення змін

#### Приклади:
```bash
# Створити каталог
harvester curator prepare "Підприємництво, торгівля та біржова діяльність"

# Перевірити та виправити каталог
harvester curator verify catalogs/catalog_20260828_102049
```

---

## 4. Сервіс витягу цитат і сумаризацій (`harvester extract`)

Інтелектуальний витяг ключових ідей, фактів та цитат за допомогою LLM.

```bash
harvester extract run [OPTIONS]
```

| Опція | Дефолт | Опис |
|---|---|---|
| `--topic`, `-t` | — | Фільтр за назвою теми |
| `--topic-code`, `-c` | — | Фільтр за кодом теми (наприклад, 076) |
| `--limit`, `-n` | 30 | Максимальна кількість документів |
| `--batch`, `-b` | 5 | Кількість паралельних обробок |
| `--dry-run`, `-d` | False | Режим тестування без запису в БД |
| `--retry-failed` | False | Повторна обробка лише документів із помилками |
| `--catalog-dir`, `-C` | — | Використання локальних PDF із каталогу |

#### Приклади:
```bash
harvester extract run --topic "Економіка" --limit 10
harvester extract run --catalog-dir catalogs/catalog_20260828_102049
```

---

## 5. Витяг літератури та добирання джерел (`harvester bibliography`)

Допоміжний сервіс добирання: сканує всі PDF у каталозі, витягує `ЛІТЕРАТУРА/REFERENCES` (LLM + fallback regex), дедуплікує, фільтрує RU (`is_russian_entry` — `.ru/.su/.рф`, `Москва`/`Издательство`, `ыэъё` без `іїєґ`), шукає кожне посилання в БД (`doi/url/title` `harvester/bibliography/searcher.py:113`) та в інтернеті (`DDGSSearchChannel` `harvester/discovery/ddgs_search.py:18` + `is_url_allowed` `harvester/net/guards.py:99`), завантажує знайдені PDF у `catalog_dir/bibliography_pdfs/` після перевірок (`200 OK`, `>5KB`, `%PDF`, `has_text_layer` `harvester/verify/pdfparse.py:32`, релевантність/інформативність) — документи → і в список, і в БД (`DocumentsRepository.insert_or_ignore` `harvester/db/repositories.py:17`), інтернет-ресурси → лише у список.

```bash
harvester bibliography scan CATALOG_DIR [OPTIONS]
```

| Опція | Дефолт | Опис |
|---|---|---|
| `CATALOG_DIR` | — | Шлях до папки каталогу (наприклад, `catalogs/catalog_20260828_135702`) |
| `--output`, `-o` | `bibliography_YYYYMMDD_HHMMSS` | Назва вихідного JSON (без розширення) |

Створює в `CATALOG_DIR/`:
- `bibliography_YYYYMMDD_HHMMSS.json` — `statistics` (`found_in_database`/`found_online`/`filtered_russian`/`pdfs_downloaded`), `explanation`
- `bibliography_YYYYMMDD_HHMMSS_literature.txt` — відформатований список
- `bibliography_YYYYMMDD_HHMMSS_found.json` — знайдені URL
- `bibliography_pdfs/*.pdf` — верифіковані завантажені PDF

```bash
harvester bibliography scan catalogs/catalog_20260828_135702
harvester bibliography scan catalogs/catalog_20260828_135702 --output my_refs
```

---

## 6. Допоміжні скрипти (`scripts/`)

Утиліти для роботи з каталогами та витягом даних.

### `extract_from_catalog.py`
Заповнення каталогу JSON витягнутими цитатами та сумаризаціями.

```bash
python scripts/extract_from_catalog.py CATALOG_PATH [OPTIONS]

# Приклад:
python scripts/extract_from_catalog.py catalogs/catalog_20260828_102049/ --limit 10
```

### `build_catalog.py`
Генерація спрощених зведених звітів та бібліографічних переліків.

```bash
python scripts/build_catalog.py [OPTIONS]
```

---

## 7. Системне адміністрування та systemd

Керування фоновим сервісом Harvester на VPS/хостингу.

```bash
# Перевірка статусу systemd сервісу
sudo systemctl status harvester

# Перезапуск / запуск / зупинка
sudo systemctl restart harvester
sudo systemctl start harvester
sudo systemctl stop harvester

# Перегляд живих логів у реальному часі
sudo journalctl -u harvester -f

# Перегляд логів з помилками
sudo journalctl -u harvester -p err --since "24 hours ago"
```
