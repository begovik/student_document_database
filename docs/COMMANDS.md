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

## 5. Цілодобова перевірка джерел (`harvester verifier`)

Фоновий сервіс 24/7, який ходить по `documents WHERE status='verified'` та перевіряє відповідність **строго `strict` профілю** `harvester/config/rules.yaml:9` (`min_page_count:3`, `min_chars_per_page:1500`, RU-фільтр) без створення нових правил. Використовує існуючі `is_document_complete()` `harvester/curator/preparer.py:38` + `analyze_pdf_quality()` `harvester/extract/pdf_quality.py:1` + `apply_all_filters()` `harvester/verify/filters.py:99`.

**Як працює:**
1. Батч `20` документів, де `verifier_checked_at IS NULL OR < now-7d` `harvester/verifier/worker.py:1` `ORDER BY verifier_checked_at NULLS FIRST`
2. Швидкі правила (без LLM): `check_strict_rules()` `harvester/verifier/rules.py:1` → якщо `fail` одразу запис `verifier_results(status='fail', comment="page_count=2 (мінімум 3)", rules_failed=[...])`
3. RU/СРСР фільтр `detect_language()` `harvester/verify/langid.py:17` → `fail`
4. LLM-верифікація (якщо пройшли 1-2): `harvester/verifier/llm_verifier.py:1` `PROMPT` (головна ціль — якісні джерела з повним текстом) → `LLMClient(keys=classify_keys, models=["gemini-3.1-flash-lite"])` `harvester/verifier/worker.py:1` з ротацією `GEMINI_DOC_VERIFIER_KEY_1 → _2 → _3 → _4` (одна модель `Gemini 3.1 Flash Lite` на всіх ключах). При `DailyLimitExhausted` → наступний ключ, при `AllLimitsExhausted` на 4-х — сон до `00:00 UTC` `harvester/verifier/worker.py:1` `_tomorrow_midnight_utc()`.

**Відмітки:** `harvester/db/migrations/004_verifier.sql:1` `verifier_results` (`document_id, profile strict, status pass/fail, comment` короткий, `rules_failed JSON, llm_status/comment/model/key_idx, checked_at/next_check_at`) + дублі `documents.verifier_status/comment/checked_at` для швидких фільтрів. Розрізняє `pass` (коментар "повна структура: титул, 12 стор, є вступ...") vs `fail` ("фрагмент 2 стор, відсутній список джерел").

**Конфігурація** `harvester/config.py:92` `VerifierConfig` (`enabled, batch_size 20, interval_s 60, recheck_days 7, llm_enabled, llm_model gemini-3.1-flash-lite`) + `harvester/config/rules.yaml:9` `strict`.

**Запуск:** автоматично `harvester/core/supervisor.py:1` (`w.verifier=1` за замовчуванням) при `harvester start`. Логи: `verifier_worker_started`, `verifier_document_check_start`, `verifier_result_saved`, `verifier_all_keys_exhausted_sleep` у `logs/harvester.log`.

**Моніторинг:**
```bash
journalctl -u harvester -f | grep verifier
PGPASSWORD=$PG_PASS psql -h 89.167.68.48 -U harvester -d harvester -c "SELECT status, count(*) FROM verifier_results GROUP BY status;"
PGPASSWORD=$PG_PASS psql -h 89.167.68.48 -U harvester -d harvester -c "SELECT verifier_status, count(*) FROM documents WHERE verifier_status IS NOT NULL GROUP BY verifier_status;"
```

---

## 6. Витяг літератури та добирання джерел (`harvester bibliography`)

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

## 7. Допоміжні скрипти (`scripts/`)

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

## 8. Системне адміністрування та systemd

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
