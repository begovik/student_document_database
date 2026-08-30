# Plan: Сервіс цілодобової перевірки джерел за strict-правилами (LLM-верифікація)

## 1. Мета
Створити окремий фоновий сервіс `harvester.verifier` (не плутати з `harvester/verify/pipeline` — разова перевірка при `probe`), який **24/7 ходить по `documents` в БД** і перевіряє відповідність **існуючим правилам** — без створення нових. Використовує профіль `strict` (`harvester/config/rules.yaml:9`).

Головна ціль проєкту (пріоритет над кількістю): збирати **якісні джерела — документи з повним текстом**, інформативні статті/монографії/посібники, придатні для наукових праць. Не тези, не зміст, не анотації, не фрагменти — лише цілісні, структуровані, логічно завершені джерела (титул, вступ/мета, розділи, висновки, список джерел). Сервіс має відмічати **пройдені**, **не відповідні** (з коментарем) і розрізняти їх.

LLM: **тільки `GEMINI_DOC_VERIFIER_KEY_1..4`** (`harvester/config.py:207` `classify_keys`), **модель `Gemini 3.1 Flash Lite`** (`harvester/config.py:140` `gemini_models[0]`). Логіка ключів: `KEY_1 + 3.1` → вичерпано → `KEY_2 + 3.1` → `KEY_3 + 3.1` → `KEY_4 + 3.1`. Якщо всі 4 вичерпані до кінця доби — **зупинка до 00:00 UTC наступної доби** (добовий ліміт `500 RPD`/`250K TPM`/`15 RPM` на ключ).

## 2. Поточний стан (що вже є)

- Правила: `harvester/config/rules.yaml:9` `strict` (`min_page_count:3`, `min_chars_per_page:1500`, `require_references:true` (планувалось `true`), `max_toc_ratio:0.15`, `reject_ppt:true`) і `harvester/config.py:291` `FilterRules` + `harvester/config.py:324` `get_filter_rules("strict")`. Сервіс **повинен** використовувати їх, не дублювати.
- Перевірка повноти: `harvester/curator/preparer.py:38` `is_document_complete(doc, rules)` — перевіряє `REQUIRED_STATUS`, `REQUIRED_FIELDS`, `title`/`authors` garbage-фільтри, `page_count < min_page_count`, `chars_per_page`, `extra.producer` (PPT), `title` презентації. Сервіс має перевикористати її.
- PDF-якість: `harvester/extract/pdf_quality.py:1` `analyze_pdf_quality()` (`page_count`, `chars_per_page`, `has_toc/references/abstract/conclusion`, `has_udc`) — також частина strict.
- RU/СРСР фільтри: `harvester/verify/filters.py:99` `apply_all_filters()` + `harvester/net/guards.py:89` `is_domain_blocked()`/`is_url_allowed()`.
- LLM-клієнт: `harvester/classify/llm.py:91` `LLMClient` з `ModelRateLimiter` `harvester/classify/ratelimit.py:1` (`gemini_rpm/rpd`, `gemma_rpm/rpd/tpm`) — зараз використовує `classify_keys` для `classify` (`harvester/core/supervisor.py:115` `gemma_per_key_model`). Новий сервіс — окремий інстанс `LLMClient(keys=classify_keys, models=["gemini-3.1-flash-lite"], gemma_only=False)` — **ті самі 4 ключі, але одна модель**, ротація ключів як вимагає ТЗ (не модель).
- БД: `harvester/db/pg_schema.sql:40` `documents` (`status`, `page_count`, `has_text_layer`, `verified_at`, `next_verify_at` `harvester/db/pg_schema.sql:79` `idx_documents_reverify WHERE status='verified'`), `harvester/db/repositories.py:13` `DocumentsRepository`. Для відміток потрібне нове поле/таблиця (див. §4).
- Текучі сервіси: `bibliography` `harvester/bibliography/service.py:56` — одноразовий скан каталогу, не 24/7 по БД.

## 3. Архітектура нового сервісу

### 3.1 Розташування
```
harvester/verifier/          # НОВИЙ пакет (не плутати з harvester/verify)
├── __init__.py
├── worker.py                # VerifierWorker — цикл while self._running
├── llm_verifier.py          # LLM-промпт та виклик Gemini 3.1 Flash Lite
├── rules.py                 # Тонка обгортка над get_filter_rules("strict") + is_document_complete + pdf_quality
└── __main__.py              # опційно python -m harvester.verifier

harvester/db/migrations/005_verifier.sql  # НОВА міграція
```

### 3.2 Конфігурація
- `harvester/config/rules.yaml:9` — `strict` вже є, додати `verifier` секцію в `harvester/config.py:92` `VerifyConfig` або новий `VerifierConfig`:
  ```yaml
  verifier:
    enabled: true
    batch_size: 20
    interval_s: 60          # пауза між батчами
    recheck_days: 7         # перепроверять пройдені через 7 днів
    llm_enabled: true
    llm_model: "gemini-3.1-flash-lite"  # фіксовано ТЗ, не gemma
    llm_max_chars: 15000    # як gemma_max_chars, але для 3.1 можна 80000
  ```
- Ключі: `harvester/config.py:221` `classify_keys` — не дублювати, інжектити в `VerifierWorker`.
- Rate limits: `harvester/classify/ratelimit.py:1` `ModelRateLimiter(gemini_rpm=15, gemini_rpd=500, gemma_rpm=50 ...)` — для `3.1` використовувати `gemini_rpm/rpd` (2/15 RPM, 36.5K/250K TPM — візьме з `harvester/config.py:152`). При `DailyLimitExhausted` — перехід на наступний ключ.

### 3.3 БД — відмітки

**Варіант A (рекомендований) — нова таблиця `verifier_results` (не чіпати `documents.status` `verified`):**
```sql
-- harvester/db/migrations/005_verifier.sql
CREATE TABLE IF NOT EXISTS verifier_results (
  id SERIAL PRIMARY KEY,
  document_id INTEGER NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
  profile TEXT NOT NULL DEFAULT 'strict', -- 'strict' / 'softier'
  status TEXT NOT NULL, -- 'pass' / 'fail' / 'error'
  comment TEXT,         -- короткий коментар: "відсутній список джерел", "фрагмент 2 стор", "немає вступу/висновків"
  rules_failed JSONB,   -- ["page_count<3", "require_references"]
  llm_status TEXT,      -- 'pass'/'fail'/'skip'/'error'
  llm_comment TEXT,     -- відповідь LLM (1-2 речення)
  llm_model TEXT,       -- 'gemini-3.1-flash-lite'
  llm_key_idx INT,      -- 0..3
  checked_at TEXT NOT NULL,
  next_check_at TEXT,   -- для recheck
  UNIQUE(document_id, profile)
);
CREATE INDEX idx_verifier_next_check ON verifier_results(next_check_at) WHERE status='pass';
CREATE INDEX idx_verifier_status ON verifier_results(status);
-- Розширити documents для швидкого фільтру (опційно)
ALTER TABLE documents ADD COLUMN IF NOT EXISTS verifier_status TEXT; -- NULL/'pass'/'fail'
ALTER TABLE documents ADD COLUMN IF NOT EXISTS verifier_comment TEXT;
ALTER TABLE documents ADD COLUMN IF NOT EXISTS verifier_checked_at TEXT;
```
Плюси: історія, розрізнення `pass` vs `fail` в одному місці, не ламає `status='verified'` (яке використовує `reverify`).

**Альтернатива B** — писати в `documents.extra` JSON `{"verifier":{"profile":"strict","status":"fail","comment":"...", "checked_at":"..."}}` — без міграції, але повільний пошук.

Обрано **A** + дублювання `verifier_status/comment/checked_at` в `documents` для швидких фільтрів (`harvester/db/repositories.py:179` `count_by_status` аналог).

### 3.4 Логіка перевірки (використовує існуючі правила)

Послідовність для кожного `doc` (в порядку дешево → дорого, щоб не палити LLM даремно):

1. **Fetch** `SELECT * FROM documents WHERE status='verified' AND (verifier_checked_at IS NULL OR verifier_checked_at < now - recheck_days) ORDER BY verifier_checked_at NULLS FIRST, verified_at ASC LIMIT batch_size` (використовує `idx_documents_reverify` як зразок).
2. **Швидкі правила (без мережі/LLM):** `is_document_complete(doc, get_filter_rules("strict"))` `harvester/curator/preparer.py:38` — перевіряє `title`, `authors`, `page_count >=3`, `has_text_layer`, garbage `Microsoft Word`, `extra.producer` PPT, `chars_per_page`. Якщо `fail` — одразу `verifier_results(status='fail', comment=reason, rules_failed=[reason])`, `documents.verifier_status='fail'`, `documents.verifier_comment=reason`, `verifier_checked_at=now`, `next_check_at=now+recheck_days`.
3. **RU/СРСР фільтр (без LLM):** `apply_all_filters(url, lang_result, year, publisher, text_sample)` `harvester/verify/filters.py:99` + `is_domain_blocked` — якщо `fail`, помітити як `fail` з коментарем `russian_language`/`domain_blacklisted`/`soviet_publisher`.
4. **PDF-якість (потребує завантаження, але кешується):** `analyze_pdf_quality(pdf_path)` `harvester/extract/pdf_quality.py:1` — перевіряє `has_toc`, `has_references`, `has_conclusion`, `has_udc`, `text_density`. Якщо `max_toc_ratio>0.15` або `text_density very_sparse` — `fail`.
5. **LLM-верифікація (дорого):** тільки якщо пройшли 2-4. Виклик `llm_verifier.py:verify_document(doc, text_sample)`. Промпт (укр, `temperature 0.1` `harvester/config.py:148`):
   ```
   Ти — верифікатор наукових джерел. ГОЛОВНА ЦІЛЬ: якісні джерела з повним текстом (титул, вступ/мета, розділи, висновки, список джерел). НЕ тези/зміст/анотації.
   Документ: заголовок={title}, автори={authors}, мова={language}, УДК={udc}, сторінок={page_count}, фрагмент={text_sample[:3000]}
   Завдання: поверни JSON {"verdict":"pass"/"fail", "comment":"коротко чому (1-2 речення, укр)", "confidence":0.0-1.0}
   Файли 1-2 стор без структури → fail ("фрагмент, відсутня структура").
   ```
   Виклик: `LLMClient(keys=classify_keys, models=["gemini-3.1-flash-lite"])` — **одна модель**, ротація ключів в `_run_phase` `harvester/classify/llm.py:250` (вже вміє `DailyLimitExhausted` + `transient 500/503` retry `harvester/classify/llm.py:341`). При `DailyLimitExhausted` — `exhausted.add((key_idx, model_idx))`, `self._advance_phase()` → наступний ключ `KEY_2 + 3.1`, і т.д. Якщо всі 4 `exhausted` → `AllLimitsExhausted`.
6. **Запис результату:** `verifier_results` + `documents` (`verifier_status`, `comment`, `checked_at`, `next_check_at`).
7. **Пауза** `interval_s` (60с) між батчами, щоб не DDOS-ити БД/LLM.

### 3.5 Ротація ключів (ТЗ)

- Ініціалізація `LLMClient(classify_keys, ["gemini-3.1-flash-lite"])` — `harvester/classify/llm.py:102` `self._keys = classify_keys (4)`, `self._gemma_models = ["gemini-3.1-flash-lite"]` (перевикористати поле), `self._phase="gemma"`? Краще додати `self._verifier_models = ["gemini-3.1-flash-lite"]` щоб не плутати.
- `_run_phase` вже підтримує ротацію `key_idx 0→1→2→3` `harvester/classify/llm.py:374` `_advance_phase()`. При `DailyLimitExhausted` (кидає `harvester/classify/ratelimit.py:1` `ModelRateLimiter.acquire()`) — позначає `exhausted`, переходить на наступний ключ **з тією ж моделлю**.
- Якщо `len(exhausted)==4` → `AllLimitsExhausted` в `complete()` `harvester/classify/llm.py:241`. Воркер ловить `AllLimitsExhausted` `harvester/core/workers.py:399` — зараз там `self._running=False` після `transient` перевірки. Для верифікатора — **не зупиняти воркер назавжди**, а **заснути до 00:00 UTC наступної доби**: `sleep_seconds = (tomorrow_midnight_utc - now).total_seconds()`, `logger.critical("verifier_all_keys_exhausted_sleep", sleep_s=...)`, `await asyncio.sleep(sleep_seconds)`, очистити `exhausted` сети, продовжити.

### 3.6 Відмітки та розрізнення

- `verifier_results.status = 'pass'` → документ якісний, `comment = "повна структура: титул, 12 стор, є вступ, 3 розділи, висновки, 15 джерел"` (генерується з `rules_failed` + `llm_comment`).
- `verifier_results.status = 'fail'` → `comment = "фрагмент 2 стор, відсутній список джерел, немає вступу (rules: page_count<3, require_references)"` або `llm_comment`.
- `documents.verifier_status` дублює для швидких запитів `harvester/db/repositories.py:179` `count_by_status` аналог `count_by_verifier_status()`.
- `documents.status` лишається `verified` — не чіпати, щоб не ламати `reverify` `harvester/config.py:92`.

### 3.7 Логування (критичні місця)

- `verifier_worker_started` / `verifier_worker_stopped` (як `classify_worker_started` `harvester/core/workers.py:357`)
- `verifier_batch_start` (batch_size, interval)
- `verifier_document_check_start` (doc_id, title)
- `verifier_rules_failed` (doc_id, rules_failed)
- `verifier_llm_call` (doc_id, model, key_idx, prompt_chars)
- `verifier_llm_ok` / `verifier_llm_error` / `verifier_llm_quota_exhausted` (як `harvester/classify/llm.py:312` `gemini_quota_exceeded`)
- `verifier_all_keys_exhausted_sleep`
- `verifier_result_saved` (doc_id, status, comment, next_check_at)

Всі через `structlog` `harvester/core/events.py:30` `setup_logging` (JSON у `logs/harvester.log`).

## 4. Реструктуризація (оптимізація спільного коду)

- **Правила:** вже централізовано `harvester/config/rules.yaml` + `harvester/config.py:291` `FilterRules` + `harvester/curator/preparer.py:38` + `harvester/extract/pdf_quality.py`. Новий сервіс **не створює** нові правила, а імпортує `get_filter_rules("strict")` `harvester/config.py:324`. Винести `is_document_complete` в `harvester/verify/rules.py` (зараз дублюється в `preparer.py` та `pdf_quality.py`) — щоб і `curator` і `verifier` імпортували одне.
- **Пошук в інтернеті:** `harvester/discovery/ddgs_search.py:18` `DDGSSearchChannel`, `harvester/net/client.py:14` `HttpClient` з `GlobalRateLimiter`/`HostRateLimiter` `harvester/core/ratelimit.py:1`/`harvester/net/client.py:21` — вже спільні, `verifier` використовує `HttpClient` для завантаження PDF (не дублювати `httpx.AsyncClient` як в `harvester/bibliography/searcher.py:141`).
- **LLM:** `harvester/classify/llm.py:91` `LLMClient` + `harvester/classify/ratelimit.py:1` `ModelRateLimiter` — вже спільні, `verifier` створює окремий інстанс `LLMClient(keys=classify_keys, models=["gemini-3.1-flash-lite"])`.

## 5. Інтеграція

- `harvester/core/supervisor.py:115` `Supervisor._start_workers()` — додати `verifier` воркер(и) (1-2, як `classify:1` `harvester/config.py:24`): `VerifierWorker` з `harvester/verifier/worker.py`.
- `harvester/cli.py:17` — `harvester verifier status` (опційно) для перевірки `verifier_results`.
- `harvester/db/migrations/005_verifier.sql` — застосувати при `harvester init-db` `harvester/db/migrations.py:1`.
- `systemd` `systemd/harvester.service` — без змін, `harvester start` підхопить новий воркер.

## 6. Тестування

- Моки: `pytest` — додати `tests/test_verifier_rules.py` (unit, `is_document_complete` з `strict` vs `softier`), `tests/test_verifier_llm.py` (mock `httpx.AsyncClient` для `generateContent`).
- Інтеграційний: `tmp_path` SQLite з `verifier_results`, `harvester bibliography scan` як приклад повного тексту.
- Ручний: `harvester verifier run --limit 5 --profile strict` (dry-run) → `logs/harvester.log` `verifier_*`.

## 7. Ризики

- **Витрата лімітів `classify`**: `verifier` і `classify` ділять **ті самі 4 ключі** `GEMINI_DOC_VERIFIER_KEY`. При `RPD 500` на ключ (2000 сумарно) — `verifier` (LLM на кожен документ) може з'їсти ліміт `classify`. Пом'якшення: `verifier` працює лише якщо `classify` черга порожня (`TasksRepository.count_pending_by_type("classify")==0`), або окремий `rate_limiter` з нижчим пріоритетом, або нічний режим (00:00-06:00).
- **Навантаження на БД**: `SELECT ... WHERE status='verified'` без `LIMIT` — повільний. Використати `idx_documents_reverify` + `verifier_checked_at`.
- **Зациклення на помилках**: документ з `fail` постійно перевіряється. Використати `next_check_at = now + interval_days_stable` (90 днів для `pass`, 30 для `fail`).

## 8. Послідовність впровадження (build)

1. Створити `harvester/verifier/` (worker, llm_verifier, rules) + `harvester/db/migrations/005_verifier.sql`
2. Розширити `harvester/config.py:92` `VerifierConfig` + `harvester/config/rules.yaml` (вже є)
3. Винести `is_document_complete` в `harvester/verify/rules.py` + рефактор `preparer.py`/`pdf_quality.py`
4. Додати воркер у `harvester/core/supervisor.py:115`
5. Тести + `ruff check` + `harvester doctor`
6. Документація `docs/COMMANDS.md` / `AGENTS.md`
