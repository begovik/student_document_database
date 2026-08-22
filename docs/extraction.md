# Витяг цитат і сумаризацій з PDF-документів (extract)

## 1. Призначення

Модуль `harvester/extract/` — окремий сервіс, який для обраних документів
 завантажує PDF, витягує текст, викликає LLM для пошуку цитат та створення
 сумаризації, і зберігає результати в таблицю `extractions`.

**Архітектура розділена на два рівні:**
1. **Harvester** — збирає документи, верифікує PDF, зберігає метадані
2. **Extract** — бере готові документи, аналізує їхній вміст (цитати + сумаризація)

---

## 2. Таблиця `extractions`

### SQLite (миграція `003_extractions.sql`)

```sql
CREATE TABLE IF NOT EXISTS extractions (
    id              INTEGER PRIMARY KEY,
    document_id     INTEGER NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
    quotations      TEXT,          -- JSON-масив цитат
    summary         TEXT,          -- JSON-об'єкт сумаризації
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL,
    UNIQUE(document_id)
);
CREATE INDEX IF NOT EXISTS idx_extractions_doc ON extractions(document_id);
```

### PostgreSQL (миграція `003_extractions.sql`)

```sql
CREATE TABLE IF NOT EXISTS extractions (
    id              SERIAL PRIMARY KEY,
    document_id     INTEGER NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
    quotations      TEXT,
    summary         TEXT,
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL,
    UNIQUE(document_id)
);
CREATE INDEX IF NOT EXISTS idx_extractions_doc ON extractions(document_id);
```

---

## 3. Структура JSON-даних

### Цитати (`quotations`)

Масив об'єктів:

```json
[
  {
    "page": 5,
    "text": "Ціна цитати: важлива думка, висновок, визначення...",
    "type": "conclusion"
  }
]
```

**Типи цитат (`type`):**

| Тип | Опис |
|---|---|
| `conclusion` | Висновок дослідження, завершальна думка |
| `definition` | Визначення поняття, терміна |
| `fact` | Факт, статистика, цифра, дані дослідження |
| `method` | Опис методу, підходу, процедури |
| `insight` | Важлива думка, ідея, яка не вписується в інші категорії |

### Сумаризація (`summary`)

Об'єкт:

```json
{
  "page": 1,
  "overview": "Короткий опис що в статті (1-2 речення)",
  "key_ideas": ["Ідея 1", "Ідея 2", "Ідея 3"],
  "methodology": "Як проводилось дослідження",
  "findings": "Основні результати",
  "conclusions": "Висновки статті",
  "authors_mentioned": ["Імя Автор1", "Імя Автор2"]
}
```

---

## 4. Архітектура модуля

### Структура файлів

```
harvester/extract/
├── __init__.py      # опис модуля
├── engine.py        # ядро: download, parse, LLM call
└── cli.py           # CLI-команда 'harvester extract'
```

### Потік обробки

```
1. CLI отримує запит (topic, limit, batch)
     ↓
2. get_documents_to_process() — SQL-запит до БД
   • Бере verified документи з canonical_url
   • Фільтрує по темі/коду (JOIN document_topics)
   • Пропускає тих, хто вже має extractions
     ↓
3. Для кожного документа створюється ExtractionJob
     ↓
4. process_document(job):
   a. download_pdf(url) — httpx, перевірка %PDF
   b. parse_pdf(file) — PyMuPDF, витяг ВСЬОГО тексту (до 100 сторінок)
   c. call_llm_for_extraction(text, title) — Gemini (3 ключі) → OpenRouter
     ↓
5. save_results() — INSERT/UPDATE в таблицю extractions
```

### engine.py — основні функції

| Функція | Призначення |
|---|---|
| `download_pdf(url)` | Завантажує PDF за URL, повертає шлях до temp-файлу |
| `parse_pdf(file, max_pages)` | Парсить PDF через PyMuPDF, витягує текст |
| `process_document(job)` | Основна логіка: завантажити → парсити → LLM → результат |
| `call_llm_for_extraction(text, title)` | Виклик LLM (Gemini/OpenRouter) |
| `call_gemini(api_key, config, messages)` | Виклик Google Gemini API |
| `call_openrouter(api_key, config, messages)` | Виклик OpenRouter API |

---

## 5. CLI-команда

### Запуск

```bash
harvester extract run [опції]
```

### Опції

| Опція | Скорочення | За замовч. | Опис |
|---|---|---|---|
| `--topic` | `-t` | — | Фільтр по назві теми (часткова підстрока) |
| `--topic-code` | `-c` | — | Фільтр по коду теми (trade, 076...) |
| `--limit` | `-n` | 30 | Максимальна кількість документів |
| `--batch` | `-b` | 5 | Паралельність завдань |
| `--dry-run` | `-d` | false | Не зберігати результати |
| `--retry-failed` | `-r` | false | Перепроцесувати тільки з помилками |
| `--skip-extracted` | — | true | Пропускати вже оброблені |

### Приклади

```bash
# Витяг для теми 076, максимум 10 документів
harvester extract run --topic-code 076 --limit 10

# Витяг для документів з теми "Підприємництво", 3 паралельних
harvester extract run --topic "Підприємництво" --batch 3

# Перепроцесувати тільки документи з помилками
harvester extract run --retry-failed

# Показати що було б зроблено (без збереження)
harvester extract run --dry-run --limit 5
```

---

## 6. LLM-промпт

Для кожного документа LLM отримує:

1. **Системний промпт** (`LLM_SYSTEM_PROMPT`):
   - Завдання: знайти цитати та створити сумаризацію
   - Формат відповіді: JSON з полями `quotations` і `summary`
   - Критерії якості для цитат (конкретні, змістовні, цитовані)
   - Структура сумаризації (overview, key_ideas, methodology, findings, conclusions)

2. **Користувацький промпт**:
   ```
   НАЗВА СТАТТІ: {title}

   ТЕКСТ СТАТТІ:
   {text — до 80 000 символів}
   ```

### Порядок виклику LLM

1. **Gemini** (перший ключ) → помилка →
2. **Gemini** (другий ключ) → помилка →
3. **Gemini** (третій ключ) → помилка →
4. **OpenRouter** (google/gemini-2.5-flash)

---

## 7. Конфігурація LLM

Використовується та ж секція `llm` з `config.yaml`, що й для класифікації:

```yaml
llm:
  enabled: true
  gemini_models: ["gemini-3.1-flash-lite", "gemini-3.5-flash-lite"]
  gemini_base_url: "https://generativelanguage.googleapis.com/v1beta"
  openrouter_model: "google/gemini-2.5-flash"
  openrouter_base_url: "https://openrouter.ai/api/v1"
  timeout_s: 60.0
  max_tokens: 2048
  temperature: 0.1
```

Ключі API беруться з `.env`:
- `GEMINI_API_KEY`, `GEMINI_API_KEY_2`, `GEMINI_API_KEY_3`
- `OPEN_ROUTER_API_KEY`

---

## 8. Синхронізація з БД

### Проблема
Таблиця `extractions` існує в **обох** базах (SQLite і PostgreSQL).
Потрібна синхронізація, щоб дані не втрачалися.

### Рішення
- Міграції застосовуються автоматично при старті:
  - SQLite: `harvester/db/migrations/003_extractions.sql`
  - PostgreSQL: `harvester/db/pg_migrations/003_extractions.sql`
- Записи зберігаються в **remote** (PostgreSQL) як source of truth
- Локальне дзеркало (SQLite) оновлюється через failover-механізм

### Статус
Таблиця `extractions` **не є частиною** основного document-flow.
Вона заповнюється **окремим** сервісом (`extract`), який працює вручну
або за розкладом.

---

## 9. Скрипт `scripts/extract_from_catalog.py` — запис у JSON-каталог

Скрипт проводить витяг для документів із JSON-каталогу та **записує результати
назад у той самий файл каталогу**:

- елементи, які вже є в таблиці `extractions`, беруться з БД (без повторних
  LLM-викликів);
- решта проходить повний цикл: завантаження PDF → LLM → збереження в БД;
- успішні елементи отримують ключі **`quotations`** (масив) та **`summary`**
  (об'єкт); застарілий ключ `error` видаляється;
- проблемні елементи отримують ключ **`error`** зі змістом помилки
  (наприклад, `"Не вдалося завантажити PDF: ConnectTimeout"`), або стандартний
  текст, якщо зміст відсутній.

Запис атомарний: спочатку у тимчасовий файл у тій самій директорії, потім
`os.replace()`. Ключі верхнього рівня каталогу (`query`, `udc_prefixes`,
`total_found` тощо) зберігаються без змін.

```bash
python scripts/extract_from_catalog.py catalogs/catalog_076.json
python scripts/extract_from_catalog.py catalogs/catalog_076.json --limit 5
python scripts/extract_from_catalog.py catalogs/catalog_076.json --dry-run
python scripts/extract_from_catalog.py catalogs/catalog_076.json --force  # ігнорувати БД
```

Приклад елементів після обробки:

```json
{
  "id": 157,
  "title": "РОЛЬ FINTECH У РОЗВИТКУ ФІНАНСОВОГО РИНКУ УКРАЇНИ",
  "...": "...",
  "quotations": [{"page": 281, "text": "FinTech – це ...", "type": "definition"}],
  "summary": {"page": 1, "overview": "...", "key_ideas": ["..."]}
}
```

```json
{
  "id": 65850,
  "title": "ОПТИМІЗАЦІЯ ОРГАНІЗАЦІЙНОЇ СТРУКТУРИ УПРАВЛІННЯ ПІДПРИЄМСТВОМ",
  "canonical_url": "https://fpnpu.cibs.ubs.edu.ua/article/download/249572/247095",
  "error": "Не вдалося завантажити PDF: ConnectTimeout"
}
```

---

## 10. Обмеження

1. **Ліміт тексту**: за замовчуванням 80 000 символів —
   `llm.max_text_chars_for_llm` (обрізається, якщо PDF довший)
2. **Ліміт сторінок**: за замовчуванням 100 сторінок PDF —
   `llm.max_pages_for_extraction`
3. **Типи файлів**: тільки PDF (перевірка magic bytes `%PDF`)
4. **Мова**: LLM аналізує українською/англійською
5. **Швидкість**: залежить від швидкості LLM-відповіді (1-5 сек на документ)

---

## 11. Приклад використання

### Повний цикл

```bash
# 1. Знайти документи з теми trade
harvester find --topic "Підприємництво, торгівля та біржова діяльність"

# 2. Витягнути цитати для 5 документів
harvester extract run --topic-code trade --limit 5

# 3. Перевірити результати
harvester extract run --dry-run --topic-code trade --limit 5
```

### Ручне додавання запису

```sql
INSERT INTO extractions (document_id, quotations, summary, created_at, updated_at)
VALUES (
    157,
    '[{"page":5,"text":"...","type":"conclusion"}]',
    '{"page":1,"overview":"...","key_ideas":["..."],"methodology":"...","findings":"...","conclusions":"...","authors_mentioned":["..."]}',
    datetime('now'),
    datetime('now')
);
```

---

*Документація до версії модуля extract v1.0 · 2026-08-22*
