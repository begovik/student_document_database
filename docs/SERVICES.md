# Сервіси Harvester — короткий огляд

> Головна ціль — збирати **якісні джерела з повним текстом**, придатні для наукових праць (титул, вступ/мета, розділи, висновки, список джерел). Не тези/зміст/анотації.

| Сервіс | Що робить (2 слова) | Ключі / Моделі | Детально |
|---|---|---|---|
| `core/supervisor` + `core/scheduler` | Оркестрація 24/7, черга `tasks`, heartbeat | — | `docs/COMMANDS.md#1-сервіс-збору-та-моніторингу-harvester` / `harvester/core/supervisor.py:1` |
| `discovery` (ddgs, openalex, crossref, unpaywall, semanticscholar, arxiv, doaj, oai) | Пошук кандидатів, сідування `search_queries` | — (без LLM; LLM-доповнення: `GEMINI_API_KEY 1-3` → `harvester/discovery/querygen_llm.py:1` `Gemini 3.1/3.5 → Gemma`) | `docs/COMMANDS.md#1` / `harvester/discovery/querygen.py:56` |
| `verify` (`VerifyWorker`, `VerifyPipeline`) | Верифікація PDF (завантаження, `%PDF`, `has_text_layer`, мова, дедуп) | — | `docs/COMMANDS.md#1` / `harvester/verify/pipeline.py:1` |
| `classify` | Класифікація тем (УДК + ключові слова + LLM) | `GEMINI_DOC_VERIFIER_KEY_1..4` → `harvester/config.py:221` `classify_keys`, моделі `gemma-4-31b-it` / `gemma-4-26b-a4b-it` `harvester/config.py:141` `ModelRateLimiter` `harvester/classify/ratelimit.py:1` | `docs/COMMANDS.md#1` / `harvester/classify/llm.py:91` |
| `extract` | Витяг цитат/сумаризацій з PDF | `GEMINI_API_KEY 1..3` `harvester/config.py:217` `gemini_keys`, `gemini-3.1-flash-lite` / `gemini-3.5-flash-lite` `harvester/config.py:140` → fallback `gemma` | `docs/COMMANDS.md#4-сервіс-витягу-цитат-і-сумаризацій-harvester-extract` / `harvester/extract/engine.py:1` |
| `curator` | Підготовка/верифікація каталогів | `GEMINI_API_KEY 1..3` (LLM-відбір `harvester/curator/selector.py:43` `call_llm_for_selection`) | `docs/COMMANDS.md#3-сервіс-кураторства-каталогів-harvester-curator` / `harvester/curator/preparer.py:1` |
| `bibliography` | Добирання з літератури каталогу | `GEMINI_API_KEY 1..3` (LLM-витяг `harvester/bibliography/service.py:140` `LLM_BIBLIO_PROMPT` → `gemini`/`gemma`), пошук `harvester/discovery/ddgs_search.py:18` + `harvester/net/guards.py:99` | `docs/COMMANDS.md#6-витяг-літератури-та-добирання-джерел-harvester-bibliography` / `harvester/bibliography/service.py:56` |
| `verifier` | 24/7 перевірка `verified` за `strict` | `GEMINI_DOC_VERIFIER_KEY_1..4` → `gemini-3.1-flash-lite` (одна модель, ротація ключів 1→2→3→4, сон до 00:00 UTC при вичерпанні) `harvester/verifier/worker.py:1` + `harvester/verifier/llm_verifier.py:1` | `docs/COMMANDS.md#5-цілодобова-перевірка-джерел-harvester-verifier` / `harvester/verifier/worker.py:1` |
| `net` | HTTP-клієнт, anti-SSRF, blacklist, rate-limit | — (`harvester/core/ratelimit.py:1` `GlobalRateLimiter`/`HostRateLimiter`) | `harvester/net/client.py:14` / `harvester/net/guards.py:89` |
| `db` | Failover `PostgreSQL ↔ SQLite`, міграції, репозиторії | `HARVESTER_PG_PASSWORD` | `docs/COMMANDS.md#2-управління-базою-даних-harvester-db-` / `harvester/db/failover.py:1` |

**Спільний код (реструктуризація):**
- Правила — `harvester/config/rules.yaml:9` `strict/softier` + `harvester/config.py:291` `FilterRules` + `harvester/curator/preparer.py:38` `is_document_complete` + `harvester/extract/pdf_quality.py:1` + `harvester/verifier/rules.py:1` (тонка обгортка, без дублювання)
- Пошук/завантаження — `harvester/net/client.py:14` `HttpClient` (`GlobalRateLimiter` `harvester/core/ratelimit.py:1`) + `harvester/discovery/ddgs_search.py:18`
- LLM — `harvester/classify/llm.py:91` `LLMClient` + `harvester/classify/ratelimit.py:1` `ModelRateLimiter` (перевикористовується всіма LLM-сервісами)
