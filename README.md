# Harvester — безперервний збирач наукових PDF-джерел

> **Головна ціль:** збирати **якісні джерела з повним текстом** — інформативні наукові/технічні статті, монографії, посібники, придатні для використання у наукових працях. Не тези, не зміст, не анотації, не фрагменти — лише цілісні, структуровані, логічно завершені документи.

Система для автоматичного пошуку, верифікації та класифікації наукових PDF-документів у відкритому інтернеті.

## Можливості

- **Мультиканальний discovery**: метапошук (DuckDuckGo/Bing/Brave), академічні API (OpenAlex, Crossref, Unpaywall), OAI-PMH репозиторії
- **Повна верифікація PDF**: завантаження, перевірка цілісності, витяг метаданих, визначення мови
- **Фільтрація**: автоматичне відсіювання російських ресурсів, російськомовних та радянських джерел
- **Дедуплікація**: за URL, SHA-256, DOI, fuzzy matching заголовків
- **Класифікація**: за УДК, OpenAlex topics, ключовими словами
- **Режим 24/7**: працює безперервно під systemd, автоматичне відновлення після збоїв

## Вимоги

- Python 3.12+
- SQLite 3.45+
- ~1 ГБ RAM
- ~5 ГБ вільного диску (для БД)

## Встановлення

```bash
# Клонування репозиторію
cd /opt/harvester

# Створення віртуального середовища
python3.12 -m venv .venv
source .venv/bin/activate

# Встановлення залежностей
pip install -e .

# Копіювання конфігурації
cp config.example.yaml config.yaml

# Редагування конфігурації (обов'язково: contact.email)
nano config.yaml

# Ініціалізація бази даних
harvester init-db

# Самодіагностика
harvester doctor
```

## Використання

### Запуск сервісу

```bash
# У foreground (для тестування)
harvester start

# Або через systemd (production)
sudo systemctl enable --now harvester
sudo journalctl -u harvester -f
```

### CLI команди

```bash
# Стан сервісу
harvester status

# Статистика по каналах
harvester stats --period 24h
harvester stats --period 7d --json

# Експорт верифікованих документів
harvester export --output docs.csv --format csv --lang uk
harvester export --output docs.jsonl --format jsonl

# Оптимізація БД
harvester vacuum
```

## Конфігурація

Основні параметри в `config.yaml`:

```yaml
contact:
  email: "your@email.com"  # ОБОВ'ЯЗКОВО для API

channels:
  ddgs:
    enabled: true
    backends: [duckduckgo, bing, brave]
  openalex:
    enabled: true
    rps: 5

filters:
  blocked_tlds: [".ru", ".su", ".рф"]
  soviet_cutoff_year: 1991

http:
  max_pdf_bytes: 209715200  # 200 МБ
  per_host_delay_ms: 2000
```

## Архітектура

```
Discovery (ddgs, OpenAlex, ...) → Черга завдань → Verify Pipeline → SQLite
```

Детальна технічна документація: [`docs/TECHNICAL_DESIGN.md`](docs/TECHNICAL_DESIGN.md)

## Структура проєкту

```
harvester/
├── core/           # supervisor, scheduler, ratelimit, circuit breaker
├── db/             # SQLite schema, connection, repositories
├── net/            # HTTP client, guards (anti-SSRF, domain filter)
├── discovery/      # канали пошуку (ddgs, openalex, ...)
├── verify/         # верифікація PDF, мова, фільтри
├── dedup/          # нормалізація URL, дедуплікація
├── classify/       # класифікація за темами
└── cli.py          # інтерфейс командного рядка
```

## Етика та право

- Збираються **лише** легально доступні open access документи
- Дотримуємось robots.txt та умов використання API
- PDF-файли не зберігаються — лише метадані та посилання
- Російські ресурси, російськомовні та радянські джерела автоматично відфільтровуються

## Ліцензія

Цей проєкт створений для некомерційного використання в освітніх цілях.

## Розробка

```bash
# Встановлення dev-залежностей
pip install -e ".[dev]"

# Запуск тестів
pytest

# Linting
ruff check harvester/
```

## Roadmap

- **v0.1** (поточна): базовий discovery через ddgs + OpenAlex, верифікація, CLI
- **v1.0**: OAI-PMH, sitemap scanner, повна класифікація
- **v2.0**: веб-дашборд, API для інтеграції, ML-класифікація
