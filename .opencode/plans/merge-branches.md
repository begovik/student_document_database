# Plan: Об'єднання гілок vps_production та jules_dev з конфігурованими правилами

## Мета
Об'єднати дві гілки в єдину кодову базу з двома профілями правил фільтрації документів:
- **standard** (vps_production) — базові фільтри
- **strict** (jules_dev) — жорсткі фільтри (мінімум 3 сторінки, повна структура)

## Аналіз різниці між гілками

### Що відрізняється:

| Файл | vps_production | jules_dev |
|------|----------------|-----------|
| `curator/preparer.py` | `page_count > 0` | `page_count >= 3` + `MIN_PAGE_COUNT = 3` |
| `curator/prompts.py` | Базовий промпт | Розширений промпт з критеріями цілісності |
| `curator/selector.py` | Без `page_count` в форматуванні | З `page_count` в форматуванні |
| `curator/verifier.py` | `page_count > 0` | `page_count >= 3` |
| `extract/pdf_quality.py` | Є | Немає |

### Що спільного:
- `curator/preparer.py` — основна логіка відбору кандидатів
- `curator/selector.py` — виклик LLM для відбору
- `curator/verifier.py` — верифікація та заміна документів

## План реалізації

### 1. Створити конфігурацію правил (`harvester/config/rules.yaml`)

```yaml
profiles:
  standard:
    description: "Базові фільтри для наукових документів"
    min_page_count: 1
    require_references: false
    require_structured_sections: false
    require_title_page: false
    llm_completeness_level: "basic"
    
  strict:
    description: "Жорсткі фільтри для повноцінних документів"
    min_page_count: 3
    require_references: true
    require_structured_sections: true
    require_title_page: true
    llm_completeness_level: "strict"

# Поточний профіль (перевизначається через CLI або ENV)
active_profile: "strict"
```

### 2. Оновити `harvester/config.py` — додати завантаження правил

- Додати клас `FilterRules` з полями:
  - `min_page_count: int`
  - `require_references: bool`
  - `require_structured_sections: bool`
  - `require_title_page: bool`
  - `llm_completeness_level: str`

- Додати функцію `get_filter_rules(profile: str) -> FilterRules`

### 3. Оновити `harvester/curator/preparer.py`

**Замінити жорстко закодовані значення:**
```python
# БУЛО:
MIN_PAGE_COUNT = 3

# СТАЛО:
rules = get_filter_rules(settings.curator_profile)
MIN_PAGE_COUNT = rules.min_page_count
```

**Оновити SQL-запити:**
```python
# БУЛО:
AND d.page_count >= 3

# СТАЛО:
AND d.page_count >= {rules.min_page_count}
```

**Оновити `is_document_complete()`:**
```python
# БУЛО:
if not value or int(value) < MIN_PAGE_COUNT:
    return False, f"page_count={value} (мінімум {MIN_PAGE_COUNT} сторінки)"

# СТАЛО:
if not value or int(value) < rules.min_page_count:
    return False, f"page_count={value} (мінімум {rules.min_page_count} сторінки)"
```

### 4. Оновити `harvester/curator/prompts.py`

**Зробити промпти динамічними:**
```python
def get_selection_prompt(rules: FilterRules) -> str:
    if rules.llm_completeness_level == "strict":
        return PROMPT_SELECT_DOCUMENTS_STRICT
    else:
        return PROMPT_SELECT_DOCUMENTS_STANDARD
```

**Додати два варіанти промптів:**
- `PROMPT_SELECT_DOCUMENTS_STANDARD` — базовий з vps_production
- `PROMPT_SELECT_DOCUMENTS_STRICT` — розширений з jules_dev

### 5. Оновити `harvester/curator/selector.py`

**Додати `page_count` до форматування кандидатів:**
```python
# Вже є в jules_dev, залишити
page_count=c.get("page_count", "?") or "?"
```

### 6. Оновити `harvester/curator/verifier.py`

**Замінити жорстко закодовані значення:**
```python
# БУЛО:
AND d.page_count >= 3

# СТАЛО:
AND d.page_count >= {rules.min_page_count}
```

### 7. Оновити CLI (`harvester/cli.py`)

**Додати опцію `--profile`:**
```bash
harvester curator prepare "Тема" --profile strict --limit 30
harvester curator prepare "Тема" --profile standard --limit 30
```

### 8. Об'єднати гілки

1. Злити `jules_dev` в `vps_production` (або навпаки)
2. Вирішити конфлікти злиття
3. Залишити обидва профілі в конфігурації

## Порядок дій

### Крок 1: Створити конфігурацію правил
- Створити `harvester/config/rules.yaml`
- Оновити `harvester/config.py` для завантаження правил

### Крок 2: Оновити код curator
- `curator/preparer.py` — використовувати правила з конфігурації
- `curator/prompts.py` — зробити промпти динамічними
- `curator/selector.py` — додати page_count до форматування
- `curator/verifier.py` — використовувати правила з конфігурації

### Крок 3: Оновити CLI
- Додати опцію `--profile` до `curator prepare`

### Крок 4: Об'єднати гілки
- Злити зміни з jules_dev в vps_production
- Вирішити конфлікти

### Крок 5: Протестувати
- Запустити `harvester curator prepare` з кожним профілем
- Порівняти результати

## Ризики та запобігання

1. **Конфлікти злиття:** Акуратно зливати, перевіряти кожен файл
2. **Зламати існуючу логіку:** Тестувати після кожного кроку
3. **Забути профіль:** Зробити `standard` профілем за замовчуванням
