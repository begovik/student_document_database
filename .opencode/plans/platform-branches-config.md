# Plan: master_backup + master_tmp, Python 3.12, config.yaml з VPS=true, catalogs/.gitignore

## Мета
Зберегти чистий `master` (як був до хостингу) у `master_backup`, створити `master_tmp` куди залити **все що наробили** (`vps_production`/`jules_dev` 0868960 + verifier/bibliography/llm-querygen), при цьому:
- Python лишається **3.12** (`pyproject.toml:requires-python >=3.12`, `ruff target-version py312`)
- `config.yaml` **не** виносити в `.gitignore` — об'єднати конфіги, керувати через `.env VPS=true`
- `catalogs/` додати в `.gitignore`

## Поточний стан
- `master` `c65dd10` — чистий до хостингу, `requires-python >=3.12`, `config.yaml mode: auto host:""`, без `AGENTS.md:1` головної цілі, без `harvester/verifier`, без `harvester/bibliography`, без `docs/SERVICES.md`
- `vps_production`/`jules_dev` `0868960`/`246fcc4` — 95 файлів diff, `requires-python >=3.11` (тимчасовий даунгрейд `f6abe6b`), `config.yaml mode: remote host:89.167.68.48` + банер головної цілі, `AGENTS.md`, `harvester/verifier`, `harvester/bibliography`, `harvester/discovery/querygen_llm.py`, `harvester/config/rules.yaml`
- `.env` вже має `VPS=true` (додав користувач)

## Кроки (без виконання зараз, лише план)

### 1. Створити дві гілки від `master`
```bash
git fetch origin
git checkout master
git branch master_backup master          # копія master до хостингу
git push origin master_backup            # бекап на remote

git checkout -b master_tmp master        # чиста основа для заливки
```

### 2. Python 3.12 — відкотити даунгрейд
- `pyproject.toml:4` залишити `requires-python = ">=3.12"` (не брати `>=3.11` з `vps_production`)
- `tool.ruff.target-version = "py312"` лишити як є
- `systemd/harvester.service:9` виправити `ExecStart=/opt/harvester/.venv/bin/harvester` (з крапкою, як в `master` і `docs`, не `venv` з `vps_production`)
- Перевірити `venv/bin/python --version` на VPS — має бути 3.12 (якщо 3.11 — оновити venv: `python3.12 -m venv .venv --upgrade`)

### 3. Об'єднаний `config.yaml` (не ігнорувати, керувати через `VPS=true`)
**Файл лишається в гіті**, але `harvester/config.py:118` `DatabaseConfig` доповнити логікою:
```python
# harvester/config.py — в load_config() або в Settings.validate
import os
if os.getenv("VPS", "").lower() == "true":
    data.setdefault("database", {})["mode"] = "remote"
    data["database"]["host"] = data["database"].get("host") or "89.167.68.48"
```
- Базовий `config.yaml` в репо: `mode: auto`, `host: ""` (як в `master`/`config.example.yaml:4`) + банер головної цілі зверху (з `vps_production`).
- На хостингу `.env` містить `VPS=true` + `HARVESTER_PG_PASSWORD` → код автоматично перемикає в `remote` без коміту хост-IP в `master`.
- Локально `.env` без `VPS` або `VPS=false` → лишається `auto` (failover, працює офлайн).

Альтернатива: лишити `config.yaml` з `mode: auto` і додати `config.production.yaml` (не в гіті) для хостингу, але ТЗ каже **об'єднати**, тому обрано `VPS` перемикач.

### 4. `catalogs/` в `.gitignore`
- Додати рядок `catalogs/` в `.gitignore:1` (зараз ігноруються лише `.venv/`, `venv/`, `data/`).
- Виконати `git rm -r --cached catalogs/` в `master_tmp` щоб видалити вже закомічені `catalogs/catalog_20260828_*/resources/*.pdf` (20 PDF ~ 2-10 МБ кожен, `git diff master..vps_production --stat` 41k рядків).
- `catalogs/` лишається локально, але не пушиться.

### 5. Залити все напрацьоване в `master_tmp`
```bash
git checkout master_tmp
git merge vps_production --no-edit   # принесе 0868960: AGENTS.md, harvester/verifier, harvester/bibliography, harvester/discovery/querygen_llm.py, harvester/config/rules.yaml, docs/SERVICES.md, harvester/extract/pdf_quality.py тощо
# Конфлікти:
# - pyproject.toml — взяти master (>=3.12)
# - systemd/harvester.service — взяти master (.venv)
# - config.yaml — взяти master (auto) + банер, VPS-логіка в config.py покриє remote
git checkout --ours pyproject.toml systemd/harvester.service
git add pyproject.toml systemd/harvester.service
# config.yaml — ручне злиття: master основа + банер головної цілі
git add config.yaml
git commit -m "master_tmp: заливка напрацьованого з vps_production + VPS=true перемикач, Python 3.12, catalogs/.gitignore"
```

### 6. Перевірка
```bash
venv/bin/ruff check harvester/
venv/bin/python -m harvester.cli --help | grep -E "curator|bibliography|verifier"
VPS=true venv/bin/python -c "from harvester.config import load_config; print(load_config().database.mode)" # має бути remote
VPS=false venv/bin/python -c "from harvester.config import load_config; print(load_config().database.mode)" # має бути auto
harvester doctor
```

### 7. Пуш (після підтвердження)
```bash
git push origin master_backup
git push origin master_tmp
# master лишається недоторканим (c65dd10) до окремого рішення
```

## Ризики
- `VPS=true` в `.env` — якщо забути на хостингу, `harvester` стартує в `auto` і впаде в SQLite (дані розійдуться). Міграція `004_verifier.sql` вже застосована на PG 89.167.68.48, локально — ні; `harvester init-db` створить `verifier_results` автоматично.
- `catalogs/` вже в історії `vps_production` — `git rm --cached` не видалить з історії, лише з індексу `master_tmp`; для повного очищення потрібен `git filter-branch` (не робити без бекапу `master_backup`).
