#!/usr/bin/env python3
"""Звіт: кількість джерел у БД по дисциплінах із docs/discipline_catalog.md.

Для кожної дисципліни рахуємо унікальні документи, у яких назва дисципліни
зустрічається в:
  - documents.title
  - documents.title_hint
  - document_refs.query_text (запит, яким документ знайдено)

Підрахунок ведеться по ВСІХ документах БД (не лише verified).
Результат записується у файл discipline_report.md у корені репозиторію,
включно з дисциплінами, де кількість = 0.
"""

import os
import re
import subprocess
from collections import OrderedDict
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
CATALOG = REPO_ROOT / "docs" / "discipline_catalog.md"
OUTPUT = REPO_ROOT / "discipline_report.md"
PG_HOST = "127.0.0.1"
PG_DB = "harvester"
PG_USER = "harvester"


def get_pg_password() -> str:
    """Отримати пароль з .env файлу."""
    env_path = REPO_ROOT / ".env"
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            if line.startswith("PG_PASS="):
                return line.split("=", 1)[1]
    return os.environ.get("PG_PASS", "")


def run_query(sql: str) -> str:
    """Виконати SQL запит до PostgreSQL, повернути stdout."""
    cmd = [
        "psql", "-h", PG_HOST, "-U", PG_USER, "-d", PG_DB,
        "-t", "-A", "-F", "|", "-c", sql,
    ]
    env = os.environ.copy()
    env["PGPASSWORD"] = get_pg_password()
    result = subprocess.run(cmd, capture_output=True, text=True, env=env, check=False)
    if result.returncode != 0:
        raise RuntimeError(f"SQL error: {result.stderr}")
    return result.stdout.strip()


def parse_catalog() -> "OrderedDict[str, list[str]]":
    """Розібрати discipline_catalog.md -> {категорія: [дисципліни]}.

    Формат рядка: "N. Назва дисципліни" (N — номер 1..263).
    Секції позначені "## Категорія".
    """
    sections: OrderedDict[str, list[str]] = OrderedDict()
    current: str | None = None
    # Скидання нумерації на кожній секції (або глобальної) — категоризуємо просто за рядком
    last_num = 0
    for raw in CATALOG.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if line.startswith("## "):
            current = line[3:].strip()
            sections.setdefault(current, [])
            last_num = 0
            continue
        if not line or line.startswith("#"):
            continue
        m = re.match(r"^(\d+)\.\s+(.+)$", line)
        if not m:
            continue
        num = int(m.group(1))
        name = m.group(2).strip()
        # Захист від пропуску номера (якщо раптом секція без нумерації з голови)
        if num < last_num:
            last_num = 0
        last_num = num
        if current is None:
            current = "Без категорії"
            sections.setdefault(current, [])
        sections[current].append(name)
    return sections


def escape_like(term: str) -> str:
    """Екранувати спецсимволи для ILIKE (%, _, \\)."""
    return term.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def count_all_disciplines(names: list[str]) -> dict[str, int]:
    """Порахувати унікальні документи для ВСІХ дисциплін.

    Завантажуємо один раз всі назви/підказки документів та запити пошуку,
    далі шукаємо підрядковий збіг у Python (case-insensitive).
    """
    print("Завантажуємо назви документів та пошукові запити з БД...")

    # (document_id -> {low(title), low(title_hint)})
    doc_titles: dict[int, tuple[str, str]] = {}
    out = run_query(
        "SELECT id, lower(title), lower(COALESCE(title_hint, '')) FROM documents;"
    )
    for line in out.splitlines():
        if not line.strip() or "|" not in line:
            continue
        did, _, rest = line.partition("|")
        t1, _, t2 = rest.partition("|") if "|" in rest else (rest, "", "")
        try:
            doc_titles[int(did.strip())] = (t1.strip(), t2.strip())
        except ValueError:
            continue

    # document_id -> set of query texts
    ref_queries: dict[int, set[str]] = {}
    out = run_query(
        "SELECT document_id, lower(query_text) FROM document_refs "
        "WHERE query_text IS NOT NULL AND query_text != '';"
    )
    for line in out.splitlines():
        if not line.strip() or "|" not in line:
            continue
        did, _, qt = line.partition("|")
        try:
            ref_queries.setdefault(int(did.strip()), set()).add(qt.strip())
        except ValueError:
            continue

    # Нормалізуємо дисципліни (lowercase) для пошуку
    lowered = [(n, n.lower()) for n in names]

    counts: dict[str, int] = {n: 0 for n in names}
    print(f"Матчимо {len(names)} дисциплін проти {len(doc_titles)} документів "
          f"та {len(ref_queries)} записів пошуку...")

    for name, name_low in lowered:
        matched: set[int] = set()
        # 1) Збіг у title / title_hint
        for did, (t1, t2) in doc_titles.items():
            if name_low in t1 or (t2 and name_low in t2):
                matched.add(did)
        # 2) Збіг у search query
        for did, queries in ref_queries.items():
            if any(name_low in q for q in queries):
                matched.add(did)
        counts[name] = len(matched)

    return counts


def build_report() -> tuple[str, int, int]:
    """Побудувати текст звіту. Повертає (text, total_disciplines, total_docs)."""
    sections = parse_catalog()
    names = [n for items in sections.values() for n in items]
    total_disc = len(names)
    print(f"Рахуємо джерела для {total_disc} дисциплін...")
    counts = count_all_disciplines(names)

    total_docs_all = 0
    total_with_docs = 0
    lines: list[str] = []

    lines.append("# 📊 Звіт: джерела по дисциплінах")
    lines.append("")
    lines.append(
        f"**Джерело переліку:** `docs/discipline_catalog.md` "
        f"({total_disc} дисциплін)."
    )
    lines.append(
        "**Підрахунок:** унікальні документи в БД (всі статуси), де назва "
        "дисципліни зустрічається в назві, підказці або пошуковому запиті, "
        "яким знайдено документ."
    )
    lines.append("")
    lines.append("---")
    lines.append("")

    for category, items in sections.items():
        lines.append(f"## {category}")
        lines.append("")
        lines.append("| № | Дисципліна | Джерел |")
        lines.append("|---|------------|-------:|")
        for idx, name in enumerate(items, 1):
            count = counts.get(name, 0)
            total_docs_all += count
            if count > 0:
                total_with_docs += 1
            lines.append(f"| {idx} | {name} | {count} |")
        lines.append("")

    lines.append("---")
    lines.append("")
    lines.append("## Зведення")
    lines.append("")
    lines.append(f"- Всього дисциплін: **{total_disc}**")
    lines.append(f"- Дисциплін з документами: **{total_with_docs}**")
    lines.append(f"- Дисциплін без документів: **{total_disc - total_with_docs}**")
    lines.append(f"- Умовна сума джерел по дисциплінах: **{total_docs_all}**")
    lines.append("")
    lines.append("> Документ може відповідати кільком дисциплінам, тому сума "
                 "по дисциплінах перевищує реальну кількість унікальних документів у БД.")

    return "\n".join(lines), total_disc, total_docs_all


def main() -> None:
    """Точка входу."""
    sections = parse_catalog()
    total_disc = sum(len(items) for items in sections.values())
    print(f"Дисциплін знайдено: {total_disc} (категорій: {len(sections)})")

    text, total_disc_out, total_docs = build_report()
    OUTPUT.write_text(text, encoding="utf-8")
    print(f"Звіт збережено: {OUTPUT}")
    print(f"Дисциплін: {total_disc_out}, сума джерел: {total_docs}")


if __name__ == "__main__":
    main()
