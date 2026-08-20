"""Перекладач діалекту SQLite -> PostgreSQL.

Єдиний шар, що перетворює репозиторійний SQL (написаний під SQLite)
у коректний PostgreSQL. Правила:

- `?` -> `$1, $2, ...` (по порядку появи, поза рядковими літералами);
- `INSERT OR IGNORE INTO` -> `INSERT INTO ... ON CONFLICT DO NOTHING`;
- `INSERT OR REPLACE INTO` -> `INSERT INTO ... ON CONFLICT DO NOTHING`;
- для таблиць з колонкою `id` до INSERT додається `RETURNING id`,
  щоб зберегти семантику `cursor.lastrowid` / `cursor.rowcount`;
- `UPDATE` / `DELETE` / `SELECT` залишаються без змін (крім плейсхолдерів).
"""

from __future__ import annotations

import re

# Таблиці, що мають серійну колонку `id` (тобто можуть повертати lastrowid).
ID_TABLES = {
    "documents",
    "tasks",
    "search_queries",
    "domains",
    "sources",
    "document_refs",
    "fetch_attempts",
    "document_mirrors",
    "topics",
    "blacklist",
    "channel_stats",
    "system_events",
}


def _statement_keyword(sql: str) -> str:
    """Перше ключове слово SQL-виразу (нормалізоване)."""
    return sql.lstrip().split(None, 1)[0].upper() if sql.strip() else ""


def replace_placeholders(sql: str) -> str:
    """`?` -> `$N`, ігноруючи `?` усередині рядкових літералів (`'...'`)
    та однострокових коментарів (`-- ...`)."""
    out: list[str] = []
    n = 0
    in_str = False
    in_comment = False
    i = 0
    L = len(sql)
    while i < L:
        ch = sql[i]
        nxt = sql[i + 1] if i + 1 < L else ""
        if in_comment:
            out.append(ch)
            if ch == "\n":
                in_comment = False
            i += 1
            continue
        if in_str:
            out.append(ch)
            if ch == "\\" and i + 1 < L:
                out.append(nxt)
                i += 2
                continue
            if ch == "'":
                in_str = False
            i += 1
            continue
        if ch == "-" and nxt == "-":
            in_comment = True
            out.append(ch)
            i += 1
            continue
        if ch == "'":
            in_str = True
            out.append(ch)
            i += 1
            continue
        if ch == "?":
            n += 1
            out.append(f"${n}")
            i += 1
            continue
        out.append(ch)
        i += 1
    return "".join(out)


def _table_name_of(sql: str) -> str:
    """Назва таблиці в INSERT: `INSERT [OR IGNORE|REPLACE] INTO <tbl> (...)`."""
    rest = sql.lstrip()
    m = re.match(
        r"INSERT(?:\s+OR\s+(?:IGNORE|REPLACE))?\s+INTO\s+(\w+)", rest, re.IGNORECASE
    )
    return m.group(1).lower().strip('"') if m else ""


def insert_id_table(sql: str) -> str | None:
    """Назва таблиці, якщо SQL — INSERT у таблицю з серійною колонкою `id`.

    Повертає lowercased назву таблиці (для автентифікації) або None.
    """
    name = _table_name_of(sql)
    return name if name and name in ID_TABLES else None


def translate_sql(sql: str) -> str:
    """Основне перетворення для одиночного виразу (execute/insert)."""
    s = sql.strip()
    kw = _statement_keyword(s)

    is_insert = kw == "INSERT"
    is_select = kw in ("SELECT", "EXPLAIN", "WITH", "VALUES", "SHOW")

    # Вилучаємо OR IGNORE / OR REPLACE і запам'ятовуємо це.
    words, ignore_mode = _replace_ignore(s)
    normalized = " ".join(words)

    # Перекладаємо плейсхолдери.
    result = replace_placeholders(normalized)

    if is_insert:
        tbl = _table_name_of(normalized)
        upper = result.upper()
        conflict_clause = ""
        if ignore_mode in ("ignore", "replace") and " ON CONFLICT" not in upper:
            conflict_clause = " ON CONFLICT DO NOTHING"
        returning = ""
        if tbl in ID_TABLES:
            returning = " RETURNING id"
        # PostgreSQL не дозволяє некваліфіковані колонки у виразах
        # ON CONFLICT DO UPDATE SET (ambiguous), SQLite — дозволяє.
        if " ON CONFLICT" in upper and tbl:
            result = _qualify_upsert_set(result, tbl)
        # Якщо вже є ON CONFLICT (upsert), повертаємо без додавання.
        return result + conflict_clause + returning

    return result


def _split_top_level_commas(s: str) -> list[str]:
    """Розбиває на частини за комами поза рядковими літералами та дужками."""
    parts: list[str] = []
    buf: list[str] = []
    depth = 0
    in_str = False
    i = 0
    L = len(s)
    while i < L:
        ch = s[i]
        if in_str:
            buf.append(ch)
            if ch == "\\" and i + 1 < L:
                buf.append(s[i + 1])
                i += 2
                continue
            if ch == "'":
                in_str = False
            i += 1
            continue
        if ch == "'":
            in_str = True
            buf.append(ch)
            i += 1
            continue
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth = max(0, depth - 1)
        if ch == "," and depth == 0:
            parts.append("".join(buf))
            buf = []
            i += 1
            continue
        buf.append(ch)
        i += 1
    parts.append("".join(buf))
    return parts


def _qualify_word(expr: str, word: str, qualifier: str) -> str:
    """Замінює самостійні входження `word` на `qualifier.word` поза літералами."""
    out: list[str] = []
    i = 0
    L = len(expr)
    in_str = False
    while i < L:
        ch = expr[i]
        if in_str:
            out.append(ch)
            if ch == "\\" and i + 1 < L:
                out.append(expr[i + 1])
                i += 2
                continue
            if ch == "'":
                in_str = False
            i += 1
            continue
        if ch == "'":
            in_str = True
            out.append(ch)
            i += 1
            continue
        if ch.isalnum() or ch == "_":
            j = i
            while j < L and (expr[j].isalnum() or expr[j] == "_"):
                j += 1
            token = expr[i:j]
            prev = expr[i - 1] if i > 0 else " "
            bare = not (prev.isalnum() or prev in "._")
            if bare and token.lower() == word.lower():
                out.append(f"{qualifier}.{token}")
            else:
                out.append(token)
            i = j
            continue
        out.append(ch)
        i += 1
    return "".join(out)


def _qualify_upsert_set(sql: str, table: str) -> str:
    """Кваліфікує колонки у `ON CONFLICT ... DO UPDATE SET` для PostgreSQL.

    PG-семантика: некваліфіковане посилання на колонку у виразі SET
    двозначне між цільовим рядком та псевдо-рядком `excluded` →
    `column reference "x" is ambiguous`. SQLite таку саму форму приймає,
    тому кваліфікація додається лише при трансляції у PG.
    """
    marker = " DO UPDATE SET "
    up = sql.upper()
    idx = up.find(marker)
    if idx < 0 or " ON CONFLICT" not in up:
        return sql

    head = sql[: idx + len(marker)]
    tail = sql[idx + len(marker):]

    assignments = _split_top_level_commas(tail)
    names: set[str] = set()
    for part in assignments:
        lhs, sep, _ = part.partition("=")
        if sep:
            name = lhs.strip().strip('"').strip()
            if name:
                names.add(name)
        for m in re.finditer(r"\bexcluded\.(\w+)", part, re.IGNORECASE):
            names.add(m.group(1))

    qualified: list[str] = []
    for part in assignments:
        lhs, sep, rhs = part.partition("=")
        if not sep:
            qualified.append(part)
            continue
        expr = rhs
        for name in names:
            expr = _qualify_word(expr, name, table)
        qualified.append(f"{lhs}={expr}")
    return head + ",".join(qualified)


def _classify(sql: str) -> str:
    """Повертає 'rows' (SELECT/INSERT з RETURNING) або 'status' (DML)."""
    kw = _statement_keyword(sql)
    if kw == "SELECT" or kw == "EXPLAIN" or kw == "WITH" or kw == "VALUES" or kw == "SHOW":
        return "rows"
    return "status"


def _replace_ignore(sql: str) -> tuple[list[str], str]:
    """Повертає (слова, режим), вилучаючи OR IGNORE / OR REPLACE з INSERT."""
    words = sql.split()
    if len(words) >= 3 and words[0].upper() == "INSERT":
        if words[1].upper() == "OR" and words[2].upper() in ("IGNORE", "REPLACE"):
            return [words[0]] + words[3:], words[2].lower()
    return words, ""


def prepare(sql: str) -> tuple[str, str]:
    """Підготувати SQL до виконання у PostgreSQL.

    Повертає `(pg_sql, mode)` де mode:
      - 'rows'   -> виконати через fetch (очікуються записи);
      - 'status' -> виконати через execute (повертає rowcount).
    """
    pg_sql = translate_sql(sql)
    if _classify(pg_sql) == "rows" or " RETURNING ID" in pg_sql.upper():
        return pg_sql, "rows"
    return pg_sql, "status"


def prepare_many(sql: str) -> str:
    """Підготувати SQL для executemany (без RETURNING)."""
    pg_sql = translate_sql(sql)
    if " RETURNING id" in pg_sql:
        pg_sql = pg_sql[: pg_sql.rindex(" RETURNING id")]
    return pg_sql


def split_statements(sql: str) -> list[str]:
    """Розбити багатовиразний SQL-файл (схему/міграцію) на окремі вирази.

    Розуміє `;` на рівні завершення виразу, ігноруючи `;` всередині рядків.
    """
    statements: list[str] = []
    buf: list[str] = []
    in_str = False
    in_comment = False
    i = 0
    L = len(sql)
    while i < L:
        ch = sql[i]
        nxt = sql[i + 1] if i + 1 < L else ""
        if in_comment:
            if ch == "\n":
                in_comment = False
            i += 1
            continue
        if in_str:
            buf.append(ch)
            if ch == "\\" and i + 1 < L:
                buf.append(nxt)
                i += 2
                continue
            if ch == "'":
                in_str = False
            i += 1
            continue
        if ch == "-" and nxt == "-":
            in_comment = True
            i += 2
            continue
        if ch == "'":
            in_str = True
            buf.append(ch)
            i += 1
            continue
        if ch == ";":
            st = "".join(buf).strip()
            if st:
                statements.append(st)
            buf = []
            i += 1
            continue
        buf.append(ch)
        i += 1
    st = "".join(buf).strip()
    if st:
        statements.append(st)
    return statements


def crowcount_from_status(status: str) -> int:
    """З рядка asyncpg (`"INSERT 0 3"`, `"UPDATE 5"`) витягти rowcount."""
    parts = status.split()
    if parts:
        try:
            return int(parts[-1])
        except ValueError:
            return -1
    return 0


_ID_INSERT_RE = re.compile(
    r"^INSERT(?:\s+OR\s+(?:IGNORE|REPLACE))?\s+INTO\s+(\w+)\s*\((.*?)\)\s*VALUES\s*\((.*)\)\s*$",
    re.IGNORECASE | re.DOTALL,
)


def inject_id(sql: str, lid: int) -> str | None:
    """Повернути SQL з явним `id = lid` (params залишаються незмінними).

    Підходить лише для простих INSERT (без ON CONFLICT-хвоста) у таблиці,
    де `id` ще не задано. Інакше повертає None.
    """
    if " ON CONFLICT" in sql.upper():
        return None
    m = _ID_INSERT_RE.match(sql)
    if not m:
        return None
    head = sql[: m.start(1)].rstrip()
    table = m.group(1)
    cols = m.group(2)
    vals = m.group(3)
    col_list = [c.strip().strip('"') for c in cols.split(",") if c.strip()]
    if not col_list or "id" in col_list:
        return None
    return f"{head} {table} (id, {cols}) VALUES ({lid}, {vals})"