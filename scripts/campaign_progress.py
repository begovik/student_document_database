"""Контроль прогресу кампанії «договір за участі неповнолітньої особи».

Read-only: лише SELECT, нічого не змінює в БД.

    venv/bin/python scripts/campaign_progress.py

Що вважається джерелом
-----------------------
Фільтр `first_seen_at >= <відсічка>` НЕ працює: паралельно з кампанією
йде bulk-сканування OpenAlex, тож «знайдено після старту» містить науку
(у першій перевірці 6 із 7 «pass» виявилися фізикою — нейтрино, cryogels,
LHC).

Робочий фільтр — перетин трьох ознак:
  1. УДК LIKE '347%' — цивільне право України. Це сигнал дисципліни,
     заповнений у 216 828 документів, надійніший за слова в назві.
  2. language = 'uk' — рішення власника: тільки українські джерела.
  3. Сильні маркери теми в назві: неповноліт, малоліт, дієздатн,
     правоздатн, підліт, дитин, опік, піклуван.
Заміряно: без назви — 1390 документів, з назвою — 42.

Триаж за фільтром відкидання з картки
-------------------------------------
Картка відкидає кримінальну, трудову та процесуальну літературу. Ті
маркери НЕ прибрані з виводу — вони позначені тегом [ВІДКИНУТО], щоб
рішення про конкретну позицію лишалося за вами.
"""

import subprocess
import sys

TARGET = 40

# Сильні маркери теми.
STRONG = ("неповноліт", "малоліт", "дієздатн", "правоздатн",
          "підліт", "дитин", "опік", "піклуван")

# Маркери, які картка відкидає. Справедливість перевірки: «Викрадення
# дитини одним із батьків, проблеми кваліфікації злочину» має
# доганястий маркер «дитин», але є кримінальною справою.
REJECT = {
    "кримінал": ("злочин", "кримінал", "кваліфікац", "карний"),
    "процедура": ("процесуальн", "судове рішення", "виконання рішення суду",
                  "замовник", "підсудн"),
    "труд": ("трудов", "звільненн", "профспілк"),
}


def _likes(words: tuple[str, ...], alias: str = "t") -> str:
    return "(" + " OR ".join(f"lower(d.{alias}) LIKE ?" for _ in words) + ")"


STRONG_SQL = _likes(STRONG)
REJECT_SQL = {
    k: _likes(v) for k, v in REJECT.items()
}

REJECT_CASE = " ".join(
    f"WHEN {sql} THEN '{tag}'" for tag, sql in REJECT_SQL.items()
)


def run_psql(sql: str) -> str:
    with open("/tmp/opencode/_progress.sql", "w") as fh:
        fh.write(sql)
    out = subprocess.run(
        ["sudo", "-n", "-u", "postgres", "psql", "-d", "harvester", "-tA",
         "-F", " | ", "-f", "/tmp/opencode/_progress.sql"],
        capture_output=True, text=True, check=False,
    )
    if out.returncode != 0:
        raise RuntimeError(out.stderr or out.stdout)
    return out.stdout.strip()


def build() -> str:
    """Один SQL: список джерел + підсумки. Параметри — через \\set."""
    strong_params = "".join(f"\\set m{i} '%{w}%'\n" for i, w in enumerate(STRONG))
    strong_or = " OR ".join(f"lower(d.title) LIKE :'m{i}'" for i in range(len(STRONG)))

    reject_parts, reject_params = [], []
    idx = len(STRONG)
    for tag, words in REJECT.items():
        parts = []
        for w in words:
            reject_params.append(f"\\set m{idx} '%{w}%'\n")
            parts.append(f"lower(d.title) LIKE :'m{idx}'")
            idx += 1
        reject_parts.append(f"WHEN ({' OR '.join(parts)}) THEN '{tag}'")
    reject_case = "CASE " + " ".join(reject_parts) + " ELSE 'OK' END"

    header = strong_params + "".join(reject_params)

    sql = f"""{header}
\\echo '<<<LIST>>>'
SELECT CASE WHEN {reject_case} <> 'OK' THEN '[ВІДКИНУТО:' || {reject_case} || '] '
            ELSE '[              ]' END
       || rpad(COALESCE(d.udc,'-'), 20) || ' | '
       || rpad(COALESCE(d.doc_type,'?'), 10) || ' | '
       || left(COALESCE(d.title,'(без назви)'), 74)
FROM documents d
JOIN verifier_results vr ON vr.document_id = d.id AND vr.profile = 'strict'
WHERE d.udc LIKE '347%' AND d.language = 'uk' AND vr.status = 'pass'
  AND ({strong_or})
ORDER BY ({reject_case} <> 'OK'), d.udc, d.title;

\\echo '<<<TOTALS>>>'
SELECT 'придатних (з триажем): ' || COUNT(*) FROM documents d
JOIN verifier_results vr ON vr.document_id = d.id AND vr.profile = 'strict'
WHERE d.udc LIKE '347%' AND d.language = 'uk' AND vr.status = 'pass'
  AND ({strong_or});

\\echo '<<<OKCOUNT>>>'
SELECT 'без позначок відкидання: ' || COUNT(*) FROM documents d
JOIN verifier_results vr ON vr.document_id = d.id AND vr.profile = 'strict'
WHERE d.udc LIKE '347%' AND d.language = 'uk' AND vr.status = 'pass'
  AND ({strong_or}) AND {reject_case} = 'OK';

\\echo '<<<QUEUE>>>'
SELECT 'api_iter ' || status || ' = ' || COUNT(*)
  FROM tasks WHERE type='api_iter' GROUP BY status;
"""
    return sql


def main() -> int:
    try:
        out = run_psql(build())
    except RuntimeError as e:
        print(f"Помилка psql: {e}")
        return 1

    blocks: dict[str, list[str]] = {}
    current = None
    for line in out.splitlines():
        if line.startswith("<<<") and line.endswith(">>>"):
            current = line[3:-3]
            blocks[current] = []
        elif current:
            blocks[current].append(line)

    items = [ln for ln in blocks.get("LIST", []) if ln.strip()]
    ok = 0
    for ln in blocks.get("TOTALS", []) + blocks.get("OKCOUNT", []):
        if ln.strip():
            print(f"  {ln.strip()}")

    ok = 0
    for ln in items:
        if "[ВІДКИНУТО" not in ln:
            ok += 1

    print(f"\n{'=' * 78}\nДжерела ({len(items)} усього, {ok} без позначок відкидання)\n{'=' * 78}")
    for ln in items:
        print(f"  {ln}")

    print(f"\n{'=' * 78}")
    pct = min(100, round(100 * ok / TARGET))
    bar = "#" * round(pct / 5) + "." * (20 - round(pct / 5))
    print(f"Ціль {TARGET} → придатних {ok}  [{bar}] {pct}%")
    if ok >= TARGET:
        print("\nЦІЛЬ ДОСЯГНУТА. Подальший збір можна зупинити:")
        print("  verifier.focus_first_seen_after вже null; опційно —")
        print("  channels.openalex.enabled: false, щоб не витрачати спільний")
        print("  денний бюджет OpenAlex на bulk-сканування.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
