"""Контроль прогресу кампанії «договір за участі неповнолітньої особи».

Read-only: лише SELECT, нічого не змінює в БД.

    venv/bin/python scripts/campaign_progress.py

Чому тут є фільтр «за темою», а не просто «знайдено після старту»
----------------------------------------------------------------
Відсічка first_seen_at відокремлює нові документи від беклогу, але НЕ
відсікає їх від bulk-сканування OpenAlex, яке працює паралельно й
приносить science-документи (у першій же перевірці такі 6 із 7 «pass»
виявилися фізикою: нейтрино, cryogels, LHC). Тому придатними вважаються
лише документи, у назві яких є маркери теми.

Фільтр маркерів навмисно подвійний:
  сильні  — неповноліт / малоліт / дієздатн / правоздатн / опік / піклуван
  слабкі  — договір / договірн / контракт
Слабкі без сильних пропускаються: «Договірне право України» само по собі
не є джерелом про договір неповнолітнього.

Остаточний фільтр — УДК (347.155 договірне право), але він наливається
за 2-4 дні, тому для оперативного контролю його замало.
"""

import subprocess
import sys

FOCUS = "2026-10-02T22:29:00"
TARGET = 40

# Сильні маркери теми.
STRONG = ["неповноліт", "малоліт", "дієздатн", "правоздатн",
          "опік", "піклуван", "підліт"]
# Слабкі: мають сенс лише разом із сильними (див. docstring).
WEAK = ["договір", "договірн", "контракт", "цивільн"]

STRONG_SQL = " OR ".join(f"d.title ILIKE '%{k}%'" for k in STRONG)
WEAK_SQL = " OR ".join(f"d.title ILIKE '%{k}%'" for k in WEAK)
ON_TOPIC = (
    f"(({STRONG_SQL}) OR (({WEAK_SQL}) AND "
    f"(d.title ILIKE '%дитин%' OR d.title ILIKE '%дитя%' OR d.title ILIKE '%особа%')))"
)

BLOCKS: list[tuple[str, str]] = [
    (
        "Воронка",
        f"""
        SELECT 'знайдено (усе)         ' || COUNT(*) FROM documents d
         WHERE d.first_seen_at >= '{FOCUS}'
        UNION ALL SELECT '  з них за темою    ' || COUNT(*) FROM documents d
         WHERE d.first_seen_at >= '{FOCUS}' AND {ON_TOPIC}
        UNION ALL SELECT '  verified          ' || COUNT(*) FROM documents d
         WHERE d.first_seen_at >= '{FOCUS}' AND d.status='verified'
        UNION ALL SELECT '  not_pdf           ' || COUNT(*) FROM documents d
         WHERE d.first_seen_at >= '{FOCUS}' AND d.status='not_pdf'
        UNION ALL SELECT '  http-помилка      ' || COUNT(*) FROM documents d
         WHERE d.first_seen_at >= '{FOCUS}' AND d.status LIKE 'http%'
        UNION ALL SELECT '  відфільтровано    ' || COUNT(*) FROM documents d
         WHERE d.first_seen_at >= '{FOCUS}' AND d.status LIKE 'filtered%'
        UNION ALL SELECT 'strict перевірено   ' || COUNT(*) FROM documents d
         JOIN verifier_results vr ON vr.document_id=d.id AND vr.profile='strict'
         WHERE d.first_seen_at >= '{FOCUS}'
        """,
    ),
    (
        "ПРИДАТНІ ДЖЕРЕЛА (strict pass + за темою)",
        f"""
        SELECT '  [' || COALESCE(d.doc_type,'?') || '] '
             || LEFT(COALESCE(d.title,'(без назви)'),78)
             || '  | УДК ' || COALESCE(d.udc,'-')
        FROM documents d JOIN verifier_results vr ON vr.document_id=d.id
            AND vr.profile='strict'
        WHERE d.first_seen_at >= '{FOCUS}' AND vr.status='pass' AND {ON_TOPIC}
        ORDER BY d.doc_type NULLS LAST, d.title
        """,
    ),
    (
        "За темою, але ще не доведені до pass (ще в роботі)",
        f"""
        SELECT '  [' || COALESCE(d.status,'?') || '/'
             || COALESCE(COALESCE(vr.status,'-'),'без вердикту') || '] '
             || LEFT(COALESCE(d.title,'(без назви)'),78)
        FROM documents d LEFT JOIN verifier_results vr ON vr.document_id=d.id
            AND vr.profile='strict'
        WHERE d.first_seen_at >= '{FOCUS}' AND {ON_TOPIC}
          AND (vr.status IS NULL OR vr.status <> 'pass')
        ORDER BY d.first_seen_at DESC LIMIT 25
        """,
    ),
    (
        "Технічний стан",
        """
        SELECT 'api_iter ' || status || ' = ' || COUNT(*)
          FROM tasks WHERE type='api_iter' GROUP BY status
        UNION ALL SELECT 'черга verify = '
            || (SELECT COUNT(*) FROM tasks WHERE type='verify')
        UNION ALL SELECT 'черга classify = '
            || (SELECT COUNT(*) FROM tasks WHERE type='classify')
        UNION ALL SELECT 'черга probe = '
            || (SELECT COUNT(*) FROM tasks WHERE type='probe')
        UNION ALL SELECT 'failed (уся черга) = '
            || (SELECT COUNT(*) FROM tasks WHERE status='failed')
        """,
    ),
]


def run_psql(sql: str) -> str:
    with open("/tmp/opencode/_progress.sql", "w") as fh:
        fh.write(sql)
    out = subprocess.run(
        ["sudo", "-n", "-u", "postgres", "psql", "-d", "harvester", "-tA",
         "-f", "/tmp/opencode/_progress.sql"],
        capture_output=True, text=True, check=False,
    )
    if out.returncode != 0:
        raise RuntimeError(out.stderr or out.stdout)
    return out.stdout.strip()


def main() -> int:
    try:
        results = [run_psql(sql) for _, sql in BLOCKS]
    except RuntimeError as e:
        print(f"Помилка psql: {e}")
        return 1

    got = 0
    for (title, _), body in zip(BLOCKS, results):
        print(f"\n{'=' * 74}\n{title}\n{'=' * 74}")
        print(f"  {body}" if body else "  (порожньо)")
        if "ПРИДАТНІ ДЖЕРЕЛА" in title:
            got = len([ln for ln in body.splitlines() if ln.strip()])

    print(f"\n{'=' * 74}")
    pct = min(100, round(100 * got / TARGET))
    bar = "#" * round(pct / 5) + "." * (20 - round(pct / 5))
    print(f"Ціль {TARGET} джерел → придатних {got}  [{bar}] {pct}%")
    if got >= TARGET:
        print("\nЦІЛЬ ДОСЯГНУТА. Зупинити збір:")
        print("  1) verifier.focus_first_seen_after: null у config.yaml")
        print("  2) sudo systemctl restart harvester")
    return 0


if __name__ == "__main__":
    sys.exit(main())
