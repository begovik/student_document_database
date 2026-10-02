"""Фінальний контроль джерел для картки.

    venv/bin/python scripts/campaign_progress.py

Read-only: лише SELECT.

Як рахується «джерело»
----------------------
Три обовʼязкові умови (усі три, інакше це не джерело):

  1. `udc LIKE '347%'` — цивільне право України. Сигнал дисципліни,
     заповнений у 216828 документів.
  2. `language = 'uk'` — рішення власника: тільки українські джерела.
  3. strict `pass` + `status='verified'` — пройшов повний конвеєр.

Плюс два фільтри якості, додані за результатами вимірювання:

  * АВТОР І РІК ОБОВʼЯЗКОВІ (`authors IS NOT NULL AND year IS NOT NULL`).
    Картка відкидає джерела без автора й вихідних даних. Заміряно:
    серед 1312 кандидатів цей фільтр знімає найгірші — роботи, де
    назвою стала назва випуску журналу або прізвище автора.
  * НЕ ЗБІРКА. 25 документів вердиктор прийняв за повноцінні праці,
    бо це PDF цілого випуску журналу: «Актуальні питання у сучасній
    науці», «Наукові перспективи № 7(49) 2024», «ТРИБУНА МОЛОДОГО
    ВЧЕНОГО». Усередині справді є статті, але це контейнер, а не
    джерело, і в курсовій на нього посилатися не можна.

Ранжування за близькістю до теми картки
---------------------------------------
Тема — «укладення і виконання договору за участі неповнолітньої
особи». Тому джерела розділено на рівні, і рівень 0 — це пряма відповідь:

  T0 неповноліт/дієздатність/опіка в назві  — пряма відповідь на картку
  T1 договір, правочин                     — ядро договірного права
  T2 зобов'язальне, майнове, забезпечення  — зобов'язальне право
  T3 особистісне, сімейне, подружжя        — смежний напрям
  T4 спадкове право                        — смежний напрям
  T5 захист прав, відповідальність          — смежний напрям

Триаж (не відкидає, а позначає)
------------------------------
Картка відкидає кримінальну, трудову та процесуальну літературу.
Такі позиції лишаються у виводі з тегом [ВІДКИНУТО:...], щоб
рішення про конкретну позицію залишалося за власником — зокрема
«ВІДКИНУТО:процедура» на «Принципи належного виконання договірних
зобов'язань» може виявитися корисним для розділу про виконання.

Спільні канали (--shared) показують, що зібрано не лише цією кампанією.
"""

import subprocess
import sys

TARGET = 40

# Рівні близькості. Перевіряються згори вниз: документ потрапляє у
# найвищий рівень, якщо хоч один маркер рівня збігається.
TIERS = [
    ("T0 неповноліт/дієздатність", (
        "неповноліт", "малоліт", "дієздатн", "правоздатн", "підліт",
        "дитин", "опік", "піклуван",
    )),
    ("T1 договір, правочин", (
        "договір", "правочин",
    )),
    ("T2 зобов'язальне, майнове", (
        "зобов'язальн", "майнов", "право власності", "забезпеченн",
    )),
    ("T3 особистісне, сімейне", (
        "особистісн", "сім'ян", "сімʼян", "подружж", "опікуват",
    )),
    ("T4 спадкове право", (
        "спадков", "заст", "наслід",
    )),
    ("T5 захист прав, відповідальність", (
        "захист прав", "права споживач", "відповідальност",
    )),
]

# Фільтр відкидання з картки.
REJECT = {
    "кримінал": ("злочин", "кримінал", "кваліфікац", "карн"),
    "процедура": ("процесуальн", "судове рішення", "виконання рішення суду",
                  "підсудн", "позовн"),
    "труд": ("трудов", "звільненн", "профспілк"),
}

# Назви, під якими Crossref віддає цілі випуски журналів як один PDF.
# Вердиктор їх приймає (статті всередині справді є), але посилатися на
# випуск у курсовій не можна — це контейнер, а не джерело.
NOT_A_SOURCE = (
    "%Актуальні питання%", "%часопис права%", "%ВІСНИК%", "%вісник%",
    "%ТРИБУНА%", "%ЦИ В І ЛЬ Н Е П Р А В О%", "%ЦИВІЛЬНЕ ПРАВО І ПРОЦЕС%",
    "%Наше право%", "%Наукові перспективи%", "%Галицькі студії%",
    "%Цивілістика%", "%ПРАВО І СУСПІЛЬСТВО%", "%Науково-практичний%",
    "%ВІДОКРЕМЛЕНІ%", "%ЗБІРНИК%", "%МАТЕРІАЛИ%",
)


# Апострофи — окрема проблема. Вони є і в українських словах маркерів
# («зобов'язальне», «сім'я»), і в назвах статей, і в тегах триажу. Два
# наслідки, обидва зламали запит:
#   * `\set m16 '%сім'ян%'` — psql не парсить, «unterminated quoted string»;
#   * `THEN 'T2 зобов'язальне'` — SQL не парсить.
# Тому порівнюємо назву й маркер БЕЗ апострофів. Це ще й виправляє
# реальну проблему вибірки: в назвах статей трапляються різні апострофи
# (ASCII ', типографський ʼ, зворотна лапка `), тож маркер «сім'ян» не
# збігався б з «сімʼян» — обидві форми зустрічаються насправді.
APOSTROPHES = ("'", "ʼ", "`")


def _strip_marks(value: str) -> str:
    for ch in APOSTROPHES:
        value = value.replace(ch, "")
    return value


def _sql_literal(value: str) -> str:
    """Екранування апострофа для SQL-рядка."""
    return "'" + value.replace("'", "''") + "'"


# Нормалізований вираз назви: без апострофів, у нижньому регістрі.
TITLE = (
    "replace(replace(replace(lower(d.title), '''', ''), 'ʼ', ''), '`', '')"
)


def build() -> str:
    """Збирає SQL. Параметри передаються через \\set — щоб не працювати
    з екрануванням лапок у назвах українських статей."""
    params: list[str] = []
    counter = 0

    def like_clause(words: tuple[str, ...]) -> str:
        """OR-умова LIKE з автоінкрементом імен psql-параметрів."""
        nonlocal counter
        parts = []
        for w in words:
            counter += 1
            params.append(f"\\set m{counter} '%{_strip_marks(w)}%'")
            parts.append(f"{TITLE} LIKE :'m{counter}'")
        return "(" + " OR ".join(parts) + ")"

    tier_when = [
        f"WHEN {like_clause(w)} THEN {_sql_literal(label)}" for label, w in TIERS
    ]
    reject_when = [
        f"WHEN {like_clause(w)} THEN {_sql_literal(tag)}" for tag, w in REJECT.items()
    ]

    tier_case = "CASE " + " ".join(tier_when) + " ELSE '' END"
    reject_case = "CASE " + " ".join(reject_when) + " ELSE '' END"

    not_src = " OR ".join(f"d.title LIKE {_sql_literal(p)}" for p in NOT_A_SOURCE)

    return f"""\\pset footer off
{chr(10).join(params)}
\\echo '<<<COUNTS>>>'
SELECT t AS рівень, count(*) AS кількість FROM (
  SELECT {tier_case} AS t FROM documents d
  JOIN verifier_results vr ON vr.document_id=d.id AND vr.profile='strict'
  WHERE d.udc LIKE '347%' AND d.language='uk' AND vr.status='pass'
    AND d.status='verified' AND d.authors IS NOT NULL AND d.year IS NOT NULL
    AND NOT ({not_src})
) s WHERE t <> '' GROUP BY t ORDER BY t;

\\echo '<<<LIST>>>'
SELECT {tier_case} AS tier,
       rpad(COALESCE(d.udc,'-'),17) || ' | ' || rpad(CAST(d.year AS text),5) || ' | '
       || CASE WHEN {reject_case} <> '' THEN '[ВІДКИНУТО:' || {reject_case} || ']'
               ELSE '                  ' END
       || ' ' || left(d.title, 62) AS rest
FROM documents d
JOIN verifier_results vr ON vr.document_id=d.id AND vr.profile='strict'
WHERE d.udc LIKE '347%' AND d.language='uk' AND vr.status='pass'
  AND d.status='verified' AND d.authors IS NOT NULL AND d.year IS NOT NULL
  AND NOT ({not_src})
  AND {tier_case} <> ''
ORDER BY tier, {reject_case} <> '', d.title;

\\echo '<<<SHARED>>>'
SELECT 'знайдено >= 2 каналами: ' || COUNT(*) FROM documents d
JOIN verifier_results vr ON vr.document_id=d.id AND vr.profile='strict'
WHERE d.udc LIKE '347%' AND d.language='uk' AND vr.status='pass'
  AND d.status='verified' AND d.authors IS NOT NULL AND d.year IS NOT NULL
  AND NOT ({not_src}) AND {tier_case} <> ''
  AND d.id IN (SELECT document_id FROM document_refs GROUP BY document_id
               HAVING COUNT(DISTINCT channel) > 1);
"""


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


def main() -> int:
    try:
        out = run_psql(build())
    except RuntimeError as e:
        print(f"Помилка psql: {e}")
        return 1

    blocks: dict[str, list[str]] = {}
    cur = None
    for line in out.splitlines():
        if line.startswith("<<<") and line.endswith(">>>"):
            cur = line[3:-3]
            blocks[cur] = []
        elif cur and line.strip():
            blocks[cur].append(line.rstrip())

    for ln in blocks.get("COUNTS", []):
        print("  " + ln.replace(" | ", " "))

    items = blocks.get("LIST", [])
    usable = [ln for ln in items if "[ВІДКИНУТО" not in ln]
    dropped = len(items) - len(usable)

    print(f"\n{'=' * 96}")
    print(f"Джерела: {len(items)}, без позначок відкидання: {len(usable)} "
          f"(відкинуто позначками: {dropped})")
    print("=" * 96)
    for ln in items:
        print("  " + ln)

    for ln in blocks.get("SHARED", []):
        print("\n  " + ln.replace(" | ", " "))

    print(f"\n{'=' * 96}")
    pct = min(100, round(100 * len(usable) / TARGET))
    bar = "#" * round(pct / 5) + "." * (20 - round(pct / 5))
    print(f"Ціль {TARGET} → придатних {len(usable)}  [{bar}] {pct}%")
    if len(usable) >= TARGET:
        print("ЦІЛЬ ДОСЯГНУТА.")
    return 0


if __name__ == "__main__":
    sys.exit(main())