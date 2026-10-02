"""Фінальний список джерел для курсової — з метаданими для опису.

    venv/bin/python scripts/export_sources.py                 # Markdown, 40
    venv/bin/python scripts/campaign_progress.py > /tmp/progress.txt

Формат — як у списку використаних джерел: автор, назва, журнал, рік,
DOI, URL. УДК залишено окремим стовпцем, бо він потрібен для
атрибуції до дисципліни «Договірне право» (класифікатор ще не
заповнений — discipline_assign наливається LLM, 2-4 дні).

Порядок: T0 (пряма відповідь на картку) → T1 (ядро договірного права) →
T2 → T4. У межах рівня — спершу без позначок відкидання, потім з
позначками, щоб рішення про конкретну позицію лишалося за вами.

Журнал береться з extra.container_title для документів, знайдених
через Crossref, і з canonical_url (домен OJS) для решти.
"""

import json
import re
import subprocess
import sys
from difflib import SequenceMatcher

# Скільки позицій вивантажувати. Картка вимагає 35-40, тому беремо
# 40 із запасом на відсів.
EXPORT_N = 40

TIERS = [
    ("T0", "неповноліт/дієздатність/опіка", (
        "неповноліт", "малоліт", "дієздатн", "правоздатн",
        "підліт", "дитин", "опік", "піклуван",
    )),
    ("T1", "договір і правочини", (
        "договір", "правочин",
    )),
    ("T2", "зобов'язальне та майнове право", (
        "зобов'язальн", "майнов", "право власності", "забезпеченн",
    )),
    ("T4", "спадкове право", (
        "спадков", "наслід",
    )),
]

REJECT = {
    "кримінал": ("злочин", "кримінал", "кваліфікац", "карн"),
    "процедура": ("процесуальн", "судове рішення", "виконання рішення суду",
                  "підсудн", "позовн"),
    "труд": ("трудов", "звільненн", "профспілк"),
}

NOT_A_SOURCE = (
    "%Актуальні питання%", "%часопис права%", "%ВІСНИК%", "%вісник%",
    "%ТРИБУНА%", "%ЦИ В І ЛЬ Н Е П Р А В О%", "%ЦИВІЛЬНЕ ПРАВО І ПРОЦЕС%",
    "%Наше право%", "%Наукові перспективи%", "%Галицькі студії%",
    "%Цивілістика%", "%ПРАВО І СУСПІЛЬСТВО%", "%Науково-практичний%",
    "%ВІДОКРЕМЛЕНІ%", "%ЗБІРНИК%", "%МАТЕРІАЛИ%",
)

APOSTROPHES = ("'", "ʼ", "`")


def _strip(value: str) -> str:
    for ch in APOSTROPHES:
        value = value.replace(ch, "")
    return value


def _lit(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


TITLE = "replace(replace(replace(lower(d.title), '''', ''), 'ʼ', ''), '`', '')"


def build() -> str:
    params: list[str] = []
    counter = 0

    def like(words):
        nonlocal counter
        parts = []
        for w in words:
            counter += 1
            params.append(f"\\set m{counter} '%{_strip(w)}%'")
            parts.append(f"{TITLE} LIKE :'m{counter}'")
        return "(" + " OR ".join(parts) + ")"

    tier_when = [
        f"WHEN {like(words)} THEN '{code}'" for code, _, words in TIERS
    ]
    reject_when = [
        f"WHEN {like(words)} THEN '{tag}'" for tag, words in REJECT.items()
    ]
    tier_case = "CASE " + " ".join(tier_when) + " ELSE '' END"
    reject_case = "CASE " + " ".join(reject_when) + " ELSE '' END"
    not_src = " OR ".join(f"d.title LIKE {_lit(p)}" for p in NOT_A_SOURCE)

    return f"""\\pset footer off
{chr(10).join(params)}
\\pset format unaligned
\\pset tuples_only on
\\pset fieldsep '\\x1f'
\\pset null '(н/д)'
SELECT {tier_case},
       {reject_case},
       COALESCE(d.udc, ''),
       COALESCE(CAST(d.year AS text), ''),
       COALESCE(d.authors, ''),
       COALESCE(d.title, ''),
       COALESCE(d.publisher, ''),
       COALESCE(d.doi, ''),
       COALESCE(d.canonical_url, ''),
       COALESCE(d.landing_url, ''),
       COALESCE(d.extra::text, '')
FROM documents d
JOIN verifier_results vr ON vr.document_id=d.id AND vr.profile='strict'
WHERE d.udc LIKE '347%' AND d.language='uk' AND vr.status='pass'
  AND d.status='verified' AND d.authors IS NOT NULL AND d.year IS NOT NULL
  AND NOT ({not_src})
  AND {tier_case} <> ''
ORDER BY {tier_case}, {reject_case} <> '', d.year DESC, d.title;
"""


def run_psql(sql: str) -> str:
    with open("/tmp/opencode/_export.sql", "w") as fh:
        fh.write(sql)
    out = subprocess.run(
        ["sudo", "-n", "-u", "postgres", "psql", "-d", "harvester",
         "-f", "/tmp/opencode/_export.sql"],
        capture_output=True, text=True, check=False,
    )
    if out.returncode != 0:
        raise RuntimeError(out.stderr or out.stdout)
    return out.stdout


def journal_from(extra: str, url: str, landing: str = "") -> str:
    """Журнал: Crossref-канал кладе його в extra.container_title.

    Таблиця sources не допомагає: у ній лише домен і платформа, а
    documents.source_id не заповнено жодним документом (виміряно: 0).
    """
    i = extra.find('"container_title": "')
    if i >= 0:
        rest = extra[i + len('"container_title": "'):]
        name = _clean(rest.split('"')[0])
        if name:
            return name
    for candidate in (url, landing):
        if candidate and "//" in candidate:
            host = candidate.split("/")[2]
            # doi.org — не журнал, це резолвер. Відкидаємо і дивимось
            # на сторінку статті.
            if not host.endswith("doi.org"):
                return host
    return ""


# Екранування \\uXXXX у значенні автора. Не живий баг, а спадок періоду
# 2026-08…2026-09: тоді authors писався з ensure_ascii=True. Заміряно —
    # серпень 139 278 екранованих із 360 710, вересень 151 087 із 319 431,
#    жовтень 0 із 5 245. Код виправлено (ensure_ascii=False), тож нові
#    документи чисті, а масовий UPDATE старових рядків непотрібний —
#    декодуємо під час вивантаження.
_ESCAPE = re.compile(r"\\(u[0-9a-fA-F]{4}|.)", re.DOTALL)


def _unescape(value: str) -> str:
    """Розкодовує JSON-екранування, включно з \\uXXXX та surrogate-парами."""

    def repl(m: re.Match) -> str:
        body = m.group(1)
        if body.startswith("u"):
            return chr(int(body[1:], 16))
        return {"n": "\n", "t": "\t", "r": "\r", "b": "", "f": ""}.get(body, body)

    return _ESCAPE.sub(repl, value)


def _clean(value: str) -> str:
    """Прибирає керуючі символи — у PDF-метаданих трапляється \\u0000
    прямо посеред імені («Паспорт\\u0000»)."""
    return "".join(ch for ch in value if ch.isprintable()).strip()


# Транслітерація кирилиці в латинку. Потрібна, щоб злити одну людину,
# записану двома каналами по-різному: Crossref віддає «Lefterova O. I.»,
# OpenAlex — «Лефтерова О.І.». Без злиття в списку джерел стоять
# дублікати, а на курсовій виглядає так, наче авторів двоє.
_TRANSLIT = str.maketrans({
    "а": "a", "б": "b", "в": "v", "г": "h", "ґ": "g", "д": "d", "е": "e",
    "є": "ie", "ж": "zh", "з": "z", "и": "y", "і": "i", "ї": "i", "й": "i",
    "к": "k", "л": "l", "м": "m", "н": "n", "о": "o", "п": "p", "р": "r",
    "с": "s", "т": "t", "у": "u", "ф": "f", "х": "kh", "ц": "ts", "ч": "ch",
    "ш": "sh", "щ": "shch", "ь": "", "ю": "iu", "я": "ia", "'": "", "’": "",
})


_CYRILLIC = re.compile(r"[а-яіїєґІЇЄҐ]")


def has_cyrillic(value: str) -> bool:
    return bool(_CYRILLIC.search(value))


def clean_title(raw: str, authors: list[str]) -> str:
    """Прибирає з назви хвіст, що прилипнув із метаданих PDF.

    Типовий артефакт: PDF-рубрикатор дописав у кінець назви ім'я автора
    — «…ВИХОВАННІ ДИТИНИ Костюк Віктор Ігорович,». У списку джерел це
    виглядає так, наче це частина назви. Лікується двома правилами:

      1. Хвіст дорівнює записуваному авторові (його ми вже знаємо).
      2. Назва голосна (великими літерами), а хвіст змішаного регістру —
         тоді хвіст це не назва, а метадані. Працює і тоді, коли канал
         не знав автора кирилицею: «Kateryna Glyniana» у базі, а в
         назві — «Глиняна Катерина Михайлівна,».
    """
    title = _clean(_unescape(raw))

    # Після імені з метаданих лишається кома або крапка — порівнюємо
    # уже обрізану назву.
    trimmed = title.rstrip(" ,;:.")
    for name in sorted(authors, key=len, reverse=True):
        if trimmed.lower().endswith(name.rstrip(".").lower()):
            title = trimmed[: len(trimmed) - len(name.rstrip("."))]
            break

    # Назва правового часопису — великими літерами. Якщо перше слово з
    # малою літерою стоїть не на початку, значить голосний префікс —
    # це справжня назва, а змішаний хвіст прилип із метаданих.
    words = title.split()
    for i, word in enumerate(words):
        if any(c.islower() for c in word):
            if i >= 8:
                title = " ".join(words[:i])
            break

    return title.rstrip(" ,;:.")


def _same_token(a: str, b: str) -> bool:
    if a == b:
        return True
    # Короткі токени надто легко плутаються між собою.
    if len(a) < 5 or len(b) < 5:
        return False
    return SequenceMatcher(None, a, b).ratio() >= 0.8


def _same_person(a: frozenset[str], b: frozenset[str]) -> bool:
    """Чи є два ключі одним автором.

    Транслітерації розходяться: Crossref дає «Khryapchenko» (піньїнька
    схема), наша транслітерація дає «Khryapchenko» → «Khriapchenko»
    (я → ia). Тому порівнюємо не рядки, а схожість токенів.
    """
    if a <= b or b <= a:
        return True
    small, big = (a, b) if len(a) <= len(b) else (b, a)
    return all(any(_same_token(s, t) for t in big) for s in small)


def _author_key(name: str) -> frozenset[str]:
    """Ключ для злиття дублів автора.

    При злитті джерел одна людина потрапляє у список двічі: прямою
    транскрипцією та у реверсованій формі («М.Н. Шалько» і «Шалько
    М.Н.»), а ще й кирилицею та латинкою. Порівнюємо не рядки, а
    множини токенів після транслітерації — прізвище не залежить ні від
    порядку, ні від алфавіту.

    Однобуквенні токени відкидаються: це ініціали, а латинська
    транскрипція їх не збігається («T. Y.» проти «Т.Й.» — різна літера
    на й). Ініціали не несуть інформації для злиття.
    """
    flat = name.lower().translate(_TRANSLIT)
    return frozenset(t for t in re.findall(r"[a-z]+", flat) if len(t) > 1)


def parse_authors(raw: str) -> list[str]:
    """Розбирає authors і зливає дублікати одного автора."""
    try:
        names = json.loads(raw)
    except (TypeError, ValueError):
        names = []
    if isinstance(names, str):
        names = [names]

    out: dict[frozenset[str], str] = {}
    for item in names:
        if not isinstance(item, str):
            continue
        name = _clean(_unescape(item))
        if not name:
            continue
        key = _author_key(name)
        if not key:
            continue
        # Канали розходяться ще й за повнотою: один дає «Bilous», другий
        # «Білоус Т.Й.». Тому збіг — не рівність множин, а входження
        # однієї в іншу: спільні токени (прізвище) і є та сама людина.
        merged = next((k for k in out if _same_person(k, key)), None)
        if merged is None:
            out[key] = name
            continue
        # Перемагає кириличний запис: для українського джерела це
        # правильне написання імені, латинка — лише транскрипція каналу.
        if has_cyrillic(name) and not has_cyrillic(out[merged]):
            out[merged] = name

    return [out[k] for k in sorted(out, key=lambda k: out[k])]


def main() -> int:
    try:
        raw = run_psql(build())
    except RuntimeError as e:
        print(f"Помилка psql: {e}")
        return 1

    fields = ("tier rej udc year authors title publisher doi url landing extra")
    names = fields.split()
    rows = []
    for line in raw.splitlines():
        if not line.strip() or line.startswith("\\"):
            continue
        parts = line.split("\x1f")
        if len(parts) < len(names):
            continue
        rows.append(dict(zip(names, parts[:len(names)], strict=True)))

    if not rows:
        print("Помилка формату виводу psql — рядки не розпарсилися.")
        return 1

    tier_label = {code: label for code, label, _ in TIERS}
    lines = ["# Список джерел", ""]
    cur_tier = None
    count = 0
    for row in rows:
        if count >= EXPORT_N:
            break
        tier = row["tier"]
        if tier != cur_tier:
            cur_tier = tier
            lines.append(f"## {tier} — {tier_label[tier]}")
            lines.append("")

        rej = row["rej"]
        tag = f" — **[ВІДКИНУТО: {rej}]**" if rej else ""
        authors = parse_authors(row["authors"])
        bits = [
            "; ".join(authors).rstrip("."),
            f"«{clean_title(row['title'], authors)}»",
            journal_from(row["extra"], row["url"], row["landing"]),
            row["year"].rstrip("."),
            # УДК з Crossref часто приходить з висячою точкою («347.1.:»)
            f"УДК {row['udc'].rstrip('.:')}" if row["udc"] else "",
        ]
        entry = ". ".join(b for b in bits if b) + tag + "."
        lines.append(f"{count + 1}. {entry}")
        if row["doi"]:
            lines.append(f"   DOI: {row['doi']}")
        # Не всі документи мають canonical_url — тоді єдине посилання
        # це DOI-резолвер.
        link = row["url"] or (f"https://doi.org/{row['doi']}" if row["doi"] else "")
        if link:
            lines.append(f"   {link}")
        count += 1
        lines.append("")

    lines.append("---")
    lines.append(f"Усього експортовано: {count} з {len(rows)} знайдених. "
                 "Повний список — `venv/bin/python scripts/campaign_progress.py`.")
    print("\n".join(lines))
    return 0


if __name__ == "__main__":
    sys.exit(main())