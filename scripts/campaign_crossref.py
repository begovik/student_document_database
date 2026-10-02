"""Кампанія «договір за участі неповнолітньої особи» через Crossref.

    venv/bin/python scripts/campaign_crossref.py            # показати
    venv/bin/python scripts/campaign_crossref.py --apply    # внести

Навіщо Crossref, а не лише OpenAlex
----------------------------------
OpenAlex без ключа рахує запити проти безкоштовного денного бюджету,
спільного на весь вихідний IP — на 02.10.2026 він був вичерпаний
(`$0.0005 remaining; resets at midnight UTC`). Кампанія на 26 запитів
там зупинилась. Crossref лімітом не має: 50 зап/с у polite-пулі, тому
він працює, коли OpenAlex недоступний.

Виміряно перед написанням (на запитах нижнього блоку):
  * `filter=type:dissertation` на українську правову тему → 0. Автореферати
    DOI не мають, тож дисертацій через Crossref не буде в принципі.
  * `type:monograph` / `book` знаходяться, але це підручники
    («навч.-метод. посіб.», «практикум», «кваліфікаційний екзамен») без
    PDF — рівно те, що картка відкидає.
  * `type:journal-article` дає десятки тисяч, у ~53% записів є пряме
    PDF-посилання, і ~53% з них реально віддають файл з цього хоста.

Що робить скрипт
----------------
Планує bounded-задачі `crossref_iter` із пріоритетом 100. Канал сам
зупиняється на max_offset (типово 200), тож прохід скінченний і не
перетвориться на безкінечне гортання.

Обмеження каналу, які варто знати перед запуском
-----------------------------------------------
* Мова не фільтрується: Crossref віддає `language: null` для українських
  журналів, а серверний фільтр `language:uk` API відкидає (HTTP 400).
  Мову визначить langid у verify-конвеєрі.
* Близько половини PDF-посилань не віддають файл (хости onua.edu.ua
  недоступні з цього сервера, частина посилань у метаданих обрізана).
  Це нормально: конвеєр запише їх як http_status, і це буде видно в
  campaign_progress.
"""

import argparse
import asyncio

from harvester.config import get_settings
from harvester.core.scheduler import Scheduler
from harvester.db.failover import build_database

CAMPAIGN_PRIORITY = 100

# Скільки рядків за запит і до якого offset гортаємо. Crossref дозволяє
# offset до 10000, але запит «захист прав неповнолітніх» має 38 034
# результати — глибше 200 результатів українських правових статей уже
# не буде, лише загальні цивільно-правові.
ROWS = 50
MAX_OFFSET = 200

# Запити відбудовані за темою картки. Формулювання орієнтуються на
# терміни, які реально є в назвах українських правових статей
# («цивільно-правове регулювання», «захист прав малолітніх»), а не на
# загальні слова — інакше Crossref домішує криміналістику й педагогіку.
QUERIES = [
    "цивільно-правове регулювання угод за участю неповнолітніх",
    "дієздатність неповнолітньої особи цивільне право",
    "укладення договору за участю неповнолітньої особи",
    "цивільний договір неповнолітній правоздатність",
    "захист прав малолітніх і неповнолітніх цивільне право",
    "правочин за участю неповнолітньої особи",
    "емансипація неповнолітньої особи цивільне право",
    "опіка та піклування цивільно-правове регулювання",
    "майнові права неповнолітніх цивільне право",
    "цивільне право України неповнолітні",
]


async def main() -> int:
    ap = argparse.ArgumentParser(
        description="Кампанія збору через Crossref (замість вичерпаного OpenAlex)"
    )
    ap.add_argument("--apply", action="store_true", help="внести задачі у чергу")
    ap.add_argument("--rows", type=int, default=ROWS)
    ap.add_argument("--max-offset", type=int, default=MAX_OFFSET)
    ap.add_argument("--only", help="підрядок запитів через кому (діагностика)")
    args = ap.parse_args()

    queries = QUERIES
    if args.only:
        wanted = [q.strip() for q in args.only.split(",") if q.strip()]
        queries = [q for q in QUERIES if q in wanted] or wanted

    db = build_database()
    await db.initialize()

    print(f"Канал crossref: enabled={get_settings().channels.crossref.enabled}, "
          f"rps={get_settings().channels.crossref.rps}")
    print(f"Запитів: {len(queries)}, rows={args.rows}, max_offset={args.max_offset}\n")
    for q in queries:
        print(f"  • {q}")

    if not args.apply:
        print("\n[DRY-RUN] Нічого не змінено. Додайте --apply.")
        await db.close()
        return 0

    scheduler = Scheduler(db)
    scheduled = 0
    for q in queries:
        task_id = await scheduler.schedule_task(
            "crossref_iter",
            {
                "query": q,
                "filter": "type:journal-article",
                "rows": args.rows,
                "offset": 0,
                "max_offset": args.max_offset,
                "priority": CAMPAIGN_PRIORITY,
            },
            priority=CAMPAIGN_PRIORITY,
        )
        if task_id:
            scheduled += 1

    print(f"\nВнесено задач crossref_iter: {scheduled} "
          f"(пріоритет {CAMPAIGN_PRIORITY})")
    print("Канал сам плануватиме наступні сторінки до max_offset, потім зупиниться.")
    print("Контроль: venv/bin/python scripts/campaign_progress.py")
    await db.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))