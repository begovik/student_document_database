"""Прицільна повторна верифікація тематичних документів кампанії.

    venv/bin/python scripts/campaign_reverify.py           # показати
    venv/bin/python scripts/campaign_reverify.py --apply   # виконати

Навіщо
------
Вердиктор ігнорував увесь документ, крім перших 3000 символів. В
українській правовій статті перші ~2500 символів — це УДК, назва, анотація
та ключові слова, тобто LLM бачив анотацію й писав «наданий фрагмент є
лише анотацією». Виміряно: усі 16 відхилених тематичних правових
документів мали саме цю причину відхилення і порожню `extra.structure`
(перевірені 04-08.09, до появи структурного аналізу).

Після виправлення (12 000 символів + «фрагмент ≠ документ» у промпті)
вердикт має бути переоцінений на реальному тексті.

Чому не чекати своєї черги
---------------------------
Черга strict-верифікатора: 242 953 документи, із них 166 554 перевірено
до 08.09. Наші 19 лежать у цій масі й набирали б вердикт дні. Тому
для них робиться дві точенкові дії:

  1) `probe` із пріоритетом 200 — перезавантажує PDF, перепарсовує його
     і записує свіжий `extra.structure` та 12 000 символів тексту.
  2) Видаляється застарілий вердикт strict — щоб документ увійшов у
     групу «ніколи не перевірявся», яка в селекторі йде першою.

Межа
----
Документи з вже наявним strict `pass` НЕ чіпаються ніде. 14 наявних
придатних джерел залишаються недоторканими.

Дії торкаються лише рядків, перелічених у виводі. Масових операцій немає.
"""

import argparse
import asyncio

from harvester.config import get_settings
from harvester.core.scheduler import Scheduler
from harvester.db.failover import build_database

# Пріоритет вище за будь-який плановий (probe з seeding = 5).
REVERIFY_PRIORITY = 200

# Сильні маркери теми: документ точно про неповнолітню особу.
STRONG = ("неповноліт", "малоліт", "дієздатн", "правоздатн", "підліт",
          "дитин", "опік", "піклуван")

# Основний фільтр — УДК, бо це сигнал дисципліни, а не слова в назві.
# 347 = цивільне право України; 347.155 — договірне право (зафіксовано
# на «УМОВИ ТА ПОРЯДОК НАДАННЯ НЕПОВНОЛІТНІМ ПОВНОЇ ЦИВІЛЬНОЇ ДІЄЗДАТНОСТІ»).
# Заповнений у 216 828 документів, тож це надійніший критерій.
#
# Назва відсікає те, що цивільне право, але не про неповнолітніх:
# без нього під відбір потрапляє 1390 документів (страхове право, спадкове,
# житлове), з ним — 42, з них 24 уже з pass.
UDC_PREFIX = "347%"

# Мова: рішення власника — тільки українські джерела. Мовний фільтр у
# verify уже відсікає російські, але у старіших рядках lang_confidence
# міг бути низьким, тому звужуємо і тут.
LANGUAGE = "uk"


def _topic_sql() -> tuple[str, list[str]]:
    """WHERE-фрагмент «цивільне право + неповнолітні» + параметри."""
    parts = ["(lower(d.title) LIKE ?)" for _ in STRONG]
    params = [f"%{w}%" for w in STRONG]
    return "(" + " OR ".join(parts) + ")", params


SELECT_SQL = """
SELECT d.id, d.title, d.udc, d.status,
       substr(COALESCE(d.verified_at,''),1,10) AS verified,
       COALESCE(vr.status, 'БЕЗ ВЕРДИКТУ') AS strict,
       left(COALESCE(vr.comment,''), 70) AS comment
FROM documents d
LEFT JOIN verifier_results vr ON vr.document_id = d.id AND vr.profile = 'strict'
WHERE d.status = 'verified'
  AND d.udc LIKE ?
  AND d.language = ?
  AND {topic}
  AND COALESCE(vr.status, '') != 'pass'
ORDER BY d.verified_at DESC
"""

async def main() -> int:
    ap = argparse.ArgumentParser(
        description="Повторна верифікація тематичних правових документів")
    ap.add_argument("--apply", action="store_true",
                    help="виконати (без — лише показати)")
    args = ap.parse_args()

    topic, topic_params = _topic_sql()
    db = build_database()
    await db.initialize()

    params = (UDC_PREFIX, LANGUAGE, *topic_params)
    rows = await db.fetchall(SELECT_SQL.format(topic=topic), params)
    print(f"Документів до повторної перевірки: {len(rows)}")
    print(f"(фільтр: udc LIKE '{UDC_PREFIX}', language='{LANGUAGE}', "
          f"маркери в назві, без strict pass)\n")
    for r in rows:
        print(f"  id={r['id']:<9} {r['strict']:<13} УДК {r['udc'] or '-':<22} "
              f"verified={r['verified'] or '?'}")
        print(f"      {(r['title'] or '(без назви)')[:92]}")
        if r["comment"]:
            print(f"      ↳ {r['comment']}")

    if not args.apply:
        print("\n[DRY-RUN] Нічого не змінено. Додайте --apply.")
        await db.close()
        return 0

    # Крок 1: прибрати застарілий вердикт strict, щоб документ потрапив
    # у групу «ніколи не перевірявся» — вона в селекторі йде першою.
    ids = [r["id"] for r in rows]
    placeholders = ",".join("?" * len(ids))
    deleted = await db.execute(
        f"DELETE FROM verifier_results WHERE profile = 'strict' "
        f"AND document_id IN ({placeholders})",
        tuple(ids),
    )

    # Крок 2: перезавантажити PDF і перепарсити — інакше LLM знову
    # побачить старе коротке text_sample і без структури.
    scheduler = Scheduler(db)
    queued = 0
    for doc_id in ids:
        task_id = await scheduler.schedule_task(
            "probe", {"document_id": doc_id}, priority=REVERIFY_PRIORITY
        )
        if task_id:
            queued += 1

    print(f"\nВидалено старих strict-вердиктів: {len(ids) if deleted is not None else 0}")
    print(f"Задач probe у черзі з пріоритетом {REVERIFY_PRIORITY}: {queued}")
    print(f"llm_max_chars, який побачить вердиктор: "
          f"{get_settings().verifier.llm_max_chars}")
    print("\nВідкат: вердикт перераховується автоматично; probe-задачі")
    print("зникають після виконання. Документи з pass не змінювалися.")
    await db.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
