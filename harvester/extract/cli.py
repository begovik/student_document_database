"""CLI-команда 'extract' для витягу цитат і сумаризацій з документів.

Використовується як subcommand в основному CLI:
    harvester extract run --topic "Підприємництво" --limit 10
    harvester extract run --topic-code trade --limit 10
    harvester extract run --retry-failed

Або просто (без 'run'):
    harvester extract --topic "Підприємництво" --limit 10
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime
from typing import Any

import structlog
import typer

from harvester.config import get_settings
from harvester.db.failover import build_database
from harvester.db.repositories import ExtractionsRepository
from harvester.extract.engine import ExtractionJob, ExtractionResult, process_document

logger = structlog.get_logger()

extract_app = typer.Typer(
    name="extract",
    help="Витяг цитат і сумаризацій з PDF-документів",
    no_args_is_help=True,
)


@extract_app.command()
def run(
    topic: str | None = typer.Option(None, "--topic", "-t", help="Фільтрувати по назві теми (часткова підстрока)"),
    topic_code: str | None = typer.Option(None, "--topic-code", "-c", help="Фільтрувати по коду теми (наприклад, 'trade', '076')"),
    limit: int = typer.Option(30, "--limit", "-n", help="Максимальна кількість документів для обробки"),
    batch: int = typer.Option(5, "--batch", "-b", help="Кількість одночасних завдань"),
    dry_run: bool = typer.Option(False, "--dry-run", "-d", help="Не зберігати результати, лише показати що було б зроблено"),
    retry_failed: bool = typer.Option(False, "--retry-failed", "-r", help="Пере процесувати тільки документи з помилками (попередні спроби не вдалися)"),
    skip_extracted: bool = typer.Option(True, "--skip-extracted/--no-skip-extracted", help="Пропускати документи, для яких вже є дані в extractions"),
):
    """Запустити витяг цитат і сумаризацій для обраного набору документів.

    За замовчуванням обирає останні 30 джерел з каталогу,
    завантажує PDF, викликає LLM для пошуку цитат і сумаризації,
    зберігає результати в таблицю extractions.

    Приклади:
        harvester extract run --topic "Підприємництво" --limit 10
        harvester extract run --topic-code trade --limit 10
        harvester extract run --retry-failed
        harvester extract run --dry-run
    """
    asyncio.run(main(topic, topic_code, limit, batch, dry_run, retry_failed, skip_extracted))


async def main(
    topic: str | None,
    topic_code: str | None,
    limit: int,
    batch: int,
    dry_run: bool,
    retry_failed: bool,
    skip_extracted: bool,
) -> None:
    settings = get_settings()
    db = build_database(settings)
    await db.initialize(sync_mirror=False)
    repo = ExtractionsRepository(db)

    try:
        # 1. Отримати список документів для обробки
        docs = await get_documents_to_process(
            db, topic=topic, topic_code=topic_code,
            limit=limit, retry_failed=retry_failed,
            skip_extracted=skip_extracted,
        )

        if not docs:
            logger.info("no_documents_to_process")
            print("Немає документів для обробки.")
            return

        print(f"\n📚 Знайдено {len(docs)} документів для обробки")
        print("=" * 80)

        # 2. Створити завдання
        jobs = [
            ExtractionJob(
                document_id=d["id"],
                canonical_url=d["canonical_url"],
                title=d["title"] or "",
            )
            for d in docs
        ]

        # 3. Обробити по черзі (конкурентно, але не більше batch)
        results: list[ExtractionResult] = []
        semaphore = asyncio.Semaphore(batch)

        async def process_with_semaphore(job: ExtractionJob) -> ExtractionResult:
            async with semaphore:
                return await process_document(job)

        tasks = [process_with_semaphore(j) for j in jobs]
        results = await asyncio.gather(*tasks)

        # 4. Зберегти результати (якщо не dry-run)
        if not dry_run:
            saved = await save_results(repo, results)
            print(f"\n✅ Збережено {saved} результатів у базу даних")
        else:
            print("\n📝 Dry-run: результати не збережено")

        # 5. Показати статистику
        successful = [r for r in results if r.success]
        failed = [r for r in results if not r.success]

        print(f"\n📊 Статистика:")
        print(f"  Всього: {len(results)}")
        print(f"  Успішно: {len(successful)}")
        print(f"  Помилки: {len(failed)}")

        if successful:
            total_quotes = sum(len(r.quotations) for r in successful)
            print(f"  Цитат знайдено: {total_quotes}")
            with_summary = sum(1 for r in successful if r.summary is not None)
            print(f"  Має сумаризацію: {with_summary}")

        if failed:
            print("\n❌ Помилки:")
            for r in failed[:10]:  # показати перші 10
                print(f"  [{r.document_id}] {r.error}")

        # 6. Показати детальні результати (якщо не dry-run)
        if not dry_run and successful:
            print("\n" + "=" * 80)
            print("РЕЗУЛЬТАТИ:")
            print("=" * 80)
            for r in successful:
                print(f"\n📄 Документ #{r.document_id}")
                print(f"  URL: {r.canonical_url}")
                print(f"  Цитат: {len(r.quotations)}")
                if r.quotations:
                    for q in r.quotations[:5]:  # показати перші 5
                        print(f"    [{q.get('page', '?')}p] ({q.get('type', 'unknown')}) {q.get('text', '')[:100]}...")
                if r.summary:
                    s = r.summary
                    print(f"  Сумаризація:")
                    print(f"    Огляд: {s.get('overview', '')[:150]}")
                    print(f"    Ідеї: {', '.join(s.get('key_ideas', [])[:5])}")
                    if s.get('methodology') and s['methodology'] != 'н/зв':
                        print(f"    Методологія: {s['methodology'][:150]}")
                    if s.get('findings') and s['findings'] != 'н/зв':
                        print(f"    Результати: {s['findings'][:150]}")
                    if s.get('conclusions') and s['conclusions'] != 'н/зв':
                        print(f"    Висновки: {s['conclusions'][:150]}")

    finally:
        await db.close()


async def get_documents_to_process(
    db,
    topic: str | None = None,
    topic_code: str | None = None,
    limit: int = 30,
    retry_failed: bool = False,
    skip_extracted: bool = True,
) -> list[dict[str, Any]]:
    """Отримати список документів для обробки."""
    if retry_failed:
        query = """
            SELECT d.id, d.title, d.canonical_url, d.authors, d.year, d.udc, d.language, d.doc_type, d.verified_at
            FROM documents d
            LEFT JOIN extractions e ON e.document_id = d.id
            WHERE d.status = 'verified'
              AND (e.id IS NULL OR e.quotations IS NULL OR e.quotations = '[]')
              AND d.canonical_url IS NOT NULL
              AND d.canonical_url != ''
            ORDER BY d.verified_at DESC NULLS LAST, d.id DESC
            LIMIT ?
        """
        rows = await db.fetchall(query, (limit * 2,))
    else:
        query = """
            SELECT d.id, d.title, d.canonical_url, d.authors, d.year, d.udc, d.language, d.doc_type, d.verified_at
            FROM documents d
            LEFT JOIN extractions e ON e.document_id = d.id
            WHERE d.status = 'verified'
              AND d.canonical_url IS NOT NULL
              AND d.canonical_url != ''
              AND (? = 0 OR e.id IS NULL OR e.quotations IS NULL OR e.quotations = '[]')
            ORDER BY d.verified_at DESC NULLS LAST, d.id DESC
            LIMIT ?
        """
        rows = await db.fetchall(query, (0 if skip_extracted else 1, limit * 2,))

    if not rows:
        return []

    # Фільтрувати по темі (якщо вказано)
    if topic or topic_code:
        filtered = []
        for row in rows:
            doc_id = row["id"]
            topics_query = """
                SELECT t.name_uk, t.code
                FROM document_topics dt
                JOIN topics t ON t.id = dt.topic_id
                WHERE dt.document_id = ?
            """
            doc_topics = await db.fetchall(topics_query, (doc_id,))
            if not doc_topics:
                continue

            topic_names = [t["name_uk"] for t in doc_topics]
            topic_codes = [t["code"] for t in doc_topics]

            if topic and not any(topic.lower() in tn.lower() for tn in topic_names):
                continue
            if topic_code and topic_code not in topic_codes:
                continue

            filtered.append(row)
        rows = filtered

    # Обмежити
    if len(rows) > limit:
        rows = rows[:limit]

    return rows


async def save_results(
    repo: ExtractionsRepository,
    results: list[ExtractionResult],
) -> int:
    """Зберегти результати витягу в базу даних (атомарно)."""
    saved = 0
    successful_results = [r for r in results if r.success]

    async with repo.db.transaction():
        for r in successful_results:
            await repo.upsert(
                document_id=r.document_id,
                quotations=r.quotations,
                summary=r.summary,
            )
            saved += 1

    return saved
