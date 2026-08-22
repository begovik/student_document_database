#!/usr/bin/env python3
"""
extract_from_catalog.py — запускає витяг цитат і сумаризацій для документів з каталогу.

Використання:
    python extract_from_catalog.py catalogs/catalog_076.json
    python extract_from_catalog.py catalogs/catalog_076.json --limit 5
    python extract_from_catalog.py catalogs/catalog_076.json --dry-run
"""

import asyncio
import json
import sys
from pathlib import Path

# Додаємо кореневу директорію в sys.path
sys.path.insert(0, str(Path(__file__).parent.parent))

from harvester.config import get_settings
from harvester.db.failover import build_database
from harvester.extract.engine import ExtractionJob, ExtractionResult, process_document
from harvester.extract.cli import get_documents_to_process, save_results


async def main(catalog_path: str, limit: int = 30, dry_run: bool = False):
    """Основна функція для обробки каталогу."""
    settings = get_settings()
    db = build_database(settings)
    await db.initialize(sync_mirror=False)

    try:
        # 1. Завантажити каталог
        with open(catalog_path, 'r', encoding='utf-8') as f:
            catalog = json.load(f)

        docs = catalog.get('documents', [])
        if not docs:
            print("Каталог порожній або не має документів.")
            return

        print(f"📚 Завантажено {len(docs)} документів з каталогу {catalog_path}")
        print(f"🔍 Запит: {catalog.get('query', 'невідомий')}")

        # 2. Фільтрувати документи з URL
        docs_with_url = [d for d in docs if d.get('canonical_url') and d['canonical_url'].strip()]
        print(f"✅ Документи з URL: {len(docs_with_url)} з {len(docs)}")

        # 3. Обмежити за лімітом
        if limit and limit < len(docs_with_url):
            docs_with_url = docs_with_url[:limit]
            print(f"📝 Обмежено до {limit} документів")

        # 4. Створити завдання
        jobs = [
            ExtractionJob(
                document_id=d['id'],
                canonical_url=d['canonical_url'],
                title=d.get('title', ''),
            )
            for d in docs_with_url
        ]

        # 5. Обробити документи
        print(f"\n🚀 Починаю обробку {len(jobs)} документів...")
        print("=" * 80)

        results: list[ExtractionResult] = []
        semaphore = asyncio.Semaphore(3)  # Обмежуємо паралельність

        async def process_with_semaphore(job: ExtractionJob) -> ExtractionResult:
            async with semaphore:
                return await process_document(job)

        # Обробляємо документи по черзі з обмеженою паралельністю
        for i, job in enumerate(jobs, 1):
            print(f"\n[{i}/{len(jobs)}] Обробляю документ #{job.document_id}: {job.title[:60]}...")
            try:
                result = await process_with_semaphore(job)
                results.append(result)
                if result.success:
                    print(f"  ✅ Успішно: {len(result.quotations)} цитат, сумаризація: {'є' if result.summary else 'немає'}")
                else:
                    print(f"  ❌ Помилка: {result.error}")
            except Exception as e:
                print(f"  ❌ Критична помилка: {e}")
                results.append(ExtractionResult(
                    document_id=job.document_id,
                    canonical_url=job.canonical_url,
                    success=False,
                    error=str(e),
                ))

        # 6. Зберегти результати
        if not dry_run:
            saved = await save_results(db, results)
            print(f"\n✅ Збережено {saved} результатів у базу даних")
        else:
            print("\n📝 Dry-run: результати не збережено")

        # 7. Показати статистику
        successful = [r for r in results if r.success]
        failed = [r for r in results if not r.success]

        print(f"\n📊 Статистика:")
        print(f"  Всього оброблено: {len(results)}")
        print(f"  Успішно: {len(successful)}")
        print(f"  Помилки: {len(failed)}")

        if successful:
            total_quotes = sum(len(r.quotations) for r in successful)
            print(f"  Цитат знайдено: {total_quotes}")
            with_summary = sum(1 for r in successful if r.summary is not None)
            print(f"  Має сумаризацію: {with_summary}")

        if failed:
            print("\n❌ Документи з помилками:")
            for r in failed[:5]:  # показати перші 5
                print(f"  [{r.document_id}] {r.error}")

        # 8. Показати детальні результати (перші 3 документа)
        if successful:
            print("\n" + "=" * 80)
            print("РЕЗУЛЬТАТИ (перші 3 документа):")
            print("=" * 80)
            for r in successful[:3]:
                print(f"\n📄 Документ #{r.document_id}")
                print(f"  URL: {r.canonical_url}")
                print(f"  Цитат: {len(r.quotations)}")
                if r.quotations:
                    for q in r.quotations[:3]:  # показати перші 3 цитати
                        print(f"    [{q.get('page', '?')}p] ({q.get('type', 'unknown')}) {q.get('text', '')[:100]}...")
                if r.summary:
                    s = r.summary
                    print(f"  Сумаризація:")
                    print(f"    Огляд: {s.get('overview', '')[:150]}")
                    if s.get('key_ideas'):
                        print(f"    Ідеї: {', '.join(s.get('key_ideas', [])[:3])}")

    finally:
        await db.close()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Витяг цитат з каталогу")
    parser.add_argument("catalog", help="Шлях до JSON-каталогу")
    parser.add_argument("--limit", type=int, default=30, help="Максимум документів")
    parser.add_argument("--dry-run", action="store_true", help="Не зберігати результати")
    args = parser.parse_args()

    asyncio.run(main(args.catalog, args.limit, args.dry_run))
