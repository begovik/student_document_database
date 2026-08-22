#!/usr/bin/env python3
"""
extract_from_catalog.py — витяг цитат і сумаризацій для документів з каталогу
та запис результатів назад у JSON-файл каталогу.

Каталог може бути:
    - Файл: catalogs/catalog_YYYYMMDD_HHMMSS.json
    - Папка: catalogs/catalog_YYYYMMDD_HHMMSS/ (всередині лежить catalog_YYYYMMDD_HHMMSS.json
      і папка resources/ з PDF-файлами)

Поведінка:
    - документи, які вже є в таблиці extractions, беруться з БД (без LLM-викликів);
    - решта проходить повний цикл: завантаження PDF -> LLM -> збереження в БД;
    - успішні елементи отримують ключі "quotations" та "summary";
    - проблемні елементи отримують ключ "error" зі змістом помилки
      (або стандартний текст, якщо зміст відсутній).

Використання:
    python extract_from_catalog.py catalogs/catalog_076.json
    python extract_from_catalog.py catalogs/catalog_076.json --limit 5
    python extract_from_catalog.py catalogs/catalog_076.json --dry-run
    python extract_from_catalog.py catalogs/catalog_076.json --force
    python extract_from_catalog.py catalogs/catalog_YYYYMMDD_HHMMSS/  # папкова структура
    python extract_from_catalog.py catalogs/catalog_YYYYMMDD_HHMMSS/catalog_YYYYMMDD_HHMMSS.json  # файл у папці
"""

import asyncio
import json
import os
import sys
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).parent.parent))

from harvester.config import get_settings
from harvester.db.failover import build_database
from harvester.db.repositories import ExtractionsRepository
from harvester.extract.engine import ExtractionJob, ExtractionResult, process_document

STANDARD_ERROR_TEXT = "Помилка витягу даних: деталі недоступні"
NO_URL_ERROR_TEXT = "Помилка витягу: у документа відсутній canonical_url"
CONCURRENCY = 3


def resolve_catalog_path(user_path: str) -> tuple[str, Path | None]:
    """Розв'язати шлях до каталогу: якщо це папка — знайти JSON всередині.

    Returns (catalog_json_path, resources_dir).
    resources_dir — це шлях до папки resources (None якщо каталог файл).
    """
    path = Path(user_path)
    if path.is_dir():
        catalog_json = path / f"{path.name}.json"
        if not catalog_json.exists():
            raise FileNotFoundError(f"JSON-файл каталогу не знайдено у папці: {catalog_json}")
        resources_dir = path / "resources"
        return str(catalog_json), resources_dir if resources_dir.exists() else None
    else:
        return str(path), None


async def load_extractions_from_db(repo: ExtractionsRepository) -> dict[int, dict[str, Any]]:
    """Завантажити всі наявні витяги з БД: {document_id: {quotations, summary}}."""
    rows = await repo.db.fetchall("SELECT document_id, quotations, summary FROM extractions")
    result: dict[int, dict[str, Any]] = {}
    for row in rows:
        doc_id = row["document_id"]
        quotations = json.loads(row["quotations"]) if row["quotations"] else []
        summary = json.loads(row["summary"]) if row["summary"] else None
        result[doc_id] = {"quotations": quotations, "summary": summary}
    return result


async def process_missing(
    jobs: list[ExtractionJob],
) -> list[ExtractionResult]:
    """Обробити відсутні документи з обмеженою паралельністю."""
    semaphore = asyncio.Semaphore(CONCURRENCY)

    async def worker(job: ExtractionJob) -> ExtractionResult:
        async with semaphore:
            print(f"  🌐 Витяг з LLM: #{job.document_id} {job.title[:60]}...")
            try:
                return await process_document(job)
            except Exception as e:
                return ExtractionResult(
                    document_id=job.document_id,
                    canonical_url=job.canonical_url,
                    success=False,
                    error=str(e),
                )

    return list(await asyncio.gather(*(worker(j) for j in jobs)))


def apply_results_to_catalog(
    catalog: dict[str, Any],
    db_data: dict[int, dict[str, Any]],
    fresh_results: list[ExtractionResult],
) -> tuple[int, int]:
    """Записати quotations/summary/error безпосередньо в елементи каталогу.

    Повертає (успішних, з помилками).
    """
    fresh_by_id = {
        r.document_id: {"success": r.success, "error": r.error}
        for r in fresh_results
    }

    ok_count = err_count = 0

    for doc in catalog.get("documents", []):
        doc_id = doc.get("id")
        if doc_id is None:
            continue

        # 1. Дані вже були в БД
        data = db_data.get(doc_id)
        if data is not None:
            doc.pop("error", None)
            doc["quotations"] = data["quotations"]
            doc["summary"] = data["summary"]
            ok_count += 1
            continue

        # 2. Свіжий результат цього запуску
        fresh = fresh_by_id.get(doc_id)
        if fresh is not None:
            if fresh["success"]:
                result = next(r for r in fresh_results if r.document_id == doc_id)
                doc.pop("error", None)
                doc["quotations"] = result.quotations
                doc["summary"] = result.summary
                ok_count += 1
            else:
                doc["error"] = fresh["error"] or STANDARD_ERROR_TEXT
                err_count += 1
            continue

        # 3. Ні в БД, ні в результатах (наприклад, dry-run або немає URL)
        if not doc.get("canonical_url"):
            doc["error"] = NO_URL_ERROR_TEXT
            err_count += 1

    return ok_count, err_count


def save_catalog_atomic(catalog_path: str, catalog: dict[str, Any]) -> None:
    """Атомарний запис каталогу: спочатку у тимчасовий файл, потім rename."""
    path_obj = Path(catalog_path)
    if path_obj.is_dir():
        # Папкова структура: записати JSON у папку
        json_path = path_obj / f"{path_obj.name}.json"
        dir_path = str(path_obj)
    else:
        # Файловий формат: записати у сам файл
        json_path = path_obj
        dir_path = str(path_obj.parent)
    
    fd, tmp_path = tempfile.mkstemp(suffix=".json", dir=dir_path)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(catalog, f, ensure_ascii=False, indent=2)
            f.write("\n")
        os.replace(tmp_path, str(json_path))
    except BaseException:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)
        raise


async def main(
    catalog_path: str,
    limit: int | None = None,
    dry_run: bool = False,
    force: bool = False,
) -> None:
    settings = get_settings()
    
    # Розв'язати шлях до каталогу (підтримка папки та файлу)
    catalog_json_path, resources_dir = resolve_catalog_path(catalog_path)
    
    db = build_database(settings)
    await db.initialize(sync_mirror=False)

    try:
        repo = ExtractionsRepository(db)

        with open(catalog_json_path, "r", encoding="utf-8") as f:
            catalog = json.load(f)

        docs = catalog.get("documents", [])
        if not docs:
            print("Каталог порожній або не має документів.")
            return

        print(f"📚 Завантажено {len(docs)} документів з каталогу {catalog_json_path}")
        print(f"🔍 Запит: {catalog.get('topic', 'невідомий')}")

        # 1. Вже витягнуті дані з БД
        db_data = {} if force else await load_extractions_from_db(repo)
        print(f"🗄️  У БД вже є витяги: {len(db_data)}")

        # 2. Документи, яких немає в БД і які мають URL
        todo = [
            d for d in docs
            if d.get("id") not in db_data
            and d.get("canonical_url")
            and d["canonical_url"].strip()
        ]
        skipped_no_url = sum(
            1 for d in docs
            if d.get("id") not in db_data
            and not (d.get("canonical_url") or "").strip()
        )
        print(f"🆕 Потрібен новий витяг: {len(todo)} (без URL: {skipped_no_url})")

        if limit is not None and limit < len(todo):
            allowed_ids = {d["id"] for d in todo[:limit]}
            todo = [d for d in todo if d["id"] in allowed_ids]
            print(f"📝 Обмежено до {limit} нових документів")

        jobs = []
        for d in todo:
            pdf_path = None
            if resources_dir and d.get("pdf_path"):
                # Використовувати локальний PDF з папки каталогу
                local_pdf = resources_dir / d["pdf_path"]
                if local_pdf.exists():
                    pdf_path = str(local_pdf)
                else:
                    # Спроба знайти PDF за іменем файлу в resources/
                    alt_pdf = resources_dir / f"{d['id']}.pdf"
                    if alt_pdf.exists():
                        pdf_path = str(alt_pdf)
                    else:
                        print(f"  📄 Локальний PDF для #{d['id']} не знайдено, буде завантажено за URL")
            
            jobs.append(ExtractionJob(
                document_id=d["id"],
                canonical_url=d.get("canonical_url") or "",
                title=d.get("title") or "",
                pdf_path=pdf_path,
            ))

        # 3. Обробка нових документів
        fresh_results: list[ExtractionResult] = []
        if jobs and not dry_run:
            print(f"\n🚀 Витягую {len(jobs)} нових документів...")
            fresh_results = await process_missing(jobs)

            successful = [r for r in fresh_results if r.success]
            failed = [r for r in fresh_results if not r.success]

            if successful:
                saved = await save_results_to_db(repo, successful)
                print(f"\n💾 Збережено в БД: {saved}")
        elif dry_run:
            print("\n📝 Dry-run: нові документи не обробляються")

        # 4. Записати все назад у JSON-каталог
        ok_count, err_count = apply_results_to_catalog(catalog, db_data, fresh_results)

        if not dry_run:
            save_catalog_atomic(catalog_path, catalog)
            print(f"\n✍️  Каталог оновлено: {catalog_path}")
        else:
            print(f"\n📝 Dry-run: каталог НЕ перезаписано (було б: ok={ok_count}, errors={err_count})")

        # 5. Статистика
        print(f"\n📊 Статистика каталогу:")
        print(f"  Всього документів: {len(docs)}")
        print(f"  З цитатами/сумаризацією: {ok_count}")
        print(f"  З помилками: {err_count}")

        total_quotes = sum(
            len(d.get("quotations") or []) for d in docs
        )
        print(f"  Цитат всього: {total_quotes}")

        errors = [d for d in docs if d.get("error")]
        if errors:
            print(f"\n❌ Елементи з помилками:")
            for d in errors:
                print(f"  [{d['id']}] {d.get('title', '')[:60]}")
                print(f"       {d['error']}")

    finally:
        await db.close()


async def save_results_to_db(repo: ExtractionsRepository, results: list[ExtractionResult]) -> int:
    """Атомарно зберегти успішні результати в БД."""
    saved = 0
    async with repo.db.transaction():
        for r in results:
            await repo.upsert(
                document_id=r.document_id,
                quotations=r.quotations,
                summary=r.summary,
            )
            saved += 1
    return saved


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Витяг цитат з каталогу + запис у JSON")
    parser.add_argument("catalog", help="Шлях до JSON-каталогу")
    parser.add_argument("--limit", type=int, default=None, help="Максимум НОВИХ документів")
    parser.add_argument("--dry-run", action="store_true", help="Нічого не змінювати")
    parser.add_argument("--force", action="store_true", help="Ігнорувати наявні витяги в БД")
    args = parser.parse_args()

    started = datetime.now()
    asyncio.run(main(args.catalog, args.limit, args.dry_run, args.force))
    print(f"\n⏱️  Час виконання: {(datetime.now() - started).total_seconds():.1f}s")
