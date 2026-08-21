#!/usr/bin/env python3
"""
catalog_generator.py — генерує JSON-каталоги з бази даних Harvester.

Використання:
    python catalog_generator.py --full          # повний каталог (всі документи з title)
    python catalog_generator.py --no-authors    # каталог без авторів
    python catalog_generator.py --topic "Підприємництво" --limit 30  # 30 джерел по темі
    python catalog_generator.py --topic-code trade --limit 30         # 30 джерел по коду топіка

Каталоги зберігаються у директорії catalogs/ (створити вручну).
"""

import argparse
import asyncio
import json
import sys
from pathlib import Path

from harvester.config import get_settings
from harvester.db.failover import build_database


async def generate_full_catalog(db, limit: int = 200000) -> list[dict]:
    """Повний каталог: усі документи з title."""
    r = db.remote
    if r is None:
        return []

    query = """
        SELECT
            d.id, d.title, d.authors, d.year, d.publisher, d.doi, d.canonical_url,
            d.landing_url, d.language, d.doc_type, d.udc, d.isbn, d.is_oa,
            d.oa_status, d.page_count, d.size_bytes, d.verified_at, d.first_seen_at,
            d.sha256, d.has_text_layer,
            COALESCE(
                (SELECT json_agg(
                    json_build_object(
                        'topic_id', t.id,
                        'topic_name', t.name_uk,
                        'score', dt.score
                    ) ORDER BY dt.score DESC)
                 FROM document_topics dt
                 JOIN topics t ON t.id = dt.topic_id
                 WHERE dt.document_id = d.id),
                '[]'
            ) AS topics_json
        FROM documents d
        WHERE d.title IS NOT NULL AND d.title != ''
        ORDER BY d.id
        LIMIT ?
    """

    rows = await r.fetchall(query, (limit,))
    docs = []
    for row in rows:
        topics = json.loads(row["topics_json"]) if row["topics_json"] else []
        docs.append({
            "id": row["id"],
            "title": row["title"],
            "authors": row["authors"] if row["authors"] and row["authors"] != "None" else None,
            "year": row["year"],
            "publisher": row["publisher"] if row["publisher"] and row["publisher"] != "None" else None,
            "doi": row["doi"],
            "canonical_url": row["canonical_url"],
            "landing_url": row["landing_url"],
            "language": row["language"],
            "doc_type": row["doc_type"],
            "udc": row["udc"],
            "isbn": row["isbn"],
            "is_oa": row["is_oa"],
            "oa_status": row["oa_status"],
            "page_count": row["page_count"],
            "size_bytes": row["size_bytes"],
            "verified_at": row["verified_at"],
            "first_seen_at": row["first_seen_at"],
            "sha256": row["sha256"],
            "has_text_layer": row["has_text_layer"],
            "topics": topics,
        })
    return docs


async def generate_no_authors_catalog(db, limit: int = 200000) -> list[dict]:
    """Каталог документів без авторів (підмножина повного каталогу)."""
    full = await generate_full_catalog(db, limit)
    return [d for d in full if not d["authors"]]


async def generate_topic_catalog(db, topic_code: str | None = None, topic_name: str | None = None,
                                limit: int = 30) -> list[dict]:
    """Каталог документів по темі (20 для 'Підприємництво, торгівля та біржова діяльність').

    Шукає документи за:
    1. topic_code (якщо вказано і топік є в базі)
    2. УДК-префікси (334.7, 339.1, 339.3, 339.5, 336.76) — для теми 076
    3. topic_name (пошук по назві)
    """
    r = db.remote
    if r is None:
        return []

    docs = []
    seen_ids = set()

    # 1. Пошук по topic_code
    if topic_code:
        query = """
            SELECT
                d.id, d.title, d.authors, d.year, d.publisher, d.doi, d.canonical_url,
                d.landing_url, d.language, d.doc_type, d.udc, d.isbn, d.is_oa,
                d.oa_status, d.page_count, d.size_bytes, d.verified_at, d.first_seen_at,
                d.sha256, d.has_text_layer,
                COALESCE(
                    (SELECT json_agg(
                        json_build_object(
                            'topic_id', t.id,
                            'topic_name', t.name_uk,
                            'score', dt.score
                        ) ORDER BY dt.score DESC)
                     FROM document_topics dt
                     JOIN topics t ON t.id = dt.topic_id
                     WHERE dt.document_id = d.id),
                    '[]'
                ) AS topics_json
            FROM documents d
            WHERE d.id IN (
                SELECT dt.document_id FROM document_topics dt
                JOIN topics t ON t.id = dt.topic_id
                WHERE t.code = ?
            )
            AND d.title IS NOT NULL AND d.title != ''
            ORDER BY d.year DESC NULLS LAST, d.id
            LIMIT ?
        """
        rows = await r.fetchall(query, (topic_code, limit * 2))
        for row in rows:
            if row["id"] not in seen_ids:
                seen_ids.add(row["id"])
                topics = json.loads(row["topics_json"]) if row["topics_json"] else []
                docs.append({
                    "id": row["id"],
                    "title": row["title"],
                    "authors": row["authors"] if row["authors"] and row["authors"] != "None" else None,
                    "year": row["year"],
                    "publisher": row["publisher"] if row["publisher"] and row["publisher"] != "None" else None,
                    "doi": row["doi"],
                    "canonical_url": row["canonical_url"],
                    "landing_url": row["landing_url"],
                    "language": row["language"],
                    "doc_type": row["doc_type"],
                    "udc": row["udc"],
                    "isbn": row["isbn"],
                    "is_oa": row["is_oa"],
                    "oa_status": row["oa_status"],
                    "page_count": row["page_count"],
                    "size_bytes": row["size_bytes"],
                    "verified_at": row["verified_at"],
                    "first_seen_at": row["first_seen_at"],
                    "sha256": row["sha256"],
                    "has_text_layer": row["has_text_layer"],
                    "topics": topics,
                })

    # 2. Пошук по УДК-префіксах (для теми 076: 334.7, 339.1, 339.3, 339.5, 336.76)
    udc_prefixes = [
        "334.7%", "334.72%", "339.1%", "339.3%", "339.5%", "336.76%",
        "334.722%", "339.13%", "339.132%", "339.138%", "339.564%",
        "334.72%", "339.138%", "339.56%", "336.76%", "334.722%",
    ]
    if not topic_code or topic_code in ("trade", "076"):
        for prefix in udc_prefixes:
            if len(docs) >= limit:
                break
            query = """
                SELECT
                    d.id, d.title, d.authors, d.year, d.publisher, d.doi, d.canonical_url,
                    d.landing_url, d.language, d.doc_type, d.udc, d.isbn, d.is_oa,
                    d.oa_status, d.page_count, d.size_bytes, d.verified_at, d.first_seen_at,
                    d.sha256, d.has_text_layer,
                    COALESCE(
                        (SELECT json_agg(
                            json_build_object(
                                'topic_id', t.id,
                                'topic_name', t.name_uk,
                                'score', dt.score
                            ) ORDER BY dt.score DESC)
                         FROM document_topics dt
                         JOIN topics t ON t.id = dt.topic_id
                         WHERE dt.document_id = d.id),
                        '[]'
                    ) AS topics_json
                FROM documents d
                WHERE d.udc LIKE ? AND d.title IS NOT NULL AND d.title != ''
                  AND d.id NOT IN (SELECT unnest(?::int[]))
                ORDER BY d.year DESC NULLS LAST, d.id
                LIMIT ?
            """
            rows = await r.fetchall(query, (prefix, list(seen_ids), (limit * 2) - len(docs)))
            for row in rows:
                if row["id"] not in seen_ids:
                    seen_ids.add(row["id"])
                    topics = json.loads(row["topics_json"]) if row["topics_json"] else []
                    docs.append({
                        "id": row["id"],
                        "title": row["title"],
                        "authors": row["authors"] if row["authors"] and row["authors"] != "None" else None,
                        "year": row["year"],
                        "publisher": row["publisher"] if row["publisher"] and row["publisher"] != "None" else None,
                        "doi": row["doi"],
                        "canonical_url": row["canonical_url"],
                        "landing_url": row["landing_url"],
                        "language": row["language"],
                        "doc_type": row["doc_type"],
                        "udc": row["udc"],
                        "isbn": row["isbn"],
                        "is_oa": row["is_oa"],
                        "oa_status": row["oa_status"],
                        "page_count": row["page_count"],
                        "size_bytes": row["size_bytes"],
                        "verified_at": row["verified_at"],
                        "first_seen_at": row["first_seen_at"],
                        "sha256": row["sha256"],
                        "has_text_layer": row["has_text_layer"],
                        "topics": topics,
                    })

    # 3. Пошук по topic_name (заповнює, якщо топік не знайдено)
    if (not topic_code or topic_code not in ("trade", "076")) and len(docs) < limit:
        if topic_name:
            query_text = f"%{topic_name}%"
            query = """
                SELECT
                    d.id, d.title, d.authors, d.year, d.publisher, d.doi, d.canonical_url,
                    d.landing_url, d.language, d.doc_type, d.udc, d.isbn, d.is_oa,
                    d.oa_status, d.page_count, d.size_bytes, d.verified_at, d.first_seen_at,
                    d.sha256, d.has_text_layer,
                    COALESCE(
                        (SELECT json_agg(
                            json_build_object(
                                'topic_id', t.id,
                                'topic_name', t.name_uk,
                                'score', dt.score
                            ) ORDER BY dt.score DESC)
                         FROM document_topics dt
                         JOIN topics t ON t.id = dt.topic_id
                         WHERE dt.document_id = d.id),
                        '[]'
                    ) AS topics_json
                FROM documents d
                WHERE (d.title LIKE ? OR d.authors LIKE ? OR d.publisher LIKE ?)
                  AND d.title IS NOT NULL AND d.title != ''
                  AND d.id NOT IN (SELECT unnest(?)::int[])
                ORDER BY d.year DESC NULLS LAST, d.id
                LIMIT ?
            """
            rows = await r.fetchall(query, (query_text, query_text, query_text, list(seen_ids), (limit * 2) - len(docs)))
            for row in rows:
                if row["id"] not in seen_ids:
                    seen_ids.add(row["id"])
                    topics = json.loads(row["topics_json"]) if row["topics_json"] else []
                    docs.append({
                        "id": row["id"],
                        "title": row["title"],
                        "authors": row["authors"] if row["authors"] and row["authors"] != "None" else None,
                        "year": row["year"],
                        "publisher": row["publisher"] if row["publisher"] and row["publisher"] != "None" else None,
                        "doi": row["doi"],
                        "canonical_url": row["canonical_url"],
                        "landing_url": row["landing_url"],
                        "language": row["language"],
                        "doc_type": row["doc_type"],
                        "udc": row["udc"],
                        "isbn": row["isbn"],
                        "is_oa": row["is_oa"],
                        "oa_status": row["oa_status"],
                        "page_count": row["page_count"],
                        "size_bytes": row["size_bytes"],
                        "verified_at": row["verified_at"],
                        "first_seen_at": row["first_seen_at"],
                        "sha256": row["sha256"],
                        "has_text_layer": row["has_text_layer"],
                        "topics": topics,
                    })

    return docs[:limit]


async def main():
    parser = argparse.ArgumentParser(description="Генератор каталогів Harvester")
    parser.add_argument("--full", action="store_true", help="Повний каталог")
    parser.add_argument("--no-authors", action="store_true", help="Каталог без авторів")
    parser.add_argument("--topic", type=str, default=None, help="Тематика (наприклад, 'Підприємництво, торгівля та біржова діяльність')")
    parser.add_argument("--topic-code", type=str, default=None, help="Код топіка (наприклад, 'trade', '076')")
    parser.add_argument("--limit", type=int, default=30, help="Максимальна кількість документів")
    parser.add_argument("--output", type=Path, default=None, help="Шлях для збереження JSON (за замовчуванням catalogs/)")
    parser.add_argument("--batch-size", type=int, default=200000, help="Ліміт вибірки для повного каталогу")

    args = parser.parse_args()

    settings = get_settings()
    db = build_database(settings)
    await db.initialize(sync_mirror=False)

    try:
        output_dir = args.output or Path("catalogs")
        output_dir.mkdir(parents=True, exist_ok=True)

        if args.full:
            docs = await generate_full_catalog(db, args.batch_size)
            path = output_dir / "catalog_full.json"
            with open(path, "w", encoding="utf-8") as f:
                json.dump(docs, f, ensure_ascii=False, indent=2)
            print(f"[+] Збережено {len(docs)} документів у {path}")

        if args.no_authors:
            docs = await generate_no_authors_catalog(db, args.batch_size)
            path = output_dir / "catalog_no_authors.json"
            with open(path, "w", encoding="utf-8") as f:
                json.dump(docs, f, ensure_ascii=False, indent=2)
            print(f"[+] Збережено {len(docs)} документів у {path}")

        if args.topic or args.topic_code:
            docs = await generate_topic_catalog(
                db,
                topic_code=args.topic_code,
                topic_name=args.topic,
                limit=args.limit,
            )
            code_str = args.topic_code or "custom"
            name_str = args.topic or "unknown"
            path = output_dir / f"catalog_{code_str}.json"
            with open(path, "w", encoding="utf-8") as f:
                json.dump(docs, f, ensure_ascii=False, indent=2)
            print(f"[+] Збережено {len(docs)} документів у {path}")

    finally:
        await db.close()


if __name__ == "__main__":
    asyncio.run(main())
