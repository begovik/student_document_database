"""Етап 1: підготовка каталогу — відбір + завантаження PDF + запис JSON."""

from __future__ import annotations

import asyncio
import json
import os
import shutil
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any

import httpx
import structlog

from harvester.config import get_settings, get_filter_rules, FilterRules
from harvester.db.failover import build_database
from harvester.db.repositories import DocumentsRepository, TopicsRepository
from harvester.curator.availability import check_availability
from harvester.curator.selector import SelectionResult, call_llm_for_selection

logger = structlog.get_logger()

# Мінімальні вимоги до документа для відбору
REQUIRED_STATUS = "verified"
REQUIRED_FIELDS = {
    "title": "Назва має бути непорожньою",
    "authors": "Автори мають бути задані",
    "language": "Мова має бути визначена",
    "canonical_url": "Має бути визначений canonical_url",
    "page_count": "Має бути визначена кількість сторінок (не менше 3)",
    "has_text_layer": "Має бути текстовий шар",
}


def is_document_complete(doc: dict[str, Any], rules: FilterRules | None = None) -> tuple[bool, str | None]:
    """Перевірити, чи документ має повний набір даних і є повноцінним цілісним джерелом."""
    import re
    
    if rules is None:
        rules = get_filter_rules()
    
    min_page_count = rules.min_page_count
    min_chars_per_page = rules.min_chars_per_page
    
    REQUIRED_FIELDS = {
        "title": "Назва має бути непорожньою",
        "authors": "Автори мають бути задані",
        "language": "Мова має бути визначена",
        "canonical_url": "Має бути визначений canonical_url",
        "page_count": f"Має бути визначена кількість сторінок (не менше {min_page_count})",
        "has_text_layer": "Має бути текстовий шар",
    }
    if doc.get("status") != REQUIRED_STATUS:
        return False, f"status={doc.get('status')} (потрібно {REQUIRED_STATUS})"

    for field, reason in REQUIRED_FIELDS.items():
        value = doc.get(field)
        if field == "has_text_layer":
            if value is None or int(value) != 1:
                return False, f"{field}={value}"
        elif field == "page_count":
            if not value or int(value) < min_page_count:
                return False, f"page_count={value} (мінімум {min_page_count} сторінки для цілісного джерела)"
        elif value is None or value == "" or value == "None":
            return False, reason

    # Додаткові перевірки якості
    title = doc.get("title", "")
    if not title or len(title.strip()) < 10:
        return False, f"title слишком короткий ({len(title)} символів)"
    title_lower = title.lower()
    if ".docx" in title_lower or ".doc" in title_lower:
        return False, "title містить розширення файлу"
    if "microsoft word" in title_lower:
        return False, "title містить 'Microsoft Word'"
    # Перевірка що title містить хоча б 2 слова з літер
    words = re.findall(r'[a-zA-Zа-яА-ЯіІєЇїЄєҐёЁ]{2,}', title)
    if len(words) < 2:
        return False, f"title не містить слів (знайдено {len(words)})"
    # Відкидати якщо title містить ".mdi", "c--", "c-document" (windows garbage)
    if ".mdi" in title_lower or "c--documents" in title_lower:
        return False, f"title містить garbage-патерн"

    # Перевірка авторів
    authors = doc.get("authors", [])
    if not authors:
        return False, "автори відсутні"
    if isinstance(authors, list):
        # Відкидати списки з одного елементом якщо це ініціали (типу "Г.А.") 
        # або загальновідомі garbage-значення
        if len(authors) == 1:
            a = authors[0]
            if re.match(r'^[А-ЩЬьюЯ]{1,3}\.[А-ЩЬьюЯ]{1,3}\.*$', a):
                return False, f"автор '{a}' виглядає як ініціали (бракує прізвища)"
            if a in ("USER", "1", "Unknown", "service", "", "Admin", "Lena"):
                return False, f"автор '{a}' некоректний"
            # Якщо це одне слово коротке (<3 літери) - швидше за все це garbage
            if len(a) < 3 and not a[0].isupper():
                return False, f"автор '{a}' некоректний"
        # Відкидати якщо всі автори некоректні
        bad_authors = []
        for a in authors:
            if a in ("USER", "1", "Unknown", "service", "", "Admin"):
                bad_authors.append(a)
            elif re.match(r'^[А-ЩЬьюЯ]{1,3}\.[А-ЩЬьюЯ]{1,3}\.*$', a):
                bad_authors.append(a)
            elif len(a.strip()) < 2:
                bad_authors.append(a)
        if len(bad_authors) == len(authors):
            return False, f"всі автори некоректні: {bad_authors[:2]}"
        if len(bad_authors) > 0:
            # Якщо більшість авторів некоректні - відкидати
            if len(bad_authors) >= len(authors) * 0.5:
                return False, f"більшість авторів некоректні: {bad_authors[:2]}"
    else:
        # Якщо автори не список - намагаємось розібрати
        try:
            parsed = json.loads(authors)
            if isinstance(parsed, list):
                authors = parsed
            else:
                return False, "автори не є списком"
        except (json.JSONDecodeError, TypeError):
            return False, "автори некоректний формат"

    if doc.get("extra") and isinstance(doc.get("extra"), str):
        try:
            extra = json.loads(doc["extra"])
            if extra.get("curator", {}).get("unavailable_since"):
                return False, "позначений як недоступний"
        except (json.JSONDecodeError, TypeError):
            pass

    # === НОВІ ПЕРЕВІРКИ ЗА ПРАВИЛАМИ ===
    
    # Перевірка щільності тексту (мінімум 1500 знаків на сторінку)
    text_length = doc.get("text_length", 0) or 0
    page_count = doc.get("page_count", 1) or 1
    if text_length > 0 and page_count > 0:
        chars_per_page = text_length / page_count
        if chars_per_page < min_chars_per_page:
            return False, f"низька щільність тексту ({chars_per_page:.0f} знаків/стор, мінімум {min_chars_per_page})"
    
    # Відкидання презентацій PowerPoint
    if rules.reject_ppt:
        extra_data = doc.get("extra")
        if extra_data and isinstance(extra_data, str):
            try:
                extra = json.loads(extra_data)
                producer = extra.get("producer", "")
                if "powerpoint" in producer.lower() or "ppt" in producer.lower():
                    return False, "презентація PowerPoint"
            except (json.JSONDecodeError, TypeError):
                pass
        # Додаткова перевірка за назвою
        title_lower = doc.get("title", "").lower()
        if "презентація" in title_lower or "presentation" in title_lower:
            return False, "презентація за назвою"
    
    return True, None


async def find_topic_in_db(db, topic_name: str) -> dict[str, Any] | None:
    """Знайти тему в БД за назвою."""
    repo = TopicsRepository(db)
    topics = await repo.list_all()
    for t in topics:
        if topic_name.lower() in t.get("name_uk", "").lower():
            return t
    return None


async def get_candidates_for_topic(
    db,
    topic_name: str,
    topic_id: int | None = None,
    udc_prefixes: list[str] | None = None,
    limit: int = 200,
    rules: FilterRules | None = None,
) -> list[dict[str, Any]]:
    """Отримати кандидатів для теми з БД."""
    if rules is None:
        rules = get_filter_rules()
    
    min_page_count = rules.min_page_count
    repo = DocumentsRepository(db)

    if topic_id:
        rows = await repo.db.fetchall(
            f"""
            SELECT d.id, d.title, d.authors, d.year, d.publisher, d.doc_type,
                   d.canonical_url, d.language, d.udc, d.page_count, d.has_text_layer,
                   d.size_bytes, d.sha256, d.status, d.extra,
                   d.verified_at, d.first_seen_at,
                   dt.score as topic_score,
                   t.id as topic_id, t.name_uk as topic_name
            FROM documents d
            LEFT JOIN document_topics dt ON dt.document_id = d.id
            LEFT JOIN topics t ON t.id = dt.topic_id
            WHERE d.status = 'verified'
              AND d.title IS NOT NULL AND d.title != ''
              AND d.authors IS NOT NULL
              AND d.language IS NOT NULL
              AND d.canonical_url IS NOT NULL AND d.canonical_url != ''
              AND d.page_count >= {min_page_count}
              AND (d.has_text_layer = 1 OR d.has_text_layer IS NULL)
              AND d.id IN (
                  SELECT dt2.document_id
                  FROM document_topics dt2
                  JOIN topics t2 ON t2.id = dt2.topic_id
                  WHERE t2.id = ?
              )
              AND (d.extra IS NULL OR d.extra NOT LIKE '%"curator"%')
            ORDER BY dt.score DESC NULLS LAST, d.year DESC NULLS LAST
            LIMIT ?
            """,
            (topic_id, limit),
        )
    elif udc_prefixes:
        udc_conditions = " OR ".join(f"d.udc LIKE '{p}%'" for p in udc_prefixes)
        rows = await repo.db.fetchall(
            f"""
            SELECT d.id, d.title, d.authors, d.year, d.publisher, d.doc_type,
                   d.canonical_url, d.language, d.udc, d.page_count, d.has_text_layer,
                   d.size_bytes, d.sha256, d.status, d.extra,
                   d.verified_at, d.first_seen_at,
                   COALESCE(dt.score, 0) as topic_score,
                   t.id as topic_id, t.name_uk as topic_name
            FROM documents d
            LEFT JOIN document_topics dt ON dt.document_id = d.id
            LEFT JOIN topics t ON t.id = dt.topic_id
            WHERE d.status = 'verified'
              AND d.title IS NOT NULL AND d.title != ''
              AND d.authors IS NOT NULL
              AND d.language IS NOT NULL
              AND d.canonical_url IS NOT NULL AND d.canonical_url != ''
              AND d.page_count >= {min_page_count}
              AND (d.has_text_layer = 1 OR d.has_text_layer IS NULL)
              AND (
                  {udc_conditions}
              )
              AND (d.extra IS NULL OR d.extra NOT LIKE '%"curator"%')
            ORDER BY dt.score DESC NULLS LAST, d.year DESC NULLS LAST
            LIMIT ?
            """,
            (limit,),
        )
    else:
        rows = await repo.db.fetchall(
            f"""
            SELECT d.id, d.title, d.authors, d.year, d.publisher, d.doc_type,
                   d.canonical_url, d.language, d.udc, d.page_count, d.has_text_layer,
                   d.size_bytes, d.sha256, d.status, d.extra,
                   d.verified_at, d.first_seen_at,
                   COALESCE(dt.score, 0) as topic_score,
                   t.id as topic_id, t.name_uk as topic_name
            FROM documents d
            LEFT JOIN document_topics dt ON dt.document_id = d.id
            LEFT JOIN topics t ON t.id = dt.topic_id
            WHERE d.status = 'verified'
              AND d.title IS NOT NULL AND d.title != ''
              AND d.authors IS NOT NULL
              AND d.language IS NOT NULL
              AND d.canonical_url IS NOT NULL AND d.canonical_url != ''
              AND d.page_count >= {min_page_count}
              AND (d.has_text_layer = 1 OR d.has_text_layer IS NULL)
              AND (d.extra IS NULL OR d.extra NOT LIKE '%"curator"%')
            ORDER BY dt.score DESC NULLS LAST, d.year DESC NULLS LAST
            LIMIT ?
            """,
            (limit,),
        )

    candidates = []
    seen_ids = set()
    for row in rows:
        if row["id"] in seen_ids:
            continue
        seen_ids.add(row["id"])
        candidates.append({
            "id": row["id"],
            "title": row["title"],
            "authors": json.loads(row["authors"]) if row["authors"] else [],
            "year": row["year"],
            "publisher": row["publisher"],
            "doc_type": row["doc_type"],
            "canonical_url": row["canonical_url"],
            "language": row["language"],
            "udc": row["udc"],
            "page_count": row["page_count"],
            "topic_score": float(row["topic_score"]) if row["topic_score"] else 0.0,
            "topic_id": row["topic_id"],
            "topic_name": row["topic_name"],
            "has_text_layer": row["has_text_layer"],
            "status": row["status"],
            "size_bytes": row["size_bytes"],
            "sha256": row["sha256"],
            "verified_at": row["verified_at"],
            "first_seen_at": row["first_seen_at"],
            "extra": row["extra"],
        })

    return candidates


async def find_replacement(
    db,
    original_doc: dict[str, Any],
    selected_ids: set[int],
    topic_id: int | None = None,
    udc_prefixes: list[str] | None = None,
    rules: FilterRules | None = None,
) -> dict[str, Any] | None:
    """Знайти заміну для недоступного документа."""
    candidates = await get_candidates_for_topic(
        db,
        topic_name=original_doc.get("topic_name", ""),
        topic_id=topic_id,
        udc_prefixes=udc_prefixes,
        limit=50,
        rules=rules,
    )

    available = [
        c for c in candidates
        if c["id"] not in selected_ids
        and is_document_complete(c)[0]
        and (await check_availability(c["canonical_url"]))[0]
    ]

    if not available:
        return None

    return available[0]  # Найкращий за score/рік


async def download_pdf_to_resources(
    url: str,
    resources_dir: Path,
    document_id: int,
    timeout_s: float = 60.0,
) -> tuple[Path | None, str | None]:
    """Завантажити PDF з URL у папку resources каталогу.

    Returns (path_to_pdf, None) if successful, or (None, error_description) if failed.
    PDF saved as: resources_dir / f"{document_id}.pdf"
    """
    settings = get_settings()
    timeout = httpx.Timeout(timeout_s, connect=10.0, read=30.0, pool=None)
    headers = {
        "User-Agent": settings.http.user_agent,
        "Accept": "application/pdf,*/*",
    }

    pdf_path = resources_dir / f"{document_id}.pdf"

    try:
        async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
            resp = await client.get(url, headers=headers)
            if resp.status_code != 200:
                reason = f"HTTP {resp.status_code}"
                logger.warning("pdf_download_failed_to_resources", url=url, document_id=document_id, status=resp.status_code)
                return None, reason

            content_type = resp.headers.get("content-type", "")
            if "pdf" not in content_type.lower() and "octet-stream" not in content_type.lower():
                if "html" in content_type.lower():
                    reason = f"не PDF (content-type={content_type})"
                    logger.warning("pdf_download_not_pdf_to_resources", url=url, document_id=document_id)
                    return None, reason

            data = resp.content
            if len(data) < 1024:
                reason = f"файл занадто малий ({len(data)} байт)"
                logger.warning("pdf_download_too_small_to_resources", url=url, document_id=document_id, size=len(data))
                return None, reason

            if data[:4] != b"%PDF":
                reason = "відсутні %PDF magic bytes"
                logger.warning("pdf_download_not_pdf_magic_to_resources", url=url, document_id=document_id)
                return None, reason

            pdf_path.write_bytes(data)
            logger.info("pdf_downloaded_to_resources", url=url, document_id=document_id, path=str(pdf_path))
            return pdf_path, None

    except Exception as e:
        detail = str(e).strip() or type(e).__name__
        reason = f"{type(e).__name__}: {detail}" if str(e).strip() else type(e).__name__
        logger.error("pdf_download_error_to_resources", url=url, document_id=document_id, error=reason)
        return None, reason


async def save_catalog_atomically(path: str, data: dict[str, Any]) -> None:
    """Атомарно записати каталог (temp файл + os.replace)."""
    dir_path = os.path.dirname(os.path.abspath(path)) or "."
    fd, tmp_path = tempfile.mkstemp(suffix=".json", dir=dir_path)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
            f.write("\n")
        os.replace(tmp_path, path)
    except BaseException:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)
        raise


async def mark_unavailable_in_db(
    db,
    doc_id: int,
    reason: str,
    replacement_id: int | None = None,
    catalog_name: str | None = None,
):
    """Позначити документ як недоступний в БД (documents.extra)."""
    import json as json_module

    now = datetime.now().isoformat()
    curator_extra = {
        "curator": {
            "unavailable_since": now,
            "reason": reason,
            "replaced_by": replacement_id,
            "catalog": catalog_name,
        }
    }

    existing = await db.fetchone("SELECT extra FROM documents WHERE id = ?", (doc_id,))
    existing_extra = {}
    if existing and existing["extra"] and existing["extra"] != "None":
        try:
            existing_extra = json_module.loads(existing["extra"])
        except (json_module.JSONDecodeError, TypeError):
            existing_extra = {}

    combined_extra = {**existing_extra, **curator_extra}

    await db.execute(
        "UPDATE documents SET extra = ? WHERE id = ?",
        (json_module.dumps(combined_extra, ensure_ascii=False), doc_id),
    )


class PrepareResult:
    """Результат підготовки каталогу."""

    def __init__(
        self,
        catalog_path: str,
        topic_name: str,
        document_count: int,
        replaced_count: int,
        unavailable_count: int,
        success_count: int,
    ):
        self.catalog_path = catalog_path
        self.topic_name = topic_name
        self.document_count = document_count
        self.replaced_count = replaced_count
        self.unavailable_count = unavailable_count
        self.success_count = success_count

    def summary(self) -> str:
        return (
            f"📚 Каталог: {self.catalog_path}\n"
            f"📖 Тема: {self.topic_name}\n"
            f"📄 Документів: {self.document_count} (успішно {self.success_count}, замінено {self.replaced_count}, недоступно {self.unavailable_count})\n"
        )


async def prepare_catalog(
    topic_name: str,
    output_dir: str = "catalogs",
    limit: int | None = None,
    dry_run: bool = False,
    profile: str = "strict",
) -> PrepareResult | None:
    """Підготувати каталог документів для теми.

    Steps:
    1. Знайти тему в БД
    2. Зібрати кандидатів (verified, повні дані)
    3. LLM-відбір оптимальної кількості та найкращих документів
    4. Перевірка доступності кожного обраного документа
    5. Заміна недоступних на схожі
    6. Запис JSON-каталогу (якщо не dry_run)

    Returns PrepareResult або None за помилки.
    """
    settings = get_settings()
    rules = get_filter_rules(profile)
    db = build_database(settings)
    await db.initialize(sync_mirror=False)

    try:
        tags = log_tags = {}
        logger.info("curator_prepare_start", topic=topic_name)

        # 1. Знайти тему в БД або визначити UDC
        topic_info = await find_topic_in_db(db, topic_name)
        topic_id = topic_info["id"] if topic_info else None

        tag = topic_info if topic_info else {"name_uk": topic_name}
        topic_name_uk = tag.get("name_uk", topic_name)
        udc_prefixes = []

        if topic_info and topic_info.get("udc_prefixes"):
            try:
                udc_prefixes = json.loads(topic_info["udc_prefixes"])
            except (TypeError, json.JSONDecodeError):
                pass

        if not topic_id and not udc_prefixes:
            logger.warning("topic_not_found_in_db", topic=topic_name)
            # Для теми без точної прив'язки використати UDC-префікси
            # як у catalog_generator для теми 076 (Підприємництво, торгівля)
            topic_name_lower = topic_name.lower()
            if "підприєм" in topic_name_lower or "торгівля" in topic_name_lower or "бірж" in topic_name_lower or "економ" in topic_name_lower or "фінанс" in topic_name_lower:
                udc_prefixes = [
                    "334.7", "334.72", "339.1", "339.3", "339.5", "336.76",
                    "334.722", "339.13", "339.132", "339.138", "339.564",
                    "334.72", "339.138", "339.56", "336.76", "334.722",
                ]
                topic_name_uk = topic_info.get("name_uk", topic_name) if topic_info else "Економіка"
            elif "інформатик" in topic_name_lower or "програмуван" in topic_name_lower or "комп'ютер" in topic_name_lower or "алгоритм" in topic_name_lower or "база даних" in topic_name_lower or "софт" in topic_name_lower or "машинне навчання" in topic_name_lower:
                udc_prefixes = ["004"]
            # Додати інші філтри за потреби

        if not topic_id and not udc_prefixes:
            logger.warning("cannot_resolve_topic_to_udc", topic=topic_name)
            return None

        logger.info("topic_resolved", topic_name=topic_name_uk, topic_id=topic_id, udc_count=len(udc_prefixes))

        # 2. Зібрати кандидатів
        candidates = await get_candidates_for_topic(
            db,
            topic_name=topic_name_uk,
            topic_id=topic_id,
            udc_prefixes=udc_prefixes,
            limit=200,
            rules=rules,
        )

        complete_candidates = []
        incomplete_count = 0
        for c in candidates:
            ok, reason = is_document_complete(c, rules)
            if ok:
                complete_candidates.append(c)
            else:
                incomplete_count += 1

        logger.info("candidates_found", total=len(candidates), complete=len(complete_candidates), incomplete=incomplete_count)

        if not complete_candidates:
            logger.warning("no_complete_candidates", topic=topic_name_uk)
            return None

        # 3. LLM-відбір
        selection = await call_llm_for_selection(topic_name_uk, complete_candidates, rules=rules)
        if selection is None:
            # Якщо LLM недоступний — обираємо перші complete_candidates
            logger.warning("llm_selection_failed_fallback_to_first", count=len(complete_candidates))
            suggested_count = min(30, len(complete_candidates))
            selection = SelectionResult(
                topic=topic_name_uk,
                candidates_count=len(complete_candidates),
                suggested_count=suggested_count,
                selected_ids=[c["id"] for c in complete_candidates[:suggested_count]],
                reasoning="LLM недоступний, обрано перші N документів",
            )

        selected_ids = set(selection.selected_ids)
        selected_docs = [c for c in complete_candidates if c["id"] in selected_ids]

        logger.info("selection_done", suggested=selection.suggested_count, actual=len(selected_docs))

        # 4. Перевірка доступності
        unavailable = []
        available = []

        for doc in selected_docs:
            available_flag, reason = await check_availability(doc["canonical_url"])
            if available_flag:
                available.append(doc)
            else:
                unavailable.append((doc, reason))
                logger.warning("document_unavailable", doc_id=doc["id"], reason=reason)

        # 5. Заміна недоступних
        replaced = []
        still_unavailable = []

        for doc, reason in unavailable:
            replacement = await find_replacement(
                db,
                doc,
                selected_ids - {doc["id"]},
                topic_id=topic_id,
                udc_prefixes=udc_prefixes,
                rules=rules,
            )
            if replacement:
                available.append(replacement)
                replaced.append((doc["id"], replacement["id"]))
                logger.info("replacement_found", original=doc["id"], replacement=replacement["id"])
            else:
                still_unavailable.append((doc["id"], reason))
                logger.warning("no_replacement_found", doc_id=doc["id"])

        if dry_run:
            logger.info("dry_run_skip_write", catalog_path="(не записано)")
            return PrepareResult(
                catalog_path="(dry-run)",
                topic_name=topic_name_uk,
                document_count=len(available),
                replaced_count=len(replaced),
                unavailable_count=len(still_unavailable),
                success_count=len(available),
            )

        # 6. Створити структуру каталогу та завантажити PDF
        now = datetime.now()
        now_str = now.strftime('%Y%m%d_%H%M%S')
        catalog_folder = f"catalog_{now_str}"
        catalog_path = os.path.join(output_dir, catalog_folder)
        resources_dir = os.path.join(catalog_path, "resources")
        catalog_json_path = os.path.join(catalog_path, f"{catalog_folder}.json")
        
        os.makedirs(resources_dir, exist_ok=True)
        logger.info("catalog_structure_created", catalog_path=catalog_path, resources_dir=resources_dir)

        # Завантажити PDF для обраних документів
        documents_data = []
        downloaded = []
        download_failed = []
        
        for doc in available:
            pdf_path, error = await download_pdf_to_resources(
                doc["canonical_url"],
                Path(resources_dir),
                doc["id"],
            )
            if pdf_path:
                downloaded.append((doc["id"], str(pdf_path)))
                doc_data = {
                    "id": doc["id"],
                    "title": doc["title"],
                    "authors": doc["authors"],
                    "year": doc["year"],
                    "publisher": doc["publisher"],
                    "doc_type": doc["doc_type"],
                    "canonical_url": doc["canonical_url"],
                    "language": doc["language"],
                    "udc": doc["udc"],
                    "page_count": doc["page_count"],
                    "size_bytes": doc["size_bytes"],
                    "sha256": doc["sha256"],
                    "has_text_layer": doc["has_text_layer"],
                    "verified_at": doc["verified_at"],
                    "first_seen_at": doc["first_seen_at"],
                    "pdf_path": f"resources/{doc['id']}.pdf",  # Відносний шлях до PDF
                }
                if doc.get("topic_id") and doc.get("topic_name"):
                    doc_data["topics"] = [{
                        "topic_id": doc["topic_id"],
                        "topic_name": doc["topic_name"],
                        "score": doc["topic_score"],
                    }]
                documents_data.append(doc_data)
            else:
                download_failed.append((doc["id"], error or "Невідомо"))
                logger.warning("pdf_download_failed", document_id=doc["id"], error=error or "Невідомо")
                # Все одно додамо документ до каталогу, але без PDF
                doc_data = {
                    "id": doc["id"],
                    "title": doc["title"],
                    "authors": doc["authors"],
                    "year": doc["year"],
                    "publisher": doc["publisher"],
                    "doc_type": doc["doc_type"],
                    "canonical_url": doc["canonical_url"],
                    "language": doc["language"],
                    "udc": doc["udc"],
                    "page_count": doc["page_count"],
                    "size_bytes": doc["size_bytes"],
                    "sha256": doc["sha256"],
                    "has_text_layer": doc["has_text_layer"],
                    "verified_at": doc["verified_at"],
                    "first_seen_at": doc["first_seen_at"],
                    "pdf_path": None,  # PDF не завантажено
                }
                if doc.get("topic_id") and doc.get("topic_name"):
                    doc_data["topics"] = [{
                        "topic_id": doc["topic_id"],
                        "topic_name": doc["topic_name"],
                        "score": doc["topic_score"],
                    }]
                documents_data.append(doc_data)

        logger.info("pdf_download_results", total=len(available), downloaded=len(downloaded), failed=len(download_failed))

        catalog_data = {
            "topic": topic_name_uk,
            "created_at": now.isoformat(),
            "total_documents": len(documents_data),
            "replaced_count": len(replaced),
            "documents": documents_data,
            "resources_dir": "resources",
        }

        # Атомарно записати JSON каталогу
        fd, tmp_json = tempfile.mkstemp(suffix=".json", dir=catalog_path)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(catalog_data, f, ensure_ascii=False, indent=2)
                f.write("\n")
            os.replace(tmp_json, catalog_json_path)
            logger.info("catalog_written", path=catalog_json_path)
        except BaseException:
            if os.path.exists(tmp_json):
                os.unlink(tmp_json)
            raise

        # Позначити недоступні в БД
        for doc_id, reason in still_unavailable:
            await mark_unavailable_in_db(db, doc_id, reason, catalog_name=catalog_folder)

        for original_id, replacement_id in replaced:
            await mark_unavailable_in_db(db, original_id, "replaced_by_curator", replacement_id, catalog_name=catalog_folder)

        # 7. Підсумок
        result = PrepareResult(
            catalog_path=catalog_folder,
            topic_name=topic_name_uk,
            document_count=len(documents_data),
            replaced_count=len(replaced),
            unavailable_count=len(still_unavailable) + len(download_failed),
            success_count=len(documents_data),
        )

        logger.info("curator_prepare_complete", **result.__dict__)
        return result

    finally:
        await db.close()
