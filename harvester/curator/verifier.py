"""Етап 2: верифікація каталогу — аналіз помилок + заміна недоступних."""

from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any

import structlog

from harvester.config import FilterRules, get_filter_rules, get_settings
from harvester.curator.availability import check_availability
from harvester.db.failover import build_database

logger = structlog.get_logger()

CATALOG_FILE_MODE = 0o644


async def find_replacement_candidates(
    db,
    original_doc: dict[str, Any],
    selected_ids: set[int],
    limit: int = 10,
    rules: FilterRules | None = None,
) -> list[dict[str, Any]]:
    """Знайти кандидатів на заміну серед наявних в БД."""
    if rules is None:
        rules = get_filter_rules()
    
    min_page_count = rules.min_page_count
    topic_ids = []
    for t in original_doc.get("topics", []):
        topic_id = t.get("topic_id")
        if topic_id is not None:
            topic_ids.append(topic_id)

    # Створити умову NOT IN з плейсхолдерами
    not_in_placeholders = ",".join("?" * len(selected_ids))
    not_in_condition = (
        f"d.id NOT IN ({not_in_placeholders})" if selected_ids else "1=1"
    )

    # Якщо документ не має topic-зв'язків, заміни все одно можна шукати
    # серед усіх якісних verified-документів.
    topic_in_condition = "1=1"
    if topic_ids:
        topic_in_placeholders = ",".join("?" * len(topic_ids))
        topic_in_condition = f"dt.topic_id IN ({topic_in_placeholders})"

    params = list(selected_ids)
    if topic_ids:
        params.extend(topic_ids)
    params.append(limit)

    rows = await db.fetchall(
        f"""
        SELECT d.id, d.title, d.authors, d.year, d.publisher, d.doc_type,
               d.canonical_url, d.language, d.udc, d.page_count,
               d.has_text_layer, d.size_bytes, d.sha256, d.status,
               dt.score as topic_score,
               t.id as topic_id, t.name_uk as topic_name
        FROM documents d
        LEFT JOIN document_topics dt ON dt.document_id = d.id
        LEFT JOIN topics t ON t.id = dt.topic_id
        WHERE d.status = 'verified'
          AND d.title IS NOT NULL AND d.title != ''
          AND d.authors IS NOT NULL
          AND d.language IS NOT NULL
          AND LOWER(d.language) NOT IN ('', 'unknown', 'und')
          AND d.canonical_url IS NOT NULL AND d.canonical_url != ''
          AND d.page_count >= {min_page_count}
          AND d.has_text_layer = 1
          AND ({not_in_condition})
          AND ({topic_in_condition})
          AND (d.extra IS NULL OR d.extra NOT LIKE '%"curator"%')
        ORDER BY dt.score DESC NULLS LAST, d.year DESC NULLS LAST
        LIMIT ?
        """,
        tuple(params),
    )

    candidates = []
    for row in rows:
        candidates.append({
            "id": row["id"],
            "title": row["title"],
            "authors": _parse_authors(row["authors"]),
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
            "size_bytes": row["size_bytes"],
            "sha256": row["sha256"],
        })

    return candidates


def _parse_authors(value: Any) -> list[str]:
    """Безпечно нормалізувати authors із JSON та старих рядкових записів."""
    if isinstance(value, list):
        return [str(author).strip() for author in value if str(author).strip()]
    if not value:
        return []
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except (json.JSONDecodeError, TypeError):
            return [value.strip()] if value.strip() else []
        if isinstance(parsed, list):
            return [str(author).strip() for author in parsed if str(author).strip()]
        return [value.strip()] if value.strip() else []
    return [str(value).strip()]


async def call_llm_for_fix(
    doc: dict[str, Any],
    error: str,
    candidates: list[dict[str, Any]],
) -> dict[str, Any] | None:
    """Викликати LLM для вирішення щодо виправлення помилки."""
    settings = get_settings()
    if not settings.llm.enabled:
        return None

    prompt = f"""Ти — куратор наукової бібліотеки. Документ у каталозі має помилку — виріши, що робити.

ДОКУМЕНТ:
  ID: {doc['id']}
  Назва: "{doc.get('title', '(без назви)')}"
  URL: {doc.get('canonical_url', 'немає')}
  Помилка: {error}

ДОСТУПНІ ЗАМІНИ:
"""
    for c in candidates:
        prompt += f"""  {c['id']}. {c['title'][:80]} |автори: {str(c.get('authors', []))[:60]} |рік: {c['year'] or '?'} |стор: {c.get('page_count', '?')} |тип: {c['doc_type']} |школа: {c['topic_score']:.2f}
"""

    prompt += """
ЗАВДАННЯ:
- "replace" + replacement_id — замінити на найкращий аналог
- "retry" — помилка тимчасова (наприклад, таймаут з'єднання), спробувати ще раз
- "skip" — немає заміни або проблема неприроджувана

ВІДПОВІДЬ (тільки JSON):
{"action": "replace|retry|skip", "replacement_id": null, "reasoning": "коротка причина"}
"""

    try:
        from harvester.classify.llm import LLMClient, LLMUnavailable

        client = LLMClient(keys=settings.gemini_keys, service="CuratorVerify")
        response = await client.complete(prompt)
        text = response.text.strip()
        decoder = json.JSONDecoder()
        result = None
        for index, char in enumerate(text):
            if char != "{":
                continue
            try:
                candidate, _ = decoder.raw_decode(text[index:])
            except json.JSONDecodeError:
                continue
            if isinstance(candidate, dict) and "action" in candidate:
                result = candidate
                break
        if not isinstance(result, dict):
            return None

        action = str(result.get("action") or "skip").strip().lower()
        if action not in {"replace", "retry", "skip"}:
            action = "skip"
        replacement_id = result.get("replacement_id")
        if isinstance(replacement_id, bool):
            replacement_id = None
        else:
            try:
                replacement_id = int(replacement_id) if replacement_id is not None else None
            except (TypeError, ValueError):
                replacement_id = None
        return {
            "action": action,
            "replacement_id": replacement_id,
            "reasoning": str(result.get("reasoning") or "").strip(),
        }
    except LLMUnavailable as e:
        logger.warning("llm_fix_unavailable", error=str(e)[:200])
    except (json.JSONDecodeError, TypeError, ValueError) as e:
        logger.warning("llm_fix_invalid_json", error=str(e)[:200])
    except Exception as e:  # noqa: BLE001
        logger.warning("llm_fix_failed", error=str(e)[:200])
    return None


async def save_catalog_atomically(path: str, data: dict[str, Any]) -> None:
    """Атомарно записати каталог."""
    dir_path = os.path.dirname(os.path.abspath(path)) or "."
    fd, tmp_path = tempfile.mkstemp(suffix=".json", dir=dir_path)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
            f.write("\n")
        os.replace(tmp_path, path)
        os.chmod(path, CATALOG_FILE_MODE)
    except BaseException:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)
        raise


def _has_catalog_extraction_data(doc: dict[str, Any]) -> bool:
    quotations = doc.get("quotations")
    if isinstance(quotations, list):
        has_quotes = any(
            isinstance(quotation, dict) and str(quotation.get("text", "")).strip()
            for quotation in quotations
        )
    else:
        has_quotes = False

    summary = doc.get("summary")
    has_summary = False
    if isinstance(summary, dict):
        sections = summary.get("sections")
        if isinstance(sections, list):
            for section in sections:
                if not isinstance(section, dict):
                    continue
                title = str(section.get("title", "")).strip()
                overview = str(section.get("overview", "")).strip()
                methodology = str(section.get("methodology", "")).strip()
                findings = str(section.get("findings", "")).strip()
                conclusions = str(section.get("conclusions", "")).strip()
                key_ideas = section.get("key_ideas", [])
                has_ideas = isinstance(key_ideas, list) and any(str(idea).strip() for idea in key_ideas)
                if any(
                    value and value not in {"н/зв", "Розділ", "Загальна сумаризація"}
                    for value in (title, overview, methodology, findings, conclusions)
                ) or has_ideas:
                    has_summary = True
                    break

    return has_quotes or has_summary


def _catalog_validation_error(doc: dict[str, Any]) -> str | None:
    error = doc.get("error")
    if error:
        return str(error)
    if not _has_catalog_extraction_data(doc):
        return "відсутні quotations і summary"
    return None


async def mark_unavailable_in_db(db, doc_id: int, reason: str, replacement_id: int | None = None, catalog_name: str | None = None):
    """Позначити документ як недоступний в БД."""
    import json as json_module

    now = datetime.now().isoformat()
    curator = {
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

    if replacement_id:
        curator["curator"]["replaced_by"] = replacement_id

    combined = {**existing_extra, **curator}

    await db.execute(
        "UPDATE documents SET extra = ? WHERE id = ?",
        (json_module.dumps(combined, ensure_ascii=False), doc_id),
    )


class VerifyResult:
    """Результат верифікації каталогу."""

    def __init__(
        self,
        catalog_path: str,
        fixed_count: int,
        replaced_count: int,
        skipped_count: int,
        retry_count: int,
        error_count: int,
    ):
        self.catalog_path = catalog_path
        self.fixed_count = fixed_count
        self.replaced_count = replaced_count
        self.skipped_count = skipped_count
        self.retry_count = retry_count
        self.error_count = error_count

    def summary(self) -> str:
        return (
            f"📚 Каталог: {self.catalog_path}\n"
            f"🔧 Виправлено: {self.fixed_count} (заміна {self.replaced_count}, пропуск {self.skipped_count}, повтор {self.retry_count})\n"
            f"⚠ Помилок: {self.error_count}\n"
        )


async def verify_catalog(
    catalog_path: str,
    dry_run: bool = False,
) -> VerifyResult | None:
    """Верифікувати каталог: знайти помилки, вирішити що робити, виправити."""
    logger.info("curator_verify_start", path=catalog_path)

    # Розв'язати шлях до каталогу: якщо це папка — знайти JSON всередині
    path_obj = Path(catalog_path)
    if path_obj.is_dir():
        catalog_json = path_obj / f"{path_obj.name}.json"
        if not catalog_json.exists():
            logger.error("catalog_not_found", path=catalog_json)
            return None
        catalog_json_path = str(catalog_json)
        resources_dir = path_obj / "resources" if (path_obj / "resources").exists() else None
    else:
        catalog_json_path = str(path_obj)
        resources_dir = None

    try:
        with open(catalog_json_path, "r", encoding="utf-8") as f:
            catalog = json.load(f)
    except FileNotFoundError:
        logger.error("catalog_not_found", path=catalog_json_path)
        return None
    except json.JSONDecodeError as e:
        logger.error("catalog_invalid_json", path=catalog_json_path, error_msg=str(e)[:100])
        return None

    settings = get_settings()
    db = build_database(settings)
    await db.initialize(sync_mirror=False)

    try:
        documents = catalog.get("documents", [])
        if not documents:
            logger.info("catalog_empty", path=catalog_path)
            return None

        # Знайти документи з помилками або порожнім витягом
        changed = False
        error_docs = []
        for i, d in enumerate(documents):
            original_error = d.get("error")
            validation_error = _catalog_validation_error(d)
            if validation_error:
                if not original_error:
                    d["error"] = validation_error
                    changed = True
                error_docs.append((i, d, validation_error, not original_error))

        if not error_docs:
            logger.info("catalog_no_errors", path=catalog_path, total=len(documents))
            return None

        logger.info("catalog_errors_found", path=catalog_path, errors=len(error_docs), total=len(documents))

        global_selected_ids = {d["id"] for d in documents if "id" in d}
        fixed = 0
        replaced = 0
        skipped = 0
        retried = 0
        errors = 0

        for idx, (orig_idx, doc, error, validation_only) in enumerate(error_docs):
            doc_id = doc.get("id")
            logger.info("processing_error", index=idx + 1, total=len(error_docs), doc_id=doc_id, error=error[:100])

            if validation_only:
                skipped += 1
                errors += 1
                logger.warning("catalog_validation_error", doc_id=doc_id, error=error)
                continue

            doc_for_lookup = {
                "id": doc_id,
                "title": doc.get("title", ""),
                "canonical_url": doc.get("canonical_url", ""),
                "topics": doc.get("topics", []),
            }

            # Знайти кандидатів на заміну
            candidates = await find_replacement_candidates(db, doc_for_lookup, global_selected_ids, limit=10)

            # Виклик LLM
            fix = await call_llm_for_fix(doc, error, candidates)

            if fix is None:
                logger.warning("llm_fix_unavailable", doc_id=doc_id)
                skipped += 1
                errors += 1
                continue

            action = fix.get("action", "skip")
            replacement_id = fix.get("replacement_id")

            if action == "replace" and replacement_id:
                # Знайти заміну в candidates
                replacement = next((c for c in candidates if c["id"] == replacement_id), None)
                if replacement:
                    # Перевірити доступність заміни
                    avail, _ = await check_availability(replacement["canonical_url"])
                    if avail:
                        # Замінити в каталозі
                        new_doc = {
                            "id": replacement["id"],
                            "title": replacement["title"],
                            "authors": replacement["authors"],
                            "year": replacement["year"],
                            "publisher": replacement["publisher"],
                            "doc_type": replacement["doc_type"],
                            "canonical_url": replacement["canonical_url"],
                            "language": replacement["language"],
                            "udc": replacement["udc"],
                            "page_count": replacement["page_count"],
                            "size_bytes": replacement["size_bytes"],
                            "sha256": replacement["sha256"],
                            "has_text_layer": replacement["has_text_layer"],
                            "topics": [{"topic_id": replacement["topic_id"], "topic_name": replacement["topic_name"], "score": replacement["topic_score"]}] if replacement.get("topic_id") else doc.get("topics", []),
                        }

                        documents[orig_idx] = new_doc
                        changed = True
                        global_selected_ids.add(replacement["id"])
                        global_selected_ids.discard(doc_id)

                        # Позначити оригінал в БД
                        await mark_unavailable_in_db(db, doc_id, "replaced_by_curator", replacement_id, catalog_path.split("/")[-1])
                        replaced += 1
                        fixed += 1
                        logger.info("replacement_done", original=doc_id, replacement=replacement_id)
                    else:
                        logger.warning("replacement_unavailable", replacement_id=replacement_id)
                        skipped += 1
                        errors += 1
                else:
                    logger.warning("replacement_not_found_in_candidates", replacement_id=replacement_id)
                    skipped += 1
                    errors += 1
            elif action == "retry":
                logger.info("retry_skipped_manual", doc_id=doc_id)
                retried += 1
                errors += 1
            else:
                # skip
                skipped += 1
                errors += 1

        if dry_run:
            logger.info("dry_run_skip_write")
            return VerifyResult(
                catalog_path=catalog_path,
                fixed_count=fixed,
                replaced_count=replaced,
                skipped_count=skipped,
                retry_count=retried,
                error_count=errors,
            )

        # Записати оновлений каталог
        catalog["fixed_at"] = datetime.now().isoformat()
        catalog["fixed_count"] = fixed
        catalog["replaced_count"] = replaced

        # Зберегти в тій же папці, що й оригінал
        if resources_dir is not None:
            # Папкова структура: catalog_folder/catalog_folder_fixed.json
            new_path = str(Path(catalog_json_path).parent / f"{path_obj.name}_fixed.json")
        else:
            # Файловий формат: catalog.json -> catalog_fixed.json
            new_path = catalog_path.replace(".json", "_fixed.json")
        if changed or fixed or replaced or skipped or retried or errors:
            await save_catalog_atomically(new_path, catalog)
        else:
            new_path = catalog_json_path

        logger.info("curator_verify_complete", path=new_path, fixed=fixed, replaced=replaced, skipped=skipped, errors=errors)
        return VerifyResult(
            catalog_path=new_path,
            fixed_count=fixed,
            replaced_count=replaced,
            skipped_count=skipped,
            retry_count=retried,
            error_count=errors,
        )

    finally:
        await db.close()
