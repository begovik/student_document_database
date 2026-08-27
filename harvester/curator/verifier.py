"""Етап 2: верифікація каталогу — аналіз помилок + заміна недоступних."""

from __future__ import annotations

import json
import asyncio
import os
import tempfile
from pathlib import Path
from datetime import datetime
from typing import Any

import structlog

from harvester.config import get_settings
from harvester.db.failover import build_database
from harvester.curator.availability import check_availability

logger = structlog.get_logger()


async def find_replacement_candidates(
    db,
    original_doc: dict[str, Any],
    selected_ids: set[int],
    limit: int = 10,
) -> list[dict[str, Any]]:
    """Знайти кандидатів на заміну серед наявних в БД."""
    from harvester.db.repositories import DocumentsRepository

    topic_ids = []
    for t in original_doc.get("topics", []):
        topic_ids.append(t.get("topic_id"))

    # Створити умову NOT IN з плейсхолдерами
    not_in_placeholders = ",".join("?" * len(selected_ids))
    not_in_condition = f"d.id NOT IN ({not_in_placeholders})"

    topic_in_condition = "1=0"
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
          AND d.canonical_url IS NOT NULL AND d.canonical_url != ''
          AND d.page_count >= 3
          AND (d.has_text_layer = 1 OR d.has_text_layer IS NULL)
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
            "size_bytes": row["size_bytes"],
            "sha256": row["sha256"],
        })

    return candidates


async def call_llm_for_fix(
    doc: dict[str, Any],
    error: str,
    candidates: list[dict[str, Any]],
) -> dict[str, Any] | None:
    """Викликати LLM для вирішення щодо виправлення помилки."""
    settings = get_settings()
    if not settings.llm.enabled:
        return None

    import aiohttp
    import re

    config = settings.llm
    gemini_keys = [k for k in [settings.gemini_api_key, settings.gemini_api_key_2, settings.gemini_api_key_3] if k]
    models = config.gemini_models or ["gemini-3.1-flash-lite"]

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

    last_error = None
    # Фаза 1: Gemini
    for model in models:
        for gemini_key in gemini_keys:
            try:
                async with aiohttp.ClientSession() as client:
                    url = f"{config.gemini_base_url}/models/{model}:generateContent?key={gemini_key}"
                    payload = {
                        "contents": [{"parts": [{"text": prompt}]}],
                        "generationConfig": {
                            "temperature": config.temperature,
                            "maxOutputTokens": config.max_tokens,
                        },
                    }
                    resp = await client.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=config.timeout_s))
                    if resp.status != 200:
                        raise RuntimeError(f"Gemini error {resp.status}")

                    data = await resp.json()
                    text = ""
                    for part in data.get("candidates", [{}])[0].get("content", {}).get("parts", []):
                        text += part.get("text", "")

                    match = re.search(r'\{[^}]*"action"[^}]*\}', text)
                    if match:
                        return json.loads(match.group())
                    break
            except Exception as e:
                last_error = e
                await asyncio.sleep(config.min_interval_s)

    # Фаза 2: Gemma (ті самі ключі, gemma_models + стиснення)
    from harvester.classify.llm import rephrase_for_gemma

    gemma_models = config.gemma_models or ["gemma-4-31b-it", "gemma-4-26b-it"]
    truncated_prompt = rephrase_for_gemma(prompt, config.gemma_max_chars)

    for model in gemma_models:
        for gemini_key in gemini_keys:
            try:
                async with aiohttp.ClientSession() as client:
                    url = f"{config.gemini_base_url}/models/{model}:generateContent?key={gemini_key}"
                    payload = {
                        "contents": [{"parts": [{"text": truncated_prompt}]}],
                        "generationConfig": {
                            "temperature": config.temperature,
                            "maxOutputTokens": config.max_tokens,
                        },
                    }
                    resp = await client.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=config.timeout_s))
                    if resp.status != 200:
                        raise RuntimeError(f"Gemma error {resp.status}")

                    data = await resp.json()
                    text = ""
                    for part in data.get("candidates", [{}])[0].get("content", {}).get("parts", []):
                        text += part.get("text", "")

                    match = re.search(r'\{[^}]*"action"[^}]*\}', text)
                    if match:
                        return json.loads(match.group())
                    break
            except Exception as e:
                last_error = e
                await asyncio.sleep(config.min_interval_s)

    logger.warning("llm_fix_failed", error=str(last_error)[:100] if last_error else "unknown")
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
    except BaseException:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)
        raise


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

        # Знайти документи з помилками
        error_docs = [(i, d) for i, d in enumerate(documents) if d.get("error")]

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

        for idx, (orig_idx, doc) in enumerate(error_docs):
            doc_id = doc.get("id")
            error = doc.get("error", "")
            logger.info("processing_error", index=idx + 1, total=len(error_docs), doc_id=doc_id, error=error[:100])

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
        await save_catalog_atomically(new_path, catalog)

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
