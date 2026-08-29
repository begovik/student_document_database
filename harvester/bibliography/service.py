"""Сервіс вилучення та пошуку бібліографічних джерел з каталогу."""

from __future__ import annotations

import asyncio
import json
import re
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import structlog

from harvester.bibliography import (
    BibliographyEntry,
    deduplicate_references,
    extract_references_from_text,
    format_references_list,
)
from harvester.bibliography.searcher import BibliographySearcher, SearchResult
from harvester.config import get_settings

logger = structlog.get_logger()

LLM_BIBLIO_PROMPT = """Ти — експерт з витягування бібліографії. У наданому тексті знайди розділ ЛІТЕРАТУРА / REFERENCES / СПИСОК ВИКОРИСТАНИХ ДЖЕРЕЛ / БІБЛІОГРАФІЯ незалежно від його розташування (навіть якщо після нього йдуть додатки на 10+ сторінок, зміст у кінці, або література в середині).

Завдання:
1. Знайди ВСІ записи літератури. Якщо розділу немає — поверни порожній список.
2. Для кожного запису витягни: raw_text (оригінальний рядок 1 запису), authors (список), title, year, source (журнал/видавництво), url, doi, language (uk/en/ru), entry_type (article/book/thesis/conference/online).
3. Якщо записів багато (>30), поверни всі — не скорочуй.
4. Не вигадуй записи, бери лише з тексту.

Формат відповіді — лише JSON:
{"references": [{"raw_text": "...", "authors": ["..."], "title": "...", "year": "...", "source": "...", "url": "...", "doi": "...", "language": "...", "entry_type": "..."}, ...]}"""


@dataclass
class CatalogScanResult:
    """Результат сканування каталогу."""
    catalog_path: str
    documents_scanned: int = 0
    references_extracted: int = 0
    references_after_dedup: int = 0
    references_filtered_russian: int = 0
    references_for_search: int = 0
    references_found_in_db: int = 0
    references_found_online: int = 0
    references_not_found: int = 0
    pdfs_downloaded: int = 0
    pdfs_saved_dir: str = ""
    documents_added_to_db: int = 0
    processing_time_s: float = 0.0
    output_file: str = ""
    details: dict = field(default_factory=dict)


@dataclass
class DocumentReferences:
    """Посилання з одного документа."""
    document_id: int
    document_title: str
    document_url: str
    references: list[BibliographyEntry] = field(default_factory=list)
    search_results: list[SearchResult] = field(default_factory=list)


class BibliographyService:
    """Сервіс вилучення та пошуку бібліографічних джерел."""
    
    def __init__(self):
        self.settings = get_settings()
        self.searcher = BibliographySearcher()
    
    async def process_catalog(
        self,
        catalog_dir: str | Path,
        output_name: str | None = None,
    ) -> CatalogScanResult:
        """Обробити каталог: вилучити літературу, знайти джерела."""
        catalog_dir = Path(catalog_dir)
        start_time = asyncio.get_event_loop().time()
        
        # Знайти JSON каталогу
        json_files = list(catalog_dir.glob("*.json"))
        if not json_files:
            logger.error("no_catalog_json", dir=str(catalog_dir))
            return CatalogScanResult(catalog_path=str(catalog_dir))
        
        # Знаходимо головний JSON каталогу (має містити "documents" та "topic")
        catalog_json = None
        for jf in json_files:
            try:
                with open(jf, "r", encoding="utf-8") as f:
                    test_data = json.load(f)
                if isinstance(test_data, dict) and "documents" in test_data and "topic" in test_data:
                    catalog_json = jf
                    break
            except (json.JSONDecodeError, UnicodeDecodeError):
                continue
        
        if catalog_json is None:
            logger.error("no_catalog_json", dir=str(catalog_dir))
            return CatalogScanResult(catalog_path=str(catalog_dir))
        with open(catalog_json, "r", encoding="utf-8") as f:
            catalog = json.load(f)
        
        resources_dir = catalog_dir / catalog.get("resources_dir", "resources")
        
        logger.info(
            "bibliography_scan_start",
            catalog=str(catalog_dir),
            topic=catalog.get("topic", ""),
            documents=len(catalog.get("documents", [])),
        )
        
        # Обробити кожен документ
        all_references: list[BibliographyEntry] = []
        all_search_results: list[SearchResult] = []
        docs_with_refs: list[DocumentReferences] = []
        
        for doc in catalog.get("documents", []):
            doc_id = doc.get("id")
            doc_title = doc.get("title", "")
            doc_url = doc.get("canonical_url", "")
            
            pdf_path = resources_dir / f"{doc_id}.pdf"
            if not pdf_path.exists():
                logger.warning("pdf_not_found", doc_id=doc_id, path=str(pdf_path))
                continue
            
            # Вилучити повний текст (для LLM — весь документ, бо додатки можуть бути 10+ сторінок після літератури)
            full_text = await self._extract_full_text(pdf_path)
            if not full_text:
                logger.warning("no_text_extracted", doc_id=doc_id)
                continue

            # Спроба LLM-витягу (знаходить ЛІТЕРАТУРА навіть якщо після неї йдуть додатки)
            # Fallback — евристика по ключових словах / regex на повному тексті
            references = await self._extract_references_with_llm(full_text, doc_title)
            if references is None:
                references = extract_references_from_text(full_text)
                if references:
                    logger.info("references_extracted_regex_fallback", doc_id=doc_id, count=len(references))
            
            if references:
                all_references.extend(references)
                docs_with_refs.append(DocumentReferences(
                    document_id=doc_id,
                    document_title=doc_title,
                    document_url=doc_url,
                    references=references,
                ))
                
                logger.info(
                    "references_extracted",
                    doc_id=doc_id,
                    count=len(references),
                )
        
        # Дедуплікація
        unique_references = deduplicate_references(all_references)

        logger.info(
            "references_deduplicated",
            total=len(all_references),
            unique=len(unique_references),
        )

        # Фільтрація російських/радянських джерел (жорстко, як і в основному пайплайні)
        from harvester.bibliography import filter_russian_entries

        filtered_unique, russian_filtered = filter_russian_entries(unique_references)
        if russian_filtered:
            logger.info(
                "russian_references_filtered",
                filtered=len(russian_filtered),
                examples=[r.raw_text[:80] for r, _ in russian_filtered[:3]],
            )
        unique_references = filtered_unique

        # Пошук в інтернеті та БД (з підключенням до БД)
        db = None
        try:
            from harvester.db.failover import build_database

            db = build_database(self.settings)
            await db.initialize()
            search_results = await self.searcher.search_references(unique_references, db=db)
        finally:
            if db:
                try:
                    await db.close()
                except Exception:
                    pass
        if db is None:
            search_results = await self.searcher.search_references(unique_references)
        
        # Підрахунок статистики (пояснення: found_in_db - знайдено точний збіг в нашій БД за DOI/URL/назвою)
        found_in_db = sum(1 for r in search_results if r.in_database)
        found_online = sum(1 for r in search_results if r.found and not r.in_database)
        not_found = sum(1 for r in search_results if not r.found)

        # Завантаження знайдених PDF у окрему папку всередині каталогу
        bibli_pdfs_dir = catalog_dir / "bibliography_pdfs"
        bibli_pdfs_dir.mkdir(parents=True, exist_ok=True)
        downloaded, added_to_db = await self._download_and_register(
            search_results, bibli_pdfs_dir, catalog.get("topic", "")
        )
        
        # Створити вихідний документ
        if output_name is None:
            output_name = f"bibliography_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        output_dir = catalog_dir
        output_file = output_dir / f"{output_name}.json"
        
        output_data = {
            "catalog": str(catalog_dir),
            "topic": catalog.get("topic", ""),
            "created_at": datetime.now().isoformat(),
            "statistics": {
                "documents_scanned": len(catalog.get("documents", [])),
                "references_extracted": len(all_references),
                "references_after_dedup": len(unique_references) + len(russian_filtered),
                "references_filtered_russian": len(russian_filtered),
                "references_for_search": len(unique_references),
                "found_in_database": found_in_db,
                "found_online": found_online,
                "not_found": not_found,
                "pdfs_downloaded": len(downloaded),
                "documents_added_to_db": len(added_to_db),
                "bibliography_pdfs_dir": str(bibli_pdfs_dir),
            },
            "filtered_russian_examples": [
                {"raw_text": r.raw_text[:200], "reason": reason} for r, reason in russian_filtered[:10]
            ],
            "documents_with_references": [
                {
                    "document_id": dr.document_id,
                    "document_title": dr.document_title,
                    "document_url": dr.document_url,
                    "reference_count": len(dr.references),
                }
                for dr in docs_with_refs
            ],
            "references": [
                {
                    "raw_text": r.raw_text[:200],
                    "authors": r.authors,
                    "title": r.title,
                    "year": r.year,
                    "source": r.source,
                    "url": r.url,
                    "doi": r.doi,
                    "entry_type": r.entry_type,
                }
                for r in unique_references
            ],
            "search_results": [
                {
                    "reference": sr.reference.raw_text[:100],
                    "found": sr.found,
                    "source_url": sr.source_url,
                    "source_type": sr.source_type,
                    "in_database": sr.in_database,
                    "document_id": sr.document_id,
                    "relevance_score": sr.relevance_score,
                    "accessibility": sr.accessibility,
                }
                for sr in search_results
            ],
            "downloaded_pdfs": downloaded,
            "added_to_db": added_to_db,
            "explanation": {
                "found_in_database": "Кількість посилань, для яких знайдено точний збіг у нашій БД (за DOI, canonical_url або назвою + автор). 0 означає, що жодне з 383 унікальних посилань не співпало з уже верифікованими документами в harvester.documents. Це нормально, якщо бібліографія містить рідкісні монографії/підручники.",
                "found_online": "Кількість посилань, для яких вдалося знайти доступний файл в інтернеті (перевірка: URL дозволено, не .ru/.su/.рф, HTTP HEAD 200, content-type PDF або filetype:pdf через DDGS). 'шукається' означає, що пошук триває/таймаут DDGS або потік був перерваний.",
                "not_found": "Посилання, для яких не знайдено ні в БД, ні в інтернеті (заблоковано, таймаут, немає результатів DDGS).",
                "filtered_russian": "Відфільтровано російських/радянських джерел за правилами harvester (TLD .ru/.su/.рф, мова ru, видавництво 'Москва', 'Издательство'). Вони не шукаються і не додаються.",
                "pdfs_downloaded": "Скільки знайдених онлайн-PDF вдалося фактично завантажити та зберегти у bibliography_pdfs/ після перевірки доступності, релевантності (ключові слова теми) та інформативності (text_layer, розмір).",
            },
        }
        
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(output_data, f, ensure_ascii=False, indent=2)
        
        # Створити текстовий файл зі списком літератури
        text_file = output_dir / f"{output_name}_literature.txt"
        literature_text = format_references_list(unique_references)
        with open(text_file, "w", encoding="utf-8") as f:
            f.write(literature_text)
        
        # Знайдені в інтернеті з URL
        found_references = [sr for sr in search_results if sr.found and sr.source_url]
        found_file = output_dir / f"{output_name}_found.json"
        found_data = [
            {
                "reference": sr.reference.raw_text[:200],
                "url": sr.source_url,
                "type": sr.source_type,
                "in_database": sr.in_database,
                "document_id": sr.document_id,
                "relevance": sr.relevance_score,
            }
            for sr in found_references
        ]
        with open(found_file, "w", encoding="utf-8") as f:
            json.dump(found_data, f, ensure_ascii=False, indent=2)
        
        processing_time = asyncio.get_event_loop().time() - start_time

        result = CatalogScanResult(
            catalog_path=str(catalog_dir),
            documents_scanned=len(catalog.get("documents", [])),
            references_extracted=len(all_references),
            references_after_dedup=len(unique_references) + len(russian_filtered),
            references_filtered_russian=len(russian_filtered),
            references_for_search=len(unique_references),
            references_found_in_db=found_in_db,
            references_found_online=found_online,
            references_not_found=not_found,
            pdfs_downloaded=len(downloaded),
            pdfs_saved_dir=str(bibli_pdfs_dir),
            documents_added_to_db=len(added_to_db),
            processing_time_s=processing_time,
            output_file=str(output_file),
            details={
                "filtered_examples": [{"raw": r.raw_text[:120], "reason": rs} for r, rs in russian_filtered[:5]],
                "downloaded": downloaded[:5],
            },
        )
        
        logger.info(
            "bibliography_scan_complete",
            documents=result.documents_scanned,
            references_extracted=result.references_extracted,
            references_unique=result.references_after_dedup,
            found_in_db=result.references_found_in_db,
            found_online=result.references_found_online,
            not_found=result.references_not_found,
            time_s=round(processing_time, 1),
        )
        
        return result
    
    async def _download_and_register(
        self, search_results: list[SearchResult], dest_dir: Path, topic: str
    ) -> tuple[list[dict], list[int]]:
        """Завантажити знайдені PDF у окрему папку всередині каталогу та зареєструвати в БД.

        - Інтернет-ресурси: перевірка доступності, релевантності, інформативності -> лише у список + завантаження
        - Документи (online_pdf, які є повноцінними): після завантаження та перевірки -> у список + у БД (insert_or_ignore)
        """
        import re as _re

        downloaded: list[dict] = []
        added_ids: list[int] = []

        # Спільні перевірки: рефакторимо правила та пошук в одне місце (harvester.net + verify)
        from harvester.net.guards import is_url_allowed
        from harvester.verify.langid import detect_language
        from harvester.verify.pdfparse import parse_pdf

        # Лінива ініціалізація БД для запису
        db = None
        try:
            from harvester.db.failover import build_database
            from harvester.db.repositories import DocumentsRepository

            db = build_database(self.settings)
            await db.initialize()
        except Exception as e:
            logger.warning("bibliography_db_init_failed", error=str(e))
            db = None

        topic_keywords = ["пальто", "швейн", "одяг", "текстиль", "конструювання", "розкрій", "пошиття", "легка промисловість"]

        for sr in search_results:
            if not sr.found or not sr.source_url:
                continue
            # Пропускаємо вже наявні в БД - вони вже у списку
            if sr.in_database:
                continue
            # Лише PDF-джерела завантажуємо; online_abstract - лише у список
            if sr.source_type not in ("online_pdf", "db_document"):
                continue

            url = sr.source_url

            # 1. Загальні перевірки документів (анти-SSRF, blacklist, RU)
            allowed, reason = await is_url_allowed(url)
            if not allowed:
                logger.info("bibliography_download_blocked", url=url, reason=reason)
                sr.accessibility = "blocked"
                continue

            # 2. Перевірка мови за заголовком/сирцем посилання (швидка)
            try:
                lang = await detect_language(sr.reference.raw_text[:2000])
                if lang.language == "ru" and lang.confidence >= 0.8:
                    logger.info("bibliography_russian_skip_download", url=url)
                    continue
            except Exception:
                pass

            # 3. Завантаження
            try:
                import httpx

                # Використовуємо спільний HttpClient якщо можливо, але тут простіше httpx
                async with httpx.AsyncClient(
                    timeout=httpx.Timeout(30, connect=10), follow_redirects=True, headers={"User-Agent": self.settings.http.user_agent}
                ) as client:
                    resp = await client.get(url, headers={"Accept": "application/pdf,*/*"})
                    if resp.status_code not in (200, 206):
                        sr.accessibility = f"http_{resp.status_code}"
                        continue
                    ctype = resp.headers.get("content-type", "")
                    data = resp.content
                    if len(data) < 5_000:
                        sr.accessibility = "too_small"
                        continue
                    if data[:4] != b"%PDF" and "pdf" not in ctype.lower() and not url.lower().endswith(".pdf"):
                        # Можливо HTML сторінка, а не PDF - вважаємо як internet resource, не завантажуємо як PDF
                        sr.source_type = "online_abstract"
                        continue

                    # 4. Перевірка інформативності (parse_pdf)
                    tmp_path = dest_dir / f"tmp_{abs(hash(url)) % 10_000_000}.pdf"
                    tmp_path.write_bytes(data)
                    try:
                        parse_res = await parse_pdf(tmp_path, max_pages=5)
                    finally:
                        try:
                            tmp_path.unlink()
                        except Exception:
                            pass

                    # 4a. Has text layer?
                    if not parse_res.has_text_layer:
                        logger.info("bibliography_skip_no_text_layer", url=url)
                        continue
                    # 4b. Page count?
                    if parse_res.page_count < 1:
                        continue
                    # 4c. Релевантність: чи містить текст ключові слова теми
                    sample = (parse_res.text[:4000] + " " + sr.reference.title).lower()
                    relevant = any(kw in sample for kw in topic_keywords) or sr.relevance_score >= 0.5
                    if not relevant:
                        logger.info("bibliography_skip_not_relevant", url=url)
                        # Все одно зберігаємо як internet resource, але не як документ? - зберігаємо файл але позначаємо
                        pass

                    # 4d. Мова вмісту (RU фільтр)
                    try:
                        lang2 = await detect_language(parse_res.text[:3000])
                        if lang2.language == "ru" and lang2.confidence >= 0.8:
                            logger.info("bibliography_skip_russian_content", url=url)
                            continue
                    except Exception:
                        pass

                    # 5. Збереження у окрему папку всередині каталогу
                    safe_name = _re.sub(r"[^a-zA-Z0-9_-]", "_", sr.reference.title[:40] or "ref")[:40]
                    filename = f"{safe_name}_{abs(hash(url)) % 1_000_000}.pdf"
                    dest = dest_dir / filename
                    dest.write_bytes(data)
                    downloaded.append({"url": url, "file": str(dest.relative_to(dest_dir.parent)), "pages": parse_res.page_count, "title": sr.reference.title[:80]})

                    # 6. Запис у БД якщо це повноцінний документ (online_pdf + пройшов перевірки)
                    if db is not None and parse_res.has_text_layer and parse_res.page_count >= 2:
                        try:
                            repo = DocumentsRepository(db)
                            # Перевірка чи вже є
                            exists = await repo.get_by_canonical_url(url)
                            if not exists:
                                # Визначаємо рік з посилання
                                year_int = None
                                try:
                                    if sr.reference.year and sr.reference.year.isdigit():
                                        y = int(sr.reference.year)
                                        if 1900 <= y <= 2030:
                                            year_int = y
                                except Exception:
                                    pass
                                new_id = await repo.insert_or_ignore(
                                    canonical_url=url,
                                    title=sr.reference.title or None,
                                    title_hint=sr.reference.title or None,
                                    authors=sr.reference.authors or None,
                                    year=year_int,
                                    publisher=sr.reference.source or None,
                                    language="uk",
                                    doc_type="article" if sr.reference.entry_type == "article" else "other",
                                    page_count=parse_res.page_count,
                                    size_bytes=len(data),
                                    has_text_layer=True,
                                    status="discovered",
                                    extra={"bibliography_found": True, "topic": topic},
                                )
                                if new_id:
                                    added_ids.append(new_id)
                                    sr.document_id = new_id
                        except Exception as e:
                            logger.warning("bibliography_db_insert_failed", url=url, error=str(e))

            except Exception as e:
                logger.warning("bibliography_download_failed", url=url, error=str(e))
                continue

        if db is not None:
            try:
                await db.close()
            except Exception:
                pass

        logger.info("bibliography_download_complete", downloaded=len(downloaded), added_to_db=len(added_ids), dest=str(dest_dir))
        return downloaded, added_ids

    async def _extract_references_with_llm(
        self, full_text: str, doc_title: str
    ) -> list[BibliographyEntry] | None:
        """Витягти бібліографію через LLM (Gemini/Gemma). Повертає None — якщо LLM недоступний/помилка, щоб зробити fallback."""
        if not self.settings.llm.enabled:
            return None
        # Обрізаємо текст під ліміт LLM (як у extract/engine.py)
        max_chars = self.settings.llm.max_text_chars_for_llm
        truncated = full_text[:max_chars]
        # Якщо текст дуже довгий — стискаємо для Gemma, але для Gemini пробуємо повний
        content = f"НАЗВА ДОКУМЕНТА: {doc_title}\n\nТЕКСТ ДОКУМЕНТА:\n{truncated}"
        messages = [
            {"role": "system", "content": LLM_BIBLIO_PROMPT},
            {"role": "user", "content": content},
        ]
        # Спробуємо викликати через існуючу логіку extract/engine.py (щоб не дублювати)
        try:
            from harvester.extract.engine import call_llm_for_extraction  # noqa: WPS433

            # Використаємо call_llm_for_extraction як обгортку, але нам потрібен лише bibliography
            # Тому викликаємо напряму Gemini логіку тут (спрощено з engine.py)
            result = await self._call_llm_bibliography(messages)
            if result is None:
                return None
            refs_data = result.get("references") if isinstance(result, dict) else None
            if not isinstance(refs_data, list):
                return None
            entries: list[BibliographyEntry] = []
            for item in refs_data:
                if not isinstance(item, dict):
                    continue
                raw = (item.get("raw_text") or item.get("raw") or "").strip()
                if not raw:
                    continue
                # Використовуємо parse для нормалізації, але беремо LLM-поля як пріоритет
                entry = BibliographyEntry(
                    raw_text=raw,
                    authors=item.get("authors") or [],
                    title=item.get("title") or "",
                    year=str(item.get("year") or ""),
                    source=item.get("source") or "",
                    pages=item.get("pages") or "",
                    doi=item.get("doi") or "",
                    url=item.get("url") or "",
                    language=item.get("language") or "",
                    entry_type=item.get("entry_type") or "unknown",
                )
                # Якщо LLM не заповнив поля — доповнюємо евристикою
                if not entry.authors or not entry.title:
                    fallback = None
                    try:
                        from harvester.bibliography import parse_reference_entry

                        fallback = parse_reference_entry(raw)
                    except Exception:
                        pass
                    if fallback:
                        if not entry.authors:
                            entry.authors = fallback.authors
                        if not entry.title:
                            entry.title = fallback.title
                        if not entry.year:
                            entry.year = fallback.year
                        if not entry.source:
                            entry.source = fallback.source
                        if not entry.url:
                            entry.url = fallback.url
                        if not entry.doi:
                            entry.doi = fallback.doi
                entries.append(entry)
            logger.info("references_extracted_llm", count=len(entries), title=doc_title[:40])
            return entries
        except Exception as e:
            logger.warning("llm_bibliography_failed", error=str(e)[:200])
            return None

    async def _call_llm_bibliography(self, messages: list[dict]) -> dict | None:
        """Низькорівневий виклик Gemini/Gemma для бібліографії (аналог call_llm_for_extraction)."""
        import json as _json

        llm_cfg = self.settings.llm
        # Gemini
        for api_key in [self.settings.gemini_api_key, self.settings.gemini_api_key_2, self.settings.gemini_api_key_3]:
            if not api_key:
                continue
            try:
                res = await self._call_gemini_bibliography(api_key, llm_cfg, messages)
                if res is not None:
                    return res
            except Exception as e:
                logger.warning("gemini_bib_failed", error=str(e)[:150])
        # Gemma fallback
        for api_key in [self.settings.gemini_api_key, self.settings.gemini_api_key_2, self.settings.gemini_api_key_3]:
            if not api_key:
                continue
            for model in llm_cfg.gemma_models:
                try:
                    from harvester.classify.llm import rephrase_for_gemma

                    # Стискаємо контент
                    orig_content = messages[1]["content"]
                    truncated = f"НАЗВА ДОКУМЕНТА: {orig_content[:200]}\n\nТЕКСТ:\n{rephrase_for_gemma(orig_content, llm_cfg.gemma_max_chars)}"
                    gemma_messages = [
                        {"role": "system", "content": LLM_BIBLIO_PROMPT},
                        {"role": "user", "content": truncated},
                    ]
                    res = await self._call_gemini_bibliography(api_key, llm_cfg, gemma_messages, model_override=model)
                    if res is not None:
                        return res
                except Exception as e:
                    logger.warning("gemma_bib_failed", model=model, error=str(e)[:150])
        return None

    async def _call_gemini_bibliography(self, api_key: str, config, messages: list[dict], model_override: str | None = None) -> dict | None:
        """Виклик Gemini API для бібліографії."""
        import aiohttp
        import json as _json

        model = model_override or (config.gemini_models[0] if config.gemini_models else "gemini-2.0-flash")
        url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={api_key}"
        # Для бібліографії потрібно більше токенів (40 записів × ~150 символів)
        biblio_tokens = max(config.max_tokens, 8192)
        payload = {
            "contents": [
                {"role": messages[0]["role"], "parts": [{"text": messages[0]["content"]}]},
                {"role": messages[1]["role"], "parts": [{"text": messages[1]["content"]}]},
            ],
            "generationConfig": {"temperature": config.temperature, "maxOutputTokens": biblio_tokens},
        }
        timeout = aiohttp.ClientTimeout(total=config.timeout_s)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(url, json=payload) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    logger.warning("gemini_bib_api_error", status=resp.status, body=body[:300])
                    return None
                data = await resp.json()
        try:
            content_text = data["candidates"][0]["content"]["parts"][0]["text"]
            if "```" in content_text:
                # Витягти JSON між ```json ... ``` або ``` ... ```
                m = re.search(r"```(?:json)?\s*(\{.*?\}|\[.*?\])\s*```", content_text, re.DOTALL)
                if m:
                    content_text = m.group(1)
                else:
                    content_text = content_text.replace("```json", "").replace("```", "").strip()
            # Спроба прямого парсингу
            try:
                result = _json.loads(content_text)
            except _json.JSONDecodeError:
                # Ремонт: прибрати trailing commas, витягти об'єкт
                fixed = re.sub(r",\s*}", "}", content_text)
                fixed = re.sub(r",\s*]", "]", fixed)
                # Витягти перший {...} або [...]
                m2 = re.search(r"(\{.*\}|\[.*\])", fixed, re.DOTALL)
                if m2:
                    fixed = m2.group(1)
                result = _json.loads(fixed)
            if isinstance(result, dict) and "references" in result:
                return result
            # Якщо LLM повернув просто список
            if isinstance(result, list):
                return {"references": result}
            # Якщо повернув {"references": [...]} але з іншою обгорткою
            if isinstance(result, dict):
                for v in result.values():
                    if isinstance(v, list) and v and isinstance(v[0], dict) and "raw_text" in v[0]:
                        return {"references": v}
            logger.warning("llm_bib_invalid_format", response=content_text[:200])
            return None
        except Exception as e:
            logger.warning("llm_bib_parse_error", error=str(e)[:150], response=str(data)[:500])
            return None

    async def _extract_full_text(self, pdf_path: Path) -> str:
        """Вилучити ПОВНИЙ текст з PDF (всі сторінки) для LLM."""
        try:
            import fitz

            doc = fitz.open(str(pdf_path))
            parts = []
            for page in doc:
                try:
                    parts.append(page.get_text() or "")
                except Exception:
                    continue
            doc.close()
            full = "\n".join(parts)
            # Обрізаємо під ліміт LLM, але зберігаємо початок і кінець (де зазвичай література)
            max_chars = self.settings.llm.max_text_chars_for_llm if self.settings.llm else 80000
            if len(full) > max_chars:
                # Залишаємо початок (титул) + кінець (література + додатки)
                keep_start = max_chars // 3
                keep_end = max_chars - keep_start
                full = full[:keep_start] + "\n\n...[обрізано]...\n\n" + full[-keep_end:]
            return full
        except Exception as e:
            logger.warning("pdf_full_text_error", path=str(pdf_path), error=str(e))
            return ""

    async def _extract_text_from_pdf(self, pdf_path: Path) -> str:
        """Вилучити текст з PDF."""
        try:
            import fitz
            
            doc = fitz.open(str(pdf_path))
            text_parts = []
            
            # Читаємо перші 10 сторінок для вилучення літератури
            pages_to_read = min(len(doc), 10)
            for page_num in range(pages_to_read):
                page = doc[page_num]
                text = page.get_text()
                if text:
                    text_parts.append(text)
            
            # Також читаємо останні сторінки (де зазвичай література)
            if len(doc) > 10:
                for page_num in range(max(10, len(doc) - 5), len(doc)):
                    page = doc[page_num]
                    text = page.get_text()
                    if text:
                        text_parts.append(text)
            
            doc.close()
            return "\n".join(text_parts)
            
        except Exception as e:
            logger.warning("pdf_text_extraction_error", path=str(pdf_path), error=str(e))
            return ""
