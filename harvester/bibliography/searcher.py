"""Модуль пошуку бібліографічних джерел в інтернеті."""

from __future__ import annotations

import asyncio
import re
from dataclasses import dataclass, field
from typing import Any

import structlog

from harvester.bibliography import BibliographyEntry
from harvester.config import get_settings

logger = structlog.get_logger()


@dataclass
class SearchResult:
    """Результат пошуку одного джерела."""
    reference: BibliographyEntry
    found: bool = False
    source_url: str = ""
    source_type: str = ""  # "db_document", "online_pdf", "online_abstract"
    relevance_score: float = 0.0
    accessibility: str = ""  # "accessible", "restricted", "not_found"
    error: str = ""
    in_database: bool = False
    document_id: int | None = None


class BibliographySearcher:
    """Сервіс пошуку бібліографічних джерел."""
    
    def __init__(self):
        self.settings = get_settings()
    
    async def search_references(
        self,
        references: list[BibliographyEntry],
        db=None,
    ) -> list[SearchResult]:
        """Знайти всі посилання в інтернеті та БД."""
        results = []
        
        for ref in references:
            result = await self._search_single_reference(ref, db)
            results.append(result)
            
            # Затримка між запитами
            await asyncio.sleep(0.5)
        
        return results
    
    async def _search_single_reference(
        self,
        ref: BibliographyEntry,
        db=None,
    ) -> SearchResult:
        """Знайти одне посилання."""
        result = SearchResult(reference=ref)
        
        # 1. Спочатку шукаємо в БД (DOI, URL, назва+автор)
        if db:
            # Спроба за DOI / URL / назвою одним запитом
            db_result = await self._search_in_database(
                db,
                doi=ref.doi or None,
                url=ref.url or None,
                title=ref.title or None,
                authors=ref.authors or None,
            )
            if db_result:
                # Перевірка: чи не є знайдений документ російським (фільтр RU)
                try:
                    from harvester.net.guards import is_domain_blocked

                    if await is_domain_blocked(db_result.get("canonical_url", "")):
                        logger.info("db_result_blocked_domain", url=db_result.get("canonical_url"))
                    else:
                        # Мовна перевірка назви
                        if ref.raw_text:
                            from harvester.verify.langid import detect_language

                            lang = await detect_language(ref.raw_text[:2000])
                            if lang.language == "ru" and lang.confidence >= 0.8:
                                logger.info("db_result_russian_filtered", title=ref.title[:40])
                            else:
                                result.found = True
                                result.in_database = True
                                result.document_id = db_result["id"]
                                result.source_url = db_result.get("canonical_url", "")
                                result.source_type = "db_document"
                                result.accessibility = "accessible"
                                result.relevance_score = 1.0
                                return result
                except Exception:
                    result.found = True
                    result.in_database = True
                    result.document_id = db_result["id"]
                    result.source_url = db_result.get("canonical_url", "")
                    result.source_type = "db_document"
                    result.accessibility = "accessible"
                    result.relevance_score = 1.0
                    return result
        
        # 2. Шукаємо в інтернеті
        if ref.doi:
            internet_result = await self._search_by_doi(ref.doi)
            if internet_result:
                result.found = True
                result.source_url = internet_result["url"]
                result.source_type = internet_result.get("type", "online_pdf")
                result.relevance_score = internet_result.get("relevance", 0.8)
                result.accessibility = "accessible"
                return result
        
        # 3. Шукаємо за назвою
        if ref.title:
            internet_result = await self._search_by_title(ref.title, ref.authors, ref.year)
            if internet_result:
                result.found = True
                result.source_url = internet_result["url"]
                result.source_type = internet_result.get("type", "online_pdf")
                result.relevance_score = internet_result.get("relevance", 0.6)
                result.accessibility = internet_result.get("accessibility", "unknown")
                return result
        
        result.accessibility = "not_found"
        return result
    
    async def _search_in_database(
        self,
        db,
        doi: str | None = None,
        url: str | None = None,
        title: str | None = None,
        authors: list[str] | None = None,
    ) -> dict | None:
        """Шукати документ в БД за DOI, URL або назвою."""
        try:
            if doi:
                rows = await db.fetchall(
                    "SELECT id, canonical_url, title FROM documents WHERE doi = ? AND status = 'verified'",
                    (doi,)
                )
                if rows:
                    return dict(rows[0])

            if url:
                # Точне співпадіння
                rows = await db.fetchall(
                    "SELECT id, canonical_url, title FROM documents WHERE canonical_url = ? AND status = 'verified'",
                    (url,)
                )
                if rows:
                    return dict(rows[0])
                # Нормалізований URL (без параметрів)
                from harvester.dedup.urlnorm import normalize_url

                try:
                    norm = normalize_url(url)
                    rows = await db.fetchall(
                        "SELECT id, canonical_url, title FROM documents WHERE canonical_url = ? AND status = 'verified'",
                        (norm,),
                    )
                    if rows:
                        return dict(rows[0])
                except Exception:
                    pass

            if title and len(title.strip()) >= 10:
                # Пошук за підстрокою назви (перші 80 символів, без лапок)
                clean = re.sub(r"[«»\"']", "", title).strip()[:80]
                # Прибираємо дуже короткі слова
                if len(clean) >= 10:
                    rows = await db.fetchall(
                        "SELECT id, canonical_url, title FROM documents WHERE status='verified' AND title LIKE ? LIMIT 3",
                        (f"%{clean[:60]}%",),
                    )
                    if rows:
                        # Додаткова перевірка: якщо автор співпадає - підвищуємо впевненість
                        if authors:
                            for r in rows:
                                db_title = (r["title"] or "").lower()
                                if clean.lower()[:30] in db_title or db_title[:30] in clean.lower():
                                    return dict(r)
                        else:
                            return dict(rows[0])
        except Exception as e:
            logger.warning("db_search_error", error=str(e))

        return None
    
    async def _search_by_doi(self, doi: str) -> dict | None:
        """Шукати документ за DOI."""
        import httpx
        
        settings = get_settings()
        url = f"https://doi.org/{doi}"
        
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                resp = await client.get(
                    url,
                    headers={"Accept": "application/pdf"},
                    follow_redirects=True,
                )
                
                if resp.status_code == 200:
                    content_type = resp.headers.get("content-type", "")
                    if "pdf" in content_type.lower():
                        return {
                            "url": str(resp.url),
                            "type": "online_pdf",
                            "relevance": 0.9,
                        }
                    else:
                        return {
                            "url": str(resp.url),
                            "type": "online_abstract",
                            "relevance": 0.5,
                        }
        except Exception as e:
            logger.warning("doi_search_error", doi=doi, error=str(e))
        
        return None
    
    async def _search_by_title(
        self,
        title: str,
        authors: list[str] | None = None,
        year: str | None = None,
    ) -> dict | None:
        """Шукати документ за назвою. З фільтром RU та перевіркою доступності."""
        from harvester.discovery.ddgs_search import DDGSSearchChannel
        from harvester.net.guards import is_url_allowed

        # Формуємо запит
        query_parts = [title]
        if authors:
            query_parts.append(authors[0].split()[0])  # Прізвище першого автора
        if year:
            query_parts.append(year)

        query = " ".join(query_parts) + " filetype:pdf"

        search_channel = DDGSSearchChannel()

        try:
            results = []
            async for candidate in search_channel.discover({"query_text": query, "max_results": 5}):
                url = candidate.url
                # Фільтр RU / заблокованих доменів - жорстко відкидаємо
                allowed, reason = await is_url_allowed(url)
                if not allowed:
                    logger.info("search_result_filtered", url=url, reason=reason)
                    continue
                # Додаткова перевірка мови заголовка кандидата (швидка евристика)
                cand_title = (candidate.title or "") + " " + (candidate.url or "")
                if any(x in cand_title.lower() for x in [".ru", ".su", "xn--p1ai"]) and "москва" in cand_title.lower():
                    continue
                results.append({
                    "url": url,
                    "title": candidate.title,
                    "type": "online_pdf",
                    "relevance": 0.6,
                    "accessibility": "unknown",
                })

            if results:
                # Перевірка доступності першого результату (HEAD)
                best = results[0]
                try:
                    import httpx

                    async with httpx.AsyncClient(timeout=10, follow_redirects=True) as client:
                        resp = await client.head(best["url"], headers={"User-Agent": get_settings().http.user_agent})
                        if resp.status_code in (200, 206):
                            ct = resp.headers.get("content-type", "")
                            if "pdf" in ct.lower() or best["url"].lower().endswith(".pdf"):
                                best["accessibility"] = "accessible"
                            else:
                                best["type"] = "online_abstract"
                                best["accessibility"] = "accessible"
                        elif resp.status_code in (403, 404, 451):
                            best["accessibility"] = "restricted"
                        else:
                            best["accessibility"] = "unknown"
                except Exception:
                    best["accessibility"] = "unknown"
                return best
        except Exception as e:
            logger.warning("title_search_error", title=title[:50], error=str(e))

        return None
