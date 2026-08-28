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
        
        # 1. Спочатку шукаємо в БД (якщо є DOI або URL)
        if db and ref.doi:
            db_result = await self._search_in_database(db, doi=ref.doi)
            if db_result:
                result.found = True
                result.in_database = True
                result.document_id = db_result["id"]
                result.source_url = db_result.get("canonical_url", "")
                result.source_type = "db_document"
                result.accessibility = "accessible"
                result.relevance_score = 1.0
                return result
        
        if db and ref.url:
            db_result = await self._search_in_database(db, url=ref.url)
            if db_result:
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
    ) -> dict | None:
        """Шукати документ в БД за DOI або URL."""
        try:
            if doi:
                rows = await db.fetchall(
                    "SELECT id, canonical_url, title FROM documents WHERE doi = ? AND status = 'verified'",
                    (doi,)
                )
                if rows:
                    return dict(rows[0])
            
            if url:
                rows = await db.fetchall(
                    "SELECT id, canonical_url, title FROM documents WHERE canonical_url = ? AND status = 'verified'",
                    (url,)
                )
                if rows:
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
        """Шукати документ за назвою."""
        from harvester.discovery.ddgs_search import DDGSSearchChannel
        
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
                results.append({
                    "url": candidate.url,
                    "title": candidate.title,
                    "type": "online_pdf",
                    "relevance": 0.6,
                    "accessibility": "unknown",
                })
            
            if results:
                # Повертаємо найкращий результат
                return results[0]
        except Exception as e:
            logger.warning("title_search_error", title=title[:50], error=str(e))
        
        return None
