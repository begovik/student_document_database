"""Сервіс вилучення та пошуку бібліографічних джерел з каталогу."""

from __future__ import annotations

import asyncio
import json
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


@dataclass
class CatalogScanResult:
    """Результат сканування каталогу."""
    catalog_path: str
    documents_scanned: int = 0
    references_extracted: int = 0
    references_after_dedup: int = 0
    references_found_in_db: int = 0
    references_found_online: int = 0
    references_not_found: int = 0
    processing_time_s: float = 0.0
    output_file: str = ""


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
            
            # Вилучити текст з PDF
            text = await self._extract_text_from_pdf(pdf_path)
            if not text:
                logger.warning("no_text_extracted", doc_id=doc_id)
                continue
            
            # Вилучити посилання
            references = extract_references_from_text(text)
            
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
        
        # Пошук в інтернеті та БД
        search_results = await self.searcher.search_references(unique_references)
        
        # Підрахунок статистики
        found_in_db = sum(1 for r in search_results if r.in_database)
        found_online = sum(1 for r in search_results if r.found and not r.in_database)
        not_found = sum(1 for r in search_results if not r.found)
        
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
                "references_after_dedup": len(unique_references),
                "found_in_database": found_in_db,
                "found_online": found_online,
                "not_found": not_found,
            },
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
            references_after_dedup=len(unique_references),
            references_found_in_db=found_in_db,
            references_found_online=found_online,
            references_not_found=not_found,
            processing_time_s=processing_time,
            output_file=str(output_file),
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
