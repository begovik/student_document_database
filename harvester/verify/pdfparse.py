import asyncio
from dataclasses import dataclass
from pathlib import Path

import structlog

logger = structlog.get_logger()


@dataclass
class PDFMetadata:
    title: str | None = None
    author: str | None = None
    subject: str | None = None
    creator: str | None = None
    producer: str | None = None
    creation_date: str | None = None
    modification_date: str | None = None


@dataclass
class PDFParseResult:
    page_count: int
    metadata: PDFMetadata
    text: str
    has_text_layer: bool
    is_encrypted: bool = False
    is_corrupt: bool = False
    error: str | None = None


async def parse_pdf(file_path: Path, max_pages: int = 3) -> PDFParseResult:
    try:
        result = await asyncio.to_thread(_parse_pdf_sync, file_path, max_pages)
        return result
    except Exception as e:
        logger.error("pdf_parse_error", path=str(file_path), error=str(e))
        return PDFParseResult(
            page_count=0,
            metadata=PDFMetadata(),
            text="",
            has_text_layer=False,
            is_corrupt=True,
            error=str(e),
        )


def _parse_pdf_sync(file_path: Path, max_pages: int) -> PDFParseResult:
    try:
        import fitz
    except ImportError:
        logger.error("pymupdf_not_installed")
        return PDFParseResult(
            page_count=0,
            metadata=PDFMetadata(),
            text="",
            has_text_layer=False,
            is_corrupt=True,
            error="PyMuPDF not installed",
        )

    try:
        doc = fitz.open(str(file_path))

        if doc.needs_pass:
            doc.close()
            return PDFParseResult(
                page_count=0,
                metadata=PDFMetadata(),
                text="",
                has_text_layer=False,
                is_encrypted=True,
            )

        page_count = len(doc)
        metadata_dict = doc.metadata or {}

        metadata = PDFMetadata(
            title=metadata_dict.get("title"),
            author=metadata_dict.get("author"),
            subject=metadata_dict.get("subject"),
            creator=metadata_dict.get("creator"),
            producer=metadata_dict.get("producer"),
            creation_date=metadata_dict.get("creationDate"),
            modification_date=metadata_dict.get("modDate"),
        )

        text_parts = []
        pages_to_read = min(page_count, max_pages)
        for page_num in range(pages_to_read):
            page = doc[page_num]
            text = page.get_text()
            if text:
                text_parts.append(text)

        doc.close()

        full_text = "\n".join(text_parts)
        has_text_layer = len(full_text.strip()) > 50

        return PDFParseResult(
            page_count=page_count,
            metadata=metadata,
            text=full_text,
            has_text_layer=has_text_layer,
        )

    except Exception as e:
        logger.error("pymupdf_parse_error", error=str(e))
        return PDFParseResult(
            page_count=0,
            metadata=PDFMetadata(),
            text="",
            has_text_layer=False,
            is_corrupt=True,
            error=str(e),
        )


def extract_udc_from_text(text: str) -> str | None:
    import re

    udc_pattern = r"УДК\s*([\d.():+\-]+)"
    match = re.search(udc_pattern, text, re.IGNORECASE)
    if match:
        return match.group(1).strip()

    udc_pattern_en = r"UDC\s*([\d.():+\-]+)"
    match = re.search(udc_pattern_en, text, re.IGNORECASE)
    if match:
        return match.group(1).strip()

    return None
