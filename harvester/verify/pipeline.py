import asyncio
import re
import tempfile
from datetime import datetime
from pathlib import Path

import structlog

from harvester.config import get_settings
from harvester.db.connection import Database
from harvester.db.repositories import (
    DocumentsRepository,
    FetchAttemptsRepository,
)
from harvester.net.client import HttpClient
from harvester.net.guards import is_url_allowed
from harvester.verify.filters import apply_all_filters
from harvester.verify.langid import detect_language
from harvester.verify.pdfparse import extract_udc_from_text, extract_year_from_text, parse_pdf
from harvester.verify.titlematch import match_title

logger = structlog.get_logger()


class VerifyResult:
    def __init__(self, success: bool, code: str, message: str | None = None):
        self.success = success
        self.code = code
        self.message = message


class VerifyPipeline:
    def __init__(self, db: Database, http_client: HttpClient):
        self.db = db
        self.http_client = http_client
        self.docs_repo = DocumentsRepository(db)
        self.attempts_repo = FetchAttemptsRepository(db)
        self.settings = get_settings()

    async def verify_document(self, doc_id: int, url: str, title_hint: str | None = None) -> VerifyResult:
        started_at = datetime.utcnow().isoformat()

        allowed, reason = await is_url_allowed(url)
        if not allowed:
            await self._log_attempt(doc_id, "precheck", url, "BLACKLISTED", started_at, error=reason)
            return VerifyResult(False, "BLACKLISTED", reason)

        try:
            result = await self._step_head(url, doc_id, started_at)
            if not result.success:
                return result

            result = await self._step_download(url, doc_id, started_at)
            if not result.success:
                return result

            file_path, file_size, sha256 = result.message.split("|")
            file_path = Path(file_path)

            try:
                result = await self._step_parse_and_verify(
                    doc_id, url, file_path, int(file_size), sha256, title_hint, started_at
                )
                return result
            finally:
                await asyncio.to_thread(file_path.unlink, missing_ok=True)

        except Exception as e:
            logger.error("verify_pipeline_error", doc_id=doc_id, url=url, error=str(e), exc_info=True)
            await self._log_attempt(doc_id, "pipeline", url, "ERROR", started_at, error=str(e))
            return VerifyResult(False, "ERROR", str(e))

    async def _step_head(self, url: str, doc_id: int, started_at: str) -> VerifyResult:
        try:
            response = await self.http_client.head(url)

            if response.status_code >= 400 and response.status_code != 405:
                await self._log_attempt(
                    doc_id,
                    "head",
                    url,
                    "HTTP_STATUS",
                    started_at,
                    http_status=response.status_code,
                )
                return VerifyResult(False, "HTTP_STATUS", f"HTTP {response.status_code}")

            content_type = response.headers.get("content-type", "").lower()
            if response.status_code != 405 and content_type and "pdf" not in content_type and "octet-stream" not in content_type:
                await self._log_attempt(doc_id, "head", url, "NOT_PDF", started_at, http_status=response.status_code)
                return VerifyResult(False, "NOT_PDF", f"Content-Type: {content_type}")

            content_length = response.headers.get("content-length")
            if content_length:
                try:
                    size = int(content_length)
                except ValueError:
                    size = 0
                if size < self.settings.http.min_pdf_bytes:
                    await self._log_attempt(doc_id, "head", url, "TOO_SMALL", started_at, http_status=response.status_code, bytes=size)
                    return VerifyResult(False, "TOO_SMALL", f"Size: {size}")
                if size > self.settings.http.max_pdf_bytes:
                    await self._log_attempt(doc_id, "head", url, "TOO_LARGE", started_at, http_status=response.status_code, bytes=size)
                    return VerifyResult(False, "TOO_LARGE", f"Size: {size}")

            return VerifyResult(True, "OK")

        except Exception as e:
            await self._log_attempt(doc_id, "head", url, "HTTP_ERROR", started_at, error=str(e))
            return VerifyResult(False, "HTTP_ERROR", str(e))

    async def _step_download(self, url: str, doc_id: int, started_at: str) -> VerifyResult:
        tmp_dir = Path(self.settings.paths.tmp_dir)
        tmp_dir.mkdir(parents=True, exist_ok=True)
        tmp_file: Path | None = None

        try:
            import os

            fd, tmp_name = tempfile.mkstemp(prefix=f"verify_{doc_id}_", suffix=".pdf", dir=tmp_dir)
            os.close(fd)
            tmp_file = Path(tmp_name)
            size, sha256, prefix = await self.http_client.stream_to_file(
                url,
                tmp_file,
                self.settings.http.max_pdf_bytes,
            )

            if size < self.settings.http.min_pdf_bytes:
                await self._log_attempt(doc_id, "download", url, "TOO_SMALL", started_at, bytes=size)
                return VerifyResult(False, "TOO_SMALL", f"Size: {size}")
            if not prefix.startswith(b"%PDF-"):
                await self._log_attempt(doc_id, "download", url, "NOT_PDF", started_at, bytes=size)
                return VerifyResult(False, "NOT_PDF", "No PDF magic bytes")

            await self._log_attempt(doc_id, "download", url, "OK", started_at, bytes=size)
            return VerifyResult(True, "OK", f"{tmp_file}|{size}|{sha256}")

        except Exception as e:
            if tmp_file is not None:
                await asyncio.to_thread(tmp_file.unlink, missing_ok=True)
            await self._log_attempt(doc_id, "download", url, "DOWNLOAD_ERROR", started_at, error=str(e))
            return VerifyResult(False, "DOWNLOAD_ERROR", str(e))

    async def _step_parse_and_verify(
        self,
        doc_id: int,
        url: str,
        file_path: Path,
        file_size: int,
        sha256: str,
        title_hint: str | None,
        started_at: str,
    ) -> VerifyResult:
        step_start = datetime.utcnow()
        try:
            parse_result = await parse_pdf(file_path, self.settings.verify.max_pages)

            if parse_result.is_encrypted:
                await self._log_attempt(doc_id, "parse", url, "ENCRYPTED", started_at)
                return VerifyResult(False, "ENCRYPTED", "PDF is encrypted")

            if parse_result.is_corrupt:
                await self._log_attempt(doc_id, "parse", url, "CORRUPT", started_at, error=parse_result.error)
                return VerifyResult(False, "CORRUPT", parse_result.error)

            if not parse_result.text or len(parse_result.text.strip()) < 500:
                await self._log_attempt(doc_id, "parse", url, "INSUFFICIENT_TEXT", started_at)
                return VerifyResult(False, "INSUFFICIENT_TEXT", "PDF не містить достатнього повного тексту")

            text_sample = (parse_result.text_sample or parse_result.text[:4000])[:4000]
            lang_text = parse_result.text[:20000]

            lang_result = await detect_language(lang_text)
            logger.debug(
                "verify_lang_detected",
                doc_id=doc_id,
                language=lang_result.language,
                confidence=lang_result.confidence,
                method=lang_result.method,
            )

            existing_doc = await self.docs_repo.get_by_id(doc_id) or {}
            duplicate = await self.docs_repo.get_by_sha256(sha256)
            if duplicate and duplicate.get("id") != doc_id:
                await self.docs_repo.update_status(doc_id, "duplicate")
                await self.db.execute(
                    "UPDATE documents SET duplicate_of = ? WHERE id = ?",
                    (duplicate["id"], doc_id),
                )
                await self._log_attempt(doc_id, "dedup", url, "DUPLICATE", started_at)
                logger.info(
                    "document_duplicate",
                    doc_id=doc_id,
                    duplicate_of=duplicate["id"],
                )
                return VerifyResult(False, "DUPLICATE", f"duplicate_of={duplicate['id']}")

            # Шукаємо рік у перших сторінках, а не в усьому тексті: роки в
            # бібліографії не повинні помилково перетворювати сучасний PDF на
            # радянське видання.
            year = existing_doc.get("year") or extract_year_from_text(parse_result.text_sample)
            publisher = existing_doc.get("publisher")
            filtered, filter_reason = await apply_all_filters(
                url,
                lang_result,
                year=year,
                publisher=publisher,
                text_sample=text_sample,
            )

            if filtered:
                if filter_reason == "russian_language":
                    status = "filtered_ru"
                elif filter_reason == "domain_blacklisted":
                    status = "filtered_domain"
                else:
                    status = "filtered_soviet"
                await self._log_attempt(doc_id, "filter", url, status.upper(), started_at)
                await self.docs_repo.update_status(doc_id, status)
                logger.info("document_filtered", doc_id=doc_id, status=status, reason=filter_reason)
                return VerifyResult(False, status, filter_reason)

            title_score, match_status = match_title(
                title_hint,
                parse_result.metadata.title,
                parse_result.text,
            )

            needs_review = match_status == "review" or match_status == "mismatch"

            udc = extract_udc_from_text(parse_result.text) if parse_result.text else None

            authors = [parse_result.metadata.author] if parse_result.metadata.author else None

            title = parse_result.metadata.title
            if not title and parse_result.text:
                from harvester.verify.pdfparse import extract_title_from_text
                title = extract_title_from_text(parse_result.text)
                if title:
                    logger.info("title_extracted_from_text", doc_id=doc_id, title=title[:80])

            structure = _detect_structure(parse_result.text)
            structure["has_title_page"] = bool(parse_result.metadata.title or title)
            structure["only_abstract"] = (
                bool(re.search(r"(?im)^\s*(?:abstract|анотація|реферат)\s*$", parse_result.text))
                and not structure["has_references"]
                and len(parse_result.text) < 2500
            )

            await self.docs_repo.update_verified(
                doc_id=doc_id,
                sha256=sha256,
                size_bytes=file_size,
                page_count=parse_result.page_count,
                language=lang_result.language,
                lang_confidence=lang_result.confidence,
                title=title,
                authors=authors,
                year=year,
                publisher=publisher,
                doc_type=None,
                udc=udc,
                has_text_layer=parse_result.has_text_layer,
                needs_review=needs_review,
                text_sample=text_sample,
                extra={
                    "pdf_metadata": {
                        key: value
                        for key, value in vars(parse_result.metadata).items()
                        if value
                    },
                    "text_length": len(parse_result.text),
                    "structure": structure,
                },
            )

            duration_ms = int((datetime.utcnow() - step_start).total_seconds() * 1000)
            await self._log_attempt(doc_id, "verify", url, "OK", started_at, duration_ms=duration_ms, bytes=file_size)

            logger.info(
                "document_verified",
                doc_id=doc_id,
                url=url,
                language=lang_result.language,
                pages=parse_result.page_count,
                size=file_size,
                title_score=title_score,
            )

            return VerifyResult(True, "OK")

        except Exception as e:
            await self._log_attempt(doc_id, "parse", url, "PARSE_ERROR", started_at, error=str(e))
            return VerifyResult(False, "PARSE_ERROR", str(e))

    async def _log_attempt(
        self,
        doc_id: int,
        kind: str,
        url: str,
        result_code: str,
        started_at: str,
        duration_ms: int | None = None,
        http_status: int | None = None,
        bytes: int | None = None,
        error: str | None = None,
    ) -> None:
        try:
            await self.attempts_repo.insert(
                document_id=doc_id,
                kind=kind,
                url=url,
                result_code=result_code,
                started_at=started_at,
                duration_ms=duration_ms,
                http_status=http_status,
                bytes=bytes,
                error=error,
            )
        except Exception as e:
            logger.error("log_attempt_error", error=str(e))


def _detect_structure(text: str) -> dict[str, object]:
    """Зберегти недорогі структурні ознаки для curator/quality-контурів."""
    heading = r"(?im)^\s*(?:\d+(?:\.\d+)*[.)]?\s+)?"
    references = bool(
        re.search(heading + r"(?:references|bibliography|література|список використаних джерел)", text)
    )
    introduction = bool(re.search(heading + r"(?:introduction|вступ|введение)", text))
    conclusion = bool(re.search(heading + r"(?:conclusion|conclusions|висновки|висновок)", text))
    numbered_sections = len(re.findall(r"(?im)^\s*\d+(?:\.\d+)*[.)]?\s+[A-ZА-ЯІЇЄҐ]", text))
    toc_lines = re.findall(r"(?im)^\s*(?:\d+(?:\.\d+)*\s+)?[^\n]{3,80}\.{2,}\s*\d+\s*$", text)
    toc_chars = sum(len(line) for line in toc_lines)
    return {
        "has_references": references,
        "has_introduction": introduction,
        "has_conclusion": conclusion,
        "structured_sections": numbered_sections >= 2,
        "numbered_sections": numbered_sections,
        "toc_ratio": round(toc_chars / max(len(text), 1), 4),
    }
