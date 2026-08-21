import hashlib
import tempfile
from datetime import datetime, timedelta
from pathlib import Path

import structlog

from harvester.config import get_settings
from harvester.db.connection import Database
from harvester.db.repositories import (
    DocumentsRepository,
    FetchAttemptsRepository,
)
from harvester.dedup.urlnorm import normalize_url
from harvester.net.client import HttpClient
from harvester.net.guards import extract_domain, is_url_allowed
from harvester.verify.filters import apply_all_filters
from harvester.verify.langid import detect_language
from harvester.verify.pdfparse import extract_udc_from_text, parse_pdf
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
                if file_path.exists():
                    file_path.unlink()

        except Exception as e:
            logger.error("verify_pipeline_error", doc_id=doc_id, url=url, error=str(e), exc_info=True)
            await self._log_attempt(doc_id, "pipeline", url, "ERROR", started_at, error=str(e))
            return VerifyResult(False, "ERROR", str(e))

    async def _step_head(self, url: str, doc_id: int, started_at: str) -> VerifyResult:
        try:
            response = await self.http_client.head(url)

            content_type = response.headers.get("content-type", "").lower()
            if content_type and "pdf" not in content_type and "octet-stream" not in content_type:
                await self._log_attempt(doc_id, "head", url, "NOT_PDF", started_at, http_status=response.status_code)
                return VerifyResult(False, "NOT_PDF", f"Content-Type: {content_type}")

            content_length = response.headers.get("content-length")
            if content_length:
                size = int(content_length)
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

        try:
            content, size = await self.http_client.stream_download(url, self.settings.http.max_pdf_bytes)

            if not content.startswith(b"%PDF-"):
                tmp_file = tmp_dir / f"verify_{doc_id}_{int(datetime.utcnow().timestamp())}.tmp"
                tmp_file.write_bytes(content)
                await self._log_attempt(doc_id, "download", url, "NOT_PDF", started_at, bytes=size)
                tmp_file.unlink()
                return VerifyResult(False, "NOT_PDF", "No PDF magic bytes")

            sha256 = hashlib.sha256(content).hexdigest()

            tmp_file = tmp_dir / f"verify_{doc_id}_{int(datetime.utcnow().timestamp())}.pdf"
            tmp_file.write_bytes(content)

            await self._log_attempt(doc_id, "download", url, "OK", started_at, bytes=size)
            return VerifyResult(True, "OK", f"{tmp_file}|{size}|{sha256}")

        except Exception as e:
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
            parse_result = await parse_pdf(file_path, self.settings.verify.first_pages_for_text)

            if parse_result.is_encrypted:
                await self._log_attempt(doc_id, "parse", url, "ENCRYPTED", started_at)
                return VerifyResult(False, "ENCRYPTED", "PDF is encrypted")

            if parse_result.is_corrupt:
                await self._log_attempt(doc_id, "parse", url, "CORRUPT", started_at, error=parse_result.error)
                return VerifyResult(False, "CORRUPT", parse_result.error)

            text_sample = parse_result.text[:4000] if parse_result.text else None

            lang_result = await detect_language(parse_result.text)
            logger.debug(
                "verify_lang_detected",
                doc_id=doc_id,
                language=lang_result.language,
                confidence=lang_result.confidence,
                method=lang_result.method,
            )

            filtered, filter_reason = await apply_all_filters(
                url,
                lang_result,
                year=None,
                publisher=None,
                text_sample=parse_result.text[:500] if parse_result.text else None,
            )

            if filtered:
                status = "filtered_ru" if "russian" in filter_reason else "filtered_soviet"
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

            await self.docs_repo.update_verified(
                doc_id=doc_id,
                sha256=sha256,
                size_bytes=file_size,
                page_count=parse_result.page_count,
                language=lang_result.language,
                lang_confidence=lang_result.confidence,
                title=title,
                authors=authors,
                doc_type="article",
                udc=udc,
                has_text_layer=parse_result.has_text_layer,
                needs_review=needs_review,
                text_sample=text_sample,
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
