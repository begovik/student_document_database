"""Сервіс витягу цитат і сумаризацій з PDF-документів."""

from __future__ import annotations

import asyncio
import json
import os
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import structlog

from harvester.config import get_settings
from harvester.net.client import get_http_client
from harvester.verify.pdfparse import parse_pdf

logger = structlog.get_logger()

LLM_SYSTEM_PROMPT = """Ти — дослідник, який аналізує наукові статті українською мовою.

Твоя задача — витягнути з тексту статті:

1. ЦИТАТИ (quotations) — це цінні висловлювання, які можна цитувати в інших роботах.
   Критерії цитати:
   - Це формулювання, яке містить важливу думку, теорему, висновок, визначення
   - Це прямий вислів автора, який має змістове навантаження
   - Це статистика, цифри, факти, які є 논거ованими
   - Це визначення понять, класифікації, методики
   - Це висновки дослідження, які можна використати в інших роботах

   Кожна цитата має бути НЕ довшою за 3 речення і НЕ коротшою за 5 слів.
   Якщо цитата дуже довга, обріж її до найбільш важливої частини.
   Якщо цитата містить формули, математичні вирази або спеціальні символи —
   заміни їх на "[...]" або опиши словами.

   ФОРМАТ цитат (JSON):
   [
     {"page": 3, "text": "Ціна цитати...", "type": "conclusion|definition|fact|method|insight"}
   ]

   page — номер сторінки документа, де знайдена цитата (завжди ціле число)

   type — категорія:
     - "conclusion" — висновок дослідження, завершальна думка
     - "definition" — визначення поняття, терміна
     - "fact" — факт, статистика, цифра, дані дослідження
     - "method" — опис методу, підходу, процедури
     - "insight" — важлива думка, ідея, яка не вписується в інші категорії

2. СУМАРИЗАЦІЯ (summary) — це структурований опис статті ПО РОЗДІЛАХ/КЛЮЧОВИХ ТЕМАХ.

   ВАЖЛИВО: НЕ роби одну загальну сумаризацію на всю статтю!
   Замість цього:
   - Якщо стаття має чітку структуру (розділи, глави, абзаци з заголовками) —
     зроби сумаризацію КОЖНОГО розділу окремо
   - Якщо розділів забагато (>5) — обери 3-5 ключових тем та зроби сумаризацію кожної
   - Якщо стаття коротка або без розділів — зроби 2-3 сумаризації за ключовими темами

   ФОРМАТ сумаризації (JSON):
   {
     "sections": [
       {
         "page": 1,
         "title": "Назва розділу/теми",
         "overview": "Короткий опис розділу (1-2 речення)",
         "key_ideas": ["Ідея 1", "Ідея 2"],
         "methodology": "Як проводилось (якщо є)",
         "findings": "Результати (якщо є)",
         "conclusions": "Висновки (якщо є)"
       }
     ],
     "authors_mentioned": ["Імя Автор1", "Імя Автор2"]
   }

   КОЖНА секція має містити:
   - page: номер сторінки, з якої взято основну інформацію
   - title: назва розділу або ключової теми (3-10 слів)
   - overview: короткий опис (1-2 речення)
   - key_ideas: список ключових ідей (2-5 штук)
   - methodology: опис методу (якщо є, інакше "н/зв")
   - findings: результати (якщо є, інакше "н/зв")
   - conclusions: висновки (якщо є, інакше "н/зв")

   authors_mentioned: автори, згадані у тексті (якщо відрізняються від заголовка)

ВАЖЛИВО:
- Відповідь має бути JSON (без markdown, без пояснень)
- Відповідь має містити БЕЗ ЗМІСТУ (лише JSON об'єкт)
- Якщо текст статті пустий або недоступний — поверни {"quotations": [], "summary": null}
- Якщо текст англійською — аналізуй як зазвичай
- Якщо текст українською — аналізуй як зазвичай
- Якщо стаття містить багато таблиць, формул, графіків — їх НЕ включай в цитати
- Мінімум 2 секції, максимум 7 (обери найважливіше)

Приклад відповіді:
{"quotations":[{"page":5,"text":"Ціна цитати...","type":"conclusion"}],"summary":{"sections":[{"page":1,"title":"Вступ","overview":"...","key_ideas":["..."],"methodology":"н/зв","findings":"н/зв","conclusions":"н/зв"},{"page":3,"title":"Методологія","overview":"...","key_ideas":["..."],"methodology":"...","findings":"н/зв","conclusions":"н/зв"}],"authors_mentioned":["..."]}}"""

# Максимальна кількість символів тексту для відправки в LLM
MAX_TEXT_CHARS_FOR_LLM = 80000  # Deprecated: use settings.llm.max_text_chars_for_llm


def _has_meaningful_quotations(quotations: Any) -> bool:
    if not isinstance(quotations, list):
        return False
    for quotation in quotations:
        if isinstance(quotation, dict) and str(quotation.get("text", "")).strip():
            return True
    return False


def _has_meaningful_summary(summary: Any) -> bool:
    if not isinstance(summary, dict):
        return False
    sections = summary.get("sections")
    if not isinstance(sections, list) or not sections:
        return False
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
            return True
    return False


@dataclass
class ExtractionResult:
    """Результат витягу для одного документа."""
    document_id: int
    canonical_url: str
    quotations: list[dict[str, Any]] = field(default_factory=list)
    summary: dict[str, Any] | None = None
    success: bool = False
    error: str | None = None
    text_pages_extracted: int = 0


@dataclass
class ExtractionJob:
    """Завдання на витяг для одного документа."""
    document_id: int
    canonical_url: str
    title: str
    pdf_path: str | None = None  # Відносний шлях до PDF (наприклад, "resources/157.pdf") або None для завантаження
    # already_extracted: bool — якщо True, пропускати


async def download_pdf(url: str, timeout_s: float = 60.0) -> tuple[Path | None, str | None]:
    """Завантажити PDF за URL у тимчасовий файл.

    Повертає (шлях_до_файлу, None) у разі успіху або
    (None, опис_помилки), якщо завантаження не вдалося.
    """
    settings = get_settings()

    headers = {
        "User-Agent": settings.http.user_agent,
        "Accept": "application/pdf,*/*",
    }

    tmp_path: Path | None = None
    try:
        client = await get_http_client()
        chunks: list[bytes] = []
        total = 0
        async with client.stream("GET", url, headers=headers, timeout=timeout_s) as resp:
            if not 200 <= resp.status_code < 300:
                reason = f"HTTP {resp.status_code}"
                logger.warning("pdf_download_failed", url=url, status=resp.status_code)
                return None, reason

            content_type = resp.headers.get("content-type", "")
            if "html" in content_type.lower():
                reason = f"відповідь не є PDF (content-type={content_type})"
                logger.warning("pdf_download_not_pdf", url=url, content_type=content_type)
                return None, reason

            async for chunk in resp.aiter_bytes(chunk_size=65536):
                total += len(chunk)
                if total > settings.http.max_pdf_bytes:
                    return None, f"файл перевищує ліміт {settings.http.max_pdf_bytes} байт"
                chunks.append(chunk)

        data = b"".join(chunks)
        if len(data) < 1024:
            reason = f"файл занадто малий ({len(data)} байт)"
            logger.warning("pdf_download_too_small", url=url, size=len(data))
            return None, reason

        if data[:4] != b"%PDF":
            reason = "відсутні %PDF magic bytes"
            logger.warning("pdf_download_not_pdf_magic", url=url)
            return None, reason

        fd, name = tempfile.mkstemp(suffix=".pdf")
        os.close(fd)
        tmp_path = Path(name)
        await asyncio.to_thread(tmp_path.write_bytes, data)
        logger.info("pdf_downloaded", url=url, bytes=len(data))
        return tmp_path, None

    except Exception as e:
        if tmp_path is not None:
            await asyncio.to_thread(tmp_path.unlink, missing_ok=True)
        detail = str(e).strip() or type(e).__name__
        reason = f"{type(e).__name__}: {detail}" if str(e).strip() else type(e).__name__
        logger.error("pdf_download_error", url=url, error=reason)
        return None, reason


async def process_document(job: ExtractionJob) -> ExtractionResult:
    """Обробити один документ: завантажити PDF, витягнути текст, викликати LLM.

    Returns ExtractionResult з результатами.
    """
    tmp_pdf: Path | None = None
    downloaded_tmp = False
    try:
        # 1. Отримати PDF (локальний або завантажити)
        logger.info("extract_start", document_id=job.document_id, url=job.canonical_url)
        
        if job.pdf_path:
            # Використовувати локальний PDF
            tmp_pdf = Path(job.pdf_path)
            if not tmp_pdf.exists():
                logger.warning("pdf_not_found_locally", document_id=job.document_id, pdf_path=job.pdf_path)
                return ExtractionResult(
                    document_id=job.document_id,
                    canonical_url=job.canonical_url,
                    success=False,
                    error=f"Локальний PDF не знайдено: {job.pdf_path}",
                )
            logger.info("using_local_pdf", document_id=job.document_id, pdf_path=job.pdf_path)
        else:
            # Завантажити PDF
            tmp_pdf, download_error = await download_pdf(job.canonical_url)
            if tmp_pdf is None:
                error_text = "Не вдалося завантажити PDF"
                if download_error:
                    error_text = f"{error_text}: {download_error}"
                return ExtractionResult(
                    document_id=job.document_id,
                    canonical_url=job.canonical_url,
                    success=False,
                    error=error_text,
                )
            downloaded_tmp = True

        # 2. Парсити PDF (витягнути весь текст, усі сторінки)
        # Максимальна кількість сторінок для витягу
        settings = get_settings()
        max_pages = settings.llm.max_pages_for_extraction
        parse_result = await parse_pdf(tmp_pdf, max_pages=max_pages)
        if parse_result.is_corrupt or parse_result.is_encrypted:
            return ExtractionResult(
                document_id=job.document_id,
                canonical_url=job.canonical_url,
                success=False,
                error=f"PDF помилковий (corrupt={parse_result.is_corrupt}, encrypted={parse_result.is_encrypted})",
            )

        text = parse_result.text
        text_pages_extracted = min(parse_result.page_count, max_pages) if text else 0

        if not text or len(text.strip()) < 100:
            return ExtractionResult(
                document_id=job.document_id,
                canonical_url=job.canonical_url,
                success=False,
                error="PDF без тексту або дуже короткий",
                text_pages_extracted=text_pages_extracted,
            )

        # Обрізати текст до максимальної довжини для LLM
        max_chars = settings.llm.max_text_chars_for_llm
        if len(text) > max_chars:
            text = text[:max_chars] + "\n\n[... текст обрізано, далі йде додатковий матеріал ...]"

        # 3. Викликати LLM для витягу цитат і сумаризації
        llm_result = await call_llm_for_extraction(text, job.title)
        if llm_result is None:
            return ExtractionResult(
                document_id=job.document_id,
                canonical_url=job.canonical_url,
                success=False,
                error="LLM виклик не вдалося або не повернув відповідь",
                text_pages_extracted=text_pages_extracted,
            )

        # 4. Парсити JSON відповідь LLM
        quotations = llm_result.get("quotations", [])
        summary = llm_result.get("summary")
        if not isinstance(quotations, list):
            quotations = []
        if summary is not None and not isinstance(summary, dict):
            summary = None

        # 5. Заповнити відсутні поля у summary (новий формат з sections)
        if summary is not None:
            # Новий формат: summary = {"sections": [...], "authors_mentioned": [...]}
            if "sections" in summary:
                sections = summary.get("sections", [])
                if not isinstance(sections, list):
                    sections = []
                # Заповнити відсутні поля в кожній секції
                for sec in sections:
                    if not isinstance(sec, dict):
                        continue
                    sec.setdefault("page", 1)
                    sec.setdefault("title", "Розділ")
                    sec.setdefault("overview", "н/зв")
                    sec.setdefault("key_ideas", [])
                    sec.setdefault("methodology", "н/зв")
                    sec.setdefault("findings", "н/зв")
                    sec.setdefault("conclusions", "н/зв")
                    if not isinstance(sec.get("key_ideas"), list):
                        sec["key_ideas"] = []
                summary["sections"] = sections
                summary.setdefault("authors_mentioned", [])
                if not isinstance(summary.get("authors_mentioned"), list):
                    summary["authors_mentioned"] = []
            else:
                # Старий формат (сумісність): конвертувати в sections
                overview = summary.get("overview", "н/зв")
                key_ideas = summary.get("key_ideas", [])
                methodology = summary.get("methodology", "н/зв")
                findings = summary.get("findings", "н/зв")
                conclusions = summary.get("conclusions", "н/зв")
                page = summary.get("page", 1)
                authors = summary.get("authors_mentioned", [])
                if not isinstance(key_ideas, list):
                    key_ideas = []
                if not isinstance(authors, list):
                    authors = []
                summary = {
                    "sections": [{
                        "page": page,
                        "title": "Загальна сумаризація",
                        "overview": overview,
                        "key_ideas": key_ideas,
                        "methodology": methodology,
                        "findings": findings,
                        "conclusions": conclusions,
                    }],
                    "authors_mentioned": authors,
                }

        if not _has_meaningful_quotations(quotations) and not _has_meaningful_summary(summary):
            return ExtractionResult(
                document_id=job.document_id,
                canonical_url=job.canonical_url,
                success=False,
                error="LLM не повернув цитат або сумаризації",
                text_pages_extracted=text_pages_extracted,
            )

        logger.info(
            "extract_success",
            document_id=job.document_id,
            url=job.canonical_url,
            quotations=len(quotations),
            summary=summary is not None,
        )

        return ExtractionResult(
            document_id=job.document_id,
            canonical_url=job.canonical_url,
            quotations=quotations,
            summary=summary,
            success=True,
            text_pages_extracted=text_pages_extracted,
        )

    except Exception as e:
        logger.error("extract_error", document_id=job.document_id, error_msg=str(e))
        return ExtractionResult(
            document_id=job.document_id,
            canonical_url=job.canonical_url,
            success=False,
            error=str(e),
        )
    finally:
        if downloaded_tmp and tmp_pdf is not None:
            await asyncio.to_thread(tmp_pdf.unlink, missing_ok=True)


async def call_llm_for_extraction(text: str, title: str) -> dict[str, Any] | None:
    """Викликати LLM для витягу цитат і сумаризації з тексту статті.

    Повертає словник з key 'quotations' (list) і 'summary' (dict | None),
    або None, якщо виклик не вдалося.
    """
    settings = get_settings()
    if not settings.llm.enabled:
        logger.warning("llm_disabled")
        return None

    content = f"{LLM_SYSTEM_PROMPT}\n\nНАЗВА СТАТТІ: {title}\n\nТЕКСТ СТАТТІ:\n{text}"
    try:
        from harvester.classify.llm import LLMClient, LLMUnavailable

        client = LLMClient(keys=settings.gemini_keys, service="Extract")
        response = await client.complete(content)
        raw = response.text.strip()
        if raw.startswith("```"):
            raw = raw.split("\n", 1)[1] if "\n" in raw else raw
            raw = raw.rsplit("```", 1)[0]
        start = raw.find("{")
        end = raw.rfind("}") + 1
        if start < 0 or end <= start:
            logger.warning("llm_extraction_json_missing", provider=response.provider)
            return None
        result = json.loads(raw[start:end])
        if not isinstance(result, dict):
            return None
        return result
    except LLMUnavailable as e:
        logger.warning("llm_extraction_unavailable", error=str(e)[:300])
    except (json.JSONDecodeError, TypeError, ValueError) as e:
        logger.warning("llm_extraction_json_error", error=str(e)[:300])
    except Exception as e:  # noqa: BLE001
        logger.error("llm_extraction_error", error=str(e)[:300])
    return None
