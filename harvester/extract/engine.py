"""Сервіс витягу цитат і сумаризацій з PDF-документів."""

from __future__ import annotations

import asyncio
import json
import os
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import httpx
import structlog

from harvester.config import get_settings
from harvester.db.failover import build_database
from harvester.verify.pdfparse import PDFParseResult, parse_pdf

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
    timeout = httpx.Timeout(timeout_s, connect=10.0, read=30.0, pool=None)

    headers = {
        "User-Agent": settings.http.user_agent,
        "Accept": "application/pdf,*/*",
    }

    try:
        async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
            resp = await client.get(url, headers=headers)
            if resp.status_code != 200:
                reason = f"HTTP {resp.status_code}"
                logger.warning("pdf_download_failed", url=url, status=resp.status_code)
                return None, reason

            content_type = resp.headers.get("content-type", "")
            if "pdf" not in content_type.lower() and not url.lower().endswith(".pdf"):
                # Можливо, це HTML-сторінка, а не PDF
                if "html" in content_type.lower():
                    reason = f"відповідь не є PDF (content-type={content_type})"
                    logger.warning("pdf_download_not_pdf", url=url, content_type=content_type)
                    return None, reason

            data = resp.content
            if len(data) < 1024:
                reason = f"файл занадто малий ({len(data)} байт)"
                logger.warning("pdf_download_too_small", url=url, size=len(data))
                return None, reason

            # Перевірка magic bytes
            if data[:4] != b"%PDF":
                reason = "відсутні %PDF magic bytes"
                logger.warning("pdf_download_not_pdf_magic", url=url)
                return None, reason

            tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".pdf")
            tmp.write(data)
            tmp.close()
            logger.info("pdf_downloaded", url=url, bytes=len(data))
            return Path(tmp.name), None

    except Exception as e:
        detail = str(e).strip() or type(e).__name__
        reason = f"{type(e).__name__}: {detail}" if str(e).strip() else type(e).__name__
        logger.error("pdf_download_error", url=url, error=reason)
        return None, reason


async def process_document(job: ExtractionJob) -> ExtractionResult:
    """Обробити один документ: завантажити PDF, витягнути текст, викликати LLM.

    Returns ExtractionResult з результатами.
    """
    tmp_pdf: Path | None = None
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
        page_count = parse_result.page_count
        text_pages_extracted = len(text.split("\n")) if text else 0

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

        # Видалити тимчасовий файл
        if tmp_pdf and tmp_pdf.exists():
            tmp_pdf.unlink()

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
        if tmp_pdf and tmp_pdf.exists():
            tmp_pdf.unlink()
        return ExtractionResult(
            document_id=job.document_id,
            canonical_url=job.canonical_url,
            success=False,
            error=str(e),
        )


async def call_llm_for_extraction(text: str, title: str) -> dict[str, Any] | None:
    """Викликати LLM для витягу цитат і сумаризації з тексту статті.

    Повертає словник з key 'quotations' (list) і 'summary' (dict | None),
    або None, якщо виклик не вдалося.
    """
    settings = get_settings()
    llm_config = settings.llm

    if not llm_config.enabled:
        logger.warning("llm_disabled")
        return None

    # Підготувати контент для відправки
    content = f"НАЗВА СТАТТІ: {title}\n\nТЕКСТ СТАТТІ:\n{text}"

    messages = [
        {"role": "system", "content": LLM_SYSTEM_PROMPT},
        {"role": "user", "content": content},
    ]

    # Спробувати Gemini (ключі з settings)
    for api_key in [settings.gemini_api_key, settings.gemini_api_key_2, settings.gemini_api_key_3]:
        if not api_key:
            continue
        try:
            result = await call_gemini(api_key, llm_config, messages)
            if result is not None:
                return result
        except Exception as e:
            logger.warning("gemini_try_failed", error_msg=str(e))

    # Спробувати Gemma (ті самі ключі, але gemma_models + стиснення тексту)
    for api_key in [settings.gemini_api_key, settings.gemini_api_key_2, settings.gemini_api_key_3]:
        if not api_key:
            continue
        for model in llm_config.gemma_models:
            try:
                from harvester.classify.llm import rephrase_for_gemma

                truncated_content = f"НАЗВА СТАТТІ: {title}\n\nТЕКСТ СТАТТІ:\n{rephrase_for_gemma(text, llm_config.gemma_max_chars)}"
                gemma_messages = [
                    {"role": "system", "content": LLM_SYSTEM_PROMPT},
                    {"role": "user", "content": truncated_content},
                ]
                result = await call_gemini(api_key, llm_config, gemma_messages, model_override=model)
                if result is not None:
                    return result
            except Exception as e:
                logger.warning("gemma_try_failed", model=model, error_msg=str(e))

    logger.error("llm_all_retries_failed")
    return None


async def call_gemini(api_key: str, config, messages: list[dict], model_override: str | None = None) -> dict[str, Any] | None:
    """Викликати Google Gemini API для витягу цитат і сумаризації."""
    import aiohttp

    model = model_override or (config.gemini_models[0] if config.gemini_models else "gemini-2.0-flash")
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={api_key}"

    payload = {
        "contents": [
            {
                "role": messages[0]["role"],
                "parts": [{"text": messages[0]["content"]}],
            },
            {
                "role": messages[1]["role"],
                "parts": [{"text": messages[1]["content"]}],
            },
        ],
        "generationConfig": {
            "temperature": config.temperature,
            "maxOutputTokens": config.max_tokens,
        },
    }

    timeout = aiohttp.ClientTimeout(total=config.timeout_s)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.post(url, json=payload) as resp:
            if resp.status != 200:
                text = await resp.text()
                logger.warning("gemini_api_error", status=resp.status, body=text[:500])
                return None
            data = await resp.json()

    # Парсити відповідь
    try:
        candidate = data["candidates"][0]
        content_text = candidate["content"]["parts"][0]["text"]

        # Видалити markdown-блоки, якщо є
        if content_text.startswith("```"):
            content_text = content_text.replace("```json", "").replace("```", "").strip()

        result = json.loads(content_text)
        # Перевірити, що це dict з quotations і summary
        if isinstance(result, dict) and "quotations" in result:
            return result
        logger.warning("llm_response_invalid_format", response=content_text[:200])
        return None
    except (KeyError, IndexError, json.JSONDecodeError) as e:
        logger.warning("llm_response_parse_error", error_msg=str(e), response=str(data)[:500])
        return None

