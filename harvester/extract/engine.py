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

2. СУМАРИЗАЦІЯ (summary) — це структурований опис статті.

   ФОРМАТ сумаризації (JSON):
   {
     "page": 1,
     "overview": "Короткий опис що в статті (1-2 речення)",
     "key_ideas": ["Ідея 1", "Ідея 2", "Ідея 3"],
     "methodology": "Як проводилось дослідження (якщо є)",
     "findings": "Основні результати (якщо є)",
     "conclusions": "Висновки статті (якщо є)",
     "authors_mentioned": ["Імя Автор1", "Імя Автор2"] — автори, згадані у тексті (якщо відрізняються від заголовка)
   }

   Усі поля мають бути НЕ порожніми, якщо в тексті немає відповідної інформації —
   використовуй "н/зв" або пропусти.

   page для сумаризації — сторінка, з якої взято основну інформацію (зазвичай 1 або перша сторінка з контентом).

ВАЖЛИВО:
- Відповідь має бути JSON (без markdown, без пояснень)
- Відповідь має містити БЕЗ ЗМІСТУ (лише JSON об'єкт)
- Якщо текст статті пустий або недоступний — поверни {"quotations": [], "summary": null}
- Якщо текст англійською — аналізуй як зазвичай
- Якщо текст українською — аналізуй як зазвичай
- Якщо стаття містить багато таблиць, формул, графіків — їх НЕ включай в цитати

Приклад відповіді:
{"quotations":[{"page":5,"text":"Ціна цитати...","type":"conclusion"}],"summary":{"page":1,"overview":"...","key_ideas":["..."],"methodology":"...","findings":"...","conclusions":"...","authors_mentioned":["..."]}}"""

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

        # 5. Заповнити відсутні поля у summary
        if summary is not None:
            summary.setdefault("overview", "н/зв")
            summary.setdefault("key_ideas", [])
            summary.setdefault("methodology", "н/зв")
            summary.setdefault("findings", "н/зв")
            summary.setdefault("conclusions", "н/зв")
            summary.setdefault("authors_mentioned", [])
            if not isinstance(summary.get("key_ideas"), list):
                summary["key_ideas"] = []
            if not isinstance(summary.get("authors_mentioned"), list):
                summary["authors_mentioned"] = []
            # Заповнити page, якщо відсутнє
            if "page" not in summary:
                summary["page"] = 1

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
        logger.error("extract_error", document_id=job.document_id, error=str(e))
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
            logger.warning("gemini_try_failed", error=str(e))

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
                logger.warning("gemma_try_failed", model=model, error=str(e))

    # Спробувати OpenRouter
    if settings.open_router_api_key:
        try:
            result = await call_openrouter(settings.open_router_api_key, llm_config, messages)
            if result is not None:
                return result
        except Exception as e:
            logger.warning("openrouter_try_failed", error=str(e))

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
        logger.warning("llm_response_parse_error", error=str(e), response=str(data)[:500])
        return None


async def call_openrouter(api_key: str, config, messages: list[dict]) -> dict[str, Any] | None:
    """Викликати OpenRouter API для витягу цитат і сумаризації."""
    import aiohttp

    model = config.openrouter_model or "google/gemini-2.5-flash"
    url = "https://openrouter.ai/api/v1/chat/completions"

    payload = {
        "model": model,
        "messages": messages,
        "temperature": config.temperature,
        "max_tokens": config.max_tokens,
    }

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    timeout = aiohttp.ClientTimeout(total=config.timeout_s)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.post(url, json=payload, headers=headers) as resp:
            if resp.status != 200:
                text = await resp.text()
                logger.warning("openrouter_api_error", status=resp.status, body=text[:500])
                return None
            data = await resp.json()

    try:
        choice = data["choices"][0]
        content_text = choice["message"]["content"]

        if content_text.startswith("```"):
            content_text = content_text.replace("```json", "").replace("```", "").strip()

        result = json.loads(content_text)
        if isinstance(result, dict) and "quotations" in result:
            return result
        logger.warning("llm_response_invalid_format", response=content_text[:200])
        return None
    except (KeyError, IndexError, json.JSONDecodeError) as e:
        logger.warning("llm_response_parse_error", error=str(e), response=str(data)[:500])
        return None
