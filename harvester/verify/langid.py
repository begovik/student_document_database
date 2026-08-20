import asyncio
import re
from dataclasses import dataclass

import structlog

logger = structlog.get_logger()


@dataclass
class LanguageResult:
    language: str
    confidence: float
    method: str


async def detect_language(text: str) -> LanguageResult:
    if not text or len(text.strip()) < 20:
        return LanguageResult("unknown", 0.0, "insufficient_text")

    try:
        result = await asyncio.to_thread(_detect_language_sync, text)
        return result
    except Exception as e:
        logger.error("language_detection_error", error=str(e))
        return LanguageResult("unknown", 0.0, "error")


def _detect_language_sync(text: str) -> LanguageResult:
    text_clean = text[:4000]

    ukr_chars = set("іїєґІЇЄҐ")
    has_ukr_chars = any(c in ukr_chars for c in text_clean)

    if has_ukr_chars:
        ukr_count = sum(1 for c in text_clean if c in ukr_chars)
        if ukr_count > 5:
            return LanguageResult("uk", 0.9, "ukr_chars")

    try:
        from lingua import Language, LanguageDetectorBuilder

        languages = [
            Language.UKRAINIAN,
            Language.RUSSIAN,
            Language.ENGLISH,
            Language.GERMAN,
            Language.FRENCH,
            Language.POLISH,
            Language.SPANISH,
        ]

        detector = LanguageDetectorBuilder.from_languages(*languages).build()
        detected = detector.detect_language_of(text_clean)

        if detected:
            lang_code = detected.iso_code_639_1.name.lower()
            confidence = 0.8

            if lang_code == "uk":
                confidence = 0.85
            elif lang_code == "ru":
                confidence = 0.85

            return LanguageResult(lang_code, confidence, "lingua")

    except ImportError:
        logger.warning("lingua_not_installed")
    except Exception as e:
        logger.warning("lingua_error", error=str(e))

    try:
        from langdetect import detect as langdetect_detect

        detected = langdetect_detect(text_clean)
        if detected:
            lang_code = detected.split("-")[0].lower()
            confidence = 0.7

            if lang_code == "uk":
                confidence = 0.75
            elif lang_code == "ru":
                confidence = 0.75

            return LanguageResult(lang_code, confidence, "langdetect")

    except ImportError:
        logger.warning("langdetect_not_installed")
    except Exception as e:
        logger.warning("langdetect_error", error=str(e))

    return LanguageResult("unknown", 0.0, "no_detector")


def is_cyrillic(text: str) -> bool:
    cyrillic_pattern = re.compile(r"[а-яА-ЯіїєґІЇЄҐ]")
    return bool(cyrillic_pattern.search(text))


def has_ukrainian_specific_chars(text: str) -> bool:
    ukr_chars = set("іїєґІЇЄҐ")
    return any(c in ukr_chars for c in text)
