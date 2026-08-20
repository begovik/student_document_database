import re
import unicodedata

import structlog
from rapidfuzz import fuzz

from harvester.config import get_settings

logger = structlog.get_logger()


def normalize_text(text: str) -> str:
    if not text:
        return ""

    text = text.lower()

    text = unicodedata.normalize("NFKD", text)
    text = "".join(c for c in text if not unicodedata.combining(c))

    text = re.sub(r"[^\w\s]", " ", text, flags=re.UNICODE)

    text = re.sub(r"\s+", " ", text)

    text = text.strip()

    return text


def match_title(title_hint: str | None, pdf_title: str | None, text_sample: str | None = None) -> tuple[int, str]:
    settings = get_settings()
    min_score = settings.verify.title_match_min
    review_score = settings.verify.title_match_review

    if not title_hint:
        return 100, "no_hint"

    title_hint_norm = normalize_text(title_hint)

    best_score = 0
    best_method = "none"

    if pdf_title:
        pdf_title_norm = normalize_text(pdf_title)

        score1 = fuzz.token_set_ratio(title_hint_norm, pdf_title_norm)
        score2 = fuzz.partial_ratio(title_hint_norm, pdf_title_norm)
        score3 = fuzz.token_sort_ratio(title_hint_norm, pdf_title_norm)

        best_score = max(score1, score2, score3)
        best_method = "pdf_metadata"

        logger.debug(
            "title_match_pdf_metadata",
            hint=title_hint[:50],
            pdf=pdf_title[:50],
            token_set=score1,
            partial=score2,
            token_sort=score3,
            best=best_score,
        )

    if text_sample and best_score < min_score:
        text_sample_norm = normalize_text(text_sample[:1200])

        score1 = fuzz.token_set_ratio(title_hint_norm, text_sample_norm)
        score2 = fuzz.partial_ratio(title_hint_norm, text_sample_norm)

        text_score = max(score1, score2)

        if text_score > best_score:
            best_score = text_score
            best_method = "text_sample"

            logger.debug(
                "title_match_text_sample",
                hint=title_hint[:50],
                text=text_sample[:100],
                token_set=score1,
                partial=score2,
                best=best_score,
            )

    if best_score >= min_score:
        return best_score, "match"
    elif best_score >= review_score:
        return best_score, "review"
    else:
        return best_score, "mismatch"
