import re
from datetime import datetime

import structlog

from harvester.config import get_settings
from harvester.verify.langid import LanguageResult

logger = structlog.get_logger()


SOVIET_PUBLISHERS_UK = [
    "наукова думка",
    "вища школа",
    "радянська школа",
    "техніка",
    "здоров'я",
    "молодь",
    "дніпро",
    "веселка",
    "музика україни",
]

SOVIET_PUBLISHERS_RU = [
    "наука",
    "мысль",
    "высшая школа",
    "просвещение",
    "мир",
    "политиздат",
    "государственное",
    "издательство",
]

SOVIET_CITIES = [
    "москва", "ленінград", "київ", "харків", "львів", "одеса",
    "дніпропетровськ", "донeцьк", "мінськ", "тбілісі", "баку",
    "ташкент", "алма-ата", "новосибірськ", "свердловськ",
]


async def check_russian_language(lang_result: LanguageResult) -> tuple[bool, str | None]:
    settings = get_settings()
    min_confidence = settings.filters.ru_lang_min_confidence

    if lang_result.language == "ru" and lang_result.confidence >= min_confidence:
        logger.info("russian_language_detected", confidence=lang_result.confidence)
        return True, "russian_language"

    return False, None


async def check_soviet_source(
    year: int | None,
    publisher: str | None,
    text_sample: str | None = None,
) -> tuple[bool, str | None]:
    settings = get_settings()
    cutoff_year = settings.filters.soviet_cutoff_year

    if year is None:
        return False, None

    if year >= cutoff_year:
        return False, None

    if publisher:
        publisher_lower = publisher.lower()
        all_soviet_publishers = SOVIET_PUBLISHERS_UK + SOVIET_PUBLISHERS_RU

        for sov_publisher in all_soviet_publishers:
            if sov_publisher in publisher_lower:
                logger.info("soviet_publisher_detected", publisher=publisher, year=year)
                return True, "soviet_publisher"

    if text_sample:
        text_lower = text_sample.lower()

        for city in SOVIET_CITIES:
            if city in text_lower:
                year_pattern = rf"{year}"
                if re.search(year_pattern, text_sample):
                    logger.info("soviet_city_and_year_detected", city=city, year=year)
                    return True, "soviet_location"

    return False, None


async def check_domain_blocked(host: str) -> tuple[bool, str | None]:
    from harvester.net.guards import is_domain_blocked

    if await is_domain_blocked(f"https://{host}"):
        logger.info("domain_blocked", host=host)
        return True, "domain_blacklisted"

    return False, None


async def apply_all_filters(
    url: str,
    lang_result: LanguageResult,
    year: int | None = None,
    publisher: str | None = None,
    text_sample: str | None = None,
) -> tuple[bool, str | None]:
    from urllib.parse import urlparse
    host = urlparse(url).netloc.split(":")[0]

    blocked, reason = await check_domain_blocked(host)
    if blocked:
        return True, reason

    is_ru, reason = await check_russian_language(lang_result)
    if is_ru:
        return True, reason

    is_soviet, reason = await check_soviet_source(year, publisher, text_sample)
    if is_soviet:
        return True, reason

    return False, None
