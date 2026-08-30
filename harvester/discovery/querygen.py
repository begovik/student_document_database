import re
from pathlib import Path

import structlog

from harvester.db.connection import Database
from harvester.db.repositories import SearchQueriesRepository

logger = structlog.get_logger()

TOPICS: list[dict] = [
    {"code": "history_ua", "name_uk": "історія України", "name_en": "history of Ukraine"},
    {"code": "law", "name_uk": "право", "name_en": "law"},
    {"code": "econ", "name_uk": "економіка", "name_en": "economics"},
    {"code": "cs", "name_uk": "програмування", "name_en": "computer science"},
    {"code": "math", "name_uk": "математика", "name_en": "mathematics"},
    {"code": "phys", "name_uk": "фізика", "name_en": "physics"},
    {"code": "chem", "name_uk": "хімія", "name_en": "chemistry"},
    {"code": "bio", "name_uk": "біологія", "name_en": "biology"},
    {"code": "med", "name_uk": "медицина", "name_en": "medicine"},
    {"code": "ped", "name_uk": "педагогіка", "name_en": "pedagogy"},
    {"code": "philol", "name_uk": "філологія", "name_en": "philology"},
    {"code": "psych", "name_uk": "психологія", "name_en": "psychology"},
    {"code": "philos", "name_uk": "філософія", "name_en": "philosophy"},
    {"code": "socio", "name_uk": "соціологія", "name_en": "sociology"},
    {"code": "polit", "name_uk": "політологія", "name_en": "political science"},
    {"code": "ecol", "name_uk": "екологія", "name_en": "ecology"},
    {"code": "build", "name_uk": "будівництво", "name_en": "civil engineering"},
    {"code": "electro", "name_uk": "електротехніка", "name_en": "electrical engineering"},
    {"code": "market", "name_uk": "маркетинг", "name_en": "marketing"},
    {"code": "manag", "name_uk": "менеджмент", "name_en": "management"},
    {"code": "agro", "name_uk": "агрономія", "name_en": "agronomy"},
    {"code": "geo", "name_uk": "географія", "name_en": "geography"},
    {"code": "stat", "name_uk": "статистика", "name_en": "statistics"},
    {"code": "ml", "name_uk": "машинне навчання", "name_en": "machine learning"},
    {"code": "journ", "name_uk": "журналістика", "name_en": "journalism"},
]

TEMPLATES_UK = [
    "{topic} filetype:pdf",
    "{topic} підручник filetype:pdf",
    '{topic} "навчальний посібник" pdf',
    "{topic} конспект лекцій filetype:pdf",
    "{topic} методичні вказівки pdf",
    "{topic} наукова стаття filetype:pdf",
]

TEMPLATES_EN = [
    "{topic} filetype:pdf",
    "{topic} textbook filetype:pdf",
    '{topic} "lecture notes" pdf',
    "{topic} scientific article filetype:pdf",
]


async def seed_queries(db: Database) -> int:
    """Заповнити search_queries стартовим набором, якщо таблиця порожня."""
    repo = SearchQueriesRepository(db)
    existing = await repo.count()
    if existing > 0:
        logger.info("queries_already_seeded", count=existing)
        return 0

    inserted = 0
    for topic in TOPICS:
        for template in TEMPLATES_UK:
            qid = await repo.insert_if_new(
                template.format(topic=topic["name_uk"]),
                region="ua-uk",
                topic_hint=topic["code"],
            )
            if qid:
                inserted += 1
        for template in TEMPLATES_EN:
            qid = await repo.insert_if_new(
                template.format(topic=topic["name_en"]),
                region="us-en",
                topic_hint=topic["code"],
            )
            if qid:
                inserted += 1

    logger.info("queries_seeded", inserted=inserted, topics=len(TOPICS))
    return inserted


DISCIPLINE_CATALOG = Path(__file__).resolve().parents[2] / "docs" / "discipline_catalog.md"

CATEGORY_CODES: dict[str, str] = {
    "мистецтво": "art_media",
    "мови": "philology",
    "філософія": "soc_phil",
    "педагогіка": "education",
    "соціологія": "sociology",
    "медицина": "med_bio_health",
    "it": "it_tech",
    "економіка": "econ_business",
    "менеджмент": "mgmt_marketing",
    "логістика": "logistics",
    "готельно": "hospitality_tourism",
}

_DISCIPLINE_RE = re.compile(r"^\d+\.\s+(.+?)\s*$")


def _category_code(header: str) -> str:
    low = header.lower()
    for key, code in CATEGORY_CODES.items():
        if key in low:
            return code
    slug = re.sub(r"[^a-z0-9]+", "-", low).strip("-")[:40] or "misc"
    return f"cat_{slug}"


def _is_mostly_ascii(name: str) -> bool:
    letters = [c for c in name if c.isalpha()]
    if not letters:
        return False
    return sum(1 for c in letters if c.isascii()) / len(letters) >= 0.8


def parse_discipline_catalog(path: Path = DISCIPLINE_CATALOG) -> list[tuple[str, str]]:
    """Розібрати docs/discipline_catalog.md на пари (код_категорії, назва_дисципліни)."""
    if not path.exists():
        logger.warning("discipline_catalog_missing", path=str(path))
        return []

    result: list[tuple[str, str]] = []
    category_code = "misc"
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if line.startswith("## "):
            category_code = _category_code(line[3:])
            continue
        match = _DISCIPLINE_RE.match(line)
        if match:
            name = re.sub(r"\s+", " ", match.group(1)).strip()
            if name:
                result.append((category_code, name))

    logger.info("discipline_catalog_parsed", disciplines=len(result), path=str(path))
    return result


async def seed_discipline_queries(db: Database) -> int:
    """Додати до search_queries запити за каталогом дисциплін (ідемпотентно).

    Викликається при кожному старті сервісу: нові дисципліни в md-каталозі
    породжують нові пошукові запити, наявні не дублюються.
    """
    repo = SearchQueriesRepository(db)
    disciplines = parse_discipline_catalog()
    if not disciplines:
        return 0

    inserted = 0
    seen: set[str] = set()
    for category_code, name in disciplines:
        if name.lower() in seen:
            continue
        seen.add(name.lower())
        for template in TEMPLATES_UK:
            qid = await repo.insert_if_new(
                template.format(topic=name),
                region="ua-uk",
                topic_hint=category_code,
            )
            if qid:
                inserted += 1
        if _is_mostly_ascii(name):
            for template in TEMPLATES_EN:
                qid = await repo.insert_if_new(
                    template.format(topic=name),
                    region="us-en",
                    topic_hint=category_code,
                )
                if qid:
                    inserted += 1

    logger.info(
        "discipline_queries_seeded", disciplines=len(seen), inserted=inserted
    )

    # LLM-доповнення для бідних тем (де шаблонні запити дали 0 results_yield)
    # Використовує Gemini 3.1/3.5 Flash Lite (GEMINI_API_KEY 1-3) з fallback на Gemma
    try:
        from harvester.discovery.querygen_llm import generate_queries_for_topic
        from harvester.config import get_settings

        settings = get_settings()
        if settings.llm.enabled and settings.gemini_keys and inserted == 0:
            # Шукаємо теми з низьким yield — кандидатів для LLM
            low = await repo.db.fetchall(
                "SELECT topic_hint, COUNT(*) as cnt, SUM(results_yield) as total_yield "
                "FROM search_queries GROUP BY topic_hint HAVING total_yield = 0 LIMIT 5"
            )
            for row in low:
                hint = row["topic_hint"]
                # Знайти назву дисципліни за hint
                name = next((n for c, n in disciplines if c == hint), hint)
                existing = [r["text"] for r in await repo.db.fetchall(
                    "SELECT text FROM search_queries WHERE topic_hint=? LIMIT 5", (hint,)
                )]
                llm_qs = await generate_queries_for_topic(name, existing_queries=[r["text"] for r in existing] if existing else None)
                for q in llm_qs[:5]:
                    qid = await repo.insert_if_new(q, region="ua-uk", topic_hint=hint)
                    if qid:
                        inserted += 1
                if llm_qs:
                    logger.info("discipline_llm_queries_added", topic_hint=hint, count=len(llm_qs))
    except Exception as e:  # noqa: BLE001
        logger.warning("discipline_llm_seed_failed", error=str(e)[:150])

    return inserted


async def seed_llm_queries_for_topic(db: Database, topic_name: str, topic_hint: str | None = None, count: int = 8) -> int:
    """Згенерувати LLM-запити для конкретної теми (викликається з add-queries або вручну)."""
    from harvester.discovery.querygen_llm import generate_queries_for_topic

    repo = SearchQueriesRepository(db)
    existing_rows = await repo.db.fetchall("SELECT text FROM search_queries WHERE topic_hint=? LIMIT 10", (topic_hint or topic_name,))
    existing = [r["text"] for r in existing_rows]
    queries = await generate_queries_for_topic(topic_name, existing_queries=existing, count=count)
    inserted = 0
    for q in queries:
        qid = await repo.insert_if_new(q, region="ua-uk", topic_hint=topic_hint or topic_name)
        if qid:
            inserted += 1
    logger.info("llm_queries_seeded", topic=topic_name[:40], inserted=inserted)
    return inserted
