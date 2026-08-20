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
