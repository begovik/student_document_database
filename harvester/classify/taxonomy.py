import json

import structlog

from harvester.db.connection import Database

logger = structlog.get_logger()

TOPICS_SEED: list[dict] = [
    {"code": "history_ua", "name_uk": "Історія України", "name_en": "History of Ukraine",
     "udc": ["94(477)", "930.2"], "kw_uk": ["історія україни", "козацтво", "голодомор", "українська революція"],
     "kw_en": ["history of ukraine", "cossacks", "holodomor"]},
    {"code": "law", "name_uk": "Право", "name_en": "Law",
     "udc": ["34"], "kw_uk": ["право", "юридичн", "конституц", "кримінальн", "цивільн", "законодавств"],
     "kw_en": ["law", "legal", "legislation", "criminal law", "civil law"]},
    {"code": "econ", "name_uk": "Економіка", "name_en": "Economics",
     "udc": ["33"], "kw_uk": ["економік", "фінанси", "бухгалтер", "підприєм", "ринок"],
     "kw_en": ["econom", "finance", "accounting", "market", "business"]},
    {"code": "cs", "name_uk": "Інформатика та ПЗ", "name_en": "Computer Science",
     "udc": ["004"], "kw_uk": ["програм", "алгоритм", "база даних", "інформатик", "комп'ютер"],
     "kw_en": ["program", "algorithm", "database", "software", "computer"]},
    {"code": "math", "name_uk": "Математика", "name_en": "Mathematics",
     "udc": ["51"], "kw_uk": ["математик", "рівняння", "теорема", "інтеграл", "диференціальн"],
     "kw_en": ["mathemat", "equation", "theorem", "integral", "differential"]},
    {"code": "phys", "name_uk": "Фізика", "name_en": "Physics",
     "udc": ["53"], "kw_uk": ["фізик", "квантов", "термодинамік", "оптик"],
     "kw_en": ["physic", "quantum", "thermodynamic", "optic"]},
    {"code": "chem", "name_uk": "Хімія", "name_en": "Chemistry",
     "udc": ["54"], "kw_uk": ["хімі", "молекул", "реакці", "органічн"],
     "kw_en": ["chemis", "molecul", "reaction", "organic"]},
    {"code": "bio", "name_uk": "Біологія", "name_en": "Biology",
     "udc": ["57"], "kw_uk": ["біолог", "генетик", "клітин", "організм"],
     "kw_en": ["biolog", "genetic", "cell", "organism"]},
    {"code": "med", "name_uk": "Медицина", "name_en": "Medicine",
     "udc": ["61"], "kw_uk": ["медицин", "клінічн", "лікуванн", "діагностик", "пацієнт"],
     "kw_en": ["medic", "clinic", "treatment", "diagnos", "patient"]},
    {"code": "ped", "name_uk": "Педагогіка та освіта", "name_en": "Pedagogy",
     "udc": ["37"], "kw_uk": ["педагогік", "освіт", "навчанн", "викладанн", "здобувач"],
     "kw_en": ["pedagog", "educat", "teaching", "learning"]},
    {"code": "philol", "name_uk": "Філологія", "name_en": "Philology",
     "udc": ["81", "82"], "kw_uk": ["мовознав", "лінгвіст", "літератур", "переклад"],
     "kw_en": ["linguist", "literature", "translation", "philolog"]},
    {"code": "psych", "name_uk": "Психологія", "name_en": "Psychology",
     "udc": ["159.9"], "kw_uk": ["психологі", "емоці", "особистіс", "пізнавальн"],
     "kw_en": ["psycholog", "emotion", "personality", "cognit"]},
    {"code": "philos", "name_uk": "Філософія", "name_en": "Philosophy",
     "udc": ["1"], "kw_uk": ["філософ", "етик", "онтолог", "гносеолог"],
     "kw_en": ["philosoph", "ethic", "ontolog", "epistemolog"]},
    {"code": "socio", "name_uk": "Соціологія", "name_en": "Sociology",
     "udc": ["316"], "kw_uk": ["соціолог", "суспільств", "соціальн"],
     "kw_en": ["sociolog", "society", "social"]},
    {"code": "polit", "name_uk": "Політологія", "name_en": "Political Science",
     "udc": ["32"], "kw_uk": ["політич", "держав", "геополіт", "вибор"],
     "kw_en": ["politic", "state", "geopolitic", "election"]},
    {"code": "ecol", "name_uk": "Екологія", "name_en": "Ecology",
     "udc": ["504"], "kw_uk": ["еколог", "довкілл", "забруднен", "клімат"],
     "kw_en": ["ecolog", "environment", "pollution", "climate"]},
    {"code": "build", "name_uk": "Будівництво", "name_en": "Civil Engineering",
     "udc": ["69"], "kw_uk": ["будівництв", "конструкц", "залізобетон", "архітектур"],
     "kw_en": ["construction", "structure", "concrete", "architectur"]},
    {"code": "electro", "name_uk": "Електротехніка", "name_en": "Electrical Engineering",
     "udc": ["621.3"], "kw_uk": ["електротех", "електроенерг", "напруг", "струм"],
     "kw_en": ["electrical", "power system", "voltage", "current"]},
    {"code": "market", "name_uk": "Маркетинг", "name_en": "Marketing",
     "udc": ["339.138"], "kw_uk": ["маркетинг", "споживач", "бренд", "реклам"],
     "kw_en": ["marketing", "consumer", "brand", "advertis"]},
    {"code": "manag", "name_uk": "Менеджмент", "name_en": "Management",
     "udc": ["005"], "kw_uk": ["менеджмент", "управлінн", "організаці", "стратегі"],
     "kw_en": ["management", "organization", "strategy", "leadership"]},
    {"code": "agro", "name_uk": "Агрономія", "name_en": "Agronomy",
     "udc": ["63"], "kw_uk": ["агроном", "ґрунт", "врожай", "сільськогосподар"],
     "kw_en": ["agronom", "soil", "crop", "agricultur"]},
    {"code": "geo", "name_uk": "Географія", "name_en": "Geography",
     "udc": ["91"], "kw_uk": ["географ", "картограф", "ландшафт", "геолог"],
     "kw_en": ["geograph", "cartograph", "landscape", "geolog"]},
    {"code": "stat", "name_uk": "Статистика", "name_en": "Statistics",
     "udc": ["311"], "kw_uk": ["статистик", "вибірк", "регрес", "імовірніс"],
     "kw_en": ["statistic", "sample", "regression", "probabilit"]},
    {"code": "ml", "name_uk": "Машинне навчання та ШІ", "name_en": "Machine Learning",
     "udc": ["004.8"], "kw_uk": ["машинне навчання", "нейронн", "штучний інтелект", "глибоке навчання"],
     "kw_en": ["machine learning", "neural", "artificial intelligence", "deep learning"]},
    {"code": "journ", "name_uk": "Журналістика", "name_en": "Journalism",
     "udc": ["070"], "kw_uk": ["журналіст", "медіа", "преса", "новин"],
     "kw_en": ["journalis", "media", "press", "news"]},
]


async def seed_topics(db: Database) -> int:
    existing = await db.fetchone("SELECT COUNT(*) as c FROM topics")
    if existing and existing["c"] > 0:
        return 0

    inserted = 0
    for t in TOPICS_SEED:
        cursor = await db.execute(
            """
            INSERT OR IGNORE INTO topics (code, name_uk, name_en, udc_prefixes, keywords_uk, keywords_en)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                t["code"],
                t["name_uk"],
                t["name_en"],
                json.dumps(t["udc"], ensure_ascii=False),
                json.dumps(t["kw_uk"], ensure_ascii=False),
                json.dumps(t["kw_en"], ensure_ascii=False),
            ),
        )
        inserted += cursor.rowcount

    logger.info("topics_seeded", inserted=inserted)
    return inserted


async def load_topics(db: Database) -> list[dict]:
    rows = await db.fetchall("SELECT * FROM topics")
    topics = []
    for row in rows:
        d = dict(row)
        d["udc_prefixes"] = json.loads(d["udc_prefixes"] or "[]")
        d["keywords_uk"] = json.loads(d["keywords_uk"] or "[]")
        d["keywords_en"] = json.loads(d["keywords_en"] or "[]")
        topics.append(d)
    return topics
