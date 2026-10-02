import asyncio
from collections.abc import AsyncIterator

import httpx
import structlog

from harvester.config import get_settings
from harvester.discovery.base import Candidate
from harvester.net.client import get_http_client

logger = structlog.get_logger()

# Без API-ключа OpenAlex рахує запити проти безкоштовного денного бюджету,
# спільного для всього IP. Заміряно 02.10.2026 о 22:32 UTC: 429 з тілом
# "Insufficient budget ... $0.0005 remaining; resets at midnight UTC".
#
# Це НЕ наша частотна помилка й не дефект каналу: о 03:00 бюджет буде
# знову повний. Тому 429 не має проходити загальним шляхом помилки, де
# кожна задача витрачає спробу з max_attempts і за 5 хвилин ретраїв
# переходить у failed — тобто 26 задач кампанії померли б до сбросу
# бюджету, не добравши жодної знахідки.
class OpenAlexBudgetExhausted(Exception):
    """Денний безкоштовний бюджет OpenAlex вичерпано; тимчасово."""

    def __init__(self, retry_after_s: int):
        self.retry_after_s = max(60, retry_after_s)
        super().__init__(
            f"OpenAlex daily budget exhausted; retry_after={self.retry_after_s}s"
        )


class OpenAlexChannel:
    name = "openalex"

    def __init__(self):
        settings = get_settings()
        self.enabled = settings.channels.openalex.enabled
        self.rps = settings.channels.openalex.rps
        self.email = settings.contact.email
        self.base_url = "https://api.openalex.org"
        self.last_next_cursor: str | None = None
        self.last_count: int = 0
        # Активний пошуковий запит поточного виклику: воркер використовує
        # його, щоб відрізнити тематичний прохід (finite) від курсорного
        # сканування (нескінченне, перезапускається раз на добу).
        self.last_search: str | None = None
        self._request_lock = asyncio.Lock()
        self._last_request_at = 0.0

    def rate_limit(self) -> float:
        return 1.0 / self.rps if self.rps > 0 else 0.0

    async def _wait_rate_limit(self) -> None:
        if self.rps <= 0:
            return
        loop = asyncio.get_running_loop()
        async with self._request_lock:
            now = loop.time()
            next_request_at = max(now, self._last_request_at + 1.0 / self.rps)
            self._last_request_at = next_request_at
        # Не утримувати mutex під час очікування: інші корутини можуть
        # зарезервувати наступні слоти, не створюючи чергу під lock.
        await asyncio.sleep(max(0.0, next_request_at - now))

    async def discover(self, task: dict) -> AsyncIterator[Candidate]:
        if not self.enabled:
            return

        cursor = str(task.get("cursor") or "*")
        filters = dict(task.get("filters", {}))
        per_page = task.get("per_page", 200)

        # `search` — окремий query-параметр OpenAlex, а НЕ фільтр.
        # Раніше `_build_filter` вміла лише language/is_oa/country/type/year,
        # тож канал міг тільки гортати ВСІ роботи UA курсором (210 902
        # знахідки), не знаючи про тему. Для тематичної кампанії це
        # марнотрата: 99% вибірки не належало запиту.
        #
        # Правильність: якщо залишити search усередині filter-рядка, API
        # відповідає 400, тому кожен пошуковий запит падав би з помилкою.
        search = filters.pop("search", None)

        params = {
            "filter": self._build_filter(filters),
            "per-page": per_page,
            "cursor": cursor,
            "mailto": self.email,
            "select": "id,doi,title,display_name,publication_year,language,type,open_access,best_oa_location,locations,primary_topic,authorships",
        }
        if search:
            params["search"] = str(search)

        self.last_next_cursor = None
        self.last_search = search
        await self._wait_rate_limit()
        logger.info(
            "openalex_query_start",
            filters=filters,
            search=(search or "")[:60],
            cursor=cursor[:20],
        )

        try:
            client = await get_http_client()
            response = await client.get(f"{self.base_url}/works", params=params)
            response.raise_for_status()

            data = response.json()
            results = data.get("results", [])
            next_cursor = data.get("meta", {}).get("next_cursor")
            self.last_next_cursor = next_cursor
            self.last_count = len(results)

            for work in results:
                candidate = self._work_to_candidate(work)
                if candidate:
                    yield candidate

            if next_cursor:
                logger.debug("openalex_has_more", next_cursor=next_cursor[:20])

            if search and not results:
                # Нуль за конкретним запитом — це не збій каналу (бекенд
                # відповів коректно, meta.count=0), і через загальний
                # лічильник request-ів його неможливо було відрізнити від
                # мовчання. Для кампанії це сигнал, що формулювання запиту
                # порожнє й варто його переписати.
                logger.info("openalex_search_empty", search=str(search)[:80])

            logger.info(
                "openalex_query_complete",
                results=len(results),
                search=(search or "")[:60],
                has_more=bool(next_cursor),
            )

        except httpx.HTTPStatusError as e:
            if e.response.status_code == 429:
                # retry-after у секундах; за відсутності — консервативна
                # година, бо бюджет скидається саме опівночі UTC.
                try:
                    retry_after = int(e.response.headers.get("retry-after", "3600"))
                except ValueError:
                    retry_after = 3600
                logger.info(
                    "openalex_budget_exhausted", retry_after_s=retry_after
                )
                raise OpenAlexBudgetExhausted(retry_after) from None
            logger.error("openalex_http_error", status=e.response.status_code, error=str(e))
            raise
        except Exception as e:
            logger.error("openalex_error", error=str(e), exc_info=True)
            raise

    def _build_filter(self, filters: dict) -> str:
        parts = []

        if filters.get("language"):
            parts.append(f"language:{filters['language']}")

        if filters.get("is_oa"):
            parts.append("open_access.is_oa:true")

        if filters.get("country_code"):
            parts.append(f"institutions.country_code:{filters['country_code']}")

        if filters.get("type"):
            parts.append(f"type:{filters['type']}")

        if filters.get("from_year"):
            parts.append(f"from_publication_date:{filters['from_year']}-01-01")

        # Точна фраза в заголовку/анотації. Це єдиний спосіб отримати
        # прицільну вибірку для порівняльного права: вільний `search`
        # не робить AND між словами, тому «capacity of minors contract»
        # підходило майже до всього корпусу (304 163 роботи).
        # Заміряно 02.10.2026: у `search` цей фільтр дає 0 (API ігнорує
        # його там), а як частина `filter` — 161 роботу, якого треба.
        ta = filters.get("title_and_abstract")
        if ta:
            # Кома — роздільник фільтрів OpenAlex, тому її не можна
            # пропускати в значенні: інакше запит розпадеться на два
            # і API поверне 400 замість передбачуваного результату.
            #
            # Пробіли згортаємо: значення — це точна фраза в лапках, і
            # подвійний пробіл після заміни коми змінив би саму фразу
            # (OpenAlex не гарантує нормалізацію), тобто фільтр став би
            # не тим, що ми задумали.
            cleaned = " ".join(str(ta).replace(",", " ").split())
            if cleaned:
                parts.append(f"title_and_abstract.search:{cleaned}")

        return ",".join(parts) if parts else "open_access.is_oa:true"

    def _work_to_candidate(self, work: dict) -> Candidate | None:
        openalex_id = work.get("id")
        doi = work.get("doi")
        if doi and doi.startswith("https://doi.org/"):
            doi = doi.replace("https://doi.org/", "")

        title = work.get("title") or work.get("display_name")
        year = work.get("publication_year")
        language = work.get("language")
        doc_type = self._map_type(work.get("type"))

        open_access = work.get("open_access", {})
        is_oa = open_access.get("is_oa", False)
        oa_status = open_access.get("oa_status")

        best_oa_location = work.get("best_oa_location") or {}
        pdf_url = best_oa_location.get("pdf_url")

        if not pdf_url:
            locations = work.get("locations", [])
            for loc in locations:
                if loc.get("pdf_url"):
                    pdf_url = loc["pdf_url"]
                    break

        if not pdf_url:
            return None

        authors = []
        for authorship in work.get("authorships", [])[:10]:
            author = authorship.get("author", {})
            name = author.get("display_name")
            if name:
                authors.append(name)

        primary_topic = work.get("primary_topic", {})
        topic_id = primary_topic.get("id") if primary_topic else None

        return Candidate(
            url=pdf_url,
            doi=doi,
            openalex_id=openalex_id,
            title=title,
            authors=authors if authors else None,
            year=year,
            language=language,
            doc_type=doc_type,
            is_oa=is_oa,
            oa_status=oa_status,
            channel=self.name,
            extra={"topic_id": topic_id},
        )

    def _map_type(self, oa_type: str | None) -> str:
        if not oa_type:
            return "other"

        type_map = {
            "article": "article",
            "journal-article": "article",
            "book": "book",
            "book-chapter": "book",
            "dissertation": "dissertation",
            "report": "report",
            "preprint": "preprint",
        }

        return type_map.get(oa_type, "other")


def create_openalex_iterators() -> list[dict]:
    iterators = []

    languages = ["uk", "en"]
    countries = ["UA"]

    for lang in languages:
        for country in countries:
            iterators.append({
                "filters": {
                    "language": lang,
                    "is_oa": True,
                    "country_code": country,
                },
                "cursor": "*",
            })

    iterators.append({
        "filters": {
            "language": "uk",
            "is_oa": True,
        },
        "cursor": "*",
    })

    return iterators
