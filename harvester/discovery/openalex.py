import asyncio
from typing import AsyncIterator

import httpx
import structlog

from harvester.config import get_settings
from harvester.discovery.base import Candidate
from harvester.net.guards import is_url_allowed

logger = structlog.get_logger()


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

    def rate_limit(self) -> float:
        return 1.0 / self.rps

    async def discover(self, task: dict) -> AsyncIterator[Candidate]:
        if not self.enabled:
            return

        cursor = task.get("cursor", "*")
        filters = task.get("filters", {})
        per_page = task.get("per_page", 200)

        params = {
            "filter": self._build_filter(filters),
            "per-page": per_page,
            "cursor": cursor,
            "mailto": self.email,
            "select": "id,doi,title,display_name,publication_year,language,type,open_access,best_oa_location,locations,primary_topic,authorships",
        }

        logger.info("openalex_query_start", filters=filters, cursor=cursor[:20])

        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
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

                logger.info("openalex_query_complete", results=len(results))

        except httpx.HTTPStatusError as e:
            logger.error("openalex_http_error", status=e.response.status_code, error=str(e))
        except Exception as e:
            logger.error("openalex_error", error=str(e), exc_info=True)

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
