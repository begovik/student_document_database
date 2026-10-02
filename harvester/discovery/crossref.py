"""Crossref як канал відкритого доступу.

    venv/bin/python -m harvester.discovery.crossref   # самоперевірка

Навіщо канал
------------
OpenAlex без API-ключа рахує запити проти безкоштовного денного бюджету,
СПІЛЬНОГО на весь вихідний IP. Виміряно 02.10.2026 о 22:32 UTC:
`Insufficient budget ... $0.0005 remaining; resets at midnight UTC`.
Тобто в момент, коли кампанії найбільше потрібен пошук, канал недоступний,
а відновитися може лише о 00:00 UTC — і те саме стосується будь-якого
іншого процесу на цьому хості.

Перевірені альтернативи станом на 02.10.2026 (усі dead end):

  * DDGS і всі пошуковики — видача отруєна для цього IP. Bing на запит
    «цивільне право України» повертає Reddit, Yahoo на «OpenAlex API» —
    купони Old Navy. Тому `site:`/бекенди не рятують.
  * Українські репозиторії недоступні: `ir.knute.edu.ua`, `zt.knute.edu.ua`,
    `ekm.univ.kiev.ua`, `ojs.dnu.dp.ua` — ConnectError. З доступних лише
    `nasplib.isofts.kiev.ua` (DSpace 7 REST живий, але індекс порожній:
    0-3 результати на запит) і `dspace.hnpu.edu.ua` (OAI-PMH працює, але
    це економічний вузол — не та дисципліна).
  * Авторефератів у Crossref немає взагалі: `filter=type:dissertation` на
    українську правову тему дає 0. Українські дисертації DOI не мають.

Crossref — єдина література, що лишилась: DOI є у українських правових
журналів, ліміт 50 зап/с у polite-пулі (без денного бюджету), і
заміряно — 19 з 36 PDF-посилань (53%) реально віддають файл з цього хоста.

Відсікання
----------
`language` НЕ підтримується фільтром на /works — перевірено, API відповідає
400 «filter-not-available: language». Тож мову визначає пізніше langid у
verify-конвеєрі, і навмисно НЕ вгадуємо її за кирилицею: так ми б назвали
українською російські тексти, а проєкт жорстко відсікає російські джерела.
"""

import asyncio
from collections.abc import AsyncIterator

import httpx
import structlog

from harvester.config import get_settings
from harvester.discovery.base import Candidate
from harvester.net.client import get_http_client

logger = structlog.get_logger()

# Crossref дозволяє offset до 10000 рядків. Для кампанії достатньо перших
# сторінок: запит «захист прав неповнолітніх» дає 38 034 результати, і
# глибша пагінація лише витрачала б час без приросту українських джерел.
DEFAULT_MAX_OFFSET = 1000
DEFAULT_ROWS = 50

# Crossref-типи → doc_type харвестера.
TYPE_MAP = {
    "journal-article": "article",
    "proceedings-article": "article",
    "book": "book",
    "book-chapter": "book",
    "edited-book": "book",
    "monograph": "book",
    "dissertation": "dissertation",
    "report": "report",
    "posted-content": "preprint",
}


class CrossrefChannel:
    name = "crossref"

    def __init__(self) -> None:
        settings = get_settings()
        self.enabled = settings.channels.crossref.enabled
        self.rps = settings.channels.crossref.rps
        self.email = settings.contact.email
        self.base_url = "https://api.crossref.org/works"
        self.last_count = 0
        self.last_next_offset: int | None = None
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
        await asyncio.sleep(max(0.0, next_request_at - now))

    async def discover(self, task: dict) -> AsyncIterator[Candidate]:
        if not self.enabled:
            return

        query = str(task.get("query") or "").strip()
        if not query:
            logger.warning("crossref_task_without_query")
            return

        # `filter` — прямий passthrough. Дозволяємо лише тип: саме
        # `language` тут недоступний (400), решта фільтрів Crossref
        # підтримує, але не всі комбінуються — помилку віддамо як є.
        crossref_filter = task.get("filter") or "type:journal-article"
        rows = min(int(task.get("rows", DEFAULT_ROWS)), 200)
        offset = int(task.get("offset", 0))
        max_offset = int(task.get("max_offset", DEFAULT_MAX_OFFSET))

        params = {
            "query.bibliographic": query,
            "filter": crossref_filter,
            "rows": rows,
            "offset": offset,
            "mailto": self.email,
        }

        self.last_count = 0
        self.last_next_offset = None
        await self._wait_rate_limit()
        logger.info(
            "crossref_query_start",
            query=query[:80],
            filter=crossref_filter,
            offset=offset,
        )

        try:
            client = await get_http_client()
            response = await client.get(self.base_url, params=params)
            response.raise_for_status()

            message = response.json().get("message", {})
            items = message.get("items", [])
            total = int(message.get("total-results") or 0)
            self.last_count = len(items)

            for item in items:
                candidate = self._work_to_candidate(item, query)
                if candidate:
                    yield candidate

            next_offset = offset + len(items)
            # Умова «next_offset < max_offset» тримає прохід скінченним:
            # інакше запит із 38 тис. результатів крутитиметься до
            # ліміту Crossref, не додаючи українських джерел.
            if items and len(items) == rows and next_offset < max_offset:
                self.last_next_offset = next_offset

            if not items:
                # Нуль за конкретним запитом — не збій, а порожня
                # формулювання; логуємо окремо, щоб це не плуталося з
                # мовчанням каналу (як у openalex_search_empty).
                logger.info("crossref_query_empty", query=query[:80], total=total)

            logger.info(
                "crossref_query_complete",
                results=len(items),
                total_results=total,
                query=query[:80],
                has_more=self.last_next_offset is not None,
            )

        except httpx.HTTPStatusError as e:
            logger.error(
                "crossref_http_error",
                status=e.response.status_code,
                query=query[:60],
            )
            raise
        except Exception as e:
            logger.exception("crossref_error", query=query[:60], error=str(e))
            raise

    def _pdf_url(self, work: dict) -> str | None:
        """Пряме посилання на PDF із deposit-метаданих Crossref.

        Беремо саме content-type='application/pdf'. Решта посилань
        (doi.org, landing page журналу) — це HTML, а конвеєр PDF-орієнтований.
        """
        for link in work.get("link") or []:
            if link.get("content-type") == "application/pdf" and link.get("URL"):
                return link["URL"]
        return None

    def _work_to_candidate(self, work: dict, query: str) -> Candidate | None:
        pdf_url = self._pdf_url(work)
        if not pdf_url:
            # Без PDF кандидат не має сенсу: verify-конвеєр усе одно
            # відкине HTML як not_pdf (заміряно 278 874 таких документів).
            return None

        titles = work.get("title") or []
        title = titles[0] if titles else None

        doi = work.get("DOI")
        landing = work.get("URL")

        authors: list[str] = []
        for author in (work.get("author") or [])[:10]:
            family = (author.get("family") or "").strip()
            given = (author.get("given") or "").strip()
            name = f"{family}, {given}" if family and given else (family or given)
            if name:
                authors.append(name)

        date_parts = (work.get("issued") or {}).get("date-parts") or [[]]
        try:
            year = int(date_parts[0][0])
        except (IndexError, TypeError, ValueError):
            year = None

        containers = work.get("container-title") or []
        container = containers[0] if containers else None

        return Candidate(
            url=pdf_url,
            landing_url=landing,
            doi=doi,
            title=title,
            authors=authors or None,
            year=year,
            publisher=work.get("publisher"),
            # Мову не визначаємо: у Crossref вона null для українських
            # журналів (перевірено), а вгадування за кирилицею назвало б
            # російські тексти українськими. Рішення — за langid у verify.
            language=None,
            doc_type=TYPE_MAP.get(work.get("type"), "other"),
            channel=self.name,
            query_text=query[:200],
            extra={"container_title": container} if container else {},
        )