"""Тести каналу Crossref."""

import httpx
import respx

from harvester.discovery.crossref import (
    DEFAULT_MAX_OFFSET,
    TYPE_MAP,
    CrossrefChannel,
)

QUERY = "дієздатність неповнолітньої особи"

WORK_PDF = {
    "DOI": "10.32837/yuv.v0i3.1944",
    "URL": "https://doi.org/10.32837/yuv.v0i3.1944",
    "type": "journal-article",
    "title": ["Емансипація неповнолітньої особи в цивільному судочинстві"],
    "container-title": ["Прикарпатський юридичний вісник"],
    "publisher": "Publishing House Helvetica",
    "issued": {"date-parts": [[2021, 4, 12]]},
    "author": [{"family": "Мельник", "given": "Олег"}],
    "language": None,
    "link": [
        {
            "URL": "https://ojs.dpu.edu.ua/index.php/irplegchr/article/download/166/165",
            "content-type": "application/pdf",
            "intended-application": "text-mining",
        },
        {
            "URL": "https://doi.org/10.32837/pyuv.v0i1.726",
            "content-type": "text/html",
        },
    ],
}


def _resp(items: list[dict], total: int | None = None) -> httpx.Response:
    return httpx.Response(
        200,
        json={"message": {"items": items, "total-results": total if total is not None else len(items)}},
    )


async def test_yields_pdf_candidate_with_metadata():
    channel = CrossrefChannel()
    channel.enabled = True
    with respx.mock:
        respx.get("https://api.crossref.org/works").mock(return_value=_resp([WORK_PDF], 1))
        got = [c async for c in channel.discover({"query": QUERY, "rows": 10})]

    assert len(got) == 1
    c = got[0]
    assert c.channel == "crossref"
    assert c.url.endswith("download/166/165")
    assert c.doi == "10.32837/yuv.v0i3.1944"
    assert c.landing_url == "https://doi.org/10.32837/yuv.v0i3.1944"
    assert c.title.startswith("Емансипація")
    assert c.year == 2021
    assert c.doc_type == "article"
    assert c.authors == ["Мельник, Олег"]
    assert c.extra["container_title"] == "Прикарпатський юридичний вісник"


async def test_language_left_undecided():
    """Мову не вгадуємо: Crossref віддає null, кирилицею ми б назвали
    українською російські тексти, а проєкт їх відсікає."""
    channel = CrossrefChannel()
    channel.enabled = True
    with respx.mock:
        respx.get("https://api.crossref.org/works").mock(return_value=_resp([WORK_PDF]))
        got = [c async for c in channel.discover({"query": QUERY})]
    assert got[0].language is None


async def test_work_without_pdf_link_is_skipped():
    """HTML-landing без PDF не дає кандидата: конвеєр усе одно
    відкинув би його як not_pdf."""
    html_only = {**WORK_PDF, "link": [{"URL": "https://journal.ua/1", "content-type": "text/html"}]}
    channel = CrossrefChannel()
    channel.enabled = True
    with respx.mock:
        respx.get("https://api.crossref.org/works").mock(return_value=_resp([html_only]))
        got = [c async for c in channel.discover({"query": QUERY})]
    assert got == []


async def test_query_without_pdf_is_counted_as_empty_pass():
    channel = CrossrefChannel()
    channel.enabled = True
    with respx.mock:
        respx.get("https://api.crossref.org/works").mock(return_value=_resp([], 0))
        got = [c async for c in channel.discover({"query": QUERY})]
    assert got == []
    assert channel.last_next_offset is None


async def test_pagination_stops_at_max_offset():
    """Скінченність проходу: без цієї умови запит із 38 тис. результатів
    крутився б до ліміту Crossref."""
    channel = CrossrefChannel()
    channel.enabled = True
    rows = [WORK_PDF] * 10
    with respx.mock:
        respx.get("https://api.crossref.org/works").mock(return_value=_resp(rows, 38034))
        [c async for c in channel.discover(
            {"query": QUERY, "rows": 10, "offset": DEFAULT_MAX_OFFSET}
        )]
    assert channel.last_next_offset is None


async def test_pagination_schedules_next_page_inside_limit():
    channel = CrossrefChannel()
    channel.enabled = True
    rows = [WORK_PDF] * 10
    with respx.mock:
        respx.get("https://api.crossref.org/works").mock(return_value=_resp(rows, 500))
        [c async for c in channel.discover({"query": QUERY, "rows": 10, "offset": 20})]
    assert channel.last_next_offset == 30


async def test_partial_page_ends_pagination():
    """Коротка сторінка = ми дійшли до кінця вибірки."""
    channel = CrossrefChannel()
    channel.enabled = True
    with respx.mock:
        respx.get("https://api.crossref.org/works").mock(return_value=_resp([WORK_PDF], 1))
        [c async for c in channel.discover({"query": QUERY, "rows": 10, "offset": 0})]
    assert channel.last_next_offset is None


async def test_disabled_channel_yields_nothing():
    channel = CrossrefChannel()
    channel.enabled = False
    assert [c async for c in channel.discover({"query": QUERY})] == []


async def test_missing_query_is_not_a_request():
    channel = CrossrefChannel()
    channel.enabled = True
    with respx.mock:
        route = respx.get("https://api.crossref.org/works")
        assert [c async for c in channel.discover({})] == []
        assert not route.called


async def test_http_error_propagates():
    channel = CrossrefChannel()
    channel.enabled = True
    with respx.mock:
        respx.get("https://api.crossref.org/works").mock(return_value=httpx.Response(503))
        try:
            [c async for c in channel.discover({"query": QUERY})]
        except httpx.HTTPStatusError:
            return
    raise AssertionError("HTTP- помилка має підніматися до воркера")


def test_type_map_covers_ukrainian_journal_types():
    assert TYPE_MAP["journal-article"] == "article"
    assert TYPE_MAP["dissertation"] == "dissertation"
    assert TYPE_MAP["monograph"] == "book"