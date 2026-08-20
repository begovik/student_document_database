from dataclasses import dataclass, field
from typing import Any, AsyncIterator, Protocol


@dataclass
class Candidate:
    url: str
    title_hint: str | None = None
    landing_url: str | None = None
    source_id: int | None = None
    doi: str | None = None
    isbn: str | None = None
    openalex_id: str | None = None
    title: str | None = None
    authors: list[str] | None = None
    year: int | None = None
    publisher: str | None = None
    language: str | None = None
    lang_confidence: float | None = None
    doc_type: str = "other"
    udc: str | None = None
    is_oa: bool = True
    oa_status: str | None = None
    channel: str | None = None
    query_text: str | None = None
    ref_url: str | None = None
    extra: dict[str, Any] = field(default_factory=dict)


class Channel(Protocol):
    name: str
    enabled: bool

    async def discover(self, task: dict) -> AsyncIterator[Candidate]:
        ...

    def rate_limit(self) -> float:
        ...
