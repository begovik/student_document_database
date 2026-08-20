import asyncio
import re
import time

import structlog

from harvester.config import get_settings

logger = structlog.get_logger()

DEFAULT_BLACKLIST_DOMAINS = [
    "elibrary.ru", "cyberleninka.ru", "dissercat.com",
    "sci-hub.se", "sci-hub.st", "sci-hub.ru",
    "libgen.is", "libgen.rs", "libgen.li",
    "annas-archive.org", "annas-archive.se",
    "twirpx.com", "twirpx.ru", "studfiles.net", "studme.org",
    "bukvar.su", "docsity.com",
]


class BlacklistService:
    """In-memory чорний список: конфіг TLD + БД (оновлення кожні 60 с). Без DB-hit на кожен URL."""

    _instance: "BlacklistService | None" = None

    def __init__(self):
        self._domains: set[str] = set()
        self._tlds: set[str] = set()
        self._regexes: list[re.Pattern] = []
        self._loaded_at = 0.0
        self._refresh_interval = 60.0
        self._lock = asyncio.Lock()
        self._db = None

        settings = get_settings()
        for tld in settings.filters.blocked_tlds:
            self._tlds.add(tld.lower())

    @classmethod
    def get(cls) -> "BlacklistService":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def set_db(self, db) -> None:
        self._db = db

    async def refresh_if_needed(self) -> None:
        if time.monotonic() - self._loaded_at < self._refresh_interval:
            return
        async with self._lock:
            if time.monotonic() - self._loaded_at < self._refresh_interval:
                return
            await self._load_from_db()
            self._loaded_at = time.monotonic()

    async def _load_from_db(self) -> None:
        if self._db is None:
            return
        try:
            rows = await self._db.fetchall("SELECT pattern, kind FROM blacklist")
            for row in rows:
                pattern, kind = row["pattern"].lower(), row["kind"]
                if kind == "domain":
                    self._domains.add(pattern)
                elif kind == "tld":
                    self._tlds.add(pattern)
                elif kind == "url_regex":
                    try:
                        self._regexes.append(re.compile(pattern, re.IGNORECASE))
                    except re.error:
                        logger.warning("blacklist_bad_regex", pattern=pattern)
            logger.debug(
                "blacklist_loaded", domains=len(self._domains), tlds=len(self._tlds)
            )
        except Exception as e:
            logger.error("blacklist_load_error", error=str(e))

    async def is_blocked_host(self, host: str) -> bool:
        await self.refresh_if_needed()
        host = host.lower()

        for tld in self._tlds:
            if host.endswith(tld):
                return True

        parts = host.split(".")
        for i in range(len(parts)):
            candidate = ".".join(parts[i:])
            if candidate in self._domains:
                return True

        return False

    async def is_blocked_url(self, url: str) -> bool:
        await self.refresh_if_needed()
        for rx in self._regexes:
            if rx.search(url):
                return True
        return False


async def seed_blacklist(db) -> int:
    """Початкове наповнення чорного списку (додаток B ТЗ)."""
    from harvester.db.repositories import BlacklistRepository

    repo = BlacklistRepository(db)
    inserted = 0
    for domain in DEFAULT_BLACKLIST_DOMAINS:
        try:
            await repo.add(domain, "domain", "default seed: russian/pirate resource")
            inserted += 1
        except Exception:
            pass
    if inserted:
        logger.info("blacklist_seeded", inserted=inserted)
    return inserted
