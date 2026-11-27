"""Демоція хостів, чия видача майже ніколи не стає документом.

Сміття в базі розподілене нерівномірно: кілька хостів віддають HTML-сторінки
(landing/bitstream-обгортки) замість PDF і саме вони згенерували десятки тисяч
задач верифікації. Фільтр за розширенням URL не працює: 43% `not_pdf`
закінчуються на `.pdf`, а 78% справжніх PDF — без розширення. Тому рішення
ухвалюється за статистикою самого хоста, а не за формою посилання.
"""

from __future__ import annotations

import time
from urllib.parse import urlparse

import structlog

from harvester.dedup.urlnorm import normalize_url

logger = structlog.get_logger()

# Пороги підібрані за фактичними даними: під них підпадає 150 хостів, які
# дали 152k записів (з них 107k — not_pdf), а втрачених verified — 137,
# тобто 0.04% усіх підтверджених документів.
MIN_DOCUMENTS = 200
MAX_VERIFIED_RATE = 0.01
REFRESH_INTERVAL_S = 1800.0

# Хост витягується з canonical_url, який уже нормалізовано (без `www.`),
# тому той самий вираз дає ключ, порівнюваний із результатом host_of().
# `instr` є в SQLite, але не в PostgreSQL, тому тримаємо два варіанти й
# послідовно пробуємо їх: читання може піти в будь-яку з двох баз.
_PG_HOSTS_SQL = """
SELECT split_part(split_part(canonical_url, '//', 2), '/', 1) AS host,
       count(*) AS total,
       sum(CASE WHEN status = 'verified' THEN 1 ELSE 0 END) AS verified
FROM documents
WHERE canonical_url LIKE 'http%'
GROUP BY 1
HAVING count(*) >= ?
"""

_SQLITE_HOSTS_SQL = """
SELECT substr(substr(canonical_url, instr(canonical_url, '//') + 2), 1,
              instr(substr(canonical_url, instr(canonical_url, '//') + 2) || '/', '/') - 1) AS host,
       count(*) AS total,
       sum(CASE WHEN status = 'verified' THEN 1 ELSE 0 END) AS verified
FROM documents
WHERE canonical_url LIKE 'http%'
GROUP BY 1
HAVING count(*) >= ?
"""


def host_of(url: str) -> str:
    """Хост у тому ж вигляді, у якому він лежить у documents.canonical_url."""
    try:
        return urlparse(normalize_url(url)).netloc.lower()
    except Exception:  # noqa: BLE001
        # Некоректний URL не має валити реєстрацію кандидата: для нього
        # просто не буде демоції.
        return ""


class HostTrust:
    """Кеш хостів, чия видача не стає документами.

    Перевіряється періодично, а не на кожен кандидат: агрегація по всій
    таблиці documents коштує близько секунди, тоді як сам лише факт
    «хост у списку» — це перевірка входження в множину.
    """

    def __init__(
        self,
        db,
        min_documents: int = MIN_DOCUMENTS,
        max_verified_rate: float = MAX_VERIFIED_RATE,
        refresh_interval_s: float = REFRESH_INTERVAL_S,
    ):
        self.db = db
        self.min_documents = min_documents
        self.max_verified_rate = max_verified_rate
        self.refresh_interval_s = refresh_interval_s
        self._demoted: set[str] = set()
        self._loaded_at = 0.0
        self._load_error_logged = False

    async def is_demoted(self, url: str) -> bool:
        """Чи варто відкинути кандидата, не витрачаючи на нього чергу."""
        await self._ensure_loaded()
        return host_of(url) in self._demoted

    async def _ensure_loaded(self) -> None:
        if time.monotonic() - self._loaded_at < self.refresh_interval_s:
            return
        await self.refresh()

    async def refresh(self) -> None:
        """Перерахувати список демоції з фактичної статистики documents."""
        rows = None
        last_error: Exception | None = None
        for sql in (_PG_HOSTS_SQL, _SQLITE_HOSTS_SQL):
            try:
                rows = await self.db.fetchall(sql, (self.min_documents,))
                break
            except Exception as e:  # noqa: BLE001
                last_error = e
        if rows is None:
            # Демоція — оптимізація, а не вимога коректності. Якщо агрегація
            # недоступна, краще не фільтрувати взагалі, ніж зупинити discovery.
            if not self._load_error_logged:
                self._load_error_logged = True
                logger.warning("host_trust_unavailable", error=str(last_error)[:200])
            self._demoted = set()
            self._loaded_at = time.monotonic()
            return

        demoted: set[str] = set()
        for row in rows:
            total = int(row["total"] or 0)
            verified = int(row["verified"] or 0)
            if total >= self.min_documents and verified / total < self.max_verified_rate:
                demoted.add(row["host"])
        self._demoted = demoted
        self._loaded_at = time.monotonic()
        logger.info(
            "host_trust_refreshed",
            demoted_hosts=len(demoted),
            min_documents=self.min_documents,
            max_verified_rate=self.max_verified_rate,
        )
