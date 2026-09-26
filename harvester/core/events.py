import logging
import sys
import time
from datetime import datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path

import structlog

# Події, що повторюються з тієї ж причини, не пишемо частіше ніж раз на
# хвилину: недоступність БД під навантаженням інакше засмічує журнал.
_EVENT_DB_FAILURE_LOG_INTERVAL = 60.0


def add_timestamp(logger, method_name, event_dict):
    event_dict["ts"] = datetime.utcnow().isoformat()
    return event_dict


def add_component(logger, method_name, event_dict):
    if "component" not in event_dict:
        event_dict["component"] = "harvester"
    return event_dict


_shared_processors = [
    add_timestamp,
    add_component,
    structlog.processors.add_log_level,
    structlog.processors.StackInfoRenderer(),
    structlog.processors.format_exc_info,
]


def setup_logging(level: str = "INFO", log_file: str | Path | None = None) -> None:
    """Налаштування логування: консоль (читабельно) + файл (JSON, ротація 20MB x 5)."""
    log_level = getattr(logging, level.upper(), logging.INFO)

    handlers: list[logging.Handler] = []

    console_handler = logging.StreamHandler(sys.stdout)
    if sys.stdout.isatty():
        console_handler.setFormatter(
            structlog.stdlib.ProcessorFormatter(processor=structlog.dev.ConsoleRenderer())
        )
    else:
        console_handler.setFormatter(
            structlog.stdlib.ProcessorFormatter(processor=structlog.processors.JSONRenderer())
        )
    handlers.append(console_handler)

    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = RotatingFileHandler(
            log_path, maxBytes=20 * 1024 * 1024, backupCount=21, encoding="utf-8"
        )
        file_handler.setFormatter(
            structlog.stdlib.ProcessorFormatter(processor=structlog.processors.JSONRenderer())
        )
        handlers.append(file_handler)

    logging.basicConfig(level=log_level, handlers=handlers, force=True)

    structlog.configure(
        processors=_shared_processors + [structlog.stdlib.ProcessorFormatter.wrap_for_formatter],
        wrapper_class=structlog.make_filtering_bound_logger(log_level),
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=False,
    )


class EventLogger:
    """WARN+ події дублюються в таблицю system_events (для CLI `harvester events`)."""

    def __init__(self, db):
        self.db = db
        # Лічильник недоступних записів у system_events: якщо БД недоступна,
        # події зникали мовчки й не лишалося жодного сліду. Лічильник
        # дозволяє помітити це в heartbeat, а не через тиждень.
        self.events_db_failures = 0
        self._last_failure_log = 0.0

    async def log(
        self,
        level: str,
        component: str,
        message: str,
        context: dict | None = None,
    ) -> None:
        # Спершу structlog: він не залежить від БД, тож подія гарантовано
        # потрапить у журнал навіть тоді, коли запис у system_events не вдався.
        logger = structlog.get_logger(component=component)
        log_func = getattr(logger, level.lower(), logger.info)
        log_func(message, **(context or {}))

        if level.upper() not in ("WARN", "ERROR", "CRITICAL"):
            return

        try:
            from harvester.db.repositories import SystemEventsRepository

            repo = SystemEventsRepository(self.db)
            await repo.log(level.upper(), component, message, context)
        except Exception as e:  # noqa: BLE001
            self.events_db_failures += 1
            # Трюс: БД недоступна — повідомлення про кожну подію перетворилося
            # б у потік повідомлень про саму помилку.
            now = time.monotonic()
            if now - self._last_failure_log >= _EVENT_DB_FAILURE_LOG_INTERVAL:
                self._last_failure_log = now
                logger.error(
                    "event_db_write_failed",
                    error=str(e)[:200],
                    error_type=type(e).__name__,
                    total_failures=self.events_db_failures,
                )

    async def info(self, component: str, message: str, context: dict | None = None) -> None:
        await self.log("INFO", component, message, context)

    async def warning(self, component: str, message: str, context: dict | None = None) -> None:
        await self.log("WARN", component, message, context)

    async def error(self, component: str, message: str, context: dict | None = None) -> None:
        await self.log("ERROR", component, message, context)

    async def debug(self, component: str, message: str, context: dict | None = None) -> None:
        await self.log("DEBUG", component, message, context)
