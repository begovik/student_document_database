"""Модуль сповіщень на пошту з rate-limiting та накопиченням помилок."""

import asyncio
import smtplib
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import structlog

from harvester.config import get_settings

logger = structlog.get_logger()

# Rate limiting: мінімум 5 хвилин між однаковими сповіщеннями
_RATE_LIMIT: dict[str, datetime] = {}
_RATE_LIMIT_INTERVAL = timedelta(minutes=5)
# Вичерпання всіх LLM-лімітів — денний стан, повторюємо не частіше ніж раз на 6 годин
_ALL_EXHAUSTED_INTERVAL = timedelta(hours=6)

# Накопичення помилок: key -> (count, first_seen, last_seen)
_ERROR_ACCUMULATOR: dict[str, tuple[int, datetime, datetime]] = {}
# Поріг для відправки сповіщення (кількість повторень)
_ERROR_THRESHOLD = 3
# Вікно накопичення (якщо помилка не повторювалась протягом цього часу — скидаємо лічильник)
_ERROR_WINDOW = timedelta(minutes=10)


def _should_send(key: str, interval: timedelta | None = None) -> bool:
    """Перевіряє чи можна відправити сповіщення (rate limiting)."""
    interval = interval or _RATE_LIMIT_INTERVAL
    now = datetime.utcnow()
    last_sent = _RATE_LIMIT.get(key)
    if last_sent and now - last_sent < interval:
        return False
    _RATE_LIMIT[key] = now
    return True


def _accumulate_error(key: str) -> tuple[bool, int]:
    """Накопичує помилку та повертає (чи потрібно відправляти, кількість).

    Якщо помилка повторюється >= _ERROR_THRESHOLD разів протягом _ERROR_WINDOW —
    повертає True і скидає лічильник.
    """
    now = datetime.utcnow()
    entry = _ERROR_ACCUMULATOR.get(key)

    if entry is None:
        # Перша поява помилки
        _ERROR_ACCUMULATOR[key] = (1, now, now)
        return False, 1

    count, first_seen, last_seen = entry

    # Якщо вікно минуло — скидаємо лічильник
    if now - last_seen > _ERROR_WINDOW:
        _ERROR_ACCUMULATOR[key] = (1, now, now)
        return False, 1

    # Накопичуємо
    count += 1
    _ERROR_ACCUMULATOR[key] = (count, first_seen, now)

    if count >= _ERROR_THRESHOLD:
        # Скидаємо лічильник після досягнення порогу
        del _ERROR_ACCUMULATOR[key]
        return True, count

    return False, count


async def send_notification(subject: str, body: str, key: str | None = None,
                            interval: timedelta | None = None) -> bool:
    """Відправити сповіщення на пошту (async, з rate limiting).

    Args:
        subject: Тема листа
        body: Тіло листа
        key: Унікальний ключ для rate limiting (якщо None — використовується subject)
        interval: Період rate limiting (якщо None — стандартні 5 хв)

    Returns:
        True якщо відправлено, False якщо пропущено або помилка
    """
    settings = get_settings()

    # Перевірити чи налаштована пошта
    if not settings.notify.enabled:
        return False

    if not settings.notify.smtp_host or not settings.notify.to_email:
        return False

    # Rate limiting
    rate_key = key or subject
    if not _should_send(rate_key, interval):
        logger.debug("notification_rate_limited", key=rate_key)
        return False

    # Відправити в окремому потоці щоб не блокувати event loop
    try:
        result = await asyncio.to_thread(_send_sync, settings, subject, body)
        if result:
            logger.info("notification_sent", subject=subject[:50])
        return result
    except Exception as e:
        logger.error("notification_failed", error_msg=str(e))
        return False


def _send_sync(settings, subject: str, body: str) -> bool:
    """Синхронна відправка листа через SMTP."""
    try:
        msg = MIMEMultipart()
        msg["From"] = settings.notify.from_email or settings.notify.smtp_user or "harvester@localhost"
        msg["To"] = settings.notify.to_email
        msg["Subject"] = f"[Harvester] {subject}"

        msg.attach(MIMEText(body, "plain", "utf-8"))

        # Підключення до SMTP
        if settings.notify.smtp_port == 465:
            server = smtplib.SMTP_SSL(settings.notify.smtp_host, settings.notify.smtp_port)
        else:
            server = smtplib.SMTP(settings.notify.smtp_host, settings.notify.smtp_port)
            if settings.notify.smtp_starttls:
                server.starttls()

        # Авторизація
        if settings.notify.smtp_user and settings.notify.smtp_password:
            server.login(settings.notify.smtp_user, settings.notify.smtp_password)

        # Відправка
        server.send_message(msg)
        server.quit()
        return True

    except Exception as e:
        logger.error("smtp_send_failed", error_msg=str(e)[:200])
        return False


async def notify_llm_failure(provider: str, model: str, error: str, doc_id: int | None = None, service: str = "LLM") -> None:
    """Сповіщення про помилку LLM-класифікації (з накопиченням)."""
    # Накопичуємо помилку — відправляємо лише після N повторень
    error_key = f"llm_error_{provider}_{model}"
    should_send, count = _accumulate_error(error_key)

    if not should_send:
        logger.debug("llm_error_accumulated", provider=provider, model=model, count=count, threshold=_ERROR_THRESHOLD)
        return

    subject = f"LLM помилка ({count}×): {provider}/{model} [{service}]"
    body = f"""Помилка LLM-класифікації в Harvester (повторилася {count} разів):

Сервіс: {service}
Провайдер: {provider}
Модель: {model}
Документ ID: {doc_id or 'невідомо'}
Помилка: {error}

Час: {datetime.utcnow().isoformat()}

---
Harvester автоматичне сповіщення"""
    await send_notification(subject, body, key=f"llm_error_{provider}")


async def notify_llm_all_exhausted(errors: list[str], service: str = "LLM") -> None:
    """Сповіщення про вичерпання всіх LLM-провайдерів.

    Це денний стан (вичерпано денну квоту), тож повідомляємо на пошту
    не частіше ніж раз на 6 годин — на відміну від 5-хвилинного інтервалу
    для звичайних помилок. Інакше воркери, що закликають LLM щохвилини,
    слали б лист щоразу спрацювання rate-limiter.
    """
    subject = f"LLM: усі провайдери вичерпані [{service}]"
    body = f"""Усі LLM-провайдери вичерпані в Harvester:

Сервіс: {service}
Помилки:
{chr(10).join(f'  - {e}' for e in errors[:10])}

{service} працює тільки на правилах fallback.

Час: {datetime.utcnow().isoformat()}

---
Harvester автоматичне сповіщення"""
    await send_notification(subject, body, key="llm_all_exhausted",
                            interval=_ALL_EXHAUSTED_INTERVAL)


async def notify_critical(component: str, message: str, error: str | None = None) -> None:
    """Сповіщення про критичні помилки (відразу, без накопичення)."""
    subject = f"Критична помилка: {component}"
    body = f"""Критична помилка в Harvester:

Компонент: {component}
Повідомлення: {message}
Помилка: {error or '—'}

Час: {datetime.utcnow().isoformat()}

---
Harvester автоматичне сповіщення"""
    await send_notification(subject, body, key=f"critical_{component}")
