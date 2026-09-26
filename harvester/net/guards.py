import ipaddress
import socket
from urllib.parse import urlparse

import structlog
import tldextract

logger = structlog.get_logger()


PRIVATE_NETWORKS = [
    ipaddress.ip_network("10.0.0.0/8"),
    ipaddress.ip_network("172.16.0.0/12"),
    ipaddress.ip_network("192.168.0.0/16"),
    ipaddress.ip_network("127.0.0.0/8"),
    ipaddress.ip_network("169.254.0.0/16"),
    ipaddress.ip_network("::1/128"),
    ipaddress.ip_network("fc00::/7"),
    ipaddress.ip_network("fe80::/10"),
    ipaddress.ip_network("100.64.0.0/10"),
]


def is_private_ip(ip_str: str) -> bool:
    try:
        ip = ipaddress.ip_address(ip_str)
        if (
            ip.is_private
            or ip.is_loopback
            or ip.is_link_local
            or ip.is_reserved
            or ip.is_multicast
            or ip.is_unspecified
        ):
            return True
        for network in PRIVATE_NETWORKS:
            if ip in network:
                return True
        return False
    except ValueError:
        return True


def extract_domain(url: str) -> str | None:
    try:
        parsed = urlparse(url)
        host = parsed.hostname
    except ValueError:
        return None
    if not host:
        return None
    return host.rstrip(".").lower()


def extract_registered_domain(url: str) -> str | None:
    extracted = tldextract.extract(url)
    if extracted.domain and extracted.suffix:
        return f"{extracted.domain}.{extracted.suffix}".lower()
    return None


def get_tld(url: str) -> str | None:
    extracted = tldextract.extract(url)
    if extracted.suffix:
        return f".{extracted.suffix}".lower()
    return None


async def check_ssrf(url: str) -> bool:
    domain = extract_domain(url)
    if not domain:
        logger.warning("ssrf_check_failed", reason="no_domain", url=url)
        return False

    try:
        import asyncio

        ip_addresses = await asyncio.wait_for(
            asyncio.to_thread(socket.getaddrinfo, domain, None), timeout=5.0
        )
        for addr_info in ip_addresses:
            ip_str = addr_info[4][0]
            if is_private_ip(ip_str):
                logger.warning("ssrf_check_failed", reason="private_ip", url=url, ip=ip_str)
                return False
    except TimeoutError:
        logger.warning("ssrf_check_failed", reason="dns_timeout", url=url)
        return False
    except socket.gaierror as e:
        logger.warning("ssrf_check_failed", reason="dns_error", url=url, error=str(e))
        return False
    except Exception as e:
        logger.error("ssrf_check_error", url=url, error=str(e))
        return False

    return True


async def is_domain_blocked(url: str) -> bool:
    from harvester.net.blacklist import BlacklistService

    domain = extract_domain(url)
    if not domain:
        return True

    return await BlacklistService.get().is_blocked_host(domain)


async def is_url_allowed(url: str) -> tuple[bool, str | None]:
    """Перевірити URL: формат, чорний список доменів, SSRF.

    Кожна відмова логується на debug: раніше викликачі бачили лише
    `False` без причини, тому незрозуміло було, скільки кандидатів
    відпало на форматі, скільки — на чорному списку, а скільки —
    через DNS/SSRF-перевірку.
    """
    try:
        parsed = urlparse(url)
    except ValueError as e:
        logger.debug("url_allowed_rejected", url=url[:150], reason="invalid_url", detail=str(e)[:100])
        return False, "invalid_url"

    if parsed.scheme.lower() not in ("http", "https"):
        logger.debug(
            "url_allowed_rejected",
            url=url[:150],
            reason="invalid_scheme",
            scheme=parsed.scheme[:20],
        )
        return False, "invalid_scheme"
    if not parsed.hostname or parsed.username is not None or parsed.password is not None:
        logger.debug(
            "url_allowed_rejected",
            url=url[:150],
            reason="invalid_host",
            has_host=bool(parsed.hostname),
        )
        return False, "invalid_host"

    from harvester.net.blacklist import BlacklistService

    blacklist = BlacklistService.get()
    host = parsed.hostname
    if await blacklist.is_blocked_host(host):
        logger.debug("url_allowed_rejected", url=url[:150], reason="domain_blocked", host=host[:100])
        return False, "domain_blocked"
    if await blacklist.is_blocked_url(url):
        logger.debug("url_allowed_rejected", url=url[:150], reason="url_blocked", host=host[:100])
        return False, "url_blocked"

    if not await check_ssrf(url):
        return False, "ssrf_blocked"

    return True, None


def validate_url_format(url: str) -> bool:
    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc])
    except Exception as e:  # noqa: BLE001
        # Раніше жодного сліду: urlparse рідко кидає, але коли кидає —
        # результат у DDGS-кандидатів просто відсікався.
        logger.debug(
            "url_format_invalid",
            url=url[:150],
            error=str(e)[:100],
            error_type=type(e).__name__,
        )
        return False
