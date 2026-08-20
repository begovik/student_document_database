import re
from urllib.parse import parse_qs, quote, unquote, urlencode, urlparse, urlunparse

import structlog

logger = structlog.get_logger()

TRACKING_PARAMS = {
    "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
    "fbclid", "gclid", "msclkid", "ref", "_ga", "_gl", "mc_cid", "mc_eid",
    "yclid", "openstat", "from", "source", "srsltid",
}


def normalize_url(url: str) -> str:
    if not url:
        return url

    url = url.strip()

    parsed = urlparse(url)

    scheme = parsed.scheme.lower()
    if scheme == "http":
        scheme = "https"

    netloc = parsed.netloc.lower()
    if netloc.startswith("www."):
        netloc = netloc[4:]

    if ":" in netloc:
        host, port = netloc.rsplit(":", 1)
        if (scheme == "https" and port == "443") or (scheme == "http" and port == "80"):
            netloc = host

    path = parsed.path
    path = re.sub(r"/+", "/", path)
    if path != "/" and path.endswith("/"):
        path = path.rstrip("/")

    path = unquote(path)
    path = quote(path, safe="/:@!$&'()*+,;=-._~")

    query_params = parse_qs(parsed.query, keep_blank_values=True)
    filtered_params = {
        k: v for k, v in query_params.items()
        if k.lower() not in TRACKING_PARAMS
    }
    query = urlencode(sorted(filtered_params.items()), doseq=True)

    normalized = urlunparse((scheme, netloc, path, parsed.params, query, ""))

    return normalized


def extract_pdf_url_from_html(html: str, base_url: str) -> str | None:
    import re
    from urllib.parse import urljoin

    patterns = [
        r'href=["\']([^"\']*\.pdf(?:\?[^"\']*)?)["\']',
        r'(?:citation_pdf_url|og:pdf)["\s]*content=["\']([^"\']+)["\']',
        r'content=["\'][^"\']*["\']\s*(?:name|property)=["\'](?:citation_pdf_url|og:pdf)["\']',
    ]

    for pattern in patterns:
        matches = re.findall(pattern, html, re.IGNORECASE)
        if matches:
            pdf_url = matches[0]
            if not pdf_url.startswith(("http://", "https://")):
                pdf_url = urljoin(base_url, pdf_url)
            return pdf_url

    return None
