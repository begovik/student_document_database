"""Зібрати PDF-файли для списку джерел картки.

    venv/bin/python scripts/fetch_card_sources.py
    venv/bin/python scripts/fetch_card_sources.py --dest data/export/dohovirne-pravo

На відміну від конвеєра верифікації, який PDF лише читає-кидає,
цей скрипт кладе файли на диск: джерела потрібні власнику курсової
як матеріал для роботи, а не як доказ того, що документ існує.

Що робить і що навмисно не робить:

  * бере ті самі 40 рядків, що й `export_sources.py`, щоб список
    джерел і каталог файлів не розходилися (спільний модуль);
  * завантажує через `HttpClient` проєкту — діють guard-и від SSRF,
    чорний список доменів, ліміт розміру PDF і смуга пропускання;
  * перевіряє сигнатуру `%PDF-` і мінімальний розмір. Посилання, де
    лежить HTML сторінки статті, а не файл, НЕ зберігаються — це
    не помилка, а «нема чого класти», і воно потрапляє в маніфест
    окремим рядком;
  * звіряє SHA-256 завантаження зі значенням у базі. Розбіжність
    не блокує файл, але потрапляє в маніфест: PDF на сервері могли
    перезалити, це корисно знати при написанні;
  * ідемпотентно: файл із тим самим іменем і тим самим хешем не
    перезавантажується.

Запис у БД не вноситься. Скрипт read-only по `documents`.

Логування: помилки WARNING, підсумок INFO, по кожному файлу DEBUG.
"""

import argparse
import asyncio
import csv
import hashlib
import re
import sys
from pathlib import Path

import httpx
import structlog

# Спершу scripts/, потім пакет: щоб скрипт запускався як
# `venv/bin/python scripts/fetch_card_sources.py` без встановлення.
# isort: off
sys.path.insert(0, str(Path(__file__).resolve().parent))

from export_sources import (
    EXPORT_N,
    build as build_sql,
    parse_authors,
    run_psql,
)
from harvester.config import get_settings
from harvester.net.client import get_http_client
from harvester.net.guards import is_url_allowed

# isort: on

log = structlog.get_logger()

DEFAULT_DEST = "data/export/dohovirne-pravo"

# Роздільники всередині назви спершу стає пробілом, інакше «малолітніх/
# неповнолітніх» склеїться в «малолітніхнеповнолітніх».
_UNSAFE = re.compile(r"[^\w\s-]", re.UNICODE)
_SPACES = re.compile(r"[\s_]+", re.UNICODE)
_SEPARATORS = re.compile(r"[/\\|,;:—–«»\"']+")


def safe_name(text: str, limit: int = 80) -> str:
    """Назва для файлу: без лапок, зірок і довжини в 255 байт."""
    cleaned = _SEPARATORS.sub(" ", text.lower())
    cleaned = _UNSAFE.sub("", cleaned)
    cleaned = _SPACES.sub("-", cleaned).strip("-")
    while len(cleaned.encode()) > limit:
        cleaned = cleaned[:-1]
    return cleaned or "dzherelo"


def file_sha256(path: Path) -> str:
    """SHA-256 наявного файлу — щоб повторний запуск теж давав маніфест."""
    hasher = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def filename(number: int, year: str, title: str) -> str:
    prefix = f"{number:02d}_{year or 'без-року'}_"
    budget = 200 - len(prefix.encode()) - len(b".pdf")
    return prefix + safe_name(title, limit=budget)[:120] + ".pdf"


def build_hash_sql(records: list[dict]) -> str:
    """SQL для звірки хешів — винесено окремо, щоб можна було перевірити."""
    have = ("збережено", "уже є")
    urls = [r["url"] for r in records if r["url"] and r["status"] in have]

    def q(value: str) -> str:
        return "'" + value.replace("'", "''") + "'"

    joined = ", ".join(q(u) for u in urls)
    bare = ", ".join(
        q(re.sub(r"^https?://(dx\.)?doi\.org/", "", r["doi"]))
        for r in records
        if r.get("doi")
    )
    return (
        # Ті самі pset-директиви, що в export_sources: у вирівняному
        # форматі psql екранує 0x1F як «\\x1f», і розбір ламається.
        # Без них запит повертає нуль рядків МОВЧКИ — не помилка.
        "\\pset footer off\n\\pset format unaligned\n\\pset tuples_only on\n"
        "\\pset fieldsep '\\x1f'\n"
        "SELECT COALESCE(canonical_url, '') || E'\\x1f' || COALESCE(doi, '')"
        " || E'\\x1f' || COALESCE(sha256, '')\n"
        f"FROM documents WHERE canonical_url IN ({joined}) OR doi IN ({bare});"
    )


def db_hashes(records: list[dict]) -> dict[str, str]:
    """SHA-256, які сервіс обчислив під час верифікації, за canonical_url.

    Звіряти варто: файл на сервері могли перезалити після перевірки, і
    тоді власник курсової читаєме не те, що оцінював вердиктор.
    """
    if not [r for r in records if r.get("url") and r.get("status") in ("збережено", "уже є")]:
        return {}

    out = {}
    for line in run_psql(build_hash_sql(records)).splitlines():
        parts = line.split("\x1f")
        if len(parts) != 3 or not parts[2]:
            continue
        canonical, doi, sha = parts
        # Ключі всі три: запис шукає за тим рядком, який стоїть у
        # маніфесті, а це може бути і прямий URL, і doi.org.
        for key in (canonical, doi, re.sub(r"^https?://(dx\.)?doi\.org/", "", doi)):
            if key:
                out[key] = sha
    return out


def parse_rows(raw: str) -> list[dict]:
    names = ["tier", "rej", "udc", "year", "authors", "title", "publisher", "doi", "url", "landing", "extra"]
    rows = []
    for line in raw.splitlines():
        if not line.strip() or line.startswith("\\"):
            continue
        parts = line.split("\x1f")
        if len(parts) < len(names):
            continue
        row = dict(zip(names, parts[: len(names)], strict=True))
        row["authors_out"] = "; ".join(parse_authors(row["authors"]))
        row["url"] = row["url"] or (f"https://doi.org/{row['doi']}" if row["doi"] else "")
        rows.append(row)
    return rows[:EXPORT_N]


async def fetch_one(client, row: dict, number: int, dest: Path) -> dict:
    """Завантажити одне джерело. Ніколи не кидає — повертає підсумок."""
    title = row["title"]
    name = filename(number, row["year"], title)
    target = dest / name
    url = row["url"]

    record = {
        "n": number,
        "file": name,
        "title": title,
        "authors": row["authors_out"],
        "year": row["year"],
        "udc": row["udc"],
        "doi": row["doi"],
        "url": url,
        "status": "",
        "bytes": "",
        "sha256": "",
        "sha256_match": "",
    }

    if not url:
        record["status"] = "НЕМАЄ URL"
        return record

    allowed, reason = await is_url_allowed(url)
    if not allowed:
        record["status"] = f"ЗАБЛОКОВАНО: {reason[:60]}"
        log.warning("fetch_blocked", n=number, url=url, reason=reason[:120])
        return record

    if target.exists():
        # Хеш рахуємо з диска, а не лишаємо порожнім: повторний запуск
        # не повинен перетворювати маніфест на напівпорожній.
        record["status"] = "уже є"
        record["bytes"] = target.stat().st_size
        record["sha256"] = await asyncio.to_thread(file_sha256, target)
        return record

    tmp = dest / (name + ".part")
    try:
        size, sha, prefix = await client.stream_to_file(url, tmp, None)
    except httpx.HTTPStatusError as e:
        tmp.unlink(missing_ok=True)
        record["status"] = f"HTTP {e.response.status_code}"
        log.warning("fetch_http_status", n=number, url=url, status=e.response.status_code)
        return record
    except httpx.TimeoutException:
        tmp.unlink(missing_ok=True)
        record["status"] = "ТАЙМАУТ"
        log.warning("fetch_timeout", n=number, url=url)
        return record
    except httpx.HTTPError as e:
        tmp.unlink(missing_ok=True)
        record["status"] = f"МЕРЕЖА: {type(e).__name__}"
        log.warning("fetch_network_error", n=number, url=url, error=str(e)[:150])
        return record
    except ValueError:
        # Ліміт max_pdf_bytes — перевище за розмір.
        tmp.unlink(missing_ok=True)
        record["status"] = "ЗАБАГАТО ВЕЛИКИЙ"
        log.warning("fetch_too_large", n=number, url=url)
        return record
    except OSError as e:
        tmp.unlink(missing_ok=True)
        record["status"] = "ПОМИЛКА ФАЙЛУ"
        log.warning("fetch_file_error", n=number, url=url, error=str(e)[:150])
        return record

    if not prefix.startswith(b"%PDF-"):
        tmp.unlink(missing_ok=True)
        record["status"] = "НЕ ФАЙЛ (HTML)"
        log.info("fetch_not_a_file", n=number, url=url, bytes=size)
        return record

    if size < get_settings().http.min_pdf_bytes:
        tmp.unlink(missing_ok=True)
        record["status"] = f"ЗАМАЛИЙ ({size} Б)"
        log.info("fetch_too_small", n=number, url=url, bytes=size)
        return record

    tmp.rename(target)
    record["status"] = "збережено"
    record["bytes"] = size
    record["sha256"] = sha
    log.debug("fetch_saved", n=number, file=name, bytes=size, sha256=sha)
    return record


async def main_async(dest: Path) -> int:
    dest.mkdir(parents=True, exist_ok=True)
    client = await get_http_client()
    try:
        rows = parse_rows(run_psql(build_sql()))
        log.info("fetch_start", count=len(rows), dest=str(dest))

        records = []
        # Послідовно: більшість джерел з одного домену, а там діє
        # per-host token bucket. 40 запитів поспіль — це кілька хвилин
        # і п'ять запитів до одного сервера, а не 40 одночасно.
        for i, row in enumerate(rows, start=1):
            records.append(await fetch_one(client, row, i, dest))
    finally:
        await client.close()

    saved = [r for r in records if r["status"] in ("збережено", "уже є")]
    other = [r for r in records if r["status"] not in ("збережено", "уже є")]

    # Звіряємо завантажене з тим, що бачить верифікатор.
    known = db_hashes(records)
    mismatched = 0
    for r in records:
        expected = known.get(r["url"]) or known.get(r["doi"])
        if r["sha256"] and expected:
            r["sha256_match"] = "так" if r["sha256"] == expected else "НІ"
            if r["sha256_match"] == "НІ":
                mismatched += 1
                log.warning("fetch_hash_mismatch", n=r["n"], url=r["url"])
        elif r["sha256"]:
            r["sha256_match"] = "немає в БД"

    manifest = dest / "MANIFEST.csv"
    with manifest.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(records[0].keys()))
        writer.writeheader()
        writer.writerows(records)

    skipped = [
        ("HTTP ", "сервер відповів помилкою"),
        ("ТАЙМАУТ", "сервер не відповів у строк"),
        ("МЕРЕЖА", "помилка зєднання"),
        ("НЕ ФАЙЛ", "посилання веде на HTML-сторінку, не на PDF"),
        ("ЗАМАЛИЙ", "файл менший за мінімальний"),
        ("ЗАБАГАТО ВЕЛИКИЙ", "файл більший за ліміт"),
        ("ЗАБЛОКОВАНО", "guard зупинив запит"),
        ("НЕМАЄ URL", "у базі немає ні URL, ні DOI"),
    ]
    pct = round(100 * len(saved) / max(1, len(records)))
    total_mb = round(sum(int(r["bytes"]) for r in saved) / 1048576.0, 1)
    matched = sum(1 for r in records if r["sha256_match"] == "так")
    lines = [
        "# Каталог джерел",
        "",
        f"Збережено **{len(saved)} із {len(records)}** джерел ({pct}%), {total_mb} МБ.",
        f"Каталог: `{dest}/`",
        "",
        "## Що всередині",
        "",
        "- `NN_<рік>_<назва>.pdf` — сам файл джерела;",
        "- `MANIFEST.csv` — таблиця: номер, назва, автори, УДК, DOI, URL,",
        "  розмір, SHA-256 і чи збігся він із тим, що обчислив сервіс;",
        "- `README.md` — цей файл.",
        "",
        "## Цілісність",
        "",
    ]
    if matched:
        lines.append(
            f"- SHA-256 збігся з базою для **{matched}** файлів: це ті самі"
            " байти, які оцінив вердиктор;"
        )
    if mismatched:
        lines.append(
            f"- **розбіжність у {mismatched} файлах** — на сервері їх перезалили"
            " після верифікації, список нижче:"
        )
        for r in records:
            if r["sha256_match"] == "НІ":
                lines.append(f"  - №{r['n']} «{r['title'][:70]}»")
    lines.append("")
    for prefix, why in skipped:
        rows_hit = [r for r in other if r["status"].startswith(prefix)]
        if rows_hit:
            lines.append(f"## {prefix} — {why} ({len(rows_hit)})")
            lines.append("")
            for r in rows_hit:
                lines.append(f"- №{r['n']} «{r['title'][:70]}» — {r['url'][:90]}")
            lines.append("")
    (dest / "README.md").write_text("\n".join(lines), encoding="utf-8")

    log.info("fetch_done", saved=len(saved), skipped=len(other), dest=str(dest))
    for prefix, _ in skipped:
        hits = [r for r in other if r["status"].startswith(prefix)]
        if hits:
            log.warning("fetch_skipped_group", kind=prefix, count=len(hits))
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="Завантажити PDF для списку джерел")
    ap.add_argument("--dest", default=DEFAULT_DEST)
    args = ap.parse_args()
    return asyncio.run(main_async(Path(args.dest)))


if __name__ == "__main__":
    sys.exit(main())