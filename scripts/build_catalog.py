#!/usr/bin/env python3
"""Створення каталогу 30 джерел з цитатами та сумаризаціями."""

import asyncio
import json
import os
import subprocess
import tempfile
import time
from datetime import datetime
from pathlib import Path

import httpx

CATALOGS_DIR = Path("/opt/harvester/catalogs")
TOPIC = "Проєктування технологічного процесу пошиття чоловічого довгого прямого пальта"
PG_HOST = "89.167.68.48"
PG_DB = "harvester"
PG_USER = "harvester"


def get_pg_password():
    """Отримати пароль з .env файлу."""
    env_path = Path("/opt/harvester/.env")
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            if line.startswith("PG_PASS="):
                return line.split("=", 1)[1]
    return os.environ.get("PG_PASS", "")


def run_query(sql: str) -> str:
    """Виконати SQL запит до PostgreSQL."""
    password = get_pg_password()
    cmd = [
        "psql", "-h", PG_HOST, "-U", PG_USER, "-d", PG_DB,
        "-t", "-A", "-F", "|", "-c", sql
    ]
    env = os.environ.copy()
    env["PGPASSWORD"] = password
    result = subprocess.run(cmd, capture_output=True, text=True, env=env)
    if result.returncode != 0:
        print(f"SQL error: {result.stderr}")
        return ""
    return result.stdout.strip()


def fetch_documents() -> list[dict]:
    """Отримати документи з БД."""
    sql = """
    SELECT 
      d.id, 
      d.title, 
      COALESCE(d.title_hint, '') as title_hint,
      COALESCE(d.authors, '[]') as authors, 
      d.year, 
      d.publisher, 
      d.doc_type, 
      d.canonical_url, 
      d.language, 
      d.udc, 
      d.page_count, 
      d.size_bytes, 
      d.sha256, 
      d.has_text_layer, 
      d.verified_at, 
      d.first_seen_at,
      e.quotations,
      e.summary::text as summary_text
    FROM extractions e
    JOIN documents d ON e.document_id = d.id
    WHERE d.status = 'verified'
      AND (
        d.title ILIKE '%пальто%' OR d.title_hint ILIKE '%пальто%'
        OR d.title ILIKE '%швейн%' OR d.title_hint ILIKE '%швейн%'
        OR d.title ILIKE '%конструювання%одяг%' OR d.title_hint ILIKE '%конструювання%одяг%'
        OR d.title ILIKE '%технологія%швейн%' OR d.title_hint ILIKE '%технологія%швейн%'
        OR d.title ILIKE '%технологія%виготовлення%одяг%' OR d.title_hint ILIKE '%технологія%виготовлення%одяг%'
        OR d.title ILIKE '%моделювання%одяг%' OR d.title_hint ILIKE '%моделювання%одяг%'
        OR d.title ILIKE '%розкрій%' OR d.title_hint ILIKE '%розкрій%'
        OR d.title ILIKE '%крій%' OR d.title_hint ILIKE '%крій%'
        OR d.title ILIKE '%пошиття%' OR d.title_hint ILIKE '%пошиття%'
        OR d.title ILIKE '%лекал%' OR d.title_hint ILIKE '%лекал%'
        OR d.title ILIKE '%текстиль%' OR d.title_hint ILIKE '%текстиль%'
        OR d.title ILIKE '%легка промисловість%' OR d.title_hint ILIKE '%легка промисловість%'
        OR d.title ILIKE '%швейн%машин%' OR d.title_hint ILIKE '%швейн%машин%'
        OR d.title ILIKE '%проектування%виготовлення%' OR d.title_hint ILIKE '%проектування%виготовлення%'
      )
    ORDER BY 
      CASE 
        WHEN d.title ILIKE '%пальто%' OR d.title_hint ILIKE '%пальто%' THEN 1
        WHEN d.title ILIKE '%швейн%' OR d.title_hint ILIKE '%швейн%' THEN 2
        WHEN d.title ILIKE '%технологія%швейн%' OR d.title_hint ILIKE '%технологія%швейн%' THEN 2
        WHEN d.title ILIKE '%технологія%виготовлення%одяг%' OR d.title_hint ILIKE '%технологія%виготовлення%одяг%' THEN 2
        WHEN d.title ILIKE '%конструювання%одяг%' OR d.title_hint ILIKE '%конструювання%одяг%' THEN 3
        WHEN d.title ILIKE '%моделювання%одяг%' OR d.title_hint ILIKE '%моделювання%одяг%' THEN 3
        WHEN d.title ILIKE '%проектування%виготовлення%' OR d.title_hint ILIKE '%проектування%виготовлення%' THEN 3
        WHEN d.title ILIKE '%пошиття%' OR d.title_hint ILIKE '%пошиття%' THEN 4
        WHEN d.title ILIKE '%крій%' OR d.title_hint ILIKE '%крій%' THEN 4
        WHEN d.title ILIKE '%розкрій%' OR d.title_hint ILIKE '%розкрій%' THEN 4
        WHEN d.title ILIKE '%лекал%' OR d.title_hint ILIKE '%лекал%' THEN 4
        WHEN d.title ILIKE '%текстиль%' OR d.title_hint ILIKE '%текстиль%' THEN 5
        ELSE 6
      END,
      e.id DESC
    LIMIT 30;
    """
    output = run_query(sql)
    docs = []
    for line in output.split("\n"):
        if not line.strip():
            continue
        parts = line.split("|")
        if len(parts) < 18:
            continue
        try:
            doc_id = int(parts[0])
            title = parts[1]
            title_hint = parts[2]
            try:
                authors = json.loads(parts[3])
            except (json.JSONDecodeError, TypeError):
                authors = []
            year = int(parts[4]) if parts[4] and parts[4] != "" else None
            publisher = parts[5] if parts[5] else None
            doc_type = parts[6] if parts[6] else "other"
            canonical_url = parts[7]
            language = parts[8] if parts[8] else "uk"
            udc = parts[9] if parts[9] else None
            page_count = int(parts[10]) if parts[10] and parts[10] != "" else None
            size_bytes = int(parts[11]) if parts[11] and parts[11] != "" else None
            sha256 = parts[12] if parts[12] else None
            has_text_layer = int(parts[13]) if parts[13] and parts[13] != "" else None
            verified_at = parts[14] if parts[14] else None
            first_seen_at = parts[15] if parts[15] else None
            try:
                quotations = json.loads(parts[16]) if parts[16] else []
            except (json.JSONDecodeError, TypeError):
                quotations = []
            try:
                summary = json.loads(parts[17]) if parts[17] else None
            except (json.JSONDecodeError, TypeError):
                summary = None
            
            docs.append({
                "id": doc_id,
                "title": title,
                "title_hint": title_hint,
                "authors": authors,
                "year": year,
                "publisher": publisher,
                "doc_type": doc_type,
                "canonical_url": canonical_url,
                "language": language,
                "udc": udc,
                "page_count": page_count,
                "size_bytes": size_bytes,
                "sha256": sha256,
                "has_text_layer": has_text_layer,
                "verified_at": verified_at,
                "first_seen_at": first_seen_at,
                "quotations": quotations,
                "summary": summary,
            })
        except (ValueError, IndexError) as e:
            print(f"Error parsing line: {e}")
            continue
    return docs


async def download_pdf(url: str, dest: Path, timeout_s: float = 60.0) -> bool:
    """Завантажити PDF за URL."""
    settings_path = Path("/opt/harvester/.env")
    user_agent = "Harvester/1.0 (research)"
    if settings_path.exists():
        for line in settings_path.read_text().splitlines():
            if line.startswith("HARVESTER_CONTACT_EMAIL="):
                email = line.split("=", 1)[1]
                user_agent = f"Harvester/1.0 ({email})"
                break
    
    headers = {"User-Agent": user_agent, "Accept": "application/pdf,*/*"}
    timeout = httpx.Timeout(timeout_s, connect=10.0, read=30.0, pool=None)
    
    try:
        async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
            resp = await client.get(url, headers=headers)
            if resp.status_code != 200:
                print(f"  HTTP {resp.status_code} for {url}")
                return False
            
            content_type = resp.headers.get("content-type", "")
            if "pdf" not in content_type.lower() and not url.lower().endswith(".pdf"):
                if "html" in content_type.lower():
                    print(f"  Not PDF (HTML) for {url}")
                    return False
            
            data = resp.content
            if len(data) < 1024:
                print(f"  Too small ({len(data)} bytes) for {url}")
                return False
            
            if data[:4] != b"%PDF":
                print(f"  No %PDF magic bytes for {url}")
                return False
            
            dest.write_bytes(data)
            return True
    except Exception as e:
        print(f"  Error downloading {url}: {e}")
        return False


async def build_catalog():
    """Створити каталог."""
    print("Отримання документів з БД...")
    docs = fetch_documents()
    print(f"Знайдено {len(docs)} документів")
    
    # Створити каталог
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    catalog_name = f"catalog_{timestamp}"
    catalog_dir = CATALOGS_DIR / catalog_name
    resources_dir = catalog_dir / "resources"
    resources_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"Створено каталог: {catalog_dir}")
    
    # Завантажити PDF
    downloaded = 0
    failed = 0
    for i, doc in enumerate(docs, 1):
        pdf_path = resources_dir / f"{doc['id']}.pdf"
        if pdf_path.exists():
            print(f"  [{i}/{len(docs)}] #{doc['id']} вже існує")
            downloaded += 1
            continue
        
        print(f"  [{i}/{len(docs)}] Завантаження #{doc['id']}...")
        success = await download_pdf(doc["canonical_url"], pdf_path)
        if success:
            downloaded += 1
            doc["pdf_path"] = f"resources/{doc['id']}.pdf"
        else:
            failed += 1
            doc["pdf_path"] = None
        
        # Невелика затримка між запитами
        await asyncio.sleep(0.5)
    
    print(f"\nЗавантажено: {downloaded}, Помилки: {failed}")
    
    # Створити JSON каталогу
    catalog_data = {
        "topic": TOPIC,
        "created_at": datetime.now().isoformat(),
        "total_documents": len(docs),
        "replaced_count": 0,
        "resources_dir": "resources",
        "documents": docs,
    }
    
    json_path = catalog_dir / f"{catalog_name}.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(catalog_data, f, ensure_ascii=False, indent=2)
    
    print(f"JSON збережено: {json_path}")
    print(f"\nКаталог створено: {catalog_dir}")
    print(f"Всього документів: {len(docs)}")
    print(f"Завантажено PDF: {downloaded}")
    
    return catalog_dir


if __name__ == "__main__":
    asyncio.run(build_catalog())
