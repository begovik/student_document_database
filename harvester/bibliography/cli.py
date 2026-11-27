"""CLI для сервісу бібліографічних джерел."""

from __future__ import annotations

import asyncio

import structlog
import typer

from harvester.bibliography.service import BibliographyService

logger = structlog.get_logger()

bibliography_app = typer.Typer(
    name="bibliography",
    help="Вилучення та пошук бібліографічних джерел з каталогів",
    no_args_is_help=True,
)


@bibliography_app.command()
def scan(
    catalog_dir: str = typer.Argument(..., help="Шлях до каталогу з документами"),
    output: str = typer.Option(None, "--output", "-o", help="Назва вихідного файлу (без розширення)"),
):
    """Просканувати каталог та вилучити бібліографічні посилання.
    
    Сервіс:
    1. Читає legacy-PDF з каталогу або тимчасово завантажує їх за URL
    2. Вилучає текст з кожного PDF і видаляє тимчасовий файл
    3. Знаходить розділ "Література" та витягує посилання
    4. Дедуплікує посилання
    5. Шукає кожне посилання в БД та інтернеті
    6. Створює JSON з результатами та текстовий файл зі списком літератури
    
    Приклади:
        harvester bibliography scan catalogs/catalog_20260828_135702
        harvester bibliography scan catalogs/catalog_20260828_135702 --output my_refs
    """
    asyncio.run(_scan_cli(catalog_dir, output))


async def _scan_cli(catalog_dir: str, output: str | None):
    try:
        service = BibliographyService()
        result = await service.process_catalog(catalog_dir, output_name=output)

        print(f"\n{'='*60}")
        print("📚 БІБЛІОГРАФІЧНИЙ АНАЛІЗ КАТАЛОГУ")
        print(f"{'='*60}")
        print(f"Каталог: {result.catalog_path}")
        print(f"Документів проскановано: {result.documents_scanned}")
        print(f"Посилань вилучено: {result.references_extracted}")
        print(f"Після дедуплікації: {result.references_after_dedup}")
        if getattr(result, "references_filtered_russian", 0):
            print(f"  └─ Відфільтровано російських/радянських: {result.references_filtered_russian} (не шукались)")
        print(f"До пошуку: {getattr(result, 'references_for_search', result.references_after_dedup)}")
        print()
        print("📊 Результати пошуку:")
        print(f"  Знайдено в БД: {result.references_found_in_db}")
        print("    → Пояснення: точний збіг у harvester.documents за DOI/URL/назвою.")
        print("      0 означає, що жодне з посилань не співпало з уже верифікованими документами.")
        print("      Це нормально для рідкісних монографій/підручників, яких ще немає в БД.")
        print(f"  Знайдено в інтернеті: {result.references_found_online}")
        print("    → Пояснення: знайдено доступний PDF через DDGS (filetype:pdf), пройшов перевірку is_url_allowed, HEAD 200, anti-SSRF.")
        print("      'шукається' з'являється якщо DDGS таймаут або перервано (120с ліміт).")
        print(f"  Не знайдено: {result.references_not_found}")
        print()
        if getattr(result, "pdfs_downloaded", 0):
            print(f"🔎 Тимчасово перевірено PDF: {result.pdfs_downloaded} (файли видалено після перевірки)")
        if getattr(result, "documents_added_to_db", 0):
            print(f"🗄  Додано в БД (discovered): {result.documents_added_to_db} (джерела-документи → і в список, і в БД)")
            print("   Інтернет-ресурси (abstract/сторінки) → лише у список (не в БД)")
        print()
        print(f"⏱ Час обробки: {result.processing_time_s:.1f} сек")
        print(f"📄 Результати: {result.output_file}")
        if getattr(result, "details", None):
            filt = result.details.get("filtered_examples") or []
            if filt:
                print("🚫 Приклади відфільтрованих RU:")
                for ex in filt[:3]:
                    print(f"   - {ex['raw'][:80]}... => {ex['reason']}")
        print(f"{'='*60}")
        print("📂 PDF не зберігаються: завантаження виконуються лише у системний temp")
        print("   для перевірки доступності, релевантності, текстового шару та фільтра RU.")
        
    except Exception as e:
        logger.error("bibliography_scan_failed", error_msg=str(e)[:200])
        print(f"❌ Помилка: {e}")
        raise typer.Exit(1)
