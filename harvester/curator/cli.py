"""CLI-команди для сервісу curator."""

from __future__ import annotations

import asyncio

import structlog
import typer

from harvester.curator.preparer import prepare_catalog
from harvester.curator.verifier import verify_catalog

logger = structlog.get_logger()

curator_app = typer.Typer(
    name="curator",
    help="Підготовка та верифікація каталогів документів",
    no_args_is_help=True,
)


@curator_app.command()
def prepare(
    topic: str = typer.Argument(..., help="Назва теми (наприклад, 'Підприємництво, торгівля та біржова діяльність')"),
    limit: int | None = typer.Option(None, "--limit", "-n", help="Максимальна кількість документів (LLM може обрати менше)"),
    output: str = typer.Option("catalogs", "--output-dir", "-o", help="Директорія для збереження каталогу"),
    dry_run: bool = typer.Option(False, "--dry-run", "-d", help="Не зберігати результат, лише показати що було б зроблено"),
    profile: str = typer.Option("strict", "--profile", "-p", help="Профіль фільтрації: strict (жорсткий) або softier (м'який)"),
):
    """Підготувати каталог документів для заданої теми.

    Сервіс:
    1. Знаходить тему в БД або визначає UDC-префікси
    2. Збирає кандидатів (verified, повні дані)
    3. LLM обирає оптимальну кількість і найкращі документи
    4. Перевіряє доступність кожного обраного документа
    5. Замінює недоступні на схожі (з БД)
    6. Записує JSON-каталог у вказану директорію

    Приклади:
        harvester curator prepare "Підприємництво, торгівля та біржова діяльність"
        harvester curator prepare "Економіка" --limit 50
        harvester curator prepare "Інформатика" --dry-run
        harvester curator prepare "Технологія виробництва одягу" --profile strict
        harvester curator prepare "Технологія виробництва одягу" --profile softier
    """
    asyncio.run(_prepare_cli(topic, limit, output, dry_run, profile))


async def _prepare_cli(topic: str, limit: int | None, output: str, dry_run: bool, profile: str):
    try:
        result = await prepare_catalog(topic, output_dir=output, limit=limit, dry_run=dry_run, profile=profile)
        if result is None:
            print("❌ Не вдалося підготувати каталог (жодних кандидатів не знайдено)")
            return

        print(result.summary())
        if dry_run:
            print("📝 Dry-run: результат не збережено")
    except Exception as e:
        logger.error("curator_prepare_failed", error_msg=str(e)[:200])
        print(f"❌ Помилка: {e}")
        raise typer.Exit(1)


@curator_app.command()
def verify(
    catalog: str = typer.Argument(..., help="Шлях до JSON-каталогу (наприклад, catalogs/catalog_076.json)"),
    dry_run: bool = typer.Option(False, "--dry-run", "-d", help="Не зберігати результат, лише показати що було б зроблено"),
):
    """Верифікувати каталог: знайти помилки, вирішити що робити, виправити.

    Сервіс:
    1. Завантажує каталог
    2. Знаходить елементи з ключем "error"
    3. Для кожного — LLM вирішує: замінити, повторити або пропустити
    4. Виконує рішення (заміна на схожий з БД, або пропуск)
    5. Записує оновлений каталог (*_fixed.json)

    Приклади:
        harvester curator verify catalogs/catalog_076.json
        harvester curator verify catalogs/catalog_076.json --dry-run
    """
    asyncio.run(_verify_cli(catalog, dry_run))


async def _verify_cli(catalog: str, dry_run: bool):
    try:
        result = await verify_catalog(catalog, dry_run=dry_run)
        if result is None:
            print("ℹ Каталог без помилок або не існує")
            return

        print(result.summary())
        if dry_run:
            print("📝 Dry-run: результат не збережено")
    except Exception as e:
        logger.error("curator_verify_failed", error_msg=str(e)[:200])
        print(f"❌ Помилка: {e}")
        raise typer.Exit(1)
