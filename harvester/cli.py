import asyncio
import csv
import json
import sys
from datetime import datetime
from pathlib import Path

import typer
from rich import print as rprint
from rich.console import Console
from rich.table import Table

from harvester.config import get_settings, load_config

app = typer.Typer(
    name="harvester",
    help="Harvester — безперервний збирач наукових PDF-джерел",
    add_completion=False,
)

console = Console()


@app.command()
def start(
    config: Path = typer.Option(
        "config.yaml",
        "--config",
        "-c",
        help="Шлях до файлу конфігурації",
    ),
):
    """Запустити сервіс у безперервному режимі"""
    from harvester.core.events import setup_logging
    from harvester.core.supervisor import Supervisor

    settings = load_config(config)
    setup_logging(settings.logging.level, settings.logging.file)

    supervisor = Supervisor(settings)

    try:
        asyncio.run(supervisor.run_forever())
    except KeyboardInterrupt:
        rprint("[yellow]Отримано сигнал зупинки[/yellow]")
    except Exception as e:
        rprint(f"[red]Критична помилка: {e}[/red]")
        sys.exit(1)


@app.command()
def status():
    """Показати поточний стан сервісу"""
    from harvester.db.connection import Database
    from harvester.db.repositories import DocumentsRepository, SettingsRepository, TasksRepository

    async def _status():
        settings = get_settings()
        db = Database(settings.db_path)
        await db.initialize()

        try:
            settings_repo = SettingsRepository(db)
            docs_repo = DocumentsRepository(db)
            tasks_repo = TasksRepository(db)

            heartbeat_raw = await settings_repo.get("heartbeat")
            heartbeat_info = "N/A"
            workers_alive = "N/A"
            if heartbeat_raw:
                try:
                    hb = json.loads(heartbeat_raw)
                    hb_ts = datetime.fromisoformat(hb["ts"])
                    age_s = int((datetime.utcnow() - hb_ts).total_seconds())
                    heartbeat_info = f"{hb['ts']} ({age_s} с тому)"
                    workers_alive = str(hb.get("workers", "?"))
                except (json.JSONDecodeError, KeyError):
                    heartbeat_info = heartbeat_raw

            doc_stats = await docs_repo.count_by_status()
            lang_stats = await docs_repo.count_by_language()
            task_stats = await tasks_repo.count_by_status()
            task_by_type = await tasks_repo.count_by_type()

            table = Table(title="Стан Harvester")
            table.add_column("Параметр", style="cyan")
            table.add_column("Значення", style="green")

            table.add_row("Heartbeat", heartbeat_info)
            table.add_row("Воркери (живі)", workers_alive)
            table.add_row("Документи (всього)", str(sum(doc_stats.values())))
            for st, cnt in sorted(doc_stats.items(), key=lambda x: -x[1]):
                table.add_row(f"  · {st}", str(cnt))
            if lang_stats:
                table.add_row("Мови (verified)", ", ".join(f"{k}:{v}" for k, v in sorted(lang_stats.items(), key=lambda x: -x[1])))
            table.add_row("Завдання (pending)", str(task_stats.get("pending", 0)))
            table.add_row("Завдання (running)", str(task_stats.get("running", 0)))
            for ttype, by_status in sorted(task_by_type.items()):
                pending = by_status.get("pending", 0)
                running = by_status.get("running", 0)
                if pending or running:
                    table.add_row(f"  · {ttype}", f"p:{pending} r:{running}")

            console.print(table)
        finally:
            await db.close()

    asyncio.run(_status())


@app.command()
def stats(
    period: str = typer.Option("24h", "--period", "-p", help="Період: 24h, 7d, 30d"),
    json_output: bool = typer.Option(False, "--json", help="Вивід у форматі JSON"),
):
    """Показати статистику по каналах"""
    from harvester.db.connection import Database
    from harvester.db.repositories import ChannelStatsRepository

    async def _stats():
        settings = get_settings()
        db = Database(settings.db_path)
        await db.initialize()

        try:
            stats_repo = ChannelStatsRepository(db)

            hours = 24
            if period.endswith("d"):
                hours = int(period[:-1]) * 24
            elif period.endswith("h"):
                hours = int(period[:-1])

            channel_stats = await stats_repo.get_summary(hours)

            if json_output:
                print(json.dumps(channel_stats, indent=2, ensure_ascii=False))
            else:
                table = Table(title=f"Статистика каналів ({period})")
                table.add_column("Канал", style="cyan")
                table.add_column("Запитів", justify="right")
                table.add_column("Успішних", justify="right", style="green")
                table.add_column("Помилок", justify="right", style="red")
                table.add_column("Знайдено", justify="right")
                table.add_column("Нових", justify="right", style="bright_green")

                for stat in channel_stats:
                    table.add_row(
                        stat["channel"],
                        str(stat["requests"]),
                        str(stat["ok"]),
                        str(stat["errors"]),
                        str(stat["items_found"]),
                        str(stat["items_new"]),
                    )

                console.print(table)
        finally:
            await db.close()

    asyncio.run(_stats())


@app.command()
def export(
    output: Path = typer.Option(..., "--output", "-o", help="Файл для експорту"),
    format: str = typer.Option("csv", "--format", "-f", help="Формат: csv, jsonl"),
    language: str = typer.Option(None, "--lang", "-l", help="Фільтр за мовою (uk, en, ...)"),
    status_filter: str = typer.Option("verified", "--status", "-s", help="Фільтр за статусом"),
):
    """Експортувати верифіковані документи"""
    from harvester.db.connection import Database

    async def _export():
        settings = get_settings()
        db = Database(settings.db_path)
        await db.initialize()

        try:
            query = "SELECT * FROM documents WHERE status = ?"
            params = [status_filter]

            if language:
                query += " AND language = ?"
                params.append(language)

            rows = await db.fetchall(query, tuple(params))

            if format == "csv":
                with open(output, "w", newline="", encoding="utf-8") as f:
                    if rows:
                        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
                        writer.writeheader()
                        for row in rows:
                            writer.writerow(dict(row))
            elif format == "jsonl":
                with open(output, "w", encoding="utf-8") as f:
                    for row in rows:
                        f.write(json.dumps(dict(row), ensure_ascii=False) + "\n")

            rprint(f"[green]Експортовано {len(rows)} документів у {output}[/green]")
        finally:
            await db.close()

    asyncio.run(_export())


@app.command()
def doctor():
    """Самодіагностика системи"""
    from harvester.db.connection import Database
    from harvester.db.migrations import get_current_version

    async def _doctor():
        console.print("[cyan]Перевірка системи...[/cyan]")

        settings = get_settings()

        if not settings.db_path.exists():
            rprint("[yellow]База даних не знайдена — буде створена при першому запуску[/yellow]")
        else:
            db = Database(settings.db_path)
            await db.initialize()

            try:
                version = await get_current_version(db)
                rprint(f"[green]✓ База даних: версія {version}[/green]")

                integrity = await db.fetchone("PRAGMA integrity_check")
                if integrity and integrity[0] == "ok":
                    rprint("[green]✓ Цілісність БД: OK[/green]")
                else:
                    rprint("[red]✗ Цілісність БД: ПОМИЛКА[/red]")
            finally:
                await db.close()

        if settings.contact.email == "you@example.org":
            rprint("[yellow]⚠ Контактний email не налаштований[/yellow]")
        else:
            rprint(f"[green]✓ Контактний email: {settings.contact.email}[/green]")

        rprint("[green]✓ Конфігурація завантажена[/green]")

    asyncio.run(_doctor())


@app.command()
def init_db():
    """Ініціалізувати базу даних"""
    from harvester.db.connection import Database
    from harvester.db.migrations import ensure_schema

    async def _init():
        settings = get_settings()
        db = Database(settings.db_path)
        await db.initialize()

        try:
            await ensure_schema(db)
            rprint("[green]✓ База даних ініціалізована[/green]")
        finally:
            await db.close()

    asyncio.run(_init())


@app.command()
def events(
    limit: int = typer.Option(30, "--limit", "-n", help="Кількість подій"),
    level: str = typer.Option(None, "--level", "-l", help="Фільтр: WARN, ERROR"),
):
    """Показати останні системні події (WARN/ERROR)"""
    from harvester.db.connection import Database
    from harvester.db.repositories import SystemEventsRepository

    async def _events():
        settings = get_settings()
        db = Database(settings.db_path)
        await db.initialize()

        try:
            repo = SystemEventsRepository(db)
            rows = await repo.get_recent(limit * 3)

            if level:
                rows = [r for r in rows if r["level"] == level.upper()]
            rows = rows[:limit]

            table = Table(title="Останні події")
            table.add_column("Час", style="dim")
            table.add_column("Рівень")
            table.add_column("Компонент", style="cyan")
            table.add_column("Повідомлення")

            for row in rows:
                lvl = row["level"]
                lvl_style = {"ERROR": "red", "WARN": "yellow", "CRITICAL": "bold red"}.get(lvl, "white")
                table.add_row(
                    row["ts"][:19],
                    f"[{lvl_style}]{lvl}[/{lvl_style}]",
                    row["component"],
                    row["message"][:80],
                )

            console.print(table)
        finally:
            await db.close()

    asyncio.run(_events())


@app.command()
def queries(
    top: int = typer.Option(20, "--top", "-n", help="Кількість запитів"),
):
    """Показати ефективність пошукових запитів"""
    from harvester.db.connection import Database
    from harvester.db.repositories import SearchQueriesRepository

    async def _queries():
        settings = get_settings()
        db = Database(settings.db_path)
        await db.initialize()

        try:
            repo = SearchQueriesRepository(db)
            rows = await repo.get_top(top)

            table = Table(title=f"Топ-{top} пошукових запитів за yield")
            table.add_column("Запит", max_width=50)
            table.add_column("Регіон")
            table.add_column("Запусків", justify="right")
            table.add_column("Нових", justify="right", style="green")
            table.add_column("Статус")

            for row in rows:
                table.add_row(
                    row["text"][:50],
                    row["region"],
                    str(row["runs"]),
                    str(row["results_yield"]),
                    row["status"],
                )

            console.print(table)
        finally:
            await db.close()

    asyncio.run(_queries())


@app.command()
def vacuum():
    """Оптимізувати базу даних"""
    from harvester.db.connection import Database

    async def _vacuum():
        settings = get_settings()
        db = Database(settings.db_path)
        await db.initialize()

        try:
            await db.execute("VACUUM")
            rprint("[green]✓ База даних оптимізована[/green]")
        finally:
            await db.close()

    asyncio.run(_vacuum())


if __name__ == "__main__":
    app()
