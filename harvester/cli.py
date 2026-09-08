import asyncio
import csv
import json
import logging
import sqlite3
import sys
from contextlib import contextmanager, nullcontext
from datetime import datetime
from pathlib import Path
from typing import Iterator

import typer
from rich import print as rprint
from rich.console import Console
from rich.table import Table

from harvester.bibliography.cli import bibliography_app
from harvester.config import get_settings, load_config
from harvester.curator.cli import curator_app
from harvester.extract.cli import extract_app

app = typer.Typer(
    name="harvester",
    help="Harvester — безперервний збирач наукових PDF-джерел",
    add_completion=False,
)

console = Console()

# Реєстрація subcommand
app.add_typer(extract_app, name="extract")
app.add_typer(curator_app, name="curator")
app.add_typer(bibliography_app, name="bibliography")


def _format_bytes(size_bytes: int) -> str:
    if size_bytes >= 1024**3:
        return f"{size_bytes / 1024**3:.2f} ГБ"
    if size_bytes >= 1024**2:
        return f"{size_bytes / 1024**2:.1f} МБ"
    return f"{size_bytes / 1024:.1f} КБ"


def _open_sqlite(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(path), timeout=0.5, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout = 500")
    return conn


def _sqlite_fetchall(path: Path, sql: str, params: tuple | None = None) -> list[sqlite3.Row]:
    if not path.exists():
        return []
    conn = _open_sqlite(path)
    try:
        cursor = conn.execute(sql, params or ())
        return cursor.fetchall()
    finally:
        conn.close()


def _sqlite_fetchone(path: Path, sql: str, params: tuple | None = None):
    rows = _sqlite_fetchall(path, sql, params)
    return rows[0] if rows else None


def _fmt_time(ts: str | None) -> str:
    """'2026-09-08T09:24:04.253993' -> '09:24'."""
    if not ts:
        return "—"
    return str(ts)[11:16]


@contextmanager
def _quiet_stdout_logs() -> Iterator[None]:
    """Скеровує консольні лог-записи з stdout у stderr (для чистого JSON-виводу)."""
    import structlog

    root = logging.getLogger()
    moved: list[logging.StreamHandler] = []
    for handler in root.handlers:
        if isinstance(handler, logging.StreamHandler) and handler.stream is sys.stdout:
            handler.stream = sys.stderr
            moved.append(handler)
    try:
        if not structlog.is_configured():
            structlog.configure(logger_factory=structlog.PrintLoggerFactory(file=sys.stderr))
        yield
    finally:
        for handler in moved:
            handler.stream = sys.stdout
        if not structlog.is_configured():
            structlog.reset_defaults()


def _emit_json(payload: dict) -> None:
    """Друкує JSON у stdout, вимкнувши лог-шум із цього ж потоку."""
    with _quiet_stdout_logs():
        print(json.dumps(payload, indent=2, ensure_ascii=False))


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
    from harvester.db.failover import build_database
    from harvester.db.repositories import DocumentsRepository, SettingsRepository, TasksRepository

    async def _status():
        settings = get_settings()
        db = build_database(settings)
        await db.initialize(sync_mirror=False, read_only=True)

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
            classified_total, classified_docs = await docs_repo.count_classified()
            task_stats = await tasks_repo.count_by_status()
            task_by_type = await tasks_repo.count_by_type()

            db_mode = "local (SQLite)"
            if db.mode == "remote":
                db_mode = "remote (PostgreSQL)"
            pending_outbox = await db.pending_outbox_count()

            table = Table(title="Стан Harvester")
            table.add_column("Параметр", style="cyan")
            table.add_column("Значення", style="green")

            table.add_row("База даних", db_mode)
            if settings.database.remote_configured and db.remote is not None:
                table.add_row("Remote налаштовано", "так")
            if pending_outbox:
                table.add_row("Outbox (очікує злиття)", str(pending_outbox))
            table.add_row("Heartbeat", heartbeat_info)
            table.add_row("Воркери (живі)", workers_alive)
            table.add_row("Документи (всього)", str(sum(doc_stats.values())))
            for st, cnt in sorted(doc_stats.items(), key=lambda x: -x[1]):
                table.add_row(f"  · {st}", str(cnt))
            if lang_stats:
                table.add_row(
                    "Мови (verified)",
                    ", ".join(f"{k}:{v}" for k, v in sorted(lang_stats.items(), key=lambda x: -x[1])),
                )
            table.add_row("Класифікації (всього)", str(classified_total))
            table.add_row("  · унікальних документів", str(classified_docs))
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
def report(
    days: int = typer.Option(7, "--days", "-d", help="Період динаміки в днях (за замовчуванням 7)"),
    json_output: bool = typer.Option(False, "--json", help="Вивід у форматі JSON"),
):
    """Зведений звіт: стан системи + динаміка перевірки джерел"""
    from harvester.db.failover import build_database
    from harvester.db.repositories import (
        ChannelStatsRepository,
        DocumentsRepository,
        TasksRepository,
        VerifierRepository,
    )

    async def _report():
        settings = get_settings()
        db = build_database(settings)

        json_ctx = _quiet_stdout_logs() if json_output else nullcontext()

        with json_ctx:
            try:
                await db.initialize(sync_mirror=False, read_only=True)

                docs_repo = DocumentsRepository(db)
                tasks_repo = TasksRepository(db)
                chan_repo = ChannelStatsRepository(db)
                verif_repo = VerifierRepository(db)
    
                doc_stats = await docs_repo.count_by_status()
                lang_stats = await docs_repo.count_by_language()
                classified_total, classified_docs = await docs_repo.count_classified()
                task_stats = await tasks_repo.count_by_status()
                task_by_type = await tasks_repo.count_by_type()
                channel_stats = await chan_repo.get_summary(days * 24)
                verif_daily = await verif_repo.daily_summary(days)
                verif_total = await verif_repo.overall_summary()
                verified_total, verified_checked = await verif_repo.coverage()
    
                db_mode = "local (SQLite)"
                if db.mode == "remote":
                    db_mode = "remote (PostgreSQL)"
    
                if json_output:
                    print(
                        json.dumps(
                            {
                                "generated_at": datetime.utcnow().isoformat(),
                                "db_mode": db_mode,
                                "documents_by_status": doc_stats,
                                "languages": lang_stats,
                                "classification_total": classified_total,
                                "classification_docs": classified_docs,
                                "tasks_by_status": task_stats,
                                "tasks_by_type": task_by_type,
                                "channels": channel_stats,
                                "verifier": {
                                    "total": verif_total,
                                    "coverage": {
                                        "verified_total": verified_total,
                                        "verified_checked": verified_checked,
                                    },
                                    "daily": verif_daily,
                                },
                            },
                            indent=2,
                            ensure_ascii=False,
                        )
                    )
                    return
    
                table1 = Table(title="Загальний стан Harvester")
                table1.add_column("Параметр", style="cyan")
                table1.add_column("Значення", style="green")
                table1.add_row("База даних", db_mode)
                table1.add_row("Документи (всього)", str(sum(doc_stats.values())))
                for st, cnt in sorted(doc_stats.items(), key=lambda x: -x[1]):
                    table1.add_row(f"  · {st}", str(cnt))
                if lang_stats:
                    table1.add_row(
                        "Мови (verified)",
                        ", ".join(f"{k}:{v}" for k, v in sorted(lang_stats.items(), key=lambda x: -x[1])),
                    )
                table1.add_row("Класифікації (всього)", str(classified_total))
                table1.add_row("  · унікальних документів", str(classified_docs))
                table1.add_row("Завдання (pending)", str(task_stats.get("pending", 0)))
                table1.add_row("Завдання (running)", str(task_stats.get("running", 0)))
                for ttype, by_status in sorted(task_by_type.items()):
                    pending = by_status.get("pending", 0)
                    running = by_status.get("running", 0)
                    if pending or running:
                        table1.add_row(f"  · {ttype}", f"p:{pending} r:{running}")
    
                table2 = Table(title=f"Перевірка джерел (verifier, {days} днів)")
                table2.add_column("День", style="cyan", width=5)
                table2.add_column("Перев.", justify="right")
                table2.add_column("Pass", justify="right", style="green")
                table2.add_column("Fail", justify="right", style="red")
                table2.add_column("Err", justify="right")
                table2.add_column("LLM", justify="right", style="bright_green")
                table2.add_column("LLM+", justify="right", style="green")
                table2.add_column("LLM-", justify="right", style="red")
                table2.add_column("1-й LLM", style="yellow", width=5)
                table2.add_column("ост.", style="yellow", width=5)
    
                for r in verif_daily:
                    table2.add_row(
                        str(r["day"])[5:],
                        str(r["checked"] or 0),
                        str(r["passed"] or 0),
                        str(r["failed"] or 0),
                        str(r["errors"] or 0),
                        str(r["llm_calls"] or 0),
                        str(r["llm_pass"] or 0),
                        str(r["llm_fail"] or 0),
                        _fmt_time(r.get("first_llm")),
                        _fmt_time(r.get("last_llm")),
                    )
    
                table3 = Table(title="Підсумок verifier")
                table3.add_column("Параметр", style="cyan")
                table3.add_column("Значення", style="green")
                table3.add_row("Перевірок (всього)", str(verif_total.get("checked", 0) or 0))
                table3.add_row("  · pass", str(verif_total.get("passed", 0) or 0))
                table3.add_row("  · fail", str(verif_total.get("failed", 0) or 0))
                table3.add_row("  · error", str(verif_total.get("errors", 0) or 0))
                table3.add_row("LLM-викликів (всього)", str(verif_total.get("llm_calls", 0) or 0))
                table3.add_row("  · pass", str(verif_total.get("llm_pass", 0) or 0))
                table3.add_row("  · fail", str(verif_total.get("llm_fail", 0) or 0))
                coverage_pct = (verified_checked / verified_total * 100) if verified_total else 0.0
                table3.add_row("Охоплення verified-документів", f"{verified_checked} / {verified_total} ({coverage_pct:.1f}%)")
    
                if channel_stats:
                    chan_rows = "\n".join(
                        f"  · {s['channel']}: нових {s.get('items_new', 0)}, помилок {s.get('errors', 0)}"
                        for s in channel_stats[:6]
                    )
                    table1.add_row("Канали (топ)", chan_rows)
    
                console.print(table1)
                console.print("")
                console.print(table3)
                console.print("")
                console.print(table2)
            finally:
                await db.close()

    asyncio.run(_report())


@app.command()
def stats(
    period: str = typer.Option("24h", "--period", "-p", help="Період: 24h, 7d, 30d"),
    json_output: bool = typer.Option(False, "--json", help="Вивід у форматі JSON"),
):
    """Показати статистику по каналах"""
    from harvester.db.failover import build_database
    from harvester.db.repositories import ChannelStatsRepository

    async def _stats():
        settings = get_settings()
        db = build_database(settings)
        await db.initialize(sync_mirror=False, read_only=True)

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
                        str(stat["channel"]),
                        str(stat.get("requests", 0) or 0),
                        str(stat.get("ok", 0) or 0),
                        str(stat.get("errors", 0) or 0),
                        str(stat.get("items_found", 0) or 0),
                        str(stat.get("items_new", 0) or 0),
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
    from harvester.db.failover import build_database

    async def _export():
        settings = get_settings()
        db = build_database(settings)
        await db.initialize(sync_mirror=False, read_only=True)

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
    from harvester.db.failover import build_database
    from harvester.db.migrations import get_current_version
    from harvester.discovery.base import IMPLEMENTED_DISCOVERY_CHANNELS, PLANNED_DISCOVERY_CHANNELS

    async def _doctor():
        console.print("[cyan]Перевірка системи...[/cyan]")

        settings = get_settings()
        enabled_channels = {
            name
            for name in IMPLEMENTED_DISCOVERY_CHANNELS
            if getattr(settings.channels, name).enabled
        }
        unimplemented_channels = sorted(
            name
            for name in PLANNED_DISCOVERY_CHANNELS
            if getattr(settings.channels, name, None)
            and getattr(settings.channels, name).enabled
        )
        if not enabled_channels:
            rprint("[red]✗ Не увімкнено жодного реалізованого discovery-каналу[/red]")
        else:
            rprint(f"[green]✓ Discovery-канали: {', '.join(sorted(enabled_channels))}[/green]")
        if unimplemented_channels:
            rprint(
                "[yellow]⚠ Увімкнено канали без адаптера: "
                f"{', '.join(unimplemented_channels)}; вони не запускатимуться[/yellow]"
            )
        if settings.workers.scanner:
            rprint(
                "[yellow]⚠ workers.scanner > 0, але scanner-воркер ще не реалізований; "
                "значення буде проігноровано[/yellow]"
            )
        db = build_database(settings)
        await db.initialize(sync_mirror=False, read_only=True)

        try:
            version = await get_current_version(db)
            if db.mode == "remote":
                healthy = await db.remote_healthy()
                rprint(
                    f"[green]✓ База даних: PostgreSQL (remote), версія {version}, "
                    f"доступна: {'так' if healthy else 'ні'}[/green]"
                )
            else:
                rprint(f"[green]✓ База даних: SQLite (local), версія {version}[/green]")
            pending = await db.pending_outbox_count()
            if pending:
                rprint(f"[yellow]⚠ Outbox: {pending} операцій очікують злиття у remote[/yellow]")

            if db.local is not None:
                integrity = await db.local.fetchone("PRAGMA integrity_check")
                if integrity and integrity[0] == "ok":
                    rprint("[green]✓ Цілісність локальної БД: OK[/green]")

            mirror = await db.mirror_status()
            if mirror == "synced":
                rprint("[green]✓ Локальне дзеркало: синхронно[/green]")
            elif mirror == "drift":
                rprint("[yellow]⚠ Локальне дзеркало: розбіжність "
                       "(відновиться при наступному restore)[/yellow]")
            elif mirror.startswith("mismatch:"):
                tbl = mirror.split(":", 1)[1]
                rprint(f"[red]✗ Локальне дзеркало: розбіжність ({tbl}) — "
                       f"виконайте harvester db-resync[/red]")
            else:
                rprint("[dim]— Локальне дзеркало: не активне (local-режим)[/dim]")
        finally:
            await db.close()

        if settings.contact.email == "you@example.org":
            rprint("[yellow]⚠ Контактний email не налаштований[/yellow]")
        else:
            rprint(f"[green]✓ Контактний email: {settings.contact.email}[/green]")

        rprint("[green]✓ Конфігурація завантажена[/green]")

    asyncio.run(_doctor())


@app.command()
def db_status():
    """Показати стан підключення до БД (remote/local, outbox)"""
    from harvester.db.failover import build_database

    async def _db_status():
        settings = get_settings()
        db = build_database(settings)
        await db.initialize(sync_mirror=False, read_only=True)

        try:
            table = Table(title="Стан бази даних")
            table.add_column("Параметр", style="cyan")
            table.add_column("Значення", style="green")

            mode = "local (SQLite)"
            if db.mode == "remote":
                mode = "remote (PostgreSQL)"
            table.add_row("Активна БД", mode)

            cfg = settings.database
            table.add_row("Remote налаштовано", "так" if cfg.remote_configured else "ні")
            if cfg.remote_configured:
                healthy = await db.remote_healthy()
                table.add_row("Remote доступна", "так" if healthy else "ні")
                table.add_row("Хост", f"{cfg.host}:{cfg.port}/{cfg.name}")
            table.add_row("Локальна БД", str(db.db_path))

            mirror = await db.mirror_status()
            if mirror == "synced":
                table.add_row("Дзеркало (local SQLite)", "синхронно")
            elif mirror == "drift":
                table.add_row("Дзеркало (local SQLite)", "[yellow]розбіжність (буде відновлено)[/yellow]")
            elif mirror.startswith("mismatch:"):
                tbl = mirror.split(":", 1)[1]
                table.add_row(
                    "Дзеркало (local SQLite)",
                    f"[yellow]розбіжність ({tbl}), потрібен db-resync[/yellow]",
                )
            else:
                table.add_row("Дзеркало (local SQLite)", "—")

            pending = await db.pending_outbox_count()
            table.add_row("Outbox (очікує злиття)", str(pending))
            table.add_row("Restore-інтервал", f"{cfg.restore_probe_interval_s} с")

            console.print(table)
        finally:
            await db.close()

    asyncio.run(_db_status())


@app.command()
def db_size():
    """Показати розмір баз даних (локальна SQLite + віддалена PostgreSQL)"""
    from harvester.db.failover import build_database

    async def _db_size():
        settings = get_settings()
        db = build_database(settings)
        await db.initialize(sync_mirror=False, read_only=True)

        try:
            table = Table(title="Розмір баз даних")
            table.add_column("Параметр", style="cyan")
            table.add_column("Значення", style="green")

            # Локальна SQLite
            db_path = settings.db_path
            if db_path.exists():
                size_bytes = db_path.stat().st_size
                table.add_row("Локальна SQLite", str(db_path))
                table.add_row("  Розмір файлу", _format_bytes(size_bytes))

                try:
                    tables = _sqlite_fetchall(
                        db_path,
                        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'",
                    )
                    for tbl_row in tables:
                        tbl = tbl_row["name"]
                        try:
                            count_row = _sqlite_fetchone(db_path, f"SELECT COUNT(*) AS c FROM {tbl}")
                            count = count_row["c"] if count_row else 0
                            if count > 0:
                                table.add_row(f"  · {tbl}", f"{count:,} рядків")
                        except Exception:
                            pass
                except Exception:
                    pass
            else:
                table.add_row("Локальна SQLite", f"[dim]{db_path} (не знайдено)[/dim]")

            # Віддалена PostgreSQL
            if db.remote is not None and db.mode == "remote":
                try:
                    row = await db.remote.fetchone(
                        "SELECT pg_database_size(current_database()) AS size_bytes"
                    )
                    if row and row["size_bytes"]:
                        pg_bytes = row["size_bytes"]
                        table.add_row(
                            "Віддалена PostgreSQL",
                            f"{settings.database.host}:{settings.database.port}/{settings.database.name}",
                        )
                        table.add_row("  Розмір БД", _format_bytes(pg_bytes))

                    pg_tables = await db.remote.fetchall(
                        "SELECT relname, n_live_tup FROM pg_stat_user_tables "
                        "WHERE schemaname = 'public' ORDER BY n_live_tup DESC LIMIT 15"
                    )
                    for tbl_row in pg_tables:
                        tbl_name = tbl_row["relname"]
                        count = tbl_row["n_live_tup"]
                        if count:
                            table.add_row(f"  · {tbl_name}", f"{count:,} рядків")
                except Exception as e:
                    table.add_row("Віддалена PostgreSQL", f"[yellow]помилка: {str(e)[:80]}[/yellow]")
            elif db.remote is not None:
                table.add_row("Віддалена PostgreSQL", "[dim]недоступна (local-режим)[/dim]")
            else:
                table.add_row("Віддалена PostgreSQL", "[dim]не налаштована[/dim]")

            console.print(table)
        finally:
            await db.close()

    asyncio.run(_db_size())


@app.command()
def db_resync():
    """Відновити локальне дзеркало SQLite з віддаленої PostgreSQL"""
    from harvester.db.failover import build_database

    async def _resync():
        settings = get_settings()
        db = build_database(settings)
        await db.initialize(sync_mirror=False)

        try:
            if db.mode != "remote":
                rprint("[red]✗ Дзеркало актуальне лише у remote-режимі "
                       "(зараз: local)[/red]")
                raise typer.Exit(1)
            ok = await db._ensure_local_mirror(force=True)
            status = await db.mirror_status()
            if ok and status == "synced":
                rprint("[green]✓ Локальне дзеркало синхронізовано[/green]")
            else:
                rprint(f"[yellow]⚠ Стан дзеркала: {status}[/yellow]")
        finally:
            await db.close()

    asyncio.run(_resync())


@app.command()
def db_seed():
    """Однократно перенести локальну БД у віддалену PostgreSQL"""
    from harvester.db.failover import build_database

    async def _seed():
        settings = get_settings()
        cfg = settings.database

        if not cfg.remote_configured:
            rprint("[red]✗ Віддалена БД не налаштована (database.host / database.dsn)[/red]")
            raise typer.Exit(1)

        db = build_database(settings)
        await db.initialize(sync_mirror=False)

        if db.remote is None or not db._remote_ever_ok:
            rprint("[red]✗ Не вдалося підключитись до віддаленої БД[/red]")
            raise typer.Exit(1)

        try:
            total = await _seed_table(db.local, db.remote, "domains", "id")
            rprint(f"[green]✓ domains: {total}[/green]")
            total = await _seed_table(db.local, db.remote, "sources", "id")
            rprint(f"[green]✓ sources: {total}[/green]")
            total = await _seed_table(db.local, db.remote, "documents", "id")
            rprint(f"[green]✓ documents: {total}[/green]")
            total = await _seed_table(db.local, db.remote, "document_mirrors", "id")
            rprint(f"[green]✓ document_mirrors: {total}[/green]")
            total = await _seed_table(db.local, db.remote, "document_refs", "id")
            rprint(f"[green]✓ document_refs: {total}[/green]")
            total = await _seed_table(db.local, db.remote, "fetch_attempts", "id")
            rprint(f"[green]✓ fetch_attempts: {total}[/green]")
            total = await _seed_table(db.local, db.remote, "tasks", "id")
            rprint(f"[green]✓ tasks: {total}[/green]")
            total = await _seed_table(db.local, db.remote, "search_queries", "id")
            rprint(f"[green]✓ search_queries: {total}[/green]")
            total = await _seed_table(db.local, db.remote, "topics", "id")
            rprint(f"[green]✓ topics: {total}[/green]")
            total = await _seed_table(db.local, db.remote, "document_topics", None)
            rprint(f"[green]✓ document_topics: {total}[/green]")
            total = await _seed_table(db.local, db.remote, "blacklist", "id")
            rprint(f"[green]✓ blacklist: {total}[/green]")
            total = await _seed_table(db.local, db.remote, "channel_stats", "id")
            rprint(f"[green]✓ channel_stats: {total}[/green]")
            total = await _seed_table(db.local, db.remote, "system_events", "id")
            rprint(f"[green]✓ system_events: {total}[/green]")
            total = await _seed_table(db.local, db.remote, "settings", None)
            rprint(f"[green]✓ settings: {total}[/green]")

            await _fix_sequences(db.remote)
            rprint("[green]✓ Секвенції оновлено[/green]")
            rprint("[green]✓ Перенесення завершено[/green]")
        finally:
            await db.close()

    asyncio.run(_seed())


async def _seed_table(local_db, remote_db, table: str, seq_col: str | None) -> int:
    """Копіює таблицю з локальної SQLite у remote PostgreSQL (з id)."""
    cols_row = await local_db.fetchone(f"SELECT * FROM {table} LIMIT 1")
    if cols_row is None:
        return 0
    cols = list(dict(cols_row).keys())
    col_sql = ", ".join(f'"{c}"' for c in cols)
    placeholders = ", ".join("?" for _ in cols)
    insert_sql = (
        f'INSERT INTO {table} ({col_sql}) VALUES ({placeholders}) ON CONFLICT DO NOTHING'
    )

    total = 0
    offset = 0
    batch_size = 500
    while True:
        rows = await local_db.fetchall(
            f'SELECT * FROM {table} LIMIT {batch_size} OFFSET {offset}'
        )
        if not rows:
            break
        batch = []
        for r in rows:
            d = dict(r)
            batch.append(tuple(d[c] for c in cols))
        await remote_db.executemany(insert_sql, batch)
        total += len(batch)
        offset += len(batch)

    if seq_col and total:
        await remote_db.execute(
            f"SELECT setval(pg_get_serial_sequence('{table}', '{seq_col}'), "
            f"COALESCE((SELECT MAX({seq_col}) FROM {table}), 1))"
        )
    return total


async def _fix_sequences(remote_db) -> None:
    for table, col in (("documents", "id"), ("tasks", "id"), ("search_queries", "id")):
        await remote_db.execute(
            f"SELECT setval(pg_get_serial_sequence('{table}', '{col}'), "
            f"COALESCE((SELECT MAX({col}) FROM {table}), 1))"
        )


@app.command()
def init_db():
    """Ініціалізувати базу даних"""
    from harvester.db.failover import build_database
    from harvester.db.migrations import ensure_schema

    async def _init():
        settings = get_settings()
        db = build_database(settings)
        await db.initialize(sync_mirror=False)

        try:
            await ensure_schema(db)
            mode = "remote" if db.mode == "remote" else "local"
            rprint(f"[green]✓ База даних ініціалізована ({mode})[/green]")
        finally:
            await db.close()

    asyncio.run(_init())


@app.command()
def events(
    limit: int = typer.Option(30, "--limit", "-n", help="Кількість подій"),
    level: str = typer.Option(None, "--level", "-l", help="Фільтр: WARN, ERROR"),
):
    """Показати останні системні події (WARN/ERROR)"""
    from harvester.db.failover import build_database
    from harvester.db.repositories import SystemEventsRepository

    async def _events():
        settings = get_settings()
        db = build_database(settings)
        await db.initialize(sync_mirror=False)

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
    from harvester.db.failover import build_database
    from harvester.db.repositories import SearchQueriesRepository

    async def _queries():
        settings = get_settings()
        db = build_database(settings)
        await db.initialize(sync_mirror=False)

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
def add_queries(
    topic: str = typer.Option(
        ...,
        "--topic",
        "-t",
        help="Основна тема для пошуку (наприклад, 'технологія пошиття пальта')",
    ),
    count: int = typer.Option(
        100,
        "--count",
        "-n",
        min=1,
        max=2000,
        help="Кількість варіантів запитів для генерації",
    ),
    lang: str = typer.Option(
        "both",
        "--lang",
        "-l",
        help="Мова запитів: uk, en, both",
    ),
    priority: int = typer.Option(
        10,
        "--priority",
        "-p",
        min=1,
        max=100,
        help="Пріоритет запитів (вищий = раніше обробляється)",
    ),
    use_llm: bool = typer.Option(
        False,
        "--llm",
        help="Додатково згенерувати запити LLM-ом (Gemini 3.1/3.5 Flash Lite → Gemma fallback, GEMINI_API_KEY 1-3)",
    ),
):
    """Додати пошукові запити для нової теми

    Генерує варіації пошукових запитів з різними типами документів,
    мовами та модифікаторами для максимального покриття.
    """
    from harvester.db.failover import build_database
    from harvester.db.repositories import SearchQueriesRepository

    # Основи теми
    TOPICS_UK = [
        topic,
        f"проєктування {topic}",
        f"розробка {topic}",
        f"технологія {topic}",
        f"конструювання {topic}",
        f"моделювання {topic}",
        f"виготовлення {topic}",
        f"виробництво {topic}",
        f"організація виробництва {topic}",
        f"технічна підготовка виробництва {topic}",
        f"технологічний процес {topic}",
        f"технологічна документація {topic}",
        f"маршрутна карта {topic}",
        f"операційна карта {topic}",
        f"нормування {topic}",
        f"собівартість {topic}",
        f"економічна ефективність {topic}",
        f"оптимізація {topic}",
        f"удосконалення {topic}",
        f"автоматизація {topic}",
    ]

    TOPICS_EN = [
        topic,
        f"design of {topic}",
        f"development of {topic}",
        f"technology of {topic}",
        f"construction of {topic}",
        f"manufacturing of {topic}",
        f"production of {topic}",
        f"production organization of {topic}",
        f"technical preparation of {topic}",
        f"technological process of {topic}",
        f"technological documentation of {topic}",
        f"route sheet {topic}",
        f"operation sheet {topic}",
        f"cost estimation of {topic}",
        f"economic efficiency of {topic}",
        f"optimization of {topic}",
        f"improvement of {topic}",
        f"automation of {topic}",
    ]

    # Типи документів
    DOC_TYPES_UK = [
        "filetype:pdf",
        "підручник filetype:pdf",
        '"навчальний посібник" pdf',
        "методичні вказівки pdf",
        "методичні рекомендації pdf",
        "конспект лекцій filetype:pdf",
        "наукова стаття filetype:pdf",
        "монографія filetype:pdf",
        "дисертація filetype:pdf",
        "автореферат filetype:pdf",
        "практикум filetype:pdf",
        "лабораторний практикум pdf",
        "курсовий проєкт filetype:pdf",
        "дипломний проєкт filetype:pdf",
        "звіт filetype:pdf",
        "патент filetype:pdf",
        "ГОСТ filetype:pdf",
        "ДСТУ filetype:pdf",
        "технічні умови filetype:pdf",
        "інструкція filetype:pdf",
    ]

    DOC_TYPES_EN = [
        "filetype:pdf",
        "textbook filetype:pdf",
        '"lecture notes" pdf',
        "methodical guidelines pdf",
        "methodical recommendations pdf",
        "scientific article filetype:pdf",
        "monograph filetype:pdf",
        "thesis filetype:pdf",
        "dissertation filetype:pdf",
        "abstract filetype:pdf",
        "practical guide filetype:pdf",
        "coursework filetype:pdf",
        "report filetype:pdf",
        "patent filetype:pdf",
        "standard filetype:pdf",
        "technical specification filetype:pdf",
        "instruction filetype:pdf",
    ]

    # Додаткові модифікатори
    MODIFIERS_UK = [
        "",
        "сучасні методи",
        "інноваційні технології",
        "САПР",
        "комп'ютерне проєктування",
        "3D моделювання",
        "стандартизація",
        "якість продукції",
        "ефективність виробництва",
        "безвідходна технологія",
        "енергозбереження",
        "матеріалознавство",
        "обладнання",
        "інструмент",
        "фурнітура",
        "розкрій матеріалу",
        "лекала",
        "викрійки",
        "технологічна послідовність",
        "час виготовлення",
    ]

    MODIFIERS_EN = [
        "",
        "modern methods",
        "innovative technologies",
        "CAD",
        "computer aided design",
        "3D modeling",
        "standardization",
        "product quality",
        "production efficiency",
        "zero waste technology",
        "energy saving",
        "material science",
        "equipment",
        "tools",
        "accessories",
        "fabric cutting",
        "patterns",
        "templates",
        "technological sequence",
        "manufacturing time",
    ]

    async def _add_queries():
        settings = get_settings()
        db = build_database(settings)
        await db.initialize(sync_mirror=False)

        try:
            repo = SearchQueriesRepository(db)
            added = 0
            skipped = 0

            queries_to_add = []

            if lang in ("uk", "both"):
                for base in TOPICS_UK:
                    for doc_type in DOC_TYPES_UK:
                        for modifier in MODIFIERS_UK:
                            if modifier:
                                query_text = f"{base} {modifier} {doc_type}"
                            else:
                                query_text = f"{base} {doc_type}"
                            queries_to_add.append((query_text, "ua-uk", topic, priority))

                            if len(queries_to_add) >= count:
                                break
                        if len(queries_to_add) >= count:
                            break
                    if len(queries_to_add) >= count:
                        break

            if lang in ("en", "both") and len(queries_to_add) < count:
                for base in TOPICS_EN:
                    for doc_type in DOC_TYPES_EN:
                        for modifier in MODIFIERS_EN:
                            if modifier:
                                query_text = f"{base} {modifier} {doc_type}"
                            else:
                                query_text = f"{base} {doc_type}"
                            queries_to_add.append((query_text, "us-en", topic, priority))

                            if len(queries_to_add) >= count:
                                break
                        if len(queries_to_add) >= count:
                            break
                    if len(queries_to_add) >= count:
                        break

            # LLM-доповнення (якісні джерела з повним текстом) — Gemini 3.1/3.5 → Gemma
            if use_llm:
                try:
                    from harvester.discovery.querygen_llm import generate_queries_for_topic

                    existing_texts = [q[0] for q in queries_to_add[:5]]
                    llm_needed = max(5, min(count // 10, 10))
                    llm_qs = await generate_queries_for_topic(
                        topic, existing_queries=existing_texts, count=llm_needed
                    )
                    for q in llm_qs:
                        queries_to_add.append((q, "ua-uk", topic, priority + 5))
                    if llm_qs:
                        rprint(f"[green]✓ LLM згенеровано: {len(llm_qs)} (Gemini 3.1/3.5 → Gemma)[/green]")
                except Exception as e:  # noqa: BLE001
                    rprint(f"[yellow]⚠ LLM-генерація не вдалася: {e}[/yellow]")

            # Додаємо запити
            for query_text, region, topic_hint, prio in queries_to_add:
                qid = await repo.insert_if_new(query_text, region=region, topic_hint=topic_hint, priority=prio)
                if qid:
                    added += 1
                else:
                    skipped += 1

            rprint(f"[green]✓ Додано запитів: {added}[/green]")
            if skipped:
                rprint(f"[yellow]Пропущено (дублікати): {skipped}[/yellow]")
            rprint(f"[cyan]Всього згенеровано: {len(queries_to_add)}[/cyan]")

        finally:
            await db.close()

    asyncio.run(_add_queries())


@app.command()
def vacuum():
    """Оптимізувати базу даних"""
    from harvester.db.failover import build_database

    async def _vacuum():
        settings = get_settings()
        db = build_database(settings)
        await db.initialize(sync_mirror=False)

        try:
            if db.local is not None:
                await db.local.execute("VACUUM")
                rprint("[green]✓ Локальна база даних оптимізована[/green]")
            else:
                rprint("[red]✗ Локальна БД недоступна[/red]")
        finally:
            await db.close()

    asyncio.run(_vacuum())


@app.command()
def find(
    topic: str = typer.Option(
        ...,
        "--topic",
        "-t",
        help="Тематика/спеціальність для пошуку (наприклад, 'Підприємництво, торгівля та біржова діяльність' або код 076)",
    ),
    limit: int = typer.Option(
        30,
        "--limit",
        "-n",
        min=1,
        max=500,
        help="Максимальна кількість документів у каталозі",
    ),
    lang: str = typer.Option(
        None,
        "--lang",
        "-l",
        help="Фільтр за мовою (uk, en, ru, de, fr, ...)",
    ),
    doc_type: str = typer.Option(
        None,
        "--type",
        "-d",
        help="Фільтр за типом документу (article, book, thesis, dissertation, methodical, report, preprint, other)"
    )):
    """Знайти літературу в базі даних за тематикою

    Пошук документів відбувається в існуючій базі.
    Фільтрується: verified-статус, повний текстовий шар, мінімальна якість,
    мова, тип документу та наявність title.
    """
    from harvester.classify.taxonomy import load_disciplines
    from harvester.config import get_filter_rules
    from harvester.db.failover import build_database

    async def _find():
        settings = get_settings()
        db = build_database(settings)
        await db.initialize(sync_mirror=False, read_only=True)

        try:
            # Спробувати знайти топік по назві (широкі теми + дисципліни каталогу)
            topics = await load_disciplines(db)
            topic_code = None
            for t in topics:
                if topic.lower() in t["name_uk"].lower() or topic.lower() in t["name_en"].lower():
                    topic_code = t["code"]
                    rprint(f"[cyan]Знайдено топік: {t['name_uk']} ({t['code']})[/cyan]")
                    break

            rules = get_filter_rules("strict")
            where = """
                AND d.status = ?
                AND d.has_text_layer = 1
                AND d.page_count >= ?
                AND LOWER(COALESCE(d.language, '')) NOT IN ('', 'unknown', 'und')
            """
            params: list = ["verified", rules.min_page_count]

            if doc_type:
                where += " AND d.doc_type = ?"
                params.append(doc_type)

            if lang:
                where += " AND d.language = ?"
                params.append(lang)

            where += " AND d.title IS NOT NULL"
            where += " AND d.title != ''"

            if topic_code:
                where += " AND d.id IN (SELECT dt.document_id FROM document_topics dt JOIN topics t ON t.id = dt.topic_id WHERE t.code = ?)"
                params.append(topic_code)
            else:
                # Пошук по УДК якщо топік не знайдено
                query_text = f"%{topic}%"
                where += " AND (d.udc LIKE ? OR d.title LIKE ? OR d.authors LIKE ?)"
                params.extend([query_text, query_text, query_text])

            query = f"""
                SELECT d.id, d.title, d.authors, d.year, d.publisher, d.language,
                       d.doc_type, d.udc, d.doi, d.canonical_url, d.isbn,
                       t.name_uk as topic_name
                FROM documents d
                LEFT JOIN document_topics dt ON dt.document_id = d.id
                LEFT JOIN topics t ON t.id = dt.topic_id
                WHERE 1=1 {where}
                ORDER BY d.year DESC NULLS LAST, d.title
                LIMIT ?
            """
            params.append(limit)

            rows = await db.fetchall(query, tuple(params))

            if not rows:
                rprint(f"[yellow]У базі не знайдено документів для теми «{topic}»[/yellow]")
                return

            table = Table(title=f"Каталог літератури: {topic}")
            table.add_column("№", style="cyan", width=4)
            table.add_column("Назва", style="green")
            table.add_column("Автори", style="yellow")
            table.add_column("Рік", justify="right", style="magenta")
            table.add_column("Тип", style="blue")
            table.add_column("Тематика", style="cyan")

            for i, row in enumerate(rows, 1):
                title_display = row["title"] or "(без назви)"
                if len(title_display) > 50:
                    title_display = title_display[:47] + "..."
                authors_display = row["authors"] or "—"
                if len(authors_display) > 30:
                    authors_display = authors_display[:27] + "..."
                year_display = str(row["year"]) if row["year"] else "—"
                doc_type_display = row["doc_type"] or "other"
                topic_display = row["topic_name"] or "—"

                table.add_row(
                    str(i),
                    title_display,
                    authors_display,
                    year_display,
                    doc_type_display,
                    topic_display,
                )

            console.print(table)
            rprint(f"[green]Знайдено {len(rows)} документів[/green]")
        finally:
            await db.close()


if __name__ == "__main__":
    app()
