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
from harvester.extract.cli import extract_app
from harvester.curator.cli import curator_app

app = typer.Typer(
    name="harvester",
    help="Harvester — безперервний збирач наукових PDF-джерел",
    add_completion=False,
)

console = Console()

# Реєстрація subcommand
app.add_typer(extract_app, name="extract")
app.add_typer(curator_app, name="curator")


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
        await db.initialize(sync_mirror=False)

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

            db_mode = "local (SQLite)"
            if db.mode == "remote":
                db_mode = "remote (PostgreSQL)"
            pending_outbox = await db.pending_outbox_count()

            table = Table(title="Стан Harvester")
            table.add_column("Параметр", style="cyan")
            table.add_column("Значення", style="green")

            table.add_row("База даних", db_mode)
            if pending_outbox:
                table.add_row("Outbox (очікує злиття)", str(pending_outbox))
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
    from harvester.db.failover import build_database
    from harvester.db.repositories import ChannelStatsRepository

    async def _stats():
        settings = get_settings()
        db = build_database(settings)
        await db.initialize(sync_mirror=False)

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
    from harvester.db.failover import build_database

    async def _export():
        settings = get_settings()
        db = build_database(settings)
        await db.initialize(sync_mirror=False)

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

    async def _doctor():
        console.print("[cyan]Перевірка системи...[/cyan]")

        settings = get_settings()
        db = build_database(settings)
        await db.initialize(sync_mirror=False)

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
        await db.initialize(sync_mirror=False)

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
    from harvester.db.postgres import PostgresDatabase

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
    Фільтрується: мова, тип документу, наявність title.
    """
    from harvester.db.failover import build_database
    from harvester.db.repositories import DocumentsRepository
    from harvester.classify.taxonomy import load_topics

    async def _find():
        settings = get_settings()
        db = build_database(settings)
        await db.initialize(sync_mirror=False)

        try:
            docs_repo = DocumentsRepository(db)

            # Спробувати знайти топік по назві
            topics = await load_topics(db)
            topic_code = None
            for t in topics:
                if topic.lower() in t["name_uk"].lower() or topic.lower() in t["name_en"].lower():
                    topic_code = t["code"]
                    rprint(f"[cyan]Знайдено топік: {t['name_uk']} ({t['code']})[/cyan]")
                    break

            where = ""
            params: list = []

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
