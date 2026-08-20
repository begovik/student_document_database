from pathlib import Path

import structlog

from harvester.db.connection import Database

logger = structlog.get_logger()

MIGRATIONS_DIR = Path(__file__).parent / "migrations"


async def get_current_version(db: Database) -> int:
    try:
        row = await db.fetchone("PRAGMA user_version")
        return row[0] if row else 0
    except Exception:
        return 0


async def set_version(db: Database, version: int) -> None:
    await db.execute(f"PRAGMA user_version = {version}")


async def apply_migrations(db: Database) -> None:
    current_version = await get_current_version(db)
    logger.info("checking_migrations", current_version=current_version)

    if current_version == 0:
        schema_path = Path(__file__).parent / "schema.sql"
        if schema_path.exists():
            schema_sql = schema_path.read_text(encoding="utf-8")
            await db.executescript(schema_sql)
            await set_version(db, 1)
            logger.info("applied_initial_schema", version=1)
            current_version = 1
        else:
            logger.error("schema_file_not_found", path=str(schema_path))
            raise FileNotFoundError(f"Schema file not found: {schema_path}")

    if MIGRATIONS_DIR.exists():
        migration_files = sorted(MIGRATIONS_DIR.glob("*.sql"))
        for mf in migration_files:
            try:
                migration_version = int(mf.stem.split("_")[0])
            except (ValueError, IndexError):
                logger.warning("migration_bad_name", file=mf.name)
                continue

            if migration_version > current_version:
                logger.info("applying_migration", file=mf.name, version=migration_version)
                sql = mf.read_text(encoding="utf-8")
                await db.executescript(sql)
                await set_version(db, migration_version)
                current_version = migration_version
                logger.info("applied_migration", version=migration_version)

    final_version = await get_current_version(db)
    logger.info("migrations_complete", current_version=final_version)


async def ensure_schema(db: Database) -> None:
    await apply_migrations(db)
