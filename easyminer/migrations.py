import asyncio
import logging

from alembic import command
from alembic.config import Config

from easyminer.config import ROOT_DIR

logger = logging.getLogger(__name__)

# Track which databases have been migrated this session
_migrated_dbs: set[str] = set()


async def run_migrations(db_url: str) -> None:
    # Skip if already migrated this session
    if db_url in _migrated_dbs:
        return

    logger.info("Running database migrations")

    # Configure Alembic
    alembic_ini = ROOT_DIR / "alembic.ini"
    alembic_cfg = Config(str(alembic_ini))
    alembic_cfg.set_main_option("sqlalchemy.url", db_url)
    alembic_cfg.set_main_option("script_location", str(ROOT_DIR / "easyminer" / "alembic"))

    # Run migrations in thread pool (alembic is sync but uses async connections)
    await asyncio.to_thread(command.upgrade, alembic_cfg, "head")

    # Mark as migrated
    _migrated_dbs.add(db_url)
    logger.info("Database migrations completed")
