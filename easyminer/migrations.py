import logging

from alembic import command
from alembic.config import Config

from easyminer.config import ROOT_DIR, settings

logger = logging.getLogger(__name__)

_migrated_dbs: set[str] = set()


def run_migrations(db_url: str) -> None:
    if settings.skip_migrations:
        logger.warning("Migrations are disabled via SKIP_MIGRATIONS setting")
        return

    if db_url in _migrated_dbs:
        logger.debug("Database already migrated this session, skipping")
        return

    logger.info("Running database migrations")

    try:
        alembic_ini = ROOT_DIR / "alembic.ini"
        if not alembic_ini.exists():
            raise FileNotFoundError(f"alembic.ini not found at {alembic_ini}")

        logger.debug(f"Loading alembic config from {alembic_ini}")
        alembic_cfg = Config(str(alembic_ini))
        alembic_cfg.set_main_option("sqlalchemy.url", db_url)
        alembic_cfg.set_main_option("script_location", str(ROOT_DIR / "easyminer" / "alembic"))

        # Suppress alembic's own logging to stdout
        alembic_cfg.set_main_option("output_encoding", "utf-8")
        alembic_cfg.attributes["configure_logger"] = False

        logger.debug("Running alembic upgrade")
        command.upgrade(alembic_cfg, "head")
        logger.info("Alembic upgrade completed")

        _migrated_dbs.add(db_url)
        logger.info("Database migrations completed successfully")
    except Exception as e:
        logger.error(f"Database migration failed: {e}", exc_info=True)
        raise
