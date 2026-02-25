from tortoise import Tortoise
import logging

from database.repositories.companyRepository import CompanyRepository

logger = logging.getLogger(__name__)


async def init_db():
    await Tortoise.init(
        db_url="sqlite://db.sqlite3",
        modules={"models": ["database.models"]},
        _create_db=True,
    )
    await Tortoise.generate_schemas()

    # Ensure companies data exists: if no Company records, bulk import from JSON
    try:
        existing = await CompanyRepository.fetch_all_companies()
        if not existing or len(existing) == 0:
            logger.info("No companies found in DB — importing from companies_list.json")
            try:
                inserted = await CompanyRepository.bulk_import_from_json()
                logger.info(f"Imported {inserted} companies into the database")
            except Exception as e:
                logger.exception("Failed to bulk import companies: %s", e)
    except Exception as e:
        logger.exception("Error checking companies table: %s", e)


async def close_db():
    await Tortoise.close_connections()
