import argparse
import asyncio
import json
import logging
import re
from datetime import datetime, time
from pathlib import Path
from typing import Any

from rich.logging import RichHandler
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from yelp_db.yelp_db.connect import db
from yelp_db.yelp_db.model import (Amenity, Business, BusinessAmenity,
                                   BusinessFoodCategory, BusinessHighlight,
                                   BusinessSearchTerm, FoodCategory, Highlight,
                                   OpenHours, SearchTerm, Weekday)

FORMAT = "%(message)s"
logging.basicConfig(
    level="INFO", format=FORMAT, datefmt="[%X]", handlers=[RichHandler()],
)
logger = logging.getLogger(__name__)


def load_data(file_path: Path) -> list:
    with file_path.open() as f:
        return [json.loads(line) for line in f]


async def get_or_create(
    session: AsyncSession,
    model: Any,
    **kwargs,
) -> Any:
    """Get an existing record or create a new one."""
    stmt = select(model).filter_by(**kwargs)
    result = await session.execute(stmt)
    instance = result.scalar_one_or_none()

    if instance:
        return instance

    instance = model(**kwargs)
    session.add(instance)
    await session.flush()
    return instance


def parse_time(time_str: str) -> time:
    """Parse time string in format '11:00 AM' or '11:30 PM' to datetime.time object."""
    try:
        return datetime.strptime(time_str.strip(), "%I:%M %p").time()
    except ValueError:
        return datetime.strptime(time_str.strip(), "%I %p").time()


def parse_hours(hours_str: str) -> list[tuple[time, time]]:
    """
    Parse hours string that might contain multiple time ranges.
    Handles special cases such as "Closed" and "Open 24 hours"
    Returns list of tuples (open_time, close_time)

    Examples:
    "11:00 AM - 3:00 PM" -> [(11:00, 15:00)]
    "11:00 AM - 3:00 PM4:30 PM - 10:00 PM" -> [(11:00, 15:00), (16:30, 22:00)]
    "Closed" -> []
    "Open 24 hours" -> [(00:00, 23:59:59)]
    """
    hours_str = hours_str.replace("(Next day)", "").strip()
    if hours_str == "Closed":
        return []
    if hours_str == "Open 24 hours":
        return [(time(0, 0), time(23, 59, 59))]

    time_ranges = re.findall(r"(\d{1,2}:\d{2} [AP]M) - (\d{1,2}:\d{2} [AP]M)", hours_str)

    if not time_ranges:
        raise ValueError(f"Could not parse time ranges from: {hours_str}")

    return [(parse_time(start), parse_time(end)) for start, end in time_ranges]


async def push_to_db(data: list[dict]):
    """Push Yelp business data to database."""
    try:
        await db.connect()

        for c, business_data in enumerate(data):
            logger.info("processing item %s/%s", c + 1, len(data))
            business = Business(
                name=business_data["name"],
                website=business_data.get("website"),
                phone_number=business_data.get("phone_number"),
                address=business_data.get("address"),
                price=business_data.get("price"),
                health_score=business_data.get("health_score"),
            )
            db.session.add(business)
            await db.session.flush()

            for hours in business_data.get("open_hours", []):
                weekday = await get_or_create(
                    db.session,
                    Weekday,
                    name=hours["weekday"],
                )

                time_ranges = parse_hours(hours["open_hours"])

                for open_time, close_time in time_ranges:
                    open_hours = OpenHours(
                        business_id=business.id,
                        weekday_id=weekday.id,
                        open_time=open_time,
                        close_time=close_time,
                    )
                    db.session.add(open_hours)

            for category in business_data.get("food_category", []):
                food_cat = await get_or_create(
                    db.session,
                    FoodCategory,
                    name=category,
                )

                business_category = BusinessFoodCategory(
                    business_id=business.id,
                    food_category_id=food_cat.id,
                )
                db.session.add(business_category)

            for term in business_data.get("related_search_terms", []):
                search_term = await get_or_create(
                    db.session,
                    SearchTerm,
                    name=term,
                )

                business_term = BusinessSearchTerm(
                    business_id=business.id,
                    search_term_id=search_term.id,
                )
                db.session.add(business_term)

            for highlight in business_data.get("highlights", []):
                highlight_obj = await get_or_create(
                    db.session,
                    Highlight,
                    name=highlight,
                )

                business_highlight = BusinessHighlight(
                    business_id=business.id,
                    highlight_id=highlight_obj.id,
                )
                db.session.add(business_highlight)

            for amenity_data in business_data.get("amenities", []):
                amenity = await get_or_create(
                    db.session,
                    Amenity,
                    name=amenity_data["amenity"],
                )

                business_amenity = BusinessAmenity(
                    business_id=business.id,
                    amenity_id=amenity.id,
                    is_available=amenity_data["is_available"],
                )
                db.session.add(business_amenity)

            await db.session.flush()

        await db.session.commit()
        logger.info("Successfully uploaded business data to database")

    except Exception:
        await db.session.rollback()
        logger.error("Failed to upload data to database", exc_info=True)

    finally:
        await db.disconnect()


def main(file: Path):
    data = load_data(file)
    asyncio.run(push_to_db(data))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="parse and upload web scraping output to database")

    parser.add_argument(
        "-f", "--file", type=Path, default=Path("../web_scraper/results.ndjson"),
        help="path to file with web-scraping results")
    args = parser.parse_args()
    main(**vars(args))
