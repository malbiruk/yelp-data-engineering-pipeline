import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd
from fastapi import Depends, FastAPI
from rich.logging import RichHandler
from sqlalchemy import and_, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from yelp_db.yelp_db.connect import db
from yelp_db.yelp_db.model import (Business, BusinessFoodCategory,
                                   FoodCategory, OpenHours, Weekday)

FORMAT = "%(message)s"
logging.basicConfig(
    level="INFO", format=FORMAT, datefmt="[%X]", handlers=[RichHandler()],
)
logger = logging.getLogger(__name__)

app = FastAPI()
LA_TZ = ZoneInfo("America/Los_Angeles")
WEEKDAY_ORDER = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]


async def get_db():
    await db.connect()
    try:
        yield db.session
    finally:
        await db.disconnect()

db_dependency = Depends(get_db)


async def select_to_df(stmt, session: AsyncSession) -> pd.DataFrame:
    """ Execute statement and return pandas dataframe """
    try:
        res = await session.execute(stmt)
        result = list(res)
        return pd.DataFrame.from_records(result, columns=res.keys())
    except Exception as e:
        logger.error(e)
        await session.rollback()


@app.get("/restaurants/category/{category}")
async def get_restaurants_by_category(
    category: int | str,
    page: int = 1,
    page_size: int = 10,
    session: AsyncSession = db_dependency,
):
    """
    Get restaurants by food category with pagination.
    Accepts both category strings and ids.
    """
    if category.isdigit():
        category_id = int(category)
    else:
        category_query = select(FoodCategory.id).where(FoodCategory.name.ilike(category))
        category_result = await session.execute(category_query)
        category_id = category_result.scalar()
        if not category_id:
            return {"error": "Category not found"}

    total_count_query = (
        select(func.count()).select_from(Business)
        .join(BusinessFoodCategory, Business.id == BusinessFoodCategory.business_id)
        .where(BusinessFoodCategory.food_category_id == category_id))

    total_count_result = await session.execute(total_count_query)
    total_count = total_count_result.scalar()

    offset = (page - 1) * page_size

    query = (
        select(Business)
        .join(BusinessFoodCategory, Business.id == BusinessFoodCategory.business_id)
        .where(BusinessFoodCategory.food_category_id == category_id)
        .limit(page_size)
        .offset(offset)
    )
    data = await select_to_df(query, session)
    return {
        "page": page,
        "page_size": page_size,
        "total_results": total_count,
        "businesses": data.to_dict(orient="records"),
    }


@app.get("/restaurants/day/{weekday}")
async def get_restaurants_by_day(
    weekday: str | int,
    page: int = 1,
    page_size: int = 10,
    session: AsyncSession = db_dependency,
):
    """
    Get restaurants open on a specific day with pagination.
    Accepts weekday number id or three-letter code (e.g. Mon, Tue etc.)
    """

    if weekday.isdigit():
        weekday_id = int(weekday)
    else:
        weekday_query = select(Weekday.id).where(Weekday.name.ilike(weekday))
        weekday_result = await session.execute(weekday_query)
        weekday_id = weekday_result.scalar()

        if not weekday_id:
            return {"error": "Invalid weekday"}

    total_count_query = (
        select(func.count())
        .select_from(Business)
        .join(OpenHours, Business.id == OpenHours.business_id)
        .where(OpenHours.weekday_id == weekday_id)
    )
    total_count_result = await session.execute(total_count_query)
    total_count = total_count_result.scalar()

    offset = (page - 1) * page_size

    query = (
        select(Business)
        .join(OpenHours, Business.id == OpenHours.business_id)
        .where(OpenHours.weekday_id == weekday_id)
        .limit(page_size)
        .offset(offset)
    )

    data = await select_to_df(query, session)

    return {
        "weekday": weekday,
        "page": page,
        "page_size": page_size,
        "total_results": total_count,
        "restaurants": data.to_dict(orient="records"),
    }


async def get_current_weekday_ids(session: AsyncSession) -> tuple[int]:
    """
    Returns the correct current and previous weekday IDs based on database weekday names.
    """
    result = await session.execute(select(Weekday.name, Weekday.id))
    weekday_records = result.fetchall()
    weekday_mapping = dict(weekday_records)
    today_name = datetime.now(LA_TZ).strftime("%a")

    today_id = weekday_mapping.get(today_name)
    if today_id is None:
        raise ValueError(f"Weekday {today_name} not found in the database!")

    sorted_weekdays = sorted(weekday_mapping.keys(), key=lambda x: WEEKDAY_ORDER.index(x))
    today_index = sorted_weekdays.index(today_name)
    previous_name = sorted_weekdays[today_index - 1]
    previous_id = weekday_mapping[previous_name]

    return today_id, previous_id


@app.get("/restaurants/now")
async def get_restaurants_open_now(
    session: AsyncSession = db_dependency,
    page: int = 1,
    page_size: int = 10,
):
    """
    Get restaurants currently opened and time until close with pagination.
    """
    now = datetime.now(LA_TZ).time()
    current_weekday, previous_weekday = await get_current_weekday_ids(session)

    open_now_filter = or_(
        and_(  # normal case
            OpenHours.weekday_id == current_weekday,
            OpenHours.open_time <= now,
            OpenHours.close_time > now,
        ),
        and_(  # overnight case, now is after open time
            OpenHours.weekday_id == current_weekday,
            OpenHours.open_time <= now,
            OpenHours.close_time < OpenHours.open_time,
        ),
        and_(  # overnight case, now is before close time of previous weekday
            OpenHours.weekday_id == previous_weekday,
            OpenHours.open_time > OpenHours.close_time,
            OpenHours.close_time > now,
        ),
    )

    total_count_query = select(func.count()).select_from(
        select(Business.id)
        .join(OpenHours, Business.id == OpenHours.business_id)
        .where(open_now_filter),
    )
    total_count_result = await session.execute(total_count_query)
    total_count = total_count_result.scalar()

    offset = (page - 1) * page_size
    query = (
        select(Business, OpenHours.close_time)
        .join(OpenHours, Business.id == OpenHours.business_id)
        .where(open_now_filter)
        .limit(page_size)
        .offset(offset)
    )

    data = await select_to_df(query, session)

    if not data.empty:
        today = datetime.now(LA_TZ).date()

        def calculate_time_until_close(close_time):
            """Adjusts close time if it's overnight"""
            close_datetime = datetime.combine(today, close_time, LA_TZ)

            if close_time < now:
                close_datetime += timedelta(days=1)

            return str(close_datetime - datetime.now(LA_TZ))

        data["time_until_close"] = data["close_time"].apply(calculate_time_until_close)

    return {
        "page": page,
        "page_size": page_size,
        "total_results": total_count,
        "restaurants": data.to_dict(orient="records"),
    }
