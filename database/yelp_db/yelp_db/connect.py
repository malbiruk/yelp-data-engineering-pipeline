import os
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy.orm import declarative_base
from yelp_db.base.connect import Database

dotenv_path = Path(__file__).parent.parent.parent /  ".env"

load_dotenv(dotenv_path)

db = Database(
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT"),
    db_name=os.getenv("DB_DB"),
)

Base = declarative_base()
