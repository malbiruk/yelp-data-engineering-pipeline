"""
this module describes tables in yelp_db
"""

from sqlalchemy import Boolean, Column, ForeignKey, Integer, String, Time
from yelp_db.yelp_db.connect import Base


class Business(Base):
    __tablename__ = "business"
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False)
    website = Column(String, nullable=True)
    phone_number = Column(String, nullable=True)
    address = Column(String, nullable=True)
    price = Column(String, nullable=True)
    health_score = Column(String, nullable=True)


class OpenHours(Base):
    __tablename__ = "open_hours"
    id = Column(Integer, primary_key=True, autoincrement=True)
    business_id = Column(Integer, ForeignKey("business.id"), nullable=False)
    open_time = Column(Time, nullable=False)
    close_time = Column(Time, nullable=False)
    weekday_id = Column(Integer, ForeignKey("weekday.id"), nullable=False)


class Weekday(Base):
    __tablename__ = "weekday"
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False)


class FoodCategory(Base):
    __tablename__ = "food_category"
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False)


class SearchTerm(Base):
    __tablename__ = "search_term"
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False)


class Highlight(Base):
    __tablename__ = "highlight"
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False)


class Amenity(Base):
    __tablename__ = "amenity"
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False)


class BusinessFoodCategory(Base):
    __tablename__ = "business_food_category"
    id = Column(Integer, primary_key=True, autoincrement=True)
    business_id = Column(Integer, ForeignKey("business.id"), nullable=False)
    food_category_id = Column(Integer, ForeignKey("food_category.id"), nullable=False)


class BusinessSearchTerm(Base):
    __tablename__ = "business_search_term"
    id = Column(Integer, primary_key=True, autoincrement=True)
    business_id = Column(Integer, ForeignKey("business.id"), nullable=False)
    search_term_id = Column(Integer, ForeignKey("search_term.id"), nullable=False)


class BusinessHighlight(Base):
    __tablename__ = "business_highlight"
    id = Column(Integer, primary_key=True, autoincrement=True)
    business_id = Column(Integer, ForeignKey("business.id"), nullable=False)
    highlight_id = Column(Integer, ForeignKey("highlight.id"), nullable=False)


class BusinessAmenity(Base):
    __tablename__ = "business_amenity"
    id = Column(Integer, primary_key=True, autoincrement=True)
    business_id = Column(Integer, ForeignKey("business.id"), nullable=False)
    amenity_id = Column(Integer, ForeignKey("amenity.id"), nullable=False)
    is_available = Column(Boolean, nullable=False)
