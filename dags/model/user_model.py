from sqlalchemy import Column, Integer, String, create_engine
from sqlalchemy.orm import DeclarativeBase

DATABASE_URL = "postgresql://airflow:airflow@postgres:5432/warehouse"


def get_db_engine():
    return create_engine(DATABASE_URL)


class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "users"

    user_id = Column(Integer, primary_key=True)
    name = Column(String(100))
    email = Column(String(100))
    city = Column(String(100))