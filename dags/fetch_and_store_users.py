from datetime import datetime

import requests
from airflow.sdk import dag, task
from sqlalchemy.orm import sessionmaker

from model.user_model import Base, User, get_db_engine


# ==========================
# Database Configuration
# ==========================

engine = get_db_engine()

Base.metadata.create_all(engine)

Session = sessionmaker(bind=engine)


@dag(
    dag_id="fetch_and_store_users",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["demo", "etl"],
)
def fetch_and_store_users():

    @task
    def fetch_users():
        pass

    @task
    def transform_users(users):
        pass

    @task
    def save_users(users):
        session = Session()
        pass

    @task
    def get_users():
        session = Session()
        pass

    @task
    def display_users(users):
        pass

    @task
    def get_total_users(users):
        pass


    # Task Flow
    fetch = fetch_users()
    transform = transform_users(fetch)
    save = save_users(transform)
    db_users = get_users()
    display = display_users(db_users)
    total = get_total_users(db_users)

    fetch >> transform >> save >> db_users >> display >> total


fetch_and_store_users()