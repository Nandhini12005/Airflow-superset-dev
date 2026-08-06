 from airflow.decorators import dag, task
from datetime import datetime

@dag(
    dag_id="Hello_world",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["hello", "demo"],
)
def hello_world():

    @task
    def print_hello():
        print("Hai")

    @task
    def print_airflow():
        print("Airflow")

    @task
    def print_done():
        print("Done")

    # Task Flow
    hello = print_hello()
    airflow = print_airflow()
    done = print_done()

    hello >> airflow >> done


hello_world()