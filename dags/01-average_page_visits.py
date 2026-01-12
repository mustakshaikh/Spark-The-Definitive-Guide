import os
import json
import random
import pendulum

# In Airflow 3.0, these imports move to airflow.sdk
from airflow.sdk import dag, task, get_current_context

@dag(
    dag_id="average_page_visits",
    start_date=pendulum.datetime(2026, 1, 10, tz="UTC"),
    schedule="* * * * *",  # CHANGED: schedule_interval -> schedule
    catchup=False,
    description=""
)
def average_page_visits():

    def get_data_path():
        context = get_current_context()
        # In Airflow 3.0, 'logical_date' is preferred over 'execution_date'
        logical_date = context["logical_date"]
        file_date = logical_date.strftime("%Y-%m-%d_%H%M")
        return f"/tmp/page_visits/{file_date}.json"

    @task
    def produce_page_visits_data():
        page_visits = [
            {"id": 1, "name": "Cozy Apartment", "price": 120, "page_visits": random.randint(0, 50)},
            {"id": 2, "name": "Luxury Condo", "price": 300, "page_visits": random.randint(0, 50)},
            {"id": 3, "name": "Modern Studio", "price": 180, "page_visits": random.randint(0, 50)},
            {"id": 4, "name": "Charming Loft", "price": 150, "page_visits": random.randint(0, 50)},
            {"id": 5, "name": "Spacious Villa", "price": 400, "page_visits": random.randint(0, 50)},
        ]
        file_path = get_data_path()

        directory = os.path.dirname(file_path)
        if not os.path.exists(directory):
            os.makedirs(directory)

        with open(file_path, "w") as f:
            json.dump(page_visits, f)

        print(f"Written to file: {file_path}")

    @task
    def process_page_visits_data():
        file_path = get_data_path()

        if not os.path.exists(file_path):
            print(f"File not found: {file_path}")
            return

        with open(file_path, "r") as f:
            page_visits = json.load(f)

        total_visits = sum(page_visit["page_visits"] for page_visit in page_visits)
        average_visits = total_visits / len(page_visits)
        
        print(f"Average number of page visits: {average_visits}")

    # Set dependency
    produce_page_visits_data() >> process_page_visits_data()

# Instantiate the DAG
demo_dag=average_page_visits()

if __name__ == "__main__":
    # This runs the DAG locally so you can see the output in the terminal
    demo_dag.test()