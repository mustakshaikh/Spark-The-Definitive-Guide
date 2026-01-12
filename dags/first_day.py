from airflow.sdk import dag, task
from datetime import datetime

@dag(
    schedule=None, 
    start_date=datetime(2023, 1, 1), 
    catchup=False
)
def simple_hello_world():
    
    @task
    def say_hello():
        print("Hello, Airflow 3.0!")

    # Call the task function to register it
    say_hello()

# Instantiate the DAG
demo_dag=simple_hello_world()

if __name__ == "__main__":
    # This runs the DAG locally so you can see the output in the terminal
    demo_dag.test()