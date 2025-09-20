from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def check_pandas():
    try:
        import pandas as pd
        print("Pandas is installed. Version:", pd.__version__)
    except ImportError:
        print("Error: Pandas is not installed.")

with DAG(
    dag_id="check_pandas_dag",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:
    
    check_pandas_task = PythonOperator(
        task_id="check_pandas",
        python_callable=check_pandas,
    )
