import os
import pendulum
import pandas as pd
from airflow import DAG
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine, text
from airflow.operators.empty import EmptyOperator


with DAG(
    dag_id="dag_naufal_download_data_zip",
    start_date=pendulum.datetime(2025, 1, 1, tz=pendulum.timezone("Asia/Jakarta")),  # ✅ pakai pendulum
    schedule_interval=None,
    catchup=False
) as dag:
    pass