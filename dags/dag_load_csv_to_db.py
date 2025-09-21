# import airflow
import os
import pendulum
import pandas as pd
from airflow import DAG
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine, text
from airflow.operators.empty import EmptyOperator

def get_list_process_data():
    input_path = Variable.get('input_path', default_var='/opt/airflow/dags/input')
    csv_mapping = Variable.get('csv_mapping', deserialize_json=True)
    folders = [
        d for d in os.listdir(input_path)
        if os.path.isdir(os.path.join(input_path, d))
    ]

    if not folders:
        return []

    files = []
    process_table = []
    for i in folders:
        base_path = f"{input_path}/{i}"
        for root, _, filenames in os.walk(base_path):
            for f in filenames:
                if f.endswith(".csv") and f.startswith(i):
                    files.append(
                        {
                            "file_path": os.path.join(root, f),
                            "batch_id": pendulum.from_format(i, "YYYYMMDD"),
                            "data_header": f.split('.')[1]
                        }
                    )
                    mapping = csv_mapping.get(f.split('.')[1])
                    if mapping:
                        process_table.append(mapping['table'])

    Variable.set('process_table', list(set(process_table)), serialize_json=True)
    return files


def extract_meta(**context):
    files = get_list_process_data()
    context['ti'].xcom_push(key="files", value=files)


def truncate_table():
    process_table = Variable.get('process_table', deserialize_json=True)
    if not process_table:
        print("No tables to truncate")
        return

    conn = BaseHook.get_connection("postgres_dna")
    conn_str = f"postgresql+psycopg2://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"
    engine = create_engine(conn_str)

    truncate_sql = ";\n".join(
        [f"TRUNCATE TABLE stg.{tbl} RESTART IDENTITY CASCADE" for tbl in process_table]
    ) + ";"

    with engine.begin() as connection:  # auto commit/rollback
        connection.execute(text(truncate_sql))

    print(f"Truncated tables: {', '.join(process_table)}")


def load_file(**context):
    ti = context['ti']
    files = ti.xcom_pull(task_ids="extract_files", key="files")
    csv_mapping = Variable.get('csv_mapping', deserialize_json=True)
    conn = BaseHook.get_connection("postgres_dna")
    conn_str = f"postgresql+psycopg2://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"
    engine = create_engine(conn_str)

    for f in files:
        data_header = f['data_header']
        mapping = csv_mapping.get(data_header)
        if not mapping:
            continue

        print("Loading file:", f["file_path"])
        df = pd.read_csv(f["file_path"], delimiter=mapping['delimiter'])
        df.columns = df.columns.str.lower()
        df['batchid'] = pd.to_datetime(f['batch_id'])
        if data_header == 'sales':
            df['salesdate'] = df['salesdate'].astype(str).apply(
                lambda x: pendulum.from_format(x, "YYYYMMDD")
            )
            df['discount'] = df['discount'].fillna(0)
        elif data_header == 'products':
            df['price'] = df['price'].str.replace(',', '').astype(float)
        print(f"Inserting CSV ==> {mapping['table']}")
        df.to_sql(
            mapping['table'],
            engine,
            schema="stg",  
            if_exists='append',
            index=False,
            chunksize=1000,
            method="multi"
        )


with DAG(
    "dag_stagging",
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Jakarta"),
    schedule_interval=None,
    catchup=False
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    extract_files = PythonOperator(
        task_id="extract_files",
        python_callable=extract_meta,
        provide_context=True
    )

    clear_table = PythonOperator(
        task_id="clear_table",
        python_callable=truncate_table,
    )

    load_files = PythonOperator(
        task_id="load_files",
        python_callable=load_file,
        provide_context=True
    )

    
    
    start>>extract_files >> clear_table >> load_files>>end
