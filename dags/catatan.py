# import airflow
import os
import pendulum
import pandas as pd
from airflow import DAG
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from sqlalchemy import create_engine, text
from airflow.utils.task_group import TaskGroup

# ----------------- Helper -----------------
def get_engine():
    conn = BaseHook.get_connection("postgres_dna")
    conn_str = f"postgresql+psycopg2://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"
    return create_engine(conn_str)

# ----------------- Staging Loader -----------------
def get_list_process_data():
    input_path = Variable.get('input_path', default_var='/opt/airflow/dags/input')
    csv_mapping = Variable.get('csv_mapping', deserialize_json=True)
    folders = [d for d in os.listdir(input_path) if os.path.isdir(os.path.join(input_path, d))]

    if not folders:
        return []

    files, process_table = [], []
    for i in folders:
        base_path = f"{input_path}/{i}"
        for root, _, filenames in os.walk(base_path):
            for f in filenames:
                if f.endswith(".csv") and f.startswith(i):
                    files.append({
                        "file_path": os.path.join(root, f),
                        "batch_id": pendulum.from_format(i, "YYYYMMDD"),
                        "data_header": f.split('.')[1]
                    })
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
        return
    engine = get_engine()
    truncate_sql = ";\n".join([f"TRUNCATE TABLE stg.{tbl} RESTART IDENTITY CASCADE" for tbl in process_table]) + ";"
    with engine.begin() as con:
        con.execute(text(truncate_sql))

def load_file(**context):
    ti = context['ti']
    files = ti.xcom_pull(task_ids="extract_files", key="files")
    csv_mapping = Variable.get('csv_mapping', deserialize_json=True)
    engine = get_engine()

    for f in files:
        mapping = csv_mapping.get(f['data_header'])
        if not mapping:
            continue
        df = pd.read_csv(f["file_path"], delimiter=mapping['delimiter'])
        df.columns = df.columns.str.lower()
        df['batchid'] = pd.to_datetime(f['batch_id'])

        if f['data_header'] == 'sales':
            df['salesdate'] = df['salesdate'].astype(str).apply(lambda x: pendulum.from_format(x, "YYYYMMDD"))
            df['discount'] = df['discount'].fillna(0)
        elif f['data_header'] == 'products':
            df['price'] = df['price'].str.replace(',', '').astype(float)

        df.to_sql(mapping['table'], engine, schema="stg", if_exists='append', index=False, chunksize=1000, method="multi")

# ----------------- Dim & Fact Loader -----------------
def get_batches(**context):
    engine = get_engine()
    with engine.begin() as con:
        result = con.execute(text("SELECT DISTINCT batchid FROM stg.products ORDER BY batchid"))
        batches = [str(row[0]) for row in result]
    context['ti'].xcom_push(key="batches", value=batches)

def load_dim_product(batch_id, **context):
    engine = get_engine()
    with engine.begin() as con:
        con.execute(text("CALL dm.sp_load_dim_product(:batch_id)"), {"batch_id": batch_id})

def load_fact_sales(batch_id, **context):
    engine = get_engine()
    with engine.begin() as con:
        con.execute(text("""
            INSERT INTO dm.fact_sales (sales_id, product_sk, sales_date, qty, amount, batchid)
            SELECT s.salesid,
                   dp.sk_product,
                   s.salesdate,
                   s.qty,
                   s.qty * s.price,
                   s.batchid
            FROM stg.sales s
            JOIN dm.dim_product dp 
              ON dp.product_id = s.productid
             AND dp.is_current = true
            WHERE s.batchid = :batch_id
        """), {"batch_id": batch_id})

def build_batch_group(dag, batch_id):
    with TaskGroup(group_id=f"batch_{batch_id}", dag=dag) as batch_group:
        with TaskGroup(group_id="dim_group") as dim_group:
            dim_product = PythonOperator(
                task_id=f"dim_product_{batch_id}",
                python_callable=load_dim_product,
                op_kwargs={"batch_id": batch_id}
            )
        with TaskGroup(group_id="fact_group") as fact_group:
            fact_sales = PythonOperator(
                task_id=f"fact_sales_{batch_id}",
                python_callable=load_fact_sales,
                op_kwargs={"batch_id": batch_id}
            )
        dim_group >> fact_group
    return batch_group

# ----------------- DAG Definition -----------------
with DAG(
    "dag_staggdding",
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Jakarta"),
    schedule_interval=None,
    catchup=False
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    extract_files = PythonOperator(task_id="extract_files", python_callable=extract_meta, provide_context=True)
    clear_table = PythonOperator(task_id="clear_table", python_callable=truncate_table)
    load_files = PythonOperator(task_id="load_files", python_callable=load_file, provide_context=True)

    get_batch_task = PythonOperator(task_id="get_batches", python_callable=get_batches, provide_context=True)

    def create_groups(**context):
        ti = context['ti']
        batches = ti.xcom_pull(task_ids="get_batches", key="batches") or []
        for b in batches:
            batch_group = build_batch_group(dag, b)
            get_batch_task >> batch_group >> end

    create_batch_groups = PythonOperator(task_id="create_batch_groups", python_callable=create_groups, provide_context=True)

    # DAG Flow
    start >> extract_files >> clear_table >> load_files >> get_batch_task >> create_batch_groups >> end
