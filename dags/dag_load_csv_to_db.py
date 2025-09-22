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


def get_engine():
    conn = BaseHook.get_connection("postgres_dna")
    conn_str = f"postgresql+psycopg2://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"
    return create_engine(conn_str)

def get_list_process_data(ti):
# 1. Get last insert date from XCom (from previous task)
    last_date_str = ti.xcom_pull(task_ids='get_start_date', key='run_date')
    if last_date_str:
        last_date = pendulum.parse(last_date_str)
    else:
        last_date = pendulum.datetime(1900, 1, 1)

    input_path = Variable.get('input_path', default_var='/opt/airflow/dags/input')
    csv_mapping = Variable.get('csv_mapping', deserialize_json=True)

    # 2. List folders
    folders = [
        d for d in os.listdir(input_path)
        if os.path.isdir(os.path.join(input_path, d))
    ]

    # 3. Filter folders by date >= last insert_date
    folders = [
        f for f in folders
        if pendulum.from_format(f, "YYYYMMDD") > last_date
    ]

    # 4. Sort ascending
    folders.sort(key=lambda x: pendulum.from_format(x, "YYYYMMDD"))

    if not folders or len(folders)<1:
        raise Exception('File is empty')

    files = []
    process_table = []

    base_path = os.path.join(input_path, folders[0])
    for root, _, filenames in os.walk(base_path):
        for f in filenames:
            if f.endswith(".csv") and f.startswith(folders[0]):
                files.append({
                            "file_path": os.path.join(root, f),
                            "batch_id": pendulum.from_format(folders[0], "YYYYMMDD"),
                            "data_header": f.split('.')[1]
                        }
                    )
                mapping = csv_mapping.get(f.split('.')[1])
                if mapping:
                    process_table.append(mapping['table'])

    Variable.set('process_table', list(set(process_table)), serialize_json=True)
    ti.xcom_push(key="run_date", value=str(pendulum.from_format(folders[0], "YYYYMMDD").to_date_string() ))
    return files


def extract_meta(ti):
    files = get_list_process_data(ti)
    ti.xcom_push(key="files", value=files)


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
    files = ti.xcom_pull(task_ids="extract_meta_data", key="files")
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

# def get_batches(**context):
#     engine = get_engine()
#     with engine.begin() as con:
#         result = con.execute(text("SELECT DISTINCT batchid FROM stg.products ORDER BY batchid"))
#         batches = [str(row[0]) for row in result]
#     context['ti'].xcom_push(key="batches", value=batches)
    
# from sqlalchemy import text

def get_start_date(ti):
    engine = get_engine()  
    with engine.begin() as con:
        result = con.execute(text(
            "SELECT COALESCE(MAX(insert_date), '1900-01-01'::DATE) FROM dm.fact_sales;"
        ))
        run_date = result.scalar()
    ti.xcom_push(key="run_date", value=str(run_date))
    return run_date


def load_dim_product(ti):
    last_date_str = ti.xcom_pull(task_ids='extract_meta_data', key='run_date')
    engine = get_engine()
    with engine.begin() as con:
        con.execute(text("CALL dm.sp_load_dim_product(:batch_id)"), {"batch_id": 
            last_date_str
            })

def load_dim_customer(ti):
    last_date_str = ti.xcom_pull(task_ids='extract_meta_data', key='run_date')
    engine = get_engine()
    with engine.begin() as con:
        con.execute(text("CALL dm.sp_load_dim_customer(:batch_id)"), {"batch_id": last_date_str})

def load_dim_employee(ti):
    last_date_str = ti.xcom_pull(task_ids='extract_meta_data', key='run_date')
    engine = get_engine()
    with engine.begin() as con:
        con.execute(text("CALL dm.sp_load_dim_employee(:batch_id)"), {"batch_id": last_date_str})
        
def load_fact_sales(ti):
    last_date_str = ti.xcom_pull(task_ids='extract_meta_data', key='run_date')
    engine = get_engine()
    with engine.begin() as con:
        con.execute(text("call dm.sp_load_fact_sales_by_batch(:batch_id)"), {"batch_id": last_date_str})



def get_batch_id(**kwargs):
    run_date = kwargs.get('run_date')
    ti = kwargs['ti']
    if run_date is not None:
        print(f"Run date is: {run_date}")
        ti.xcom_push(key="run_date", value=str(run_date))
    else:
        
        run_date= get_start_date(ti)
        print(f"Run date is {run_date} or not provided")

with DAG(
    "dag_stagging",
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Jakarta"),
    schedule_interval=None,
    catchup=False
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    
    t_get_batch_id = PythonOperator(
        task_id="get_start_date",
        python_callable=get_batch_id,
        provide_context=True   
    )
    t_extract_files = PythonOperator(
        task_id="extract_meta_data",
        python_callable=extract_meta,
        provide_context=True
    )

    t_clear_table = PythonOperator(
        task_id="clear_table",
        python_callable=truncate_table,
    )

    t_load_files = PythonOperator(
        task_id="load_stagging",
        python_callable=load_file,
        provide_context=True
    )

    t_load_dim_customer = PythonOperator(
        task_id="load_dim_customer",
        python_callable=load_dim_customer,
        provide_context=True
    )
    t_load_dim_employee = PythonOperator(
        task_id="load_dim_employee",
        python_callable=load_dim_employee,
        provide_context=True
    )
    t_load_dim_product= PythonOperator(
        task_id="load_dim_product",
        python_callable=load_dim_product,
        provide_context=True
    )
    t_load_fact_sales= PythonOperator(
        task_id="load_fact_sales",
        python_callable=load_fact_sales,
        provide_context=True
    )
    
    start>>t_get_batch_id>>t_extract_files >> t_clear_table >> t_load_files >> [t_load_dim_customer,t_load_dim_employee,t_load_dim_product]>> t_load_fact_sales >>end
