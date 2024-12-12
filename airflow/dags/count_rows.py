from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import datetime

def count_rows_in_table():
    
    pg_hook = PostgresHook(postgres_conn_id='postgres')
    
    sql = "SELECT COUNT(*) FROM salesStreamed;"
    result = pg_hook.get_first(sql)
    
    print(f"Row count in the table: {result[0]}")
    return result[0]

with DAG(
    'postgres_row_count_dag',
    description='A simple DAG to count rows in a Postgres table',
    schedule_interval=None,
    start_date=datetime(2024, 12, 9),
    catchup=False
) as dag:
    
    count_rows_task = PythonOperator(
        task_id='count_rows_in_table_task',
        python_callable=count_rows_in_table
    )
    
    count_rows_task
