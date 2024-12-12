from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.bash_operator import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


from datetime import datetime
import csv

from train import main as main_revenue_model
from train_units import main as main_units_model


def check_row_count():

    pg_hook = PostgresHook(postgres_conn_id='postgres')
    row_count = pg_hook.get_first("SELECT COUNT(*) FROM salesStreamed")[0]
    is_sufficient = row_count >= 50000
    return is_sufficient


def drop_duplicates():

    pg_hook = PostgresHook(postgres_conn_id='postgres')

    pg_hook.run("""
        CREATE TABLE union_table AS
        SELECT * FROM Sales
        UNION
        SELECT * FROM salesStreamed
        """)
    
    pg_hook.run("""
        CREATE TABLE rows_not_in_a AS
        SELECT * FROM union_table
        EXCEPT
        SELECT * FROM Sales
        """)
    
    insert_query = """
        INSERT INTO Sales
        SELECT * FROM rows_not_in_a
        """
    pg_hook.run(insert_query)

    pg_hook.run("DROP TABLE IF EXISTS union_table")
    pg_hook.run("DROP TABLE IF EXISTS rows_not_in_a")
    pg_hook.run("DELETE FROM salesStreamed")


# def download_data():

#     pg_hook = PostgresHook(postgres_conn_id='postgres')
#     output_file = '/tmp/sales_data.csv'

#     pg_hook.copy_expert(
#             f"COPY (SELECT * FROM \"Sales\") TO '{output_file}' WITH CSV HEADER",
#             None
#     )
def download_data():

    pg_hook = PostgresHook(postgres_conn_id='postgres')
    output_file = '/tmp/sales_data.csv'

    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute("SELECT * FROM Sales")
    rows = cursor.fetchall()

    with open(output_file, mode='w', newline='') as file:
        writer = csv.writer(file)
        
        columns = [desc[0] for desc in cursor.description]
        writer.writerow(columns)
        writer.writerows(rows)

    cursor.close()
    connection.close()


with DAG(
    'data_pipeline',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args={
        'owner': 'airflow',
        'retries': 1,
    }
) as dag:
    
    row_count_check = ShortCircuitOperator(
        task_id='check_table_row_count',
        python_callable=check_row_count,
    )
    
    drop_duplicates_task = PythonOperator(
        task_id='drop_duplicates',
        python_callable=drop_duplicates
    )

    download_data_task = PythonOperator(
        task_id='download_data',
        python_callable=download_data,
    )

    model_revenue_task = SparkSubmitOperator(
        task_id="model_revenue",
        application="./train.py",
        conn_id="spark_conn"
    )

    # model_revenue_task = PythonOperator(
    #     task_id="model_revenue",
    #     python_callable=main_revenue_model
    # )

    model_units_task = SparkSubmitOperator(
        task_id="model_units",
        application="./train_units.py",
        conn_id="spark_conn"
    )

    # model_units_task = PythonOperator(
    #     task_id="model_units",
    #     python_callable=main_units_model
    # )

    model_tarball_task = BashOperator(
        task_id="model_tarball",
        bash_command='tar -czvf revenue_model.tar.gz -C /tmp model_revenue'
    )

    model_tarball2_task = BashOperator(
        task_id="model_tarball2",
        bash_command='tar -czvf units_model.tar.gz -C /tmp units_model'
    )

    cleanup_task = BashOperator(
        task_id='cleanup',
        bash_command='rm -f /tmp/sales_data.csv'
    )
    

    row_count_check >> drop_duplicates_task >> download_data_task >> model_revenue_task >> [model_tarball_task, model_tarball2_task] >> cleanup_task