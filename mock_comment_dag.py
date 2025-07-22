from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow_func import *

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='mock_comment_dag',
    default_args=default_args,
    start_date=datetime(2025, 7, 16),
    schedule_interval="*/1 * * * *",
    catchup=False
) as dag:

    mock_comments_op = PythonOperator(
        task_id='generate_mock_comments',
        python_callable=generate_mock_comments_task,
        provide_context=True
    )

    apply_valid_mock_op = PythonOperator(
        task_id='apply_valid_mock_comments',
        python_callable=apply_valid_mock_comments_task,
        provide_context=True
    )

    load_comments_op = PythonOperator(
        task_id='load_mock_comments',
        python_callable=load_mock_comments_task,
        provide_context=True
    )

    apply_scd2_op = PythonOperator(
        task_id='apply_scd2',
        python_callable=apply_scd2_task,
        provide_context=True
    )

    mock_comments_op >> apply_valid_mock_op >> load_comments_op >> apply_scd2_op