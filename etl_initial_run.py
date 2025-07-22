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
    dag_id='etl_initial_dag',
    default_args=default_args,
    start_date=datetime(2025, 7, 6),
    schedule_interval=None,
    catchup=False
) as dag:
    
    create_tables_op = PythonOperator(
        task_id='create_tables',
        python_callable=create_tables_task
    )

    fetch_comments_op = PythonOperator(
        task_id='fetch_comments',
        python_callable=fetch_comments_task,
        provide_context=True
    )

    apply_valid_comments_op = PythonOperator(
        task_id='apply_valid_comments',
        python_callable=apply_valid_comments_task,
        provide_context=True
    )

    load_comments_op = PythonOperator(
        task_id='load_comments',
        python_callable=load_comments_task,
        provide_context=True
    )

    fetch_posts_op = PythonOperator(
        task_id='fetch_posts',
        python_callable=fetch_posts_task,
        provide_context=True
    )

    apply_valid_posts_op = PythonOperator(
        task_id='apply_valid_posts',
        python_callable=apply_valid_posts_task,
        provide_context=True
    )

    load_posts_op = PythonOperator(
        task_id='load_posts',
        python_callable=load_posts_task,
        provide_context=True
    )

    # DAG execution graph
    create_tables_op >> fetch_comments_op >> apply_valid_comments_op >> load_comments_op
    create_tables_op >> fetch_posts_op >> apply_valid_posts_op >> load_posts_op