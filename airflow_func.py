from dagster import op
import config
from data_loader_airflow import (
    DataLoadManager, extract_comment_values, extract_post_values,
    insert_comments_query, insert_posts_query,
    comments_table_schema, posts_table_schema
)


def create_tables_task(**context):
    cfg = config.CONFIG
    loader = DataLoadManager(cfg["DRIVER"], cfg["SERVER"], cfg["DATABASE"])
    loader.drop_table_if_exists("stg_comments")
    loader.drop_table_if_exists("stg_posts")
    loader.create_table("stg_comments", comments_table_schema)
    loader.create_table("stg_posts", posts_table_schema)
    loader.close_connection()


def fetch_comments_task(**context):
    cfg = config.CONFIG
    loader = DataLoadManager(cfg["DRIVER"], cfg["SERVER"], cfg["DATABASE"])
    data = loader.fetch_data_from_api(config.COMMENTS_API_URL)
    fetched_time = loader.fetched_time
    loader.close_connection()
    context['ti'].xcom_push(key='comments_data', value={"data": data, "fetched_time": fetched_time})


def fetch_posts_task(**context):
    cfg = config.CONFIG
    loader = DataLoadManager(cfg["DRIVER"], cfg["SERVER"], cfg["DATABASE"])
    data = loader.fetch_data_from_api(config.POSTS_API_URL)
    fetched_time = loader.fetched_time
    loader.close_connection()
    context['ti'].xcom_push(key='posts_data', value={"data": data, "fetched_time": fetched_time})


def apply_valid_comments_task(**context):
    payload = context['ti'].xcom_pull(task_ids='fetch_comments', key='comments_data')
    cfg = config.CONFIG
    loader = DataLoadManager(cfg["DRIVER"], cfg["SERVER"], cfg["DATABASE"])
    result = loader.apply_valid_cols(payload["data"], payload["fetched_time"])
    loader.close_connection()
    context['ti'].xcom_push(key='valid_comments', value=result)


def apply_valid_posts_task(**context):
    payload = context['ti'].xcom_pull(task_ids='fetch_posts', key='posts_data')
    cfg = config.CONFIG
    loader = DataLoadManager(cfg["DRIVER"], cfg["SERVER"], cfg["DATABASE"])
    result = loader.apply_valid_cols(payload["data"], payload["fetched_time"])
    loader.close_connection()
    context['ti'].xcom_push(key='valid_posts', value=result)


def load_comments_task(**context):
    comments = context['ti'].xcom_pull(task_ids='apply_valid_comments', key='valid_comments')
    cfg = config.CONFIG
    loader = DataLoadManager(cfg["DRIVER"], cfg["SERVER"], cfg["DATABASE"])
    loader.load_data("stg_comments", comments, insert_comments_query, extract_comment_values)
    loader.close_connection()


def load_posts_task(**context):
    posts = context['ti'].xcom_pull(task_ids='apply_valid_posts', key='valid_posts')
    cfg = config.CONFIG
    loader = DataLoadManager(cfg["DRIVER"], cfg["SERVER"], cfg["DATABASE"])
    loader.load_data("stg_posts", posts, insert_posts_query, extract_post_values)
    loader.close_connection()


def generate_mock_comments_task(**context):
    cfg = config.CONFIG
    loader = DataLoadManager(cfg["DRIVER"], cfg["SERVER"], cfg["DATABASE"])
    max_comment_id = loader.get_max_id("stg_comments")
    max_post_id = loader.get_max_item_id("stg_comments")
    comments_data = []
    for _ in range(3):
        comments_data.append(loader.fetch_mock_comments_new(max_comment_id, max_post_id))
        max_comment_id += 1
        max_post_id += 1
        comments_data.append(loader.fetch_mock_comments_edited(max_comment_id))
    fetched_time = loader.fetched_time
    loader.close_connection()
    context['ti'].xcom_push(key='mock_comments_data', value={"data": comments_data, "fetched_time": fetched_time})


def apply_valid_mock_comments_task(**context):
    payload = context['ti'].xcom_pull(task_ids='generate_mock_comments', key='mock_comments_data')
    cfg = config.CONFIG
    loader = DataLoadManager(cfg["DRIVER"], cfg["SERVER"], cfg["DATABASE"])
    result = loader.apply_valid_cols(payload["data"], payload["fetched_time"])
    loader.close_connection()
    context['ti'].xcom_push(key='valid_mock_comments', value=result)


def load_mock_comments_task(**context):
    comments = context['ti'].xcom_pull(task_ids='apply_valid_mock_comments', key='valid_mock_comments')
    cfg = config.CONFIG
    loader = DataLoadManager(cfg["DRIVER"], cfg["SERVER"], cfg["DATABASE"])
    loader.load_data("stg_comments", comments, insert_comments_query, extract_comment_values)
    loader.close_connection()


def apply_scd2_task(**context):
    cfg = config.CONFIG
    loader = DataLoadManager(cfg["DRIVER"], cfg["SERVER"], cfg["DATABASE"])
    loader.apply_scd_2("stg_comments")
    loader.apply_scd_2("stg_posts")
    loader.close_connection()
    