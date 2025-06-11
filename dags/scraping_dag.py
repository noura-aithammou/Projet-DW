from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    'owner': 'noura',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'scraping_reviews_dag',
    default_args=default_args,
    description='Scrape Google Maps reviews and insert into PostgreSQL',
    schedule_interval='@daily',  
    start_date=days_ago(1),
    catchup=False,
)


run_scraping_script = BashOperator(
    task_id='run_scraping_script',
    bash_command='/home/noura/venv/bin/python /home/noura/airflow/scripts/script1.py',
    dag=dag,
)

insert_data_to_postgres = BashOperator(
    task_id='insert_data_to_postgres',
    bash_command='/home/noura/venv/bin/python /home/noura/airflow/scripts/insert_into_postgres.py',
    dag=dag,
)

run_scraping_script >> insert_data_to_postgres