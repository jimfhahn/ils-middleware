from datetime import datetime, timedelta

from aws_sns import SubscribeOperator
from sinopia import UpdateIdentifier
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator



default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "provider": None,
}

with DAG(
    "symphony",
    default_args=default_args,
    description="Stanford Symphony DAG",
    schedule_interval=timedelta(minutes=5),
    start_date=datetime(2021, 8, 24),
    tags=["symphony"],
    catchup=False,
) as dag:
    # Monitors SNS for Cornell topic
    listen_sns = SubscribeOperator(topic="{{dag_run.conf.get('name')}}")

    urls = ["http://sinopia-example.io/{{dag_run.conf.get('name')}}/01",
            "http://sinopia-example.io/{{dag_run.conf.get('name')}}/02",
            "http://sinopia-example.io/{{dag_run.conf.get('name')}}/03"]

    # Retrive MARC from Sinopia API


    # Updates Sinopia URLS with HRID
    update_sinopia =  UpdateIdentifier(urls=urls)

listen_sns >> update_sinopia
