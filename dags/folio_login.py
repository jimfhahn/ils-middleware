"""FOLIO Operators and Functions for Institutional DAGs."""
import logging
import json

# from airflow.operators.bash import BashOperator
# from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator

def FolioLogin(**kwargs) -> SimpleHttpOperator:
    tenant = kwargs.get("tenant")
    okapi_url = kwargs.get("okapi_url")
    conn_id = kwargs.get("conn_id")
    dag = kwargs.get("dag")
    username = kwargs.get("username")
    password = kwargs.get("password")

    return SimpleHttpOperator(
        task_id="folio_login",
        http_conn_id=conn_id,
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
            "x-okapi-tenant": tenant
        },
        endpoint="authn/login",
        response_filter=lambda response: response.json().get("x-okapi-token"),
        dag=dag,
        data=json.dumps({"username": username, "password": password}),
    )
