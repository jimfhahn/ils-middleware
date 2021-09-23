"""Symphony Login."""
import json

from airflow.providers.http.operators.http import SimpleHttpOperator


def SymphonyLogin(**kwargs) -> SimpleHttpOperator:
    app_id = kwargs.get("app_id")
    client_id = kwargs.get("client_id")
    conn_id = kwargs.get("conn_id")
    dag = kwargs.get("dag")
    login = kwargs.get("login")
    password = kwargs.get("password")

    return SimpleHttpOperator(
        task_id="symphony_login",
        http_conn_id=conn_id,
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
            "SD-originating-app-id": app_id,
            "x-sirs-clientID": client_id,
        },
        endpoint="user/staff/login",
        response_filter=lambda response: response.json().get("sessionToken"),
        dag=dag,
        data=json.dumps({"login": login, "password": password}),
    )
