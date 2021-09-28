"""FOLIO Operators and Functions for Institutional DAGs."""
from airflow.providers.http.operators.http import SimpleHttpOperator


def FolioRequest(**kwargs) -> SimpleHttpOperator:
    tenant = kwargs.get("tenant")
    conn_id = kwargs.get("conn_id")
    dag = kwargs.get("dag")
    token = kwargs.get("x-okapi-token")
    method = kwargs.get("method")
    endpoint = kwargs.get("endpoint")
    data = kwargs.get("data")

    return SimpleHttpOperator(
        task_id="folio_request",
        http_conn_id=conn_id,
        method=method,
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
            "x-okapi-tenant": tenant,
            "x-okapi-token": token,
        },
        data=data,
        endpoint=endpoint,
        dag=dag,
        response_check=lambda response: response.json.get({}),
    )
