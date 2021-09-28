"""FOLIO Operators and Functions for Institutional DAGs."""
from airflow.providers.http.operators.http import SimpleHttpOperator


def FolioRequest(**kwargs) -> SimpleHttpOperator:
    tenant = kwargs.get("tenant")
    conn_id = kwargs.get("conn_id")
    dag = kwargs.get("dag")
    token = kwargs.get("x-okapi-token")
    response_filter = kwargs.get("filter")
    method = kwargs.get("method")
    endpoint = kwargs.get("endpoint")
    data = kwargs.get("data")

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "x-okapi-tenant": tenant,
    }

    if token:
        headers["x-okapi-token"] = token

    return SimpleHttpOperator(
        task_id="folio_request",
        http_conn_id=conn_id,
        method=method,
        headers=headers,
        data=data,
        endpoint=endpoint,
        dag=dag,
        response_filter=response_filter,
    )
