"""Request function for Symphony Web Services"""

from airflow.providers.http.operators.http import SimpleHttpOperator


def SymphonyRequest(**kwargs) -> SimpleHttpOperator:
    app_id = kwargs.get("app_id")
    client_id = kwargs.get("client_id")
    conn_id = kwargs.get("conn_id")
    dag = kwargs.get("dag")
    data = kwargs.get("data")
    endpoint = kwargs.get("endpoint")
    override_headers = kwargs.get("headers", {})
    response_filter = kwargs.get("filter")
    session_token = kwargs.get("session_token")
    task_id = kwargs.get("task_id")

    headers = {
        "Content-Type": "application/vnd.sirsidynix.roa.resource.v2+json",
        "Accept": "application/vnd.sirsidynix.roa.resource.v2+json",
        "sd-originating-app-id": app_id,
        "x-sirs-clientID": client_id,
    }

    if session_token:
        headers["x-sirs-sessionToken"] = session_token

    # Incoming headers either override existing or add new header
    for key, value in override_headers.items():
        headers[key] = value

    return SimpleHttpOperator(
        task_id=task_id,
        http_conn_id=conn_id,
        endpoint=endpoint,
        data=data,
        headers=headers,
        response_filter=response_filter,
        dag=dag,
    )
