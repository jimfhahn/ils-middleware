"""New Record in Symphony"""
import json
from airflow.providers.http.operators.http import SimpleHttpOperator


def NewMARCtoSymphony(**kwargs) -> SimpleHttpOperator:
    """Creates a new record in Symphony and returns the new CatKey"""
    app_id = kwargs.get("app_id")
    client_id = kwargs.get("client_id")
    conn_id = kwargs.get("conn_id")
    dag = kwargs.get("dag")
    marc_json = kwargs.get("marc_json")
    library_key = kwargs.get("library_key")
    session_token = kwargs.get("session_token")

    payload = {
        "@resource": "/catalog/bib",
        "catalogFormat": {"@resource": "/policy/catalogFormat", "@key": "MARC"},
        "shadowed": False,
        "bib": marc_json,
        "callList": [
            {
                "@resource": "/catalog/call",
                "callNumber": "AUTO",
                "classification": {"@resource": "/policy/classification", "@key": "LC"},
                "library": {"@resource": "/policy/library", "@key": f"{library_key}"},
            }
        ],
    }

    return SimpleHttpOperator(
        task_id="post_new_symphony",
        http_conn_id=conn_id,
        endpoint="catalog/bib",
        data=json.dumps(payload),
        headers={
            "Content-Type": "application/vnd.sirsidynix.roa.resource.v2+json",
            "Accept": "application/vnd.sirsidynix.roa.resource.v2+json",
            "sd-originating-app-id": app_id,
            "x-sirs-clientID": client_id,
            "x-sirs-sessionToken": session_token,
        },
        response_filter=lambda response: response.json().get("@key"),
        dag=dag,
    )
