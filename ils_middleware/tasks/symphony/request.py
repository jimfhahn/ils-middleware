"""Request function for Symphony Web Services"""
import logging
import requests  # type: ignore

from airflow.hooks.base_hook import BaseHook

logger = logging.getLogger(__name__)


def SymphonyRequest(**kwargs) -> str:
    app_id = kwargs.get("app_id")
    client_id = kwargs.get("client_id")
    conn_id = kwargs.get("conn_id", "")
    data = kwargs.get("data")
    endpoint = kwargs.get("endpoint", "")
    override_headers = kwargs.get("headers", {})
    response_filter = kwargs.get("filter")
    session_token = kwargs.get("token")

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

    logger.info(f"Headers {headers}")
    # Generate Symphony URL based on the Airflow Connection and endpoint
    symphony_conn = BaseHook.get_connection(conn_id)

    symphony_uri = symphony_conn.host + endpoint

    symphony_result = requests.post(symphony_uri, data=data, headers=headers)
    if symphony_result.status_code > 399:
        msg = f"Symphony Web Service Call to {symphony_uri} Failed with {symphony_result.status_code}\n{symphony_result.text}"
        logger.error(msg)
        raise Exception(msg)

    logger.debug(
        f"Symphony Results symphony_result {symphony_result.status_code}\n{symphony_result.text}"
    )
    if response_filter:
        return response_filter(symphony_result)
    return symphony_result.text
