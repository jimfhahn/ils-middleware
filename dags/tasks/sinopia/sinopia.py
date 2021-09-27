import logging

from airflow.operators.python import PythonOperator


def UpdateIdentifier(**kwargs) -> PythonOperator:
    """Add Identifier to Sinopia."""
    return PythonOperator(
        task_id="sinopia-id-update",
        python_callable=sinopia_update,
        op_kwargs={"urls": kwargs.get("urls", []), "identifer": kwargs.get("id")},
    )


def sinopia_update(**kwargs):
    """Stub for updating Sinopia RDF resource with identifier."""
    urls = kwargs.get("urls")
    identifier = kwargs.get("identifier")
    logging.info(f"Starts updating Sinopia {len(urls)} resources")
    for url in urls:
        logging.info(f"Would PUT to Sinopia API {identifier} for {url}")
    logging.info(f"Ends updating Sinopia {identifier}")
