import logging

from airflow.operators.python import PythonOperator


def UpdateIdentifier(**kwargs) -> PythonOperator:
    """Add Identifier to Sinopia."""
    task_instance = kwargs.get("task_instance")
    instance = task_instance.xcom_pull(
        task_ids="process_symphony.download_symphony_marc"
    )

    return sinopia_update(instance["id"])


def sinopia_update(instance_id):
    """Stub for updating Sinopia RDF resource with identifier."""
    logging.info(f"Ends updating Sinopia {instance_id}")
