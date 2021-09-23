import logging
import json
from urllib.parse import urlparse
from os import path, getenv

from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.contrib.hooks.aws_lambda_hook import AwsLambdaHook


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


def Rdf2Marc(**kwargs) -> BashOperator:
    """Runs rdf2marc on a BF Instance URL"""
    instance_uri = kwargs.get("instance_uri")
    instance_path = urlparse(instance_uri).path
    instance_id = path.split(instance_path)[-1]

    rdf2marc_lambda = getenv('RDF2MARC_LAMBDA')
    s3_bucket = getenv('MARC_S3_BUCKET')
    s3_record_path = f"airflow/{instance_id}/record"
    marc_path = f"{s3_record_path}.mar"
    marc_text_path = f"{s3_record_path}.txt"
    marc_err_path = f"{s3_record_path}.err"

    lambda_hook = AwsLambdaHook(
        rdf2marc_lambda,
        log_type='None',
        qualifier='$LATEST',
        invocation_type='RequestResponse',
        config=None,
        aws_conn_id='aws_lambda_connection'
    )

    params = {
        'instance_uri': instance_uri,
        'bucket': s3_bucket,
        'marc_path': marc_path,
        'marc_txt_path': marc_text_path,
        'error_path': marc_err_path
    }

    # TODO: Determine what should be returned/saved from result
    result = lambda_hook.invoke_lambda(payload=json.dumps(params))
    logging.info(f"RESULT = {result}")

    return "success"
