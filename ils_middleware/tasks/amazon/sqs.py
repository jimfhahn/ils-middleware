"""Custom Operator using AWS SQSSensor."""
import logging
import json

from airflow.models import Variable
from airflow.providers.amazon.aws.sensors.sqs import SQSSensor

logger = logging.getLogger(__name__)


# Should return aws_sqs_sensor operator
# https://airflow.apache.org/docs/apache-airflow/1.10.12/_api/airflow/contrib/sensors/aws_sqs_sensor/index.html
def SubscribeOperator(**kwargs) -> SQSSensor:
    """Subscribe to a topic to filter SQS messages."""
    queue_name = kwargs.get("queue", "")
    # Defaults to Sinopia dev environment
    sinopia_env = kwargs.get("sinopia_env", "dev")
    aws_sqs_url = Variable.get(f"SQS_{sinopia_env.upper()}")
    return SQSSensor(
        aws_conn_id=f"aws_sqs_{sinopia_env}",
        sqs_queue=f"{aws_sqs_url}{queue_name}",
        task_id="sqs-sensor",
        dag=kwargs.get("dag"),
        max_messages=1,
    )


def parse_messages(**kwargs) -> str:
    """Parses SQS Message Body into constituent part."""
    task_instance = kwargs["task_instance"]
    raw_sqs_message = task_instance.xcom_pull(key="messages", task_ids=["sqs-sensor"])[
        0
    ]
    message_body = json.loads(raw_sqs_message[0].get("Body"))
    task_instance.xcom_push(key="email", value=message_body["user"]["email"])
    task_instance.xcom_push(key="resource_uri", value=message_body["resource"]["uri"])
    return "completed_parse"
