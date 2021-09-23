"""Custom Operator using AWS SQSSensor."""
from airflow.models import Variable
from airflow.providers.amazon.aws.sensors.sqs import SQSSensor


# Should return aws_sqs_sensor operator
# https://airflow.apache.org/docs/apache-airflow/1.10.12/_api/airflow/contrib/sensors/aws_sqs_sensor/index.html
def SubscribeOperator(**kwargs) -> SQSSensor:
    """Subscribe to a topic to filter SQS messages."""
    queue_name = kwargs.get("queue", "")
    # Defaults to Sinopia dev environment
    sinopia_env = kwargs.get("sinopia_env", "dev").upper()
    aws_sqs_url = Variable.get(f"SQS_{sinopia_env}")
    return SQSSensor(
        aws_conn_id=f"aws_sqs_{sinopia_env}",
        sqs_queue=f"{aws_sqs_url}{queue_name}",
        task_id="sqs-sensor",
        dag=kwargs.get("dag"),
    )
