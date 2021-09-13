import logging
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.sqs import SQSSensor



# Should return aws_sqs_sensor operator
# https://airflow.apache.org/docs/apache-airflow/1.10.12/_api/airflow/contrib/sensors/aws_sqs_sensor/index.html
def SubscribeOperator(**kwargs) -> PythonOperator:
    """Subscribes to a topic to filter SQS messages"""
    topic = kwargs.get("topic", "")
    return SQSSensor(
       aws_conn_id='aws_conn_id',
       sqs_queue='https://sqs.us-west-2.amazonaws.com/418214828013/stanford',
       task_id="dev-sqs-stanford",
       # region_name="us-west-2"
       # profile_name="dev-jpnelson"
        # python_callable=subscribe,
        # sqs_queue='TestILSIntegration'
    )


def subscribe(topic: str):
    logging.info(f"subscribe to {topic} on AWS SNS")
