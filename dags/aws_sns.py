import logging
from airflow.operators.python import PythonOperator


# Should return aws_sqs_sensor operator
# https://airflow.apache.org/docs/apache-airflow/1.10.12/_api/airflow/contrib/sensors/aws_sqs_sensor/index.html
def SubscribeOperator(**kwargs) -> PythonOperator:
    """Subscribes to a topic to filter SQS messages"""
    topic = kwargs.get('topic', '')
    return PythonOperator(
        task_id=f"subscribe-topic",
        python_callable=subscribe,
        op_kwargs= {'topic': topic}
    )

def subscribe(topic: str):
    logging.info(f"Would subscribe to {topic} on AWS SNS")
