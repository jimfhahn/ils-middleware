import pytest

from airflow import models
from airflow.utils.dag_cycle_tester import check_cycle


@pytest.fixture
def mock_sinopia_env(monkeypatch) -> None:
    monkeypatch.setenv("AIRFLOW_VAR_SINOPIA_ENV", "test")
    monkeypatch.setenv("AIRFLOW_VAR_SQS_URL", "http://aws.com/12345/")
    monkeypatch.setenv("AIRFLOW_VAR_RDF2MARC_LAMBDA", "")
    monkeypatch.setenv("AIRFLOW_VAR_MARC_S3_BUCKET", "")
    monkeypatch.setenv("AIRFLOW_VAR_SINOPIA_ENV", "")
    monkeypatch.setenv("AIRFLOW_VAR_SYMPHONY_APP_ID", "")
    monkeypatch.setenv(
        "AIRFLOW_VAR_STANFORD_SYMPHONY_LOGIN", ""
    )  # TODO: try to simulate this coming from vault
    monkeypatch.setenv(
        "AIRFLOW_VAR_STANFORD_SYMPHONY_PASSWORD", ""
    )  # TODO: try to simulate this coming from vault


def _test_dag_integrity(module):
    dag_objects = [var for var in vars(module).values() if isinstance(var, models.DAG)]
    assert dag_objects

    # For every DAG object, test for cycles
    for dag in dag_objects:
        check_cycle(dag)


# TODO: test that upstream task failure triggers notify_on_task_failure in stanford dag
def test_stanford_dag_success(mock_sinopia_env):
    from ils_middleware.dags import stanford

    _test_dag_integrity(stanford)
