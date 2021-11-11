"""Test Symphony Operators and functions."""
import io

import pytest
from datetime import datetime

from airflow import DAG

from airflow.contrib.hooks.aws_lambda_hook import AwsLambdaHook
from airflow.models.taskinstance import TaskInstance
from airflow.operators.dummy import DummyOperator

from ils_middleware.tasks.sinopia.rdf2marc import Rdf2Marc

mock_200_response = {
    "Payload": io.StringIO("{}"),
    "ResponseMetadata": {"HTTPHeaders": {}},
    "StatusCode": 200,
}


def test_task():
    start_date = datetime(2021, 9, 20)
    test_dag = DAG(
        "test_dag", default_args={"owner": "airflow", "start_date": start_date}
    )
    return DummyOperator(task_id="test", dag=test_dag)


task_instance = TaskInstance(test_task())


@pytest.fixture
def mock_resource():
    return {
        "user": "jpnelson",
        "group": "stanford",
        "editGroups": ["other", "pcc"],
        "templateId": "ld4p:RT:bf2:Monograph:Instance:Un-nested",
        "types": ["http://id.loc.gov/ontologies/bibframe/Instance"],
        "bfAdminMetadataRefs": [
            "https://api.development.sinopia.io/resource/7f775ec2-4fe8-48a6-9cb4-5b218f9960f1",
            "https://api.development.sinopia.io/resource/bc9e9939-45b3-4122-9b6d-d800c130c576",
        ],
        "bfItemRefs": [],
        "bfInstanceRefs": [],
        "bfWorkRefs": [
            "https://api.development.sinopia.io/resource/6497a461-42dc-42bf-b433-5e47c73f7e89"
        ],
        "id": "7b55e6f7-f91e-4c7a-bbcd-c074485ad18d",
        "uri": "https://api.development.sinopia.io/resource/7b55e6f7-f91e-4c7a-bbcd-c074485ad18d",
        "timestamp": "2021-10-29T20:30:58.821Z",
    }


@pytest.fixture
def mock_resources(mock_resource):
    return [
        {
            "email": "dscully@stanford.edu",
            "resource_uri": "http://example.com/rdf/0000-1111-2222-3333",
            "resource": mock_resource,
        },
        {
            "email": "fmulder@stanford.edu",
            "resource_uri": "http://example.com/rdf/0000-1111-2222-3333",
            "resource": mock_resource,
        },
    ]


@pytest.fixture
def mock_task_instance(monkeypatch, mock_resources):
    def mock_xcom_pull(*args, **kwargs):
        return mock_resources

    monkeypatch.setattr(TaskInstance, "xcom_pull", mock_xcom_pull)


@pytest.fixture
def mock_lambda(monkeypatch):
    def mock_invoke_lambda(*args, **kwargs):
        return mock_200_response

    monkeypatch.setattr(AwsLambdaHook, "invoke_lambda", mock_invoke_lambda)


@pytest.fixture
def mock_complete_results():
    return {
        "complete": ["http://example.com/rdf/0000-1111-2222-3333", "http://example.com/rdf/0000-1111-2222-3333"],
        "errors": []
    }

def test_Rdf2Marc(mock_task_instance, mock_lambda, mock_complete_results):
    # payload = {"instance_uri": "http://example.com/rdf/0000-1111-2222-3333"}
    assert Rdf2Marc(task_instance=task_instance) == mock_complete_results


@pytest.fixture
def mock_failed_lambda(monkeypatch):
    def mock_invoke_lambda(*args, **kwargs):
        return {
            "StatusCode": 200,
            "Payload": io.StringIO(
                """{ "errorMessage": "AdminMetadata (bf:adminMetadata) not specified for Instance"}"""
            ),
            "ResponseMetadata": {"HTTPHeaders": {"x-amz-function-error": "Unhandled"}},
        }

    monkeypatch.setattr(AwsLambdaHook, "invoke_lambda", mock_invoke_lambda)


@pytest.fixture
def mock_error_results():
    return {
        "complete": [],
        "errors": [
            {
                "instance_uri": "http://example.com/rdf/0000-1111-2222-3333",
                "message": "RDF2MARC conversion failed for http://example.com/rdf/0000-1111-2222-3333, error: AdminMetadata (bf:adminMetadata) not specified for Instance"
            },
            {
                "instance_uri": "http://example.com/rdf/0000-1111-2222-3333",
                "message": "RDF2MARC conversion failed for http://example.com/rdf/0000-1111-2222-3333, error: AdminMetadata (bf:adminMetadata) not specified for Instance"
            }
        ]
    }


def test_Rdf2Marc_LambdaError(mock_task_instance, mock_failed_lambda, mock_error_results):
    assert Rdf2Marc(task_instance=task_instance) == mock_error_results
