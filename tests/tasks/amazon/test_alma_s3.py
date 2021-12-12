"""Test the Alma AWS S3 tasks properly name and load files."""
import pytest
from unittest import mock

from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from ils_middleware.tasks.amazon.alma_s3 import get_from_alma_s3

from tasks import test_task_instance


@pytest.fixture
def mock_env_vars(monkeypatch) -> None:
    monkeypatch.setenv("AIRFLOW_VAR_MARC_S3_BUCKET", "alma-marc-test")


@pytest.fixture
def mock_s3_hook(monkeypatch):
    def mock_download_file(*args, **kwargs):
        return "tests/fixtures/record.mar"

    monkeypatch.setattr(S3Hook, "download_file", mock_download_file)
