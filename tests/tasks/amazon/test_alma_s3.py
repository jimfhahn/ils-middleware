"""Test the Alma AWS S3 tasks properly name and load files."""
import pytest

from airflow.providers.amazon.aws.hooks.s3 import S3Hook


@pytest.fixture
def mock_env_vars(monkeypatch) -> None:
    monkeypatch.setenv("AIRFLOW_VAR_MARC_S3_BUCKET", "alma-marc-test")


@pytest.fixture
def mock_s3_hook(monkeypatch):
    def mock_download_file(*args, **kwargs):
        return "tests/fixtures/record.mar"

    monkeypatch.setattr(S3Hook, "download_file", mock_download_file)
