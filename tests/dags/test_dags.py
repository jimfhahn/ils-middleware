import glob
import pytest
from os import path
from airflow import models


DAG_PATHS = glob.glob(path.join(path.dirname(__file__), "..", "..", "dags", "*.py"))


@pytest.mark.parametrize("dag_path", DAG_PATHS)
def test_dag_integrity(dag_path):
    dag_name = path.basename(dag_path)
    module = _import_file(dag_name, dag_path)

    dag_objects = [var for var in vars(module).values() if isinstance(var, models.DAG)]
    assert dag_objects


def _import_file(module_name, module_path):
    import importlib.util

    spec = importlib.util.spec_from_file_location(module_name, str(module_path))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module
