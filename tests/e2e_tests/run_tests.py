import pytest
from airflow.sdk import task


@task
def run_e2e_tests():
    exit_code = pytest.main(
        ["dags/data_pipelines_annuaire/tests/e2e_tests", "-vv", "--log-cli-level=DEBUG"]
    )
    if exit_code > 0:
        raise RuntimeError("Pytest Fail!!!!!")
