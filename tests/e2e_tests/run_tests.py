import pytest


def run_e2e_tests():
    exit_code = pytest.main(["dags/dag_datalake_sirene/tests/e2e_tests", "-vv"])
    if exit_code > 0:
        raise RuntimeError("Pytest Fail!!!!!")
