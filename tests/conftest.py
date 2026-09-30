import pytest

from data_pipelines_annuaire.config import API_URL
from data_pipelines_annuaire.tests.response_tester import APIResponseTester


@pytest.fixture
def api_response_tester():
    return APIResponseTester(API_URL)
