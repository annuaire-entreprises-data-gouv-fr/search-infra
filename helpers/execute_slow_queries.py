import logging

import requests
from airflow.sdk import task

from data_pipelines_annuaire.config import API_URL
from data_pipelines_annuaire.helpers.slow_requests import SLOW_REQUESTS

logger = logging.getLogger(__name__)


@task
def execute_slow_requests():
    session = requests.Session()
    base_url = API_URL
    for query in SLOW_REQUESTS:
        try:
            path = f"/search?{query}"
            logger.info(f"******* Searching query : {query}")
            response = session.get(url=base_url + path)
            logger.info(f"******* Request status : {response.status_code}")
            response.raise_for_status()
        except requests.exceptions.RequestException as error:
            raise SystemExit(error)
