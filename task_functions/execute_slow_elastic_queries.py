import logging
import requests

from dag_datalake_sirene.task_functions.global_variables import AIO_URL
from dag_datalake_sirene.task_functions.slow_requests import SLOW_REQUESTS


def execute_slow_requests():
    session = requests.Session()
    base_url = AIO_URL
    for query in SLOW_REQUESTS:
        try:
            path = f"/search?{query}"
            logging.info(f"******* Searching query : {query}")
            response = session.get(url=base_url + path)
            logging.info(f"******* Request status : {response.status_code}")
            response.raise_for_status()
        except requests.exceptions.RequestException as error:
            raise SystemExit(error)
