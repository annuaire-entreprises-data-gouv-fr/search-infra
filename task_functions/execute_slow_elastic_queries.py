import logging
import requests

from dag_datalake_sirene.task_functions.global_variables import AIO_URL


def execute_slow_requests():
    session = requests.Session()
    base_url = AIO_URL
    slow_queries = ["q=rue", "q=rue%20de%20la", "q=france"]
    for query in slow_queries:
        try:
            path = f"/search?{query}"
            logging.info(f"******* Searching query : {query}")
            response = session.get(url=base_url + path)
            logging.info(f"******* Request status : {response.status_code}")
            response.raise_for_status()
        except requests.exceptions.RequestException as error:
            raise SystemExit(error)
