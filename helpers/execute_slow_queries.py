import logging
import requests

from helpers.slow_requests import SLOW_REQUESTS
from helpers.settings import Settings

def execute_slow_requests():
    session = requests.Session()
    base_url = Settings.API_URL
    for query in SLOW_REQUESTS:
        try:
            path = f"/search?{query}"
            logging.info(f"******* Searching query : {query}")
            response = session.get(url=base_url + path)
            logging.info(f"******* Request status : {response.status_code}")
            response.raise_for_status()
        except requests.exceptions.RequestException as error:
            raise SystemExit(error)
