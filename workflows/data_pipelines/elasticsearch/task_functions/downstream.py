import requests
import time
import logging

from dag_datalake_sirene.config import (
    ELASTIC_DOWNSTREAM_URLS,
    ELASTIC_DOWNSTREAM_USER,
    ELASTIC_DOWNSTREAM_PASSWORD,
    ELASTIC_DOWNSTREAM_ALIAS,
)


def wait_for_downstream_import(**kwargs):
    next_color = kwargs["ti"].xcom_pull(key="next_color", task_ids="get_colors")
    elastic_index = f"siren-{next_color}"

    downstream_urls = ELASTIC_DOWNSTREAM_URLS.split(",")

    if len(downstream_urls) == 0:
        return

    logging.info(f"Waiting for {elastic_index} to be imported on {downstream_urls}")

    pending = downstream_urls
    completed = []

    waited_for = 0
    timeout = 7200

    while len(pending) > 0 and waited_for < timeout:
        for url in pending:
            response = requests.get(
                f"{ url }/{ELASTIC_DOWNSTREAM_ALIAS}",
                auth=(ELASTIC_DOWNSTREAM_USER, ELASTIC_DOWNSTREAM_PASSWORD),
            )

            if response.status_code == 404:
                continue

            indices = list(response.json().keys())

            if elastic_index in indices:
                logging.info(f"Index available on {url}")
                completed.append(url)

        pending = [url for url in downstream_urls if url not in completed]

        if len(pending) > 0:
            time.sleep(5)
            waited_for += 5

    if len(pending) > 0:
        raise Exception("Downstream import is taking too long")
