import requests
import time
import logging

from helpers.settings import Settings

def wait_for_downstream_import(**kwargs):
    elastic_index = kwargs["ti"].xcom_pull(
        key="elastic_index",
        task_ids="get_next_index_name",
        dag_id=Settings.AIRFLOW_ELK_DAG_NAME,
        include_prior_dates=True,
    )

    wait_for_downstream_index_import(elastic_index)


def wait_for_downstream_rollback_import(**kwargs):
    elastic_index = kwargs["ti"].xcom_pull(
        key="elastic_index",
        task_ids="rollback_elastic_index",
    )

    wait_for_downstream_index_import(elastic_index)


def wait_for_downstream_index_import(elastic_index):
    downstream_urls = Settings.ELASTIC_DOWNSTREAM_URLS.split(",")

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
                f"{ url }/{Settings.ELASTIC_DOWNSTREAM_ALIAS}",
                auth=(Settings.ELASTIC_DOWNSTREAM_USER, Settings.ELASTIC_DOWNSTREAM_PASSWORD),
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
