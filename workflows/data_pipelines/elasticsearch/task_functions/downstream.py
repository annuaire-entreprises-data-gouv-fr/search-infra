import logging
import time

import requests
from elasticsearch import NotFoundError
from elasticsearch_dsl import connections

from dag_datalake_sirene.config import (
    AIRFLOW_ELK_DAG_NAME,
    ELASTIC_DOWNSTREAM_ALIAS,
    ELASTIC_DOWNSTREAM_PASSWORD,
    ELASTIC_DOWNSTREAM_URLS,
    ELASTIC_DOWNSTREAM_USER,
)


def wait_for_downstream_import(**kwargs):
    elastic_index = kwargs["ti"].xcom_pull(
        key="elastic_index",
        task_ids="get_next_index_name",
        dag_id=AIRFLOW_ELK_DAG_NAME,
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
                f"{url}/{elastic_index}/_stats",
                auth=(ELASTIC_DOWNSTREAM_USER, ELASTIC_DOWNSTREAM_PASSWORD),
            )

            if response.status_code == 404:
                continue
            if response.status_code != 200:
                logging.warning(f"Erreur {response.status_code} sur {url}: {response.text}")
                continue
            
            data = response.json()

            shards = data.get("_shards", {})
            total = shards.get("total")
            successful = shards.get("successful")
            failed = shards.get("failed")

            if failed == 0 and total == successful and total > 0:
                logging.info(f"Index available on {url}")
                completed.append(url)
            else:
                continue


        pending = [url for url in downstream_urls if url not in completed]

        if len(pending) > 0:
            time.sleep(5)
            waited_for += 5

    if len(pending) > 0:
        raise Exception("Downstream import is taking too long")

def update_downstream_alias(**kwargs):
    urls = [url.strip() for url in ELASTIC_DOWNSTREAM_URLS.split(",")]
    aliases = ["siren-reader", "siren-blue", "siren-green"]
    elastic_index = kwargs["ti"].xcom_pull(
        key="elastic_index",
        task_ids="get_next_index_name",
        dag_id=AIRFLOW_ELK_DAG_NAME,
        include_prior_dates=True,
    )

    for url in urls:
        logging.info(f"Connecting to downstream cluster {url}")
        es = Elasticsearch(
            hosts=[url],
            http_auth=(ELASTIC_DOWNSTREAM_USER, ELASTIC_DOWNSTREAM_PASSWORD),
            retry_on_timeout=True,
        )

        actions = []

        for alias in aliases:
            try:
                config = es.indices.get_alias(name=alias)
                indices = list(config.keys())
            except NotFoundError:
                indices = []

            for idx in indices:
                actions.append({"remove": {"index": idx, "alias": alias}})

        for alias in aliases:
            actions.append({"add": {"index": new_index, "alias": alias}})

        logging.info(
            f"[{url}] Updating aliases {aliases}: add {new_index}, remove from old indexes"
        )

        # Envoi de la requête
        if actions:
            es.indices.update_aliases({"actions": actions})
