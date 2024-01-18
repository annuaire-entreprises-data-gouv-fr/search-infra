import logging
from datetime import datetime
from elasticsearch_dsl import connections

from dag_datalake_sirene.workflows.data_pipelines.elasticsearch.create_index import (
    ElasticCreateIndex,
)
from dag_datalake_sirene.helpers.sqlite_client import SqliteClient

# fmt: off
from dag_datalake_sirene.workflows.data_pipelines.elasticsearch.sqlite.\
    fields_to_index import (
    select_fields_to_index_query,
)
from dag_datalake_sirene.workflows.data_pipelines.elasticsearch.\
    indexing_unite_legale import (
    index_unites_legales_by_chunk,
)
# fmt: on
from dag_datalake_sirene.config import (
    AIRFLOW_ELK_DATA_DIR,
    ELASTIC_URL,
    ELASTIC_USER,
    ELASTIC_PASSWORD,
    ELASTIC_BULK_SIZE,
    ELASTIC_MAX_LIVE_VERSIONS,
)


def get_next_index(**kwargs):
    current_date = datetime.today().strftime("%Y%m%d")
    elastic_index_name = f"siren-{current_date}"
    kwargs["ti"].xcom_push(key="elastic_index_name", value=elastic_index_name)


def create_elastic_index(**kwargs):
    elastic_index_name = kwargs["ti"].xcom_pull(
        key="elastic_index_name", task_ids="get_next_index_name"
    )
    logging.info(f"******************** Index to create: {elastic_index_name}")
    create_index = ElasticCreateIndex(
        elastic_url=ELASTIC_URL,
        elastic_index_name=elastic_index_name,
        elastic_user=ELASTIC_USER,
        elastic_password=ELASTIC_PASSWORD,
        elastic_bulk_size=ELASTIC_BULK_SIZE,
    )
    create_index.execute()


def fill_elastic_siren_index(**kwargs):
    elastic_index_name = kwargs["ti"].xcom_pull(
        key="elastic_index_name", task_ids="get_next_index_name"
    )
    sqlite_client = SqliteClient(AIRFLOW_ELK_DATA_DIR + "sirene.db")
    sqlite_client.execute(select_fields_to_index_query)

    connections.create_connection(
        hosts=[ELASTIC_URL],
        http_auth=(ELASTIC_USER, ELASTIC_PASSWORD),
        retry_on_timeout=True,
    )
    elastic_connection = connections.get_connection()

    doc_count = index_unites_legales_by_chunk(
        cursor=sqlite_client.db_cursor,
        elastic_connection=elastic_connection,
        elastic_bulk_size=ELASTIC_BULK_SIZE,
        elastic_index_name=elastic_index_name,
    )
    kwargs["ti"].xcom_push(key="doc_count", value=doc_count)
    sqlite_client.commit_and_close_conn()


def check_elastic_index(**kwargs):
    doc_count = kwargs["ti"].xcom_pull(
        key="doc_count", task_ids="fill_elastic_siren_index"
    )
    logging.info(f"******************** Documents indexed: {doc_count}")

    if float(doc_count) < 26000000:
        raise ValueError(
            f"*******The data has not been correctly indexed: "
            f"{doc_count} documents indexed."
        )


def delete_previous_elastic_indices(**kwargs):
    connections.create_connection(
        hosts=[ELASTIC_URL],
        http_auth=(ELASTIC_USER, ELASTIC_PASSWORD),
        retry_on_timeout=True,
    )

    elastic_connection = connections.get_connection()

    indices = elastic_connection.cat.indices(index="siren-*", format="json")
    indices = [
        index
        for index in indices
        if index["index"] not in ["siren-green", "siren-blue"]
    ]
    indices = list(sorted(indices, key=lambda index: index["index"]))

    to_remove = indices[:-ELASTIC_MAX_LIVE_VERSIONS]

    for index in to_remove:
        logging.info(f'Removing index {index["index"]}')
        elastic_connection.indices.delete(index=index["index"])
