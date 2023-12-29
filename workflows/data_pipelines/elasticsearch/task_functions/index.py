import logging
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
)


def create_elastic_index(**kwargs):
    next_color = kwargs["ti"].xcom_pull(key="next_color", task_ids="get_colors")
    elastic_index = f"siren-{next_color}"
    logging.info(f"******************** Index to create: {elastic_index}")
    create_index = ElasticCreateIndex(
        elastic_url=ELASTIC_URL,
        elastic_index=elastic_index,
        elastic_user=ELASTIC_USER,
        elastic_password=ELASTIC_PASSWORD,
        elastic_bulk_size=ELASTIC_BULK_SIZE,
    )
    create_index.execute()


def fill_elastic_siren_index(**kwargs):
    next_color = kwargs["ti"].xcom_pull(key="next_color", task_ids="get_colors")
    elastic_index = f"siren-{next_color}"
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
        elastic_index=elastic_index,
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
