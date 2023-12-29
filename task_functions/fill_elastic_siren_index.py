from dag_datalake_sirene.elasticsearch.indexing_unite_legale import (
    index_unites_legales_by_chunk,
)
from elasticsearch_dsl import connections
from dag_datalake_sirene.sqlite.sqlite_client import SqliteClient
from dag_datalake_sirene.sqlite.queries.select_fields_to_index import (
    select_fields_to_index_query,
)
from dag_datalake_sirene.config import (
    AIRFLOW_INDEXING_DATA_DIR,
    ELASTIC_URL,
    ELASTIC_USER,
    ELASTIC_PASSWORD,
    ELASTIC_BULK_SIZE,
)


def fill_elastic_siren_index(**kwargs):
    next_color = kwargs["ti"].xcom_pull(key="next_color", task_ids="get_colors")
    elastic_index = f"siren-{next_color}"
    sqlite_client = SqliteClient(AIRFLOW_INDEXING_DATA_DIR + "sirene.db")
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
