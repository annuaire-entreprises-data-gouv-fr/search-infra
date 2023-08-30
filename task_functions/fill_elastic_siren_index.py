from dag_datalake_sirene.elasticsearch.indexing_unite_legale import (
    index_unites_legales_by_chunk,
)
from dag_datalake_sirene.elasticsearch.indexing_association import (
    index_association_by_chunk,
)
from elasticsearch_dsl import connections

from dag_datalake_sirene.sqlite.sqlite_client import SqliteClient


from dag_datalake_sirene.sqlite.queries.select_fields_to_index import (
    select_unite_legale_fields_to_index_query,
    select_association_fields_to_index_query,
)
from dag_datalake_sirene.task_functions.global_variables import (
    SIRENE_DATABASE_LOCATION,
    ELASTIC_URL,
    ELASTIC_USER,
    ELASTIC_PASSWORD,
    ELASTIC_BULK_SIZE,
)


def fill_elastic_index(**kwargs):
    next_color = kwargs["ti"].xcom_pull(key="next_color", task_ids="get_colors")
    elastic_index = f"siren-{next_color}"
    connections.create_connection(
        hosts=[ELASTIC_URL],
        http_auth=(ELASTIC_USER, ELASTIC_PASSWORD),
        retry_on_timeout=True,
    )
    elastic_connection = connections.get_connection()

    sqlite_client = SqliteClient(SIRENE_DATABASE_LOCATION)

    sqlite_client.execute(select_unite_legale_fields_to_index_query)

    doc_count_siren = index_unites_legales_by_chunk(
        cursor=sqlite_client.db_cursor,
        elastic_connection=elastic_connection,
        elastic_bulk_size=ELASTIC_BULK_SIZE,
        elastic_index=elastic_index,
    )
    kwargs["ti"].xcom_push(key="doc_count_siren", value=doc_count_siren)

    sqlite_client.execute(select_association_fields_to_index_query)
    doc_count_rna = index_association_by_chunk(
        cursor=sqlite_client.db_cursor,
        elastic_connection=elastic_connection,
        elastic_bulk_size=ELASTIC_BULK_SIZE,
        elastic_index=elastic_index,
    )
    kwargs["ti"].xcom_push(key="doc_count_rna", value=doc_count_rna)

    sqlite_client.commit_and_close_conn()
