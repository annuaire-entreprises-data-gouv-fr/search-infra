import logging
from datetime import datetime

from airflow.sdk import get_current_context, task
from elasticsearch import NotFoundError
from elasticsearch.dsl import connections

from data_pipelines_annuaire.config import (
    AIRFLOW_ELK_DATA_DIR,
    ELASTIC_BULK_SIZE,
    ELASTIC_BULK_THREAD_COUNT,
    ELASTIC_MAX_LIVE_VERSIONS,
    ELASTIC_MIN_DOC_COUNT_EXPECTED,
    ELASTIC_PASSWORD,
    ELASTIC_URL,
    ELASTIC_USER,
)
from data_pipelines_annuaire.helpers import Notification
from data_pipelines_annuaire.helpers.sqlite_client import SqliteClient
from data_pipelines_annuaire.workflows.data_pipelines.elasticsearch.create_index import (
    ElasticCreateIndex,
)
from data_pipelines_annuaire.workflows.data_pipelines.elasticsearch.indexing_unite_legale import (
    index_unites_legales_by_chunk,
)
from data_pipelines_annuaire.workflows.data_pipelines.elasticsearch.sqlite.fields_to_index import (
    select_fields_to_index_query,
)


@task
def get_next_index_name():
    current_date = datetime.today().strftime("%Y%m%d%H%M%S")
    elastic_index = f"siren-{current_date}"
    ti = get_current_context()["ti"]
    ti.xcom_push(key="elastic_index", value=elastic_index)


@task
def create_elastic_index():
    ti = get_current_context()["ti"]
    elastic_index = ti.xcom_pull(key="elastic_index", task_ids="get_next_index_name")
    logging.info(f"******************** Index to create: {elastic_index}")
    create_index = ElasticCreateIndex(
        elastic_url=ELASTIC_URL,
        elastic_index=elastic_index,
        elastic_user=ELASTIC_USER,
        elastic_password=ELASTIC_PASSWORD,
        elastic_bulk_size=ELASTIC_BULK_SIZE,
    )
    create_index.execute()


@task
def fill_elastic_siren_index():
    ti = get_current_context()["ti"]
    elastic_index = ti.xcom_pull(key="elastic_index", task_ids="get_next_index_name")
    sqlite_client = SqliteClient(AIRFLOW_ELK_DATA_DIR + "sirene.db")
    sqlite_client.execute(select_fields_to_index_query)

    connections.create_connection(
        hosts=[ELASTIC_URL],
        basic_auth=(ELASTIC_USER, ELASTIC_PASSWORD),
        retry_on_timeout=True,
    )
    elastic_connection = connections.get_connection()

    doc_count = index_unites_legales_by_chunk(
        cursor=sqlite_client.db_cursor,
        elastic_connection=elastic_connection,
        elastic_bulk_thread_count=ELASTIC_BULK_THREAD_COUNT,
        elastic_bulk_size=ELASTIC_BULK_SIZE,
        elastic_index=elastic_index,
    )
    ti.xcom_push(key="doc_count", value=doc_count)
    sqlite_client.commit_and_close_conn()


@task
def check_elastic_index():
    ti = get_current_context()["ti"]
    doc_count = ti.xcom_pull(key="doc_count", task_ids="fill_elastic_siren_index")

    if int(doc_count) < ELASTIC_MIN_DOC_COUNT_EXPECTED:
        failure_message = (
            f"*******The data has not been correctly indexed: "
            f"{doc_count} documents indexed."
            f"Expected at least {ELASTIC_MIN_DOC_COUNT_EXPECTED}."
        )
        ti.xcom_push(key=Notification.notification_xcom_key, value=failure_message)
        raise ValueError(failure_message)

    success_message = (
        f"DAG d'indexation a été exécuté avec succès."
        f"\n - Nombre de documents indexés : {doc_count}"
    )
    ti.xcom_push(key=Notification.notification_xcom_key, value=success_message)
    logging.info(success_message)


@task
def delete_previous_elastic_indices():
    connections.create_connection(
        hosts=[ELASTIC_URL],
        basic_auth=(ELASTIC_USER, ELASTIC_PASSWORD),
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
        logging.info(f"Removing index {index['index']}")
        elastic_connection.indices.delete(index=index["index"])


@task
def update_elastic_alias():
    """
    The annuaire-entreprises-search-api queries the "siren-reader" index alias to process user requests.
    The "siren-reader" index alias acts as a symbolic link to the current live index and should be associated to one and only one siren index at any given time.

    This function performs an atomic update of the alias to attach the new live index and detach any other index without any downtime.

    Example:
        Given that the siren-reader is associated to the index "siren-20240206011523"
        And that the new siren index is "siren-20240208001729"
        When called, this function detach the "siren-20240206011523" index from the alias "siren-reader"
        And attach the "siren-20240208001729" index to the alias "siren-reader"

    @see: https://www.elastic.co/guide/en/elasticsearch/reference/current/aliases.html
    """

    connections.create_connection(
        hosts=[ELASTIC_URL],
        basic_auth=(ELASTIC_USER, ELASTIC_PASSWORD),
        retry_on_timeout=True,
    )

    elastic_connection = connections.get_connection()

    alias = "siren-reader"
    ti = get_current_context()["ti"]
    elastic_index = ti.xcom_pull(key="elastic_index", task_ids="get_next_index_name")

    indices = []

    try:
        config = elastic_connection.indices.get_alias(name=alias)
        indices = config.keys() if config is not None else []
    except NotFoundError:
        pass

    actions = [
        {
            "remove": {
                "index": index,
                "alias": alias,
            }
        }
        for index in indices
    ]

    actions.append({"add": {"index": elastic_index, "alias": alias}})

    logging.info(
        f"Updating alias siren-reader : add {elastic_index}, remove {', '.join(indices)}"
    )

    elastic_connection.indices.update_aliases(actions=actions)
