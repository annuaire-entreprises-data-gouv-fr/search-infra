import logging
import time
from datetime import UTC, datetime

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
    ELASTIC_REQUEST_TIMEOUT,
    ELASTIC_URL,
    ELASTIC_USER,
    INDEXING_PARALLEL_TASKS,
)
from data_pipelines_annuaire.helpers import Notification
from data_pipelines_annuaire.helpers.sqlite_client import SqliteClient
from data_pipelines_annuaire.workflows.data_pipelines.elasticsearch.create_index import (
    ElasticCreateIndex,
)
from data_pipelines_annuaire.workflows.data_pipelines.elasticsearch.indexing_fondation import (
    index_fondations_by_chunk,
)
from data_pipelines_annuaire.workflows.data_pipelines.elasticsearch.indexing_unite_legale import (
    index_unites_legales_by_chunk,
)
from data_pipelines_annuaire.workflows.data_pipelines.elasticsearch.sqlite.fields_to_index import (
    select_fields_to_index_query,
)
from data_pipelines_annuaire.workflows.data_pipelines.elasticsearch.sqlite.fondations_to_index import (
    select_fondations_to_index_query,
)

logger = logging.getLogger(__name__)

ELASTIC_COUNT_MAX_RETRIES = 5
ELASTIC_COUNT_RETRY_INTERVAL = 5


def get_elastic_connection():
    connections.create_connection(
        hosts=[ELASTIC_URL],
        basic_auth=(ELASTIC_USER, ELASTIC_PASSWORD),
        retry_on_timeout=True,
        # Parallel bulk indexing increases the risk of timeouts
        request_timeout=ELASTIC_REQUEST_TIMEOUT,
    )
    return connections.get_connection()


@task
def get_next_index_name():
    current_date = datetime.now(tz=UTC).strftime("%Y%m%d%H%M%S")
    elastic_index = f"siren-{current_date}"
    ti = get_current_context()["ti"]
    ti.xcom_push(key="elastic_index", value=elastic_index)


@task
def create_elastic_index():
    """
    Create the index, then optimise it for bulk inserts :
    * `index.refresh_interval: -1,`: prevents Elasticsearch from performing any refreshes during the bulk indexing process.
    * `index.translog.durability: async`: stops it from writing transaction logs to disk on every request.
    Async translog risks data loss on crash but since we restart from scratch in this case this is a non-issue.
    """
    ti = get_current_context()["ti"]
    elastic_index = ti.xcom_pull(key="elastic_index", task_ids="get_next_index_name")
    logger.info(f"******************** Index to create: {elastic_index}")
    create_index = ElasticCreateIndex(
        elastic_url=ELASTIC_URL,
        elastic_index=elastic_index,
        elastic_user=ELASTIC_USER,
        elastic_password=ELASTIC_PASSWORD,
        elastic_bulk_size=ELASTIC_BULK_SIZE,
    )
    create_index.execute()
    get_elastic_connection().indices.put_settings(
        index=elastic_index,
        body={
            "index.refresh_interval": -1,
            "index.translog.durability": "async",
        },
    )


@task
def compute_siren_ranges():
    """
    Each parallel task should index the same number of documents to share the load evenly.
    As a proxy we split the unités légales to N equal ranges.
    Each range is defined by a first and last SIREN.
    None means the beginning or end of the `unite_legale` table.
    """
    siren_ranges = []
    siren_start = None
    with SqliteClient(AIRFLOW_ELK_DATA_DIR + "sirene.db") as sqlite_client:
        unites_legales_count = sqlite_client.get_table_count("unite_legale")
        for parallel_task in range(1, INDEXING_PARALLEL_TASKS):
            offset = parallel_task * unites_legales_count // INDEXING_PARALLEL_TASKS
            siren_end = sqlite_client.execute(
                f"SELECT siren FROM unite_legale ORDER BY siren LIMIT 1 OFFSET {offset}"
            ).fetchone()[0]
            if siren_end and siren_end != siren_start:
                siren_ranges.append(
                    {"siren_start": siren_start, "siren_end": siren_end}
                )
                siren_start = siren_end
    siren_ranges.append({"siren_start": siren_start, "siren_end": None})

    logger.info(
        f"Indexing {unites_legales_count} unites legales in "
        f"{len(siren_ranges)} parallel tasks: {siren_ranges}"
    )
    return siren_ranges


@task
def fill_elastic_siren_index(siren_range):
    ti = get_current_context()["ti"]
    elastic_index = ti.xcom_pull(key="elastic_index", task_ids="get_next_index_name")
    with SqliteClient(
        AIRFLOW_ELK_DATA_DIR + "sirene.db", check_same_thread=False
    ) as sqlite_client:
        query = select_fields_to_index_query(**siren_range)
        sqlite_client.execute(query)

        doc_count = index_unites_legales_by_chunk(
            cursor=sqlite_client.db_cursor,
            elastic_connection=get_elastic_connection(),
            elastic_bulk_thread_count=ELASTIC_BULK_THREAD_COUNT,
            elastic_bulk_size=ELASTIC_BULK_SIZE,
            elastic_index=elastic_index,
        )
    return doc_count


@task
def restore_elastic_index_settings():
    """Put the index settings back to the defaults."""
    ti = get_current_context()["ti"]
    elastic_index = ti.xcom_pull(key="elastic_index", task_ids="get_next_index_name")
    elastic_connection = get_elastic_connection()
    elastic_connection.indices.put_settings(
        index=elastic_index,
        body={
            "index.refresh_interval": None,
            "index.translog.durability": None,
        },
    )
    # Refresh was disabled so we force it
    elastic_connection.indices.refresh(index=elastic_index)


@task
def fill_elastic_fondation_index():
    """
    Index the fondations that have no SIRET.
    Those with a SIRET are already indexed with their unite_legale equivalent.
    """
    ti = get_current_context()["ti"]
    elastic_index = ti.xcom_pull(key="elastic_index", task_ids="get_next_index_name")
    with SqliteClient(AIRFLOW_ELK_DATA_DIR + "sirene.db") as sqlite_client:
        sqlite_client.execute(select_fondations_to_index_query)

        doc_count = index_fondations_by_chunk(
            cursor=sqlite_client.db_cursor,
            elastic_connection=get_elastic_connection(),
            elastic_bulk_thread_count=ELASTIC_BULK_THREAD_COUNT,
            elastic_bulk_size=ELASTIC_BULK_SIZE,
            elastic_index=elastic_index,
        )
    ti.xcom_push(key="fondation_doc_count", value=doc_count)


def count_indexed_documents(elastic_index):
    """Ask Elasticsearch how many documents the index contains."""
    elastic_connection = get_elastic_connection()
    for attempt in range(ELASTIC_COUNT_MAX_RETRIES):
        doc_count = int(
            elastic_connection.cat.count(
                index=elastic_index, params={"format": "json"}
            )[0]["count"]
        )
        if doc_count > 0:
            return doc_count

        if attempt < ELASTIC_COUNT_MAX_RETRIES - 1:
            logger.warning(
                f"Document count is zero. Retrying in "
                f"{ELASTIC_COUNT_RETRY_INTERVAL} seconds..."
            )
            time.sleep(ELASTIC_COUNT_RETRY_INTERVAL)

    logger.error("Max retries reached. Document count is still zero.")
    return 0


@task
def check_elastic_index():
    ti = get_current_context()["ti"]
    elastic_index = ti.xcom_pull(key="elastic_index", task_ids="get_next_index_name")
    parallel_task_doc_counts = ti.xcom_pull(task_ids="fill_elastic_siren_index") or []
    fondation_doc_count = ti.xcom_pull(
        key="fondation_doc_count",
        task_ids="fill_elastic_fondation_index",
    )
    doc_count = count_indexed_documents(elastic_index)
    logger.info(f"Documents indexed per parallel task: {parallel_task_doc_counts}.")

    if int(doc_count) < ELASTIC_MIN_DOC_COUNT_EXPECTED:
        failure_message = (
            f"*******The data has not been correctly indexed: "
            f"{doc_count} documents indexed."
            f"Expected at least {ELASTIC_MIN_DOC_COUNT_EXPECTED}."
        )
        ti.xcom_push(key=Notification.notification_xcom_key, value=failure_message)
        raise ValueError(failure_message)

    success_message = (
        f"Nombre de documents indexés : {doc_count}<br/>"
        f"Fondations sans SIRET indexés en plus : {fondation_doc_count}"
    )
    ti.xcom_push(key=Notification.notification_xcom_key, value=success_message)
    logger.info(success_message)


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
    indices = sorted(indices, key=lambda index: index["index"])

    to_remove = indices[:-ELASTIC_MAX_LIVE_VERSIONS]

    for index in to_remove:
        logger.info(f"Removing index {index['index']}")
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

    logger.info(
        f"Updating alias siren-reader : add {elastic_index}, remove {', '.join(indices)}"
    )

    elastic_connection.indices.update_aliases(actions=actions)
