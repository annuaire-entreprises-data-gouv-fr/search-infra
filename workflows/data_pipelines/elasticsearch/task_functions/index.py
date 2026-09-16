import logging
import time
from datetime import UTC, datetime
from itertools import pairwise

from airflow.sdk import get_current_context, task
from airflow.task.trigger_rule import TriggerRule
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
    INDEXING_SIREN_RANGES,
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


@task
def prepare_elastic_index_for_bulk():
    """Turn off the work Elasticsearch does between bulks, for the whole indexing.

    Cannot live inside `fill_elastic_siren_index` any more: that task now runs once per
    siren shard, and the first shard to finish would restore the settings under the
    others.
    """
    ti = get_current_context()["ti"]
    elastic_index = ti.xcom_pull(key="elastic_index", task_ids="get_next_index_name")
    get_elastic_connection().indices.put_settings(
        index=elastic_index,
        body={
            "index.refresh_interval": -1,
            "index.translog.durability": "async",
        },
    )


@task(trigger_rule=TriggerRule.ALL_DONE)
def restore_elastic_index_settings():
    """Put the index back the way the API expects it, whatever the shards did.

    ALL_DONE, so a failed shard cannot leave the index with refresh disabled. It is a
    leaf: the failure still propagates through the shards themselves, which
    `fill_elastic_fondation_index` waits on.
    """
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
    # One explicit refresh so that the document count read downstream is exact.
    elastic_connection.indices.refresh(index=elastic_index)


def compute_siren_ranges(sqlite_client, shard_count):
    """Split the unites legales into `shard_count` contiguous ranges of siren.

    Balanced by row count rather than by siren prefix: siren are handed out
    sequentially, so a fixed split of the 9-digit space would leave some shards several
    times larger than others, and the slowest shard sets the wall clock. Reading a
    boundary walks the unique index on siren, no table access.

    The ranges tile the whole table: the first has no lower bound, the last no upper
    one, and each bound is shared with its neighbour. A boundary that repeats (fewer
    rows than shards) is dropped rather than producing an empty duplicate range.
    """
    unites_legales_count = sqlite_client.get_table_count("unite_legale")

    boundaries = []
    for shard in range(1, shard_count):
        offset = shard * unites_legales_count // shard_count
        boundary = sqlite_client.execute(
            "SELECT siren FROM unite_legale ORDER BY siren LIMIT 1 OFFSET ?",
            (offset,),
        ).fetchone()
        if boundary and boundary[0] not in boundaries:
            boundaries.append(boundary[0])

    limits = [None, *boundaries, None]
    return unites_legales_count, [
        {"siren_start": start, "siren_end": end} for start, end in pairwise(limits)
    ]


@task
def compute_siren_shards():
    sqlite_client = SqliteClient(AIRFLOW_ELK_DATA_DIR + "sirene.db")
    sqlite_client.tune_for_large_scan()
    unites_legales_count, siren_ranges = compute_siren_ranges(
        sqlite_client, INDEXING_SIREN_RANGES
    )
    sqlite_client.commit_and_close_conn()

    logger.info(
        f"Indexing {unites_legales_count} unites legales in "
        f"{len(siren_ranges)} shards: {siren_ranges}"
    )
    return siren_ranges


@task
def fill_elastic_siren_index(siren_range):
    ti = get_current_context()["ti"]
    elastic_index = ti.xcom_pull(key="elastic_index", task_ids="get_next_index_name")
    # parallel_bulk drains the document generator (which reads from this cursor) in its
    # own thread pool task handler, so the connection must allow cross-thread use.
    # Access stays sequential: the query is issued here, the cursor is drained by that
    # single handler thread, and the connection is closed here once bulk indexing is over.
    sqlite_client = SqliteClient(
        AIRFLOW_ELK_DATA_DIR + "sirene.db", check_same_thread=False
    )
    sqlite_client.tune_for_large_scan()
    query, params = select_fields_to_index_query(**siren_range)
    sqlite_client.execute(query, params)

    doc_count = index_unites_legales_by_chunk(
        cursor=sqlite_client.db_cursor,
        elastic_connection=get_elastic_connection(),
        elastic_bulk_thread_count=ELASTIC_BULK_THREAD_COUNT,
        elastic_bulk_size=ELASTIC_BULK_SIZE,
        elastic_index=elastic_index,
    )
    sqlite_client.commit_and_close_conn()
    return doc_count


@task
def fill_elastic_fondation_index():
    """
    Index the fondations that have no SIRET.
    Those with a SIRET are already indexed with their unite_legale equivalent.
    """
    ti = get_current_context()["ti"]
    elastic_index = ti.xcom_pull(key="elastic_index", task_ids="get_next_index_name")
    sqlite_client = SqliteClient(AIRFLOW_ELK_DATA_DIR + "sirene.db")
    sqlite_client.execute(select_fondations_to_index_query)

    connections.create_connection(
        hosts=[ELASTIC_URL],
        basic_auth=(ELASTIC_USER, ELASTIC_PASSWORD),
        retry_on_timeout=True,
    )
    elastic_connection = connections.get_connection()

    doc_count = index_fondations_by_chunk(
        cursor=sqlite_client.db_cursor,
        elastic_connection=elastic_connection,
        elastic_bulk_thread_count=ELASTIC_BULK_THREAD_COUNT,
        elastic_bulk_size=ELASTIC_BULK_SIZE,
        elastic_index=elastic_index,
    )
    ti.xcom_push(key="fondation_doc_count", value=doc_count)
    sqlite_client.commit_and_close_conn()


def count_indexed_documents(elastic_connection, elastic_index):
    """Ask Elasticsearch how many documents the index holds.

    Called once, here, rather than at the end of each indexing task: `_cat/count` can
    force Lucene to refresh the last bulk into a segment, which amplifies segment
    merging and slows the indexing down. The retry absorbs the lag between the refresh
    and the count being visible.
    """
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
    # One value per siren shard, since fill_elastic_siren_index is a mapped task.
    shard_doc_counts = ti.xcom_pull(task_ids="fill_elastic_siren_index") or []
    fondation_doc_count = ti.xcom_pull(
        key="fondation_doc_count",
        task_ids="fill_elastic_fondation_index",
    )
    doc_count = count_indexed_documents(get_elastic_connection(), elastic_index)
    # Informational only, and read from a mapped task: never let its shape fail a run
    # whose documents are already indexed.
    logger.info(
        f"Documents indexed per siren shard: {shard_doc_counts}, "
        f"counted in the index: {doc_count}"
    )

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
