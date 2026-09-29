import logging
from contextlib import contextmanager

from elastic_transport import OrjsonSerializer
from elasticsearch.helpers import parallel_bulk

from data_pipelines_annuaire.workflows.data_pipelines.elasticsearch.mapping_index import (
    StructureMapping,
)

# fmt: off
from data_pipelines_annuaire.workflows.data_pipelines.elasticsearch\
    .process_unites_legales import process_unites_legales

# fmt: on
logger = logging.getLogger(__name__)

LOG_EVERY_N_DOCUMENTS = 1_000_000
MAX_LOGGED_FAILURES = 20
JSON_MIMETYPE = "application/json"


@contextmanager
def orjson_bulk_serializer(elastic_connection):
    """Encode the bulk payloads with orjson, ~7x faster than the standard library on
    documents of this shape.

    `parallel_bulk` resolves its serializer through
    `client.transport.serializers.get_serializer("application/json")`, so replacing
    that entry swaps the implementation for the whole bulk. `OrjsonSerializer`
    subclasses `JsonSerializer` and only overrides `json_dumps` / `json_loads`, so the
    `default` hook (date, UUID, Decimal) is unchanged.

    The swap is scoped to the indexing: the connection is a process-wide singleton and
    nothing else in the DAG needs it.
    """
    serializers = elastic_connection.transport.serializers
    original_serializer = serializers.get_serializer(JSON_MIMETYPE)
    serializers.serializers[JSON_MIMETYPE] = OrjsonSerializer()
    try:
        yield
    finally:
        serializers.serializers[JSON_MIMETYPE] = original_serializer


def doc_unite_legale_generator(data, elastic_index):
    # Serialize the instance into a dictionary so that it can be saved in elasticsearch.
    for index, document in enumerate(data):
        etablissements_count = len(document["unite_legale"]["etablissements"])
        # If ` unité légale` had more than 100 `établissements`, the main document is
        # separated into smaller documents consisting of 100 établissements each
        if etablissements_count > 100:
            smaller_document = document.copy()
            etablissements = document["unite_legale"]["etablissements"]
            etablissements_left = etablissements_count
            etablissements_indexed = 0
            while etablissements_left > 0:
                # min is used for the last iteration
                number_etablissements_to_add = min(etablissements_left, 100)
                # Select a 100 etablissements from the main document,
                # and use it as a list for the smaller document
                smaller_document["unite_legale"]["etablissements"] = etablissements[
                    etablissements_indexed : etablissements_indexed
                    + number_etablissements_to_add
                ]
                etablissements_left = etablissements_left - 100
                etablissements_indexed += 100
                yield StructureMapping(
                    meta={
                        "index": elastic_index,
                        "id": f"{smaller_document['identifiant']}-"
                        f"{etablissements_indexed}",
                    },
                    **smaller_document,
                ).to_dict(include_meta=True)
        # Otherwise, (the document has less than 100 établissements), index document
        # as is
        else:
            yield StructureMapping(
                meta={
                    "index": elastic_index,
                    "id": f"{document['identifiant']}-100",
                },
                **document,
            ).to_dict(include_meta=True)


def generate_unite_legale_docs(cursor, elastic_bulk_size, elastic_index):
    # Lazily stream documents to index: pull a batch from SQLite, clean it,
    # and yield each resulting document. Feeding a single long-lived generator to
    # parallel_bulk lets the read/transform overlap with the ES bulk requests
    # instead of running them serially per batch.
    unite_legale_columns = tuple(x[0] for x in cursor.description)
    while chunk_unites_legales_sqlite := cursor.fetchmany(elastic_bulk_size):
        liste_unites_legales_sqlite = tuple(
            dict(zip(unite_legale_columns, unite_legale))
            for unite_legale in chunk_unites_legales_sqlite
        )
        yield from doc_unite_legale_generator(
            process_unites_legales(liste_unites_legales_sqlite), elastic_index
        )


def index_unites_legales_by_chunk(
    cursor,
    elastic_connection,
    elastic_bulk_thread_count,
    elastic_bulk_size,
    elastic_index,
):
    """Index the documents the cursor yields, and return how many were indexed.

    The index settings (`refresh_interval`, `translog.durability`) are handled by the
    tasks surrounding this one: the function is called once per siren shard, so it can
    neither disable refresh on entry nor restore it on exit without fighting the other
    shards.
    """
    doc_count = 0
    failure_count = 0
    next_log_at = LOG_EVERY_N_DOCUMENTS

    # A single parallel_bulk call over one long-lived generator keeps all
    # `elastic_bulk_thread_count` threads saturated while the generator reads ahead
    # from SQLite, overlapping read/transform with the ES bulk requests.
    # raise_on_* are disabled so that a failed document does not abort the whole
    # stream: failures are counted and the task fails at the end instead, which tells
    # us how many documents are missing rather than losing them silently.
    with orjson_bulk_serializer(elastic_connection):
        for success, details in parallel_bulk(
            elastic_connection,
            generate_unite_legale_docs(cursor, elastic_bulk_size, elastic_index),
            thread_count=elastic_bulk_thread_count,
            chunk_size=elastic_bulk_size,
            raise_on_exception=False,
            raise_on_error=False,
        ):
            if success:
                doc_count += 1
                if doc_count >= next_log_at:
                    logger.info(f"Number of documents indexed: {doc_count}")
                    next_log_at += LOG_EVERY_N_DOCUMENTS
            else:
                failure_count += 1
                if failure_count <= MAX_LOGGED_FAILURES:
                    logger.error(f"A document failed to index: {details}")

    logger.info(f"Number of documents indexed: {doc_count}")

    if failure_count:
        raise Exception(
            f"{failure_count} documents failed to index "
            f"({doc_count} succeeded). See the logs for the first "
            f"{MAX_LOGGED_FAILURES} failures."
        )

    return doc_count
