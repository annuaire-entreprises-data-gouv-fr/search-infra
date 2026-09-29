import logging

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
    """Lazy stream of the documents to index.

    Args:
        cursor (sqlite3.Cursor): Cursor on which the indexing query was executed.
        elastic_bulk_size (int): Number of rows fetched from SQLite per batch.
        elastic_index (str): Name of the index the documents are sent to.

    Yields:
        dict: A document ready for the bulk API, metadata included.
    """
    unite_legale_columns = tuple(x[0] for x in cursor.description)
    while chunk_unites_legales_sqlite := cursor.fetchmany(elastic_bulk_size):
        yield from doc_unite_legale_generator(
            process_unites_legales(
                dict(zip(unite_legale_columns, unite_legale))
                for unite_legale in chunk_unites_legales_sqlite
            ),
            elastic_index,
        )


def index_unites_legales_by_chunk(
    cursor,
    elastic_connection,
    elastic_bulk_thread_count,
    elastic_bulk_size,
    elastic_index,
):
    """Index documents yielded from the cursor and return how many were indexed."""
    doc_count = 0
    failure_count = 0
    next_log_at = LOG_EVERY_N_DOCUMENTS

    # Bulk index documents into elasticsearch using the parallel version of the
    # bulk helper that runs in multiple threads
    # The bulk helper accept an instance of Elasticsearch class and an
    # iterable, a generator in our case
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
