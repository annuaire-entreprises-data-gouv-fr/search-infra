import logging
import time

from elasticsearch.helpers import parallel_bulk

from data_pipelines_annuaire.workflows.data_pipelines.elasticsearch.mapping_index import (
    StructureMapping,
)

# fmt: off
from data_pipelines_annuaire.workflows.data_pipelines.elasticsearch\
    .process_unites_legales import process_unites_legales

# fmt: on


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
    # parallel_bulk lets the read/transform (main thread) overlap with the ES bulk
    # requests (worker threads) instead of running them serially per batch.
    unite_legale_columns = tuple(x[0] for x in cursor.description)
    processed_count = 0
    chunk_unites_legales_sqlite = cursor.fetchmany(elastic_bulk_size)
    while chunk_unites_legales_sqlite:
        liste_unites_legales_sqlite = tuple(
            dict(zip(unite_legale_columns, unite_legale))
            for unite_legale in chunk_unites_legales_sqlite
        )
        chunk_unites_legales_processed = process_unites_legales(
            liste_unites_legales_sqlite
        )
        yield from doc_unite_legale_generator(
            chunk_unites_legales_processed, elastic_index
        )
        processed_count += len(chunk_unites_legales_sqlite)
        if processed_count % 1000000 == 0:
            logging.info(f"Processed {processed_count} unites legales")
        chunk_unites_legales_sqlite = cursor.fetchmany(elastic_bulk_size)


def index_unites_legales_by_chunk(
    cursor,
    elastic_connection,
    elastic_bulk_thread_count,
    elastic_bulk_size,
    elastic_index,
):
    # Indexing performance : do not refresh the index nor fsync the translog on
    # every request while bulk loading.
    elastic_connection.indices.put_settings(
        index=elastic_index,
        body={
            "index.refresh_interval": -1,
            "index.translog.durability": "async",
        },
    )

    doc_count = 0
    # A single parallel_bulk call over one long-lived generator keeps all
    # `elastic_bulk_thread_count` threads saturated while the generator reads
    # ahead from SQLite, overlapping read/transform with the ES bulk requests.
    # raise_on_* are disabled so a single failed document is logged instead of
    # aborting the whole stream; the final cat.count gates correctness.
    for success, details in parallel_bulk(
        elastic_connection,
        generate_unite_legale_docs(cursor, elastic_bulk_size, elastic_index),
        thread_count=elastic_bulk_thread_count,
        chunk_size=elastic_bulk_size,
        queue_size=elastic_bulk_thread_count * 2,
        raise_on_exception=False,
        raise_on_error=False,
    ):
        if success:
            doc_count += 1
            if doc_count % 1000000 == 0:
                logging.info(f"Number of documents indexed: {doc_count}")
        else:
            logging.error(f"A document failed to index: {details}")

    # rollback to the original values
    elastic_connection.indices.put_settings(
        index=elastic_index,
        body={
            "index.refresh_interval": None,
            "index.translog.durability": None,
        },
    )

    # Indexing performance :
    #
    # The _/cat/count/{index} is called only once at the end of the indexing process
    # and not after each pushed bulk
    #
    # i.e. the _cat/count/{index} produce a query that may force Lucene to refresh the
    # last bulk into a segment
    # meaning that it would amplify the amount of segment merge and slowdown the
    # indexing process

    # Add wait and retry mechanism for zero count
    max_retries = 5
    retry_interval = 5  # seconds

    for attempt in range(max_retries):
        doc_count = int(
            elastic_connection.cat.count(
                index=elastic_index, params={"format": "json"}
            )[0]["count"]
        )

        if doc_count > 0:
            break

        if attempt < max_retries - 1:
            logging.warning(
                f"Document count is zero. Retrying in {retry_interval} seconds..."
            )
            time.sleep(retry_interval)
        else:
            logging.error("Max retries reached. Document count is still zero.")

    return doc_count
