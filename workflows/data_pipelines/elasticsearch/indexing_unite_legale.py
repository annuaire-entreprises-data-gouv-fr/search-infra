import logging
import time

from elasticsearch.helpers import parallel_bulk

from dag_datalake_sirene.workflows.data_pipelines.elasticsearch.mapping_index import (
    StructureMapping,
)

# fmt: off
from dag_datalake_sirene.workflows.data_pipelines.elasticsearch\
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


def index_unites_legales_by_chunk(
    cursor,
    elastic_connection,
    elastic_bulk_thread_count,
    elastic_bulk_size,
    elastic_index,
):
    # Indexing performance : do not refresh the index while indexing
    elastic_connection.indices.put_settings(
        index=elastic_index, body={"index.refresh_interval": -1}
    )

    logger = 0
    doc_count = 0
    chunk_unites_legales_sqlite = 1
    while chunk_unites_legales_sqlite:
        chunk_unites_legales_sqlite = cursor.fetchmany(elastic_bulk_size)
        unite_legale_columns = tuple([x[0] for x in cursor.description])
        liste_unites_legales_sqlite = []
        # Group all fetched unites_legales from sqlite in one list
        for unite_legale in chunk_unites_legales_sqlite:
            liste_unites_legales_sqlite.append(
                {
                    unite_legale_columns: value
                    for unite_legale_columns, value in zip(
                        unite_legale_columns, unite_legale
                    )
                }
            )

        liste_unites_legales_sqlite = tuple(liste_unites_legales_sqlite)

        chunk_unites_legales_processed = process_unites_legales(
            liste_unites_legales_sqlite
        )
        logger += 1
        if logger % 100000 == 0:
            logging.info(f"logger={logger}")
        try:
            chunk_doc_generator = doc_unite_legale_generator(
                chunk_unites_legales_processed, elastic_index
            )
            # Bulk index documents into elasticsearch using the parallel version of the
            # bulk helper that runs in multiple threads
            # The bulk helper accept an instance of Elasticsearch class and an
            # iterable, a generator in our case
            for success, details in parallel_bulk(
                elastic_connection,
                chunk_doc_generator,
                thread_count=elastic_bulk_thread_count,
                chunk_size=elastic_bulk_size,
            ):
                if not success:
                    raise Exception(f"A file_access document failed: {details}")
                else:
                    doc_count += 1
        except Exception as e:
            logging.error(f"Failed to send to Elasticsearch: {e}")
        logging.info(f"Number of documents indexed: {doc_count}")

    # rollback to the original value
    elastic_connection.indices.put_settings(
        index=elastic_index, body={"index.refresh_interval": None}
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
