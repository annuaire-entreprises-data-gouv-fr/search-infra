import logging

from dag_datalake_sirene.elasticsearch.mapping_sirene_index import \
    ElasticsearchSireneIndex
from dag_datalake_sirene.elasticsearch.process_unites_legales import \
    process_unites_legales
from elasticsearch import helpers


def elasticsearch_doc_generator(data):
    # Serialize the instance into a dictionary so that it can be saved in elasticsearch.
    for index, document in enumerate(data):
        yield ElasticsearchSireneIndex(
            meta={"id": document["siret_siege"]}, **document
        ).to_dict(include_meta=True)


def index_unites_legales_by_chunk(
    cursor, elastic_connection, elastic_bulk_size, elastic_index
):
    logger = 0
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
        if logger % 1000 == 0:
            logging.info(f"logger={logger}")
        try:
            chunk_doc_generator = elasticsearch_doc_generator(
                chunk_unites_legales_processed
            )
            # Bulk index documents into elasticsearch using the parallel version of the
            # bulk helper that runs in multiple threads
            # The bulk helper accept an instance of Elasticsearch class and an
            # iterable, a generator in our case
            for success, details in helpers.parallel_bulk(
                elastic_connection, chunk_doc_generator, chunk_size=elastic_bulk_size
            ):
                if not success:
                    raise Exception(f"A file_access document failed: {details}")
        except Exception as e:
            logging.error(f"Failed to send to Elasticsearch: {e}")
        doc_count = elastic_connection.cat.count(
            index=elastic_index, params={"format": "json"}
        )[0]["count"]
        logging.info(f"Number of documents indexed: {doc_count}")
    return doc_count
