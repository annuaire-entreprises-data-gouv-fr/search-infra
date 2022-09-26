import logging
import time

from dag_datalake_sirene.elasticsearch.mapping_sirene_index import (
    ElasticsearchEtablissement,
    ElasticsearchSireneIndex,
)
from dag_datalake_sirene.elasticsearch.process_etablissements import (
    process_etablissements,
)
from elasticsearch import helpers


def elasticsearch_doc_siret_generator(data, elastic_index):
    # Serialize the instance into a dictionary so that it can be saved in elasticsearch.
    for index, document in enumerate(data):
        yield ElasticsearchEtablissement(
            meta={
                "id": document["siret"],
                "routing": document["siren"],
                "index": elastic_index,
            },
            **document,
        ).to_dict(include_meta=True)


def index_etablissements_by_chunk(
    cursor, elastic_connection, elastic_bulk_size, elastic_index
):
    logger = 0
    chunk_etablissements_sqlite = 1
    while chunk_etablissements_sqlite:
        chunk_etablissements_sqlite = cursor.fetchmany(elastic_bulk_size)
        etablissement_columns = tuple([x[0] for x in cursor.description])
        liste_etablissements_sqlite = []
        # Group all fetched unites_legales from sqlite in one list
        for etablissement in chunk_etablissements_sqlite:
            liste_etablissements_sqlite.append(
                {
                    etablissement_columns: value
                    for etablissement_columns, value in zip(
                        etablissement_columns, etablissement
                    )
                }
            )

        liste_etablissements_sqlite = tuple(liste_etablissements_sqlite)

        chunk_etablissements_processed = process_etablissements(
            liste_etablissements_sqlite
        )
        # logging.info(f"{chunk_etablissements_processed[0]}")
        logger += 1
        if logger % 1000 == 0:
            logging.info(f"logger={logger}")
        try:
            chunk_doc_generator = elasticsearch_doc_siret_generator(
                chunk_etablissements_processed, elastic_index
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
            time.sleep(1)
        except Exception as e:
            logging.error(f"Failed to send to Elasticsearch: {e}")
        doc_count = elastic_connection.cat.count(
            index=elastic_index, params={"format": "json"}
        )[0]["count"]
        logging.info(f"Number of documents indexed: {doc_count}")
    return doc_count
