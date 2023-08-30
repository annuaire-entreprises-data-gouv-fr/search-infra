import logging

from dag_datalake_sirene.elasticsearch.mapping_index import (
    StructureMapping,
)

from dag_datalake_sirene.elasticsearch.process_association import (
    process_association,
)

from elasticsearch.helpers import parallel_bulk


def doc_association_generator(data):
    # Serialize the instance into a dictionary so that it can be saved in elasticsearch.
    for index, document in enumerate(data):
        yield StructureMapping(
            meta={"id": document["identifiant"]}, **document
        ).to_dict(include_meta=True)


def index_association_by_chunk(
    cursor, elastic_connection, elastic_bulk_size, elastic_index
):
    logger = 0
    chunk_associations_sqlite = 1
    while chunk_associations_sqlite:
        chunk_associations_sqlite = cursor.fetchmany(elastic_bulk_size)
        association_columns = tuple([x[0] for x in cursor.description])
        liste_associations_sqlite = []
        # Group all fetched associations from sqlite in one list
        for association in chunk_associations_sqlite:
            liste_associations_sqlite.append(
                {
                    association_columns: value
                    for association_columns, value in zip(
                        association_columns, association
                    )
                }
            )

        liste_associations_sqlite = tuple(liste_associations_sqlite)

        chunk_associations_processed = process_association(liste_associations_sqlite)
        logger += 1
        if logger % 100000 == 0:
            logging.info(f"logger={logger}")
        try:
            chunk_doc_generator = doc_association_generator(
                chunk_associations_processed
            )
            # Bulk index documents into elasticsearch using the parallel version of the
            # bulk helper that runs in multiple threads
            # The bulk helper accept an instance of Elasticsearch class and an
            # iterable, a generator in our case
            for success, details in parallel_bulk(
                elastic_connection, chunk_doc_generator, chunk_size=elastic_bulk_size
            ):
                if not success:
                    raise Exception(f"A file_access document failed: {details}")
        except Exception as e:
            logging.error(f"Failed to send to Elasticsearch: {e}")
        doc_count = elastic_connection.cat.count(
            index=elastic_index, params={"format": "json"}
        )[0]["count"]
        logging.info(f"Number of associations documents indexed: {doc_count}")
    return doc_count
