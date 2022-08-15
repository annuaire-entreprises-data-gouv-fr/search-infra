import logging

from dag_datalake_sirene.elasticsearch.mapping_siren import Siren
from dag_datalake_sirene.elasticsearch.process_doc import process_doc
from elasticsearch import helpers


def doc_generator(data):
    # Serialize the instance into a dictionary so that it can be saved in elasticsearch.
    for index, document in enumerate(data):
        yield Siren(meta={"id": document["siret_siege"]}, **document).to_dict(
            include_meta=True
        )


def index_by_chunk(cursor, elastic_connection, elastic_bulk_size, elastic_index):
    i = 0
    res = 1
    while res:
        res = cursor.fetchmany(elastic_bulk_size)
        columns = tuple([x[0] for x in cursor.description])
        res = tuple([{column: val for column, val in zip(columns, x)} for x in res])
        res2 = process_doc(res)
        i = i + 1
        if i % 1000 == 0:
            logging.info("i={i}")
        try:
            for success, details in helpers.parallel_bulk(
                elastic_connection, doc_generator(res2), chunk_size=elastic_bulk_size
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
