import logging

from elasticsearch.helpers import parallel_bulk

from data_pipelines_annuaire.workflows.data_pipelines.elasticsearch.mapping_index import (
    StructureMapping,
)
from data_pipelines_annuaire.workflows.data_pipelines.elasticsearch.structure_type import (
    StructureType,
)


def format_adresse(fondation):
    parts = [fondation.get(field) for field in ["adresse", "code_postal", "ville"]]
    return " ".join(part.strip() for part in parts if part and part.strip())


def doc_fondation_generator(fondations, elastic_index):
    for fondation in fondations:
        yield StructureMapping(
            meta={"index": elastic_index, "id": fondation["numero_rnf"]},
            identifiant=fondation["numero_rnf"],
            type_structure=[StructureType.FONDATION],
            nom_complet=fondation["titre"],
            adresse=format_adresse(fondation),
            fondation=fondation,
        ).to_dict(include_meta=True)


def index_fondations_by_chunk(
    cursor,
    elastic_connection,
    elastic_bulk_thread_count,
    elastic_bulk_size,
    elastic_index,
):
    doc_count = 0
    chunk_fondations_sqlite = cursor.fetchmany(elastic_bulk_size)
    while chunk_fondations_sqlite:
        fondation_columns = tuple([x[0] for x in cursor.description])
        fondations = tuple(
            dict(zip(fondation_columns, fondation))
            for fondation in chunk_fondations_sqlite
        )

        chunk_doc_generator = doc_fondation_generator(fondations, elastic_index)
        for success, details in parallel_bulk(
            elastic_connection,
            chunk_doc_generator,
            thread_count=elastic_bulk_thread_count,
            chunk_size=elastic_bulk_size,
        ):
            if not success:
                raise Exception(f"A fondation document failed: {details}")
            doc_count += 1

        chunk_fondations_sqlite = cursor.fetchmany(elastic_bulk_size)

    logging.info(f"Number of fondations indexed: {doc_count}")
    return doc_count
