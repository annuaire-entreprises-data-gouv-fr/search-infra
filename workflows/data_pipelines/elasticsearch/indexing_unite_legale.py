import logging
import time
import multiprocessing
import sqlite3


from elasticsearch.helpers import parallel_bulk

from data_pipelines_annuaire.workflows.data_pipelines.elasticsearch.mapping_index import (
    StructureMapping,
)

# fmt: off
from data_pipelines_annuaire.workflows.data_pipelines.elasticsearch\
    .process_unites_legales import process_unites_legales

# fmt: on
from data_pipelines_annuaire.config import (
    ELASTIC_URL,
    ELASTIC_USER,
    ELASTIC_PASSWORD,
    ELASTIC_INDEX_POOL_PROCESSES,
)

# fmt: on
logger = logging.getLogger(__name__)



# ---------------------------------------------------------------
# Contexte par process worker : rempli une seule fois par fils
# ---------------------------------------------------------------
_worker_ctx = {}


def _worker_init(
    db_path: str,
    sql_query: str,
    elastic_index: str,
    per_worker_thread_count: int,
    bulk_size: int,
) -> None:
    """
    Exécuté une fois par process fils (via Pool initializer).
    Les connexions ne sont JAMAIS héritées du parent ni passées en argument.
    """
    from elasticsearch import connections as es_connections

    # SQLite : connexion dédiée au process, en lecture seule de facto
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    cursor = conn.cursor()
    # On borne la requête existante sur la plage de siren du worker.
    # La requête est enveloppée en sous-requête : elle doit exposer une
    # colonne `siren` et ne pas contenir de LIMIT.
    cursor.execute(
        f"SELECT * FROM ({sql_query}) WHERE siren BETWEEN ? AND ?",
        (None, None),  # remplacé ci-dessous par les bornes réelles
    )
    cursor.close()

    _worker_ctx["conn"] = conn
    _worker_ctx["query"] = sql_query
    _worker_ctx["elastic_index"] = elastic_index
    _worker_ctx["thread_count"] = per_worker_thread_count
    _worker_ctx["bulk_size"] = bulk_size

    es_connections.create_connection(
        hosts=[ELASTIC_URL],
        basic_auth=(ELASTIC_USER, ELASTIC_PASSWORD),
        retry_on_timeout=True,
    )
    _worker_ctx["es"] = es_connections.get_connection()


def _index_siren_range(args: tuple) -> dict:
    """Indexe la plage (start_siren, end_siren). Retourne le nb de docs."""
    start, end = args
    cursor = _worker_ctx["conn"].cursor()
    cursor.execute(
        f"SELECT * FROM ({_worker_ctx['query']}) WHERE siren BETWEEN ? AND ?",
        (start, end),
    )

    doc_count = 0
    chunk = cursor.fetchmany(_worker_ctx["bulk_size"])
    while chunk:
        columns = tuple(x[0] for x in cursor.description)
        rows = tuple(
            {col: value for col, value in zip(columns, row)}
            for row in chunk
        )
        processed = process_unites_legales(rows)
        docs = doc_unite_legale_generator(
            processed, _worker_ctx["elastic_index"]
        )
        for success, details in parallel_bulk(
            _worker_ctx["es"],
            docs,
            thread_count=_worker_ctx["thread_count"],  # 1-2 par worker
            chunk_size=_worker_ctx["bulk_size"],
        ):
            if not success:
                raise Exception(f"A file_access document failed: {details}")
            doc_count += 1
        chunk = cursor.fetchmany(_worker_ctx["bulk_size"])

    cursor.close()
    logger.info(f"Range [{start} - {end}] done: {doc_count} docs")
    return {"range": (start, end), "doc_count": doc_count}

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
    db_path: str,
    sql_query: str,
    elastic_connection,
    elastic_bulk_thread_count: int,
    elastic_bulk_size: int,
    elastic_index: str,
    pool_process_count: int = ELASTIC_INDEX_POOL_PROCESSES,
) -> int:
    # Indexing performance : do not refresh the index while indexing.
    # Fait par le parent UNE fois, pas dans les workers.
    elastic_connection.indices.put_settings(
        index=elastic_index, body={"index.refresh_interval": -1}
    )

    logger.info("=== CODE VERSION 2026-09-08-POOL ===")

    # Bornes globales + découpage équilibré des plages de siren
    probe = sqlite3.connect(db_path)
    min_s, max_s = probe.execute(
        f"SELECT MIN(siren), MAX(siren) FROM ({select_query_probe})"
    ).fetchone()
    probe.close()

    n = pool_process_count
    step = (int(max_s) - int(min_s)) // n + 1
    ranges = [
        (int(min_s) + i * step, min(int(min_s) + (i + 1) * step - 1, int(max_s)))
        for i in range(n)
    ]

    # Le budget total de threads ES est réparti entre les workers
    per_worker_threads = max(1, elastic_bulk_thread_count // n)

    total_docs = 0
    ctx = multiprocessing.get_context("fork")  # Linux/docker
    try:
        with ctx.Pool(
            processes=n,
            initializer=_worker_init,
            initargs=(
                db_path,
                select_query_probe,   # la requête SQL select_fields_to_index_query
                elastic_index,
                per_worker_threads,
                elastic_bulk_size,
            ),
            maxtasksperchild=1,
        ) as pool:
            for res in pool.imap_unordered(_index_siren_range, ranges):
                logger.info(f"Pool result: {res}")
                total_docs += res["doc_count"]
    finally:
        elastic_connection.indices.put_settings(
            index=elastic_index, body={"index.refresh_interval": None}
        )

    logger.info(f"Total number of documents indexed: {total_docs}")
    return total_docs
