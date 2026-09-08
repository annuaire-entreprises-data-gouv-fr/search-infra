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
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    _worker_ctx.update(
        conn=conn,
        query=sql_query,
        elastic_index=elastic_index,
        thread_count=per_worker_thread_count,
        bulk_size=bulk_size,
    )
    from elasticsearch import Elasticsearch
    _worker_ctx["es"] = Elasticsearch(
        hosts=[ELASTIC_URL],
        basic_auth=(ELASTIC_USER, ELASTIC_PASSWORD),
        retry_on_timeout=True,
    )



def _index_siren_range(args: tuple) -> dict:
    """Indexe la plage (start_siren, end_siren). Retourne le nb de docs."""
    start, end = args
    t0 = time.monotonic()
    log_counter = 0
    cursor = _worker_ctx["conn"].cursor()
    cursor.execute(
        f"SELECT * FROM ({_worker_ctx['query']}) WHERE siren BETWEEN ? AND ? ORDER BY siren",
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
        log_counter += len(rows)
        if log_counter % 100_000 < _worker_ctx["bulk_size"]:
            logger.info(
                f"Range [{start}-{end}]: {log_counter} rows processed, "
                f"{doc_count} docs sent, {time.monotonic() - t0:.0f}s elapsed"
            )
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
    select_query_probe: str,
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


    inner_query = select_query_probe.strip().rstrip(";")
    # Bornes globales + découpage équilibré des plages de siren
    probe = sqlite3.connect(db_path)
    min_s, max_s = probe.execute(
        f"SELECT MIN(siren), MAX(siren) FROM ({inner_query})"
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
                inner_query,   # la requête SQL select_fields_to_index_query
                elastic_index,
                per_worker_threads,
                elastic_bulk_size,
            ),
            maxtasksperchild=1,
        ) as pool:
            for res in pool.imap_unordered(_index_siren_range, ranges):
                done_elapsed = res.get("elapsed", "?")
                logger.info(
                    f"[PROGRESS] Range {res['range']} done: "
                    f"{res['doc_count']} docs, elapsed={res}s"
                )
                total_docs += res["doc_count"]
    finally:
        elastic_connection.indices.put_settings(
            index=elastic_index, body={"index.refresh_interval": None}
        )

    logger.info(f"Total number of documents indexed: {total_docs}")
    return total_docs
