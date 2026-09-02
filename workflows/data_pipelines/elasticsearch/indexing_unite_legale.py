import cProfile
import io
import logging
import pstats
import time
from contextlib import contextmanager
from dataclasses import dataclass, field

from elastic_transport import OrjsonSerializer
from elasticsearch.helpers import expand_action, parallel_bulk

from data_pipelines_annuaire.workflows.data_pipelines.elasticsearch.mapping_index import (
    StructureMapping,
)

# fmt: off
from data_pipelines_annuaire.workflows.data_pipelines.elasticsearch\
    .process_unites_legales import process_unites_legales

# fmt: on
logger = logging.getLogger(__name__)

# TEMPORARY, to be removed once the transform has been profiled: number of unites
# legales to profile at the start of the task, 0 to disable.
PROFILE_TRANSFORM_UNITES_LEGALES = 50_000

LOG_EVERY_N_DOCUMENTS = 1_000_000
MAX_LOGGED_FAILURES = 20
JSON_MIMETYPE = "application/json"


@dataclass
class ProducerTimings:
    """Time spent by the producer thread, split by phase.

    `ThreadPool.imap` drains its iterable from a single task handler thread, so that
    one thread runs everything up to the bulk queue: our generator (fetch, transform,
    build_documents), then `expand_action` and the JSON encoding of every document
    (see TimedJsonSerializer). All five phases are therefore on the critical path and
    all five are accumulated here.

    Only actual work is accumulated: while the producer is blocked by the bulk queue
    back-pressure it sits inside `queue.put` and no timer is running. `wall clock -
    busy` is thus the time spent waiting on Elasticsearch, and `busy / wall clock`
    tells whether the pipeline is producer-bound or Elasticsearch-bound.
    """

    unites_legales_read: int = field(default=0)
    fetch: float = field(default=0.0)
    transform: float = field(default=0.0)
    build_documents: float = field(default=0.0)
    expand: float = field(default=0.0)
    serialize: float = field(default=0.0)

    @property
    def bulk_encode(self) -> float:
        return self.expand + self.serialize

    @property
    def busy(self) -> float:
        return self.fetch + self.transform + self.build_documents + self.bulk_encode


class TimedJsonSerializer:
    """Wraps a JSON serializer to measure the document encoding it does.

    `parallel_bulk` chunks its actions through `_chunk_actions`, which calls
    `serializer.dumps` twice per document (the action header, then the source) in the
    thread that drains our generator. That encoding costs as much as any other
    producer phase but used to be invisible, which made the remaining wall clock look
    like Elasticsearch back-pressure when part of it was our own CPU.

    Only `dumps` is timed: `loads` is called by the bulk worker threads when decoding
    the responses, off the producer thread, and is delegated untouched.
    """

    def __init__(self, serializer, timings) -> None:
        self._serializer = serializer
        self._timings = timings

    def __getattr__(self, name):
        return getattr(self._serializer, name)

    def dumps(self, data):
        started_at = time.perf_counter()
        serialized = self._serializer.dumps(data)
        self._timings.serialize += time.perf_counter() - started_at
        return serialized


def timed_expand_action(timings):
    def expand_and_time(action):
        started_at = time.perf_counter()
        expanded = expand_action(action)
        timings.expand += time.perf_counter() - started_at
        return expanded

    return expand_and_time


@contextmanager
def orjson_bulk_serializer(elastic_connection, timings):
    """Encode the bulk payloads with orjson, and keep measuring the cost.

    `parallel_bulk` resolves its serializer through
    `client.transport.serializers.get_serializer("application/json")`, so replacing
    that entry swaps the implementation for the whole bulk. The 2026-08-31 run spent
    1690s of its 8688s encoding documents with the standard library; orjson measured
    ~7x faster on documents of this shape. `OrjsonSerializer` subclasses
    `JsonSerializer` and only overrides `json_dumps` / `json_loads`, so the `default`
    hook (date, UUID, Decimal) is unchanged.

    The swap is scoped to the indexing: the connection is a process-wide singleton and
    nothing else in the DAG needs it. The timing wrapper stays on top so the gain shows
    up in the logs instead of being assumed.
    """
    serializers = elastic_connection.transport.serializers
    original_serializer = serializers.get_serializer(JSON_MIMETYPE)
    serializers.serializers[JSON_MIMETYPE] = TimedJsonSerializer(
        OrjsonSerializer(), timings
    )
    try:
        yield
    finally:
        serializers.serializers[JSON_MIMETYPE] = original_serializer


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


def log_transform_profile(profiler, unites_legales_read):
    stats_stream = io.StringIO()
    statistics = pstats.Stats(profiler, stream=stats_stream)
    statistics.sort_stats("cumulative").print_stats(30)
    statistics.sort_stats("tottime").print_stats(30)
    logger.info(
        f"Transform profile over the first {unites_legales_read} unites legales. "
        f"The transform timings of that window are inflated by the profiler, the rest "
        f"of the run is unaffected.\n{stats_stream.getvalue()}"
    )


def generate_unite_legale_docs(cursor, elastic_bulk_size, elastic_index, timings):
    # Lazily stream documents to index: pull a batch from SQLite, clean it,
    # and yield each resulting document. Feeding a single long-lived generator to
    # parallel_bulk lets the read/transform overlap with the ES bulk requests
    # instead of running them serially per batch.
    #
    # Each phase is materialised rather than chained lazily so that its cost can be
    # measured separately, see ProducerTimings.
    unite_legale_columns = tuple(x[0] for x in cursor.description)
    # `transform` is the largest phase of the run and calls ~40 helpers per unite
    # legale, so it needs a profile rather than a guess. Over the first chunks only.
    profiler = cProfile.Profile() if PROFILE_TRANSFORM_UNITES_LEGALES else None
    while True:
        started_at = time.perf_counter()
        chunk_unites_legales_sqlite = cursor.fetchmany(elastic_bulk_size)
        timings.fetch += time.perf_counter() - started_at

        if not chunk_unites_legales_sqlite:
            return

        started_at = time.perf_counter()
        if profiler:
            profiler.enable()
        liste_unites_legales_sqlite = tuple(
            dict(zip(unite_legale_columns, unite_legale))
            for unite_legale in chunk_unites_legales_sqlite
        )
        chunk_unites_legales_processed = process_unites_legales(
            liste_unites_legales_sqlite
        )
        if profiler:
            profiler.disable()
        timings.transform += time.perf_counter() - started_at

        started_at = time.perf_counter()
        documents = list(
            doc_unite_legale_generator(chunk_unites_legales_processed, elastic_index)
        )
        timings.build_documents += time.perf_counter() - started_at

        timings.unites_legales_read += len(chunk_unites_legales_sqlite)

        if profiler and timings.unites_legales_read >= PROFILE_TRANSFORM_UNITES_LEGALES:
            log_transform_profile(profiler, timings.unites_legales_read)
            profiler = None

        yield from documents


def log_indexing_progress(doc_count, started_at, timings):
    elapsed = time.perf_counter() - started_at
    waiting = elapsed - timings.busy
    logger.info(
        f"Indexed {doc_count} documents from {timings.unites_legales_read} unites "
        f"legales in {elapsed:.0f}s. Producer busy {timings.busy:.0f}s "
        f"({timings.busy / elapsed:.0%} of wall clock): "
        f"sqlite fetch {timings.fetch:.0f}s, "
        f"transform {timings.transform:.0f}s, "
        f"document build {timings.build_documents:.0f}s, "
        f"bulk encode {timings.bulk_encode:.0f}s "
        f"(expand {timings.expand:.0f}s + json {timings.serialize:.0f}s). "
        f"Waiting on elasticsearch {waiting:.0f}s ({waiting / elapsed:.0%})"
    )


def index_unites_legales_by_chunk(
    cursor,
    elastic_connection,
    elastic_bulk_thread_count,
    elastic_bulk_size,
    elastic_index,
):
    """Index the documents the cursor yields, and return how many were indexed.

    The index settings (`refresh_interval`, `translog.durability`) are handled by the
    tasks surrounding this one: the function is called once per siren shard, so it can
    neither disable refresh on entry nor restore it on exit without fighting the other
    shards.
    """
    timings = ProducerTimings()
    started_at = time.perf_counter()
    doc_count = 0
    failure_count = 0
    next_log_at = LOG_EVERY_N_DOCUMENTS

    # A single parallel_bulk call over one long-lived generator keeps all
    # `elastic_bulk_thread_count` threads saturated while the generator reads ahead
    # from SQLite, overlapping read/transform with the ES bulk requests.
    # raise_on_* are disabled so that a failed document does not abort the whole
    # stream: failures are counted and the task fails at the end instead, which tells
    # us how many documents are missing rather than losing them silently.
    with orjson_bulk_serializer(elastic_connection, timings):
        for success, details in parallel_bulk(
            elastic_connection,
            generate_unite_legale_docs(
                cursor, elastic_bulk_size, elastic_index, timings
            ),
            thread_count=elastic_bulk_thread_count,
            chunk_size=elastic_bulk_size,
            expand_action_callback=timed_expand_action(timings),
            raise_on_exception=False,
            raise_on_error=False,
        ):
            if success:
                doc_count += 1
                if doc_count >= next_log_at:
                    log_indexing_progress(doc_count, started_at, timings)
                    next_log_at += LOG_EVERY_N_DOCUMENTS
            else:
                failure_count += 1
                if failure_count <= MAX_LOGGED_FAILURES:
                    logger.error(f"A document failed to index: {details}")

    log_indexing_progress(doc_count, started_at, timings)

    if failure_count:
        raise Exception(
            f"{failure_count} documents failed to index "
            f"({doc_count} succeeded). See the logs for the first "
            f"{MAX_LOGGED_FAILURES} failures."
        )

    return doc_count
