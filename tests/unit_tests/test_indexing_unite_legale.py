from types import SimpleNamespace

import pytest
from elastic_transport import SerializerCollection
from elasticsearch import Elasticsearch
from elasticsearch.helpers import expand_action, parallel_bulk

from data_pipelines_annuaire.workflows.data_pipelines.elasticsearch.indexing_unite_legale import (
    JSON_MIMETYPE,
    ProducerTimings,
    TimedJsonSerializer,
    time_bulk_encoding,
    timed_expand_action,
)


@pytest.fixture
def timings():
    return ProducerTimings()


@pytest.fixture
def elastic_connection():
    return SimpleNamespace(
        transport=SimpleNamespace(serializers=SerializerCollection())
    )


def test_busy_sums_every_phase_running_on_the_producer_thread():
    timings = ProducerTimings(
        fetch=1.0, transform=2.0, build_documents=4.0, expand=8.0, serialize=16.0
    )

    assert timings.bulk_encode == 24.0
    assert timings.busy == 31.0


def test_timed_json_serializer_returns_the_wrapped_result_and_accumulates(timings):
    serializer = SerializerCollection().get_serializer(JSON_MIMETYPE)
    timed_serializer = TimedJsonSerializer(serializer, timings)

    assert timed_serializer.dumps({"siren": "356000000"}) == serializer.dumps(
        {"siren": "356000000"}
    )
    assert timings.serialize > 0

    first_measure = timings.serialize
    timed_serializer.dumps({"siren": "552100554"})

    assert timings.serialize > first_measure


def test_timed_json_serializer_delegates_everything_but_dumps(timings):
    serializer = SerializerCollection().get_serializer(JSON_MIMETYPE)
    timed_serializer = TimedJsonSerializer(serializer, timings)

    # Responses are decoded by the bulk worker threads, so loads must stay untimed.
    assert timed_serializer.loads(b'{"siren": "356000000"}') == {"siren": "356000000"}
    assert timings.serialize == 0
    assert timed_serializer.mimetype == serializer.mimetype


def test_timed_expand_action_matches_expand_action_and_accumulates(timings):
    document = {"_index": "siren-20260827", "_id": "356000000-100", "nom": "SNCF"}

    expanded = timed_expand_action(timings)(dict(document))

    assert expanded == expand_action(dict(document))
    assert timings.expand > 0


def test_time_bulk_encoding_installs_the_timing_serializer(elastic_connection, timings):
    serializers = elastic_connection.transport.serializers
    original_serializer = serializers.get_serializer(JSON_MIMETYPE)

    with time_bulk_encoding(elastic_connection, timings):
        installed_serializer = serializers.get_serializer(JSON_MIMETYPE)
        installed_serializer.dumps({"siren": "356000000"})

    assert isinstance(installed_serializer, TimedJsonSerializer)
    assert timings.serialize > 0
    assert serializers.get_serializer(JSON_MIMETYPE) is original_serializer


def test_time_bulk_encoding_restores_the_serializer_on_failure(
    elastic_connection, timings
):
    serializers = elastic_connection.transport.serializers
    original_serializer = serializers.get_serializer(JSON_MIMETYPE)

    with pytest.raises(ValueError), time_bulk_encoding(elastic_connection, timings):
        raise ValueError("indexing failed")

    assert serializers.get_serializer(JSON_MIMETYPE) is original_serializer


def test_parallel_bulk_feeds_both_counters(timings):
    """Guard the wiring: parallel_bulk must reach the wrapped serializer and the
    wrapped expand_action, otherwise the run reports a silent zero and the missing
    wall clock is blamed on Elasticsearch again."""
    client = Elasticsearch("http://elastic.test:9200")
    documents = [
        {"_index": "siren-20260827", "_id": f"{siren}-100", "nom_complet": "SNCF"}
        for siren in range(100)
    ]

    def fake_bulk(*args, operations, **kwargs):
        # One response item per action/source pair of the ndjson payload.
        return SimpleNamespace(
            body={
                "errors": False,
                "items": [
                    {"index": {"status": 201}} for _ in range(len(operations) // 2)
                ],
            }
        )

    client.bulk = fake_bulk

    with time_bulk_encoding(client, timings):
        indexed = sum(
            success
            for success, _ in parallel_bulk(
                client,
                documents,
                thread_count=2,
                chunk_size=10,
                expand_action_callback=timed_expand_action(timings),
            )
        )

    assert indexed == len(documents)
    assert timings.expand > 0
    assert timings.serialize > 0
