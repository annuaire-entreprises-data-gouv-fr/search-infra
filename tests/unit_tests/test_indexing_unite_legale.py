import logging
from types import SimpleNamespace

import pytest
from elastic_transport import JsonSerializer, OrjsonSerializer, SerializerCollection
from elasticsearch import Elasticsearch
from elasticsearch.helpers import expand_action, parallel_bulk

from data_pipelines_annuaire.workflows.data_pipelines.elasticsearch import (
    indexing_unite_legale,
)
from data_pipelines_annuaire.workflows.data_pipelines.elasticsearch.indexing_unite_legale import (
    JSON_MIMETYPE,
    ProducerTimings,
    TimedJsonSerializer,
    doc_unite_legale_generator,
    generate_unite_legale_docs,
    orjson_bulk_serializer,
    timed_expand_action,
)
from data_pipelines_annuaire.workflows.data_pipelines.elasticsearch.structure_type import (
    StructureType,
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


def test_orjson_bulk_serializer_installs_orjson_and_times_it(
    elastic_connection, timings
):
    serializers = elastic_connection.transport.serializers
    original_serializer = serializers.get_serializer(JSON_MIMETYPE)

    with orjson_bulk_serializer(elastic_connection, timings):
        installed_serializer = serializers.get_serializer(JSON_MIMETYPE)
        installed_serializer.dumps({"siren": "356000000"})

    assert isinstance(installed_serializer, TimedJsonSerializer)
    assert isinstance(installed_serializer._serializer, OrjsonSerializer)
    assert timings.serialize > 0
    assert serializers.get_serializer(JSON_MIMETYPE) is original_serializer


def test_orjson_bulk_serializer_restores_the_serializer_on_failure(
    elastic_connection, timings
):
    serializers = elastic_connection.transport.serializers
    original_serializer = serializers.get_serializer(JSON_MIMETYPE)

    with pytest.raises(ValueError), orjson_bulk_serializer(elastic_connection, timings):
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

    with orjson_bulk_serializer(client, timings):
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


def processed_unite_legale(nombre_etablissements=1):
    """A document in the shape `doc_unite_legale_generator` receives, with the value
    types that survive `process_unites_legales`: accents, None, bool, int, float,
    the `StructureType` enum, empty and nested containers."""
    etablissement = {
        "siret": "35600000000048",
        "nom_complet": "SOCIÉTÉ NATIONALE DES CHEMINS DE FER FRANÇAIS",
        "adresse": "2 PLACE AUX ÉTOILES 93200 SAINT-DENIS",
        "est_siege": True,
        "ancien_siege": False,
        "latitude": 48.936,
        "longitude": 2.357,
        "liste_idcc": None,
        "liste_rge": [],
        "liste_uai": ["0930943C"],
        "successions": {"predecesseurs": [], "successeurs": None},
        "date_creation": "1983-01-01",
    }
    return {
        "identifiant": "356000000",
        "type_structure": [StructureType.UNITE_LEGALE, StructureType.FONDATION],
        "nom_complet": "SOCIÉTÉ NATIONALE DES CHEMINS DE FER FRANÇAIS",
        "fondation": None,
        "unite_legale": {
            "siren": "356000000",
            "nom_complet": "SOCIÉTÉ NATIONALE DES CHEMINS DE FER FRANÇAIS",
            "sigle": "SNCF",
            "est_association": False,
            "nombre_etablissements_ouverts": 2843,
            "facteur_taille_entreprise": 25.5,
            "date_mise_a_jour": "2026-08-31T09:44:01",
            "date_creation_unite_legale": "1983-01-01",
            "bilan_financier": {"ca": 1000000, "resultat_net": -50000},
            "immatriculation": {},
            "liste_dirigeants": ["JEAN DUPONT"],
            "etablissements": [
                dict(etablissement, siret=f"3560000000{index:04d}")
                for index in range(nombre_etablissements)
            ],
        },
    }


@pytest.mark.parametrize("nombre_etablissements", [1, 150])
def test_orjson_encodes_our_documents_exactly_like_the_standard_library(
    nombre_etablissements,
):
    """orjson is stricter than the standard library (no tuples, no non-str keys) and
    encodes dates natively instead of going through `default`. Guard that the payloads
    we actually send are unchanged — including the >100 établissements split, whose
    documents go through a different branch."""
    documents = list(
        doc_unite_legale_generator(
            [processed_unite_legale(nombre_etablissements)], "siren-20260831"
        )
    )

    assert documents
    for document in documents:
        assert OrjsonSerializer().dumps(document) == JsonSerializer().dumps(document)


class FakeCursor:
    """Minimal stand-in for the sqlite cursor drained by the producer."""

    description = (("siren",),)

    def __init__(self, rows):
        self._rows = list(rows)

    def fetchmany(self, size):
        batch, self._rows = self._rows[:size], self._rows[size:]
        return batch


@pytest.fixture
def cursor_and_transform(monkeypatch):
    monkeypatch.setattr(
        indexing_unite_legale,
        "process_unites_legales",
        lambda rows: [processed_unite_legale() for _ in rows],
    )
    return FakeCursor([("356000000",)] * 4)


def transform_profiles(caplog):
    return [
        record.message
        for record in caplog.records
        if "Transform profile" in record.message
    ]


def test_transform_profile_is_logged_once_when_armed(
    cursor_and_transform, caplog, monkeypatch, timings
):
    monkeypatch.setattr(indexing_unite_legale, "PROFILE_TRANSFORM_UNITES_LEGALES", 2)

    with caplog.at_level(logging.INFO):
        list(
            generate_unite_legale_docs(
                cursor_and_transform, 1, "siren-20260831", timings
            )
        )

    profiles = transform_profiles(caplog)
    assert len(profiles) == 1
    assert "Ordered by: cumulative time" in profiles[0]
    assert "Ordered by: internal time" in profiles[0]


def test_transform_is_not_profiled_when_disabled(
    cursor_and_transform, caplog, monkeypatch, timings
):
    monkeypatch.setattr(indexing_unite_legale, "PROFILE_TRANSFORM_UNITES_LEGALES", 0)

    with caplog.at_level(logging.INFO):
        documents = list(
            generate_unite_legale_docs(
                cursor_and_transform, 1, "siren-20260831", timings
            )
        )

    assert documents
    assert transform_profiles(caplog) == []
