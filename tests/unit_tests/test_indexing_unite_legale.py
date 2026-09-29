from types import SimpleNamespace

import pytest
from elastic_transport import JsonSerializer, OrjsonSerializer, SerializerCollection

from data_pipelines_annuaire.workflows.data_pipelines.elasticsearch import (
    indexing_unite_legale,
)
from data_pipelines_annuaire.workflows.data_pipelines.elasticsearch.indexing_unite_legale import (
    JSON_MIMETYPE,
    doc_unite_legale_generator,
    generate_unite_legale_docs,
    orjson_bulk_serializer,
)
from data_pipelines_annuaire.workflows.data_pipelines.elasticsearch.structure_type import (
    StructureType,
)


@pytest.fixture
def elastic_connection():
    return SimpleNamespace(
        transport=SimpleNamespace(serializers=SerializerCollection())
    )


def test_orjson_bulk_serializer_installs_orjson(elastic_connection):
    serializers = elastic_connection.transport.serializers
    original_serializer = serializers.get_serializer(JSON_MIMETYPE)

    with orjson_bulk_serializer(elastic_connection):
        installed_serializer = serializers.get_serializer(JSON_MIMETYPE)

    assert isinstance(installed_serializer, OrjsonSerializer)
    assert serializers.get_serializer(JSON_MIMETYPE) is original_serializer


def test_orjson_bulk_serializer_restores_the_serializer_on_failure(
    elastic_connection,
):
    serializers = elastic_connection.transport.serializers
    original_serializer = serializers.get_serializer(JSON_MIMETYPE)

    with pytest.raises(ValueError), orjson_bulk_serializer(elastic_connection):
        raise ValueError("indexing failed")

    assert serializers.get_serializer(JSON_MIMETYPE) is original_serializer


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


def test_generate_unite_legale_docs_streams_every_chunk(cursor_and_transform):
    documents = list(
        generate_unite_legale_docs(cursor_and_transform, 3, "siren-20260831")
    )

    assert len(documents) == 4
    assert all(document["_index"] == "siren-20260831" for document in documents)
