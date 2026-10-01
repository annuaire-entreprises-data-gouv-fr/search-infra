import json
from unittest import mock

from data_pipelines_annuaire.workflows.adc.communes import processor
from data_pipelines_annuaire.workflows.adc.communes.config import (
    SOURCE_EFFECTIFS,
    SOURCE_SIRENE,
)
from data_pipelines_annuaire.workflows.adc.communes.processor import (
    CommunePayload,
    SortedGroups,
)


def test_sorted_groups_skips_unknown_codes_and_returns_empty_for_missing():
    groups = SortedGroups(
        iter([("01001", "a"), ("01001", "b"), ("01003", "c"), ("01005", "d")])
    )

    assert groups.pop("01001") == [("01001", "a"), ("01001", "b")]
    assert groups.pop("01002") == []
    assert groups.pop("01004") == []
    assert groups.skipped_codes == ["01003"]
    assert groups.pop("01005") == [("01005", "d")]
    assert groups.pop("01006") == []


def test_sorted_groups_counts_codes_after_the_last_requested_one():
    groups = SortedGroups(iter([("01001", "a"), ("98735", "b"), ("98735", "c")]))

    assert groups.pop("01001") == [("01001", "a")]
    groups.skip_remaining()
    assert groups.skipped_codes == ["98735"]
    assert groups.skipped_rows == 2


def test_commune_payload_merge_and_format():
    arrondissement_1 = CommunePayload()
    arrondissement_1.effectifs[("2024", "Commerce")] = 10
    arrondissement_1.effectifs[("2025", "Commerce")] = 5
    arrondissement_1.etablissements.append({"siret": "1"})
    arrondissement_1.flux["2026-01"] = [2, 1]

    arrondissement_2 = CommunePayload()
    arrondissement_2.effectifs[("2025", "Commerce")] = 3
    arrondissement_2.effectifs[("2025", "Construction")] = 20
    arrondissement_2.etablissements.append({"siret": "2"})
    arrondissement_2.flux["2025-12"] = [1, 0]
    arrondissement_2.flux["2026-01"] = [1, 1]

    commune = CommunePayload()
    commune.merge(arrondissement_1)
    commune.merge(arrondissement_2)

    assert commune.to_dict() == {
        "effectif_salaries": {
            "2025": [
                {"effectif": 20, "grand_secteur_activite": "Construction"},
                {"effectif": 8, "grand_secteur_activite": "Commerce"},
            ],
            "2024": [{"effectif": 10, "grand_secteur_activite": "Commerce"}],
        },
        "etablissements_sirene": [{"siret": "1"}, {"siret": "2"}],
        "flux_ouverture_etablissements": [
            {"mois": "2025-12", "ouvertures": 1, "fermetures": 0},
            {"mois": "2026-01", "ouvertures": 3, "fermetures": 2},
        ],
    }
    assert list(commune.to_dict()["effectif_salaries"]) == ["2025", "2024"]


def test_empty_commune_payload_keeps_schema():
    assert CommunePayload().to_dict() == {
        "effectif_salaries": {},
        "etablissements_sirene": [],
        "flux_ouverture_etablissements": [],
    }


def test_write_json_files_one_envelope_per_data_type(tmp_path):
    payload = CommunePayload()
    payload.etablissements.append({"siret": "1"})
    dates = {SOURCE_SIRENE: "2026-10-01", SOURCE_EFFECTIFS: "2026-05-29"}

    with mock.patch.object(processor, "JSON_OUTPUT_DIR", f"{tmp_path}/"):
        processor._write_json_files("75056", payload, dates)
        assert sorted(processor._list_local_files()) == [
            "75056/effectif_salaries.json",
            "75056/etablissements_sirene.json",
            "75056/flux_ouverture_etablissements.json",
        ]

    def read(name):
        return json.loads((tmp_path / "75056" / f"{name}.json").read_text())

    assert read("etablissements_sirene") == {
        "date_mise_a_jour": "2026-10-01",
        "source": SOURCE_SIRENE,
        "donnees": [{"siret": "1"}],
    }
    assert read("effectif_salaries") == {
        "date_mise_a_jour": "2026-05-29",
        "source": SOURCE_EFFECTIFS,
        "donnees": {},
    }
    assert read("flux_ouverture_etablissements")["donnees"] == []
