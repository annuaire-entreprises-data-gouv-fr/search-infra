import json

from dag_datalake_sirene.data_enrichment import (
    format_adresse_complete,
    format_coordonnees,
    format_departement,
    format_dirigeants_pm,
    format_dirigeants_pp,
    format_nom,
    format_nom_complet,
    is_entrepreneur_individuel,
    label_section_from_activite,
)
from dag_datalake_sirene.helpers.utils import get_empty_string_if_none


def process_etablissements(chunk_etablissements_sqlite):
    list_etablissements_processed = []
    for etablissement in chunk_etablissements_sqlite:
        etablissement_processed = {}
        for field in etablissement:
            etablissement_processed[field] = etablissement[field]
        etablissement_processed["unite_etablissement"] = {
            "name": "etablissement",
            "parent": etablissement["siren"],
        }
        list_etablissements_processed.append(etablissement_processed)
    return list_etablissements_processed
