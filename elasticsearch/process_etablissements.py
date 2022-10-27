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
        etablissement_processed["nom_complet"] = format_nom_complet(
            etablissement["nom"],
            etablissement["nom_usage"],
            etablissement["nom_raison_sociale"],
            etablissement["sigle"],
            etablissement["prenom"],
        )
        del etablissement["nom"]
        del etablissement["nom_usage"]
        del etablissement["nom_raison_sociale"]
        del etablissement["sigle"]
        del etablissement["prenom"]
        
        etablissement_processed["adresse"] = format_adresse_complete(
            etablissement_processed["complement_adresse"],
            etablissement_processed["numero_voie"],
            etablissement_processed["indice_repetition"],
            etablissement_processed["type_voie"],
            etablissement_processed["libelle_voie"],
            etablissement_processed["libelle_commune"],
            etablissement_processed["libelle_cedex"],
            etablissement_processed["distribution_speciale"],
            etablissement_processed["commune"],
            etablissement_processed["cedex"],
            etablissement_processed["libelle_commune_etranger"],
            etablissement_processed["libelle_pays_etranger"],
        )
        etablissement_processed["concat_nom_enseigne_adresse_siren_siret"] = (
                get_empty_string_if_none(etablissement_processed["nom_complet"])
                + " "
                + get_empty_string_if_none(
            etablissement_processed["enseigne_1"])
                + " "
                + get_empty_string_if_none(
            etablissement_processed["enseigne_2"])
                + " "
                + get_empty_string_if_none(
            etablissement_processed["enseigne_3"])
                + " "
                + get_empty_string_if_none(
            etablissement_processed["adresse"])
                + " "
                + get_empty_string_if_none(etablissement_processed["siren"])
                + " "
                + get_empty_string_if_none(etablissement_processed["siret"])
        ).strip()
        etablissement_processed["unite_etablissement"] = {
            "name": "etablissement",
            "parent": etablissement["siren"],
        }
        list_etablissements_processed.append(etablissement_processed)
    return list_etablissements_processed
