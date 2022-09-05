import json

from dag_datalake_sirene.data_enrichment import (
    format_adresse_complete,
    format_coordonnees,
    format_departement,
    format_nom_complet,
    is_entrepreneur_individuel,
    label_section_from_activite,
)
from dag_datalake_sirene.helpers.utils import (
    get_empty_string_if_none,
    normalize_string,
)


def process_unites_legales(chunk_unites_legales_sqlite):
    list_unites_legales_processed = []
    for unite_legale in chunk_unites_legales_sqlite:
        unite_legale_processed = {}
        for field in unite_legale:
            if field in ["enseignes", "adresses"]:
                unite_legale_processed[field] = json.loads(unite_legale[field])
            else:
                unite_legale_processed[field] = unite_legale[field]

        # Enseignes
        unite_legale_processed["liste_enseignes"] = []
        for enseigne in unite_legale_processed["enseignes"]:
            unite_legale_processed["liste_enseignes"].extend(
                [enseigne["enseigne_1"], enseigne["enseigne_2"], enseigne["enseigne_3"]]
            )
        unite_legale_processed["liste_enseignes"] = list(
            set(filter(None, unite_legale_processed["liste_enseignes"]))
        )
        del unite_legale_processed["enseignes"]

        # Adresses
        unite_legale_processed["liste_adresses"] = []
        for adresse in unite_legale_processed["adresses"]:
            unite_legale_processed["liste_adresses"].append(
                format_adresse_complete(
                    adresse["complement_adresse"],
                    adresse["numero_voie"],
                    adresse["indice_repetition"],
                    adresse["type_voie"],
                    adresse["libelle_voie"],
                    adresse["libelle_commune"],
                    adresse["libelle_cedex"],
                    adresse["distribution_speciale"],
                    adresse["commune"],
                    adresse["cedex"],
                    adresse["libelle_commune_etranger"],
                    adresse["libelle_pays_etranger"],
                )
            )
        unite_legale_processed["liste_adresses"] = list(
            set(filter(None, unite_legale_processed["liste_adresses"]))
        )
        del unite_legale_processed["adresses"]

        unite_legale_processed["adresse_etablissement"] = format_adresse_complete(
            unite_legale_processed["complement_adresse"],
            unite_legale_processed["numero_voie"],
            unite_legale_processed["indice_repetition"],
            unite_legale_processed["type_voie"],
            unite_legale_processed["libelle_voie"],
            unite_legale_processed["libelle_commune"],
            unite_legale_processed["libelle_cedex"],
            unite_legale_processed["distribution_speciale"],
            unite_legale_processed["commune"],
            unite_legale_processed["cedex"],
            unite_legale_processed["libelle_commune_etranger"],
            unite_legale_processed["libelle_pays_etranger"],
        )

        unite_legale_processed["nom_complet"] = format_nom_complet(
            unite_legale["nom"],
            unite_legale["nom_usage"],
            unite_legale["nom_raison_sociale"],
            unite_legale["sigle"],
            unite_legale["prenom"],
        )

        # Replace missing values with 0
        unite_legale_processed["nombre_etablissements_ouverts"] = (
            0
            if unite_legale_processed["nombre_etablissements_ouverts"] is None
            else unite_legale_processed["nombre_etablissements_ouverts"]
        )

        unite_legale_processed["liste_dirigeants"] = []

        unite_legale_processed["dirigeants_pp"] = json.loads(
            unite_legale["dirigeants_pp"]
        )
        for dirigeant_pp in unite_legale_processed["dirigeants_pp"]:
            unite_legale_processed["liste_dirigeants"].append(
                dirigeant_pp["prenoms"] + " " + dirigeant_pp["noms"]
            )

        # Dirigeants
        unite_legale_processed["liste_dirigeants"] = []
        unite_legale_processed["dirigeants_pp"] = json.loads(
            unite_legale["dirigeants_pp"]
        )

        for dirigeant_pp in unite_legale_processed["dirigeants_pp"]:
            dirigeant_pp["noms"] = normalize_string(dirigeant_pp["noms"])
            dirigeant_pp["prenoms"] = normalize_string(dirigeant_pp["prenoms"])
            unite_legale_processed["liste_dirigeants"].append(
                dirigeant_pp["prenoms"] + " " + dirigeant_pp["noms"]
            )

        unite_legale_processed["dirigeants_pm"] = json.loads(
            unite_legale["dirigeants_pm"]
        )
        for dirigeant_pm in unite_legale_processed["dirigeants_pm"]:
            dirigeant_pm["denomination"] = normalize_string(
                dirigeant_pm["denomination"]
            )
            unite_legale_processed["liste_dirigeants"].append(
                dirigeant_pm["denomination"]
            )

        # Elus
        unite_legale_processed["colter_elus"] = json.loads(
            unite_legale["colter_elus"]
        )

        unite_legale_processed[
            "is_entrepreneur_individuel"
        ] = is_entrepreneur_individuel(unite_legale["nature_juridique_unite_legale"])

        if unite_legale_processed["is_entrepreneur_individuel"] == "true":
            unite_legale_processed["liste_dirigeants"].append(
                unite_legale_processed["nom_complet"]
            )

        unite_legale_processed[
            "section_activite_principale"
        ] = label_section_from_activite(
            unite_legale["activite_principale_unite_legale"]
        )

        unite_legale_processed["departement"] = format_departement(
            unite_legale["commune"]
        )
        unite_legale_processed["coordonnees"] = format_coordonnees(
            unite_legale["longitude"], unite_legale["latitude"]
        )
        unite_legale_processed["concat_enseigne_adresse"] = (
            unite_legale_processed["liste_enseignes"]
            + unite_legale_processed["liste_adresses"]
        )

        unite_legale_processed["concat_nom_adr_siren"] = (
            get_empty_string_if_none(unite_legale_processed["nom_complet"])
            + " "
            + get_empty_string_if_none(unite_legale_processed["adresse_etablissement"])
            + " "
            + get_empty_string_if_none(unite_legale["siren"])
        ).strip()
        list_unites_legales_processed.append(unite_legale_processed)
    return list_unites_legales_processed
