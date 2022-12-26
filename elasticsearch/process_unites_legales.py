import json

from dag_datalake_sirene.data_enrichment import (
    create_list_names_elus,
    format_adresse_complete,
    format_coordonnees,
    format_departement,
    format_dirigeants_pm,
    format_dirigeants_pp,
    format_etablissements,
    format_nom,
    format_nom_complet,
    format_siege,
    is_entrepreneur_individuel,
    label_section_from_activite,
)
from dag_datalake_sirene.helpers.utils import (
    get_empty_string_if_none,
    sqlite_str_to_bool,
)


def process_unites_legales(chunk_unites_legales_sqlite):
    list_unites_legales_processed = []
    for unite_legale in chunk_unites_legales_sqlite:
        unite_legale_processed = {}
        for field in unite_legale:
            if field in ["colter_elus"]:
                unite_legale_processed[field] = json.loads(unite_legale[field])
            else:
                unite_legale_processed[field] = unite_legale[field]

        # Nom complet
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

        # Activite principale
        unite_legale_processed[
            "section_activite_principale"
        ] = label_section_from_activite(
            unite_legale["activite_principale_unite_legale"]
        )

        # Entrepreneur individuel
        unite_legale_processed[
            "est_entrepreneur_individuel"
        ] = is_entrepreneur_individuel(unite_legale["nature_juridique_unite_legale"])

        # Dirigeants
        unite_legale_processed["liste_dirigeants"] = []
        (
            unite_legale_processed["dirigeants_pp"],
            unite_legale_processed["liste_dirigeants"],
        ) = format_dirigeants_pp(
            unite_legale["dirigeants_pp"], unite_legale_processed["liste_dirigeants"]
        )

        (
            unite_legale_processed["dirigeants_pm"],
            unite_legale_processed["liste_dirigeants"],
        ) = format_dirigeants_pm(
            unite_legale["dirigeants_pm"], unite_legale_processed["liste_dirigeants"]
        )

        if unite_legale_processed["est_entrepreneur_individuel"]:
            unite_legale_processed["liste_dirigeants"].append(
                unite_legale_processed["nom_complet"]
            )
            unite_legale_processed["dirigeants_pp"] = []
            unite_legale_processed["dirigeants_pp"].append({})
            unite_legale_processed["dirigeants_pp"][0]["nom"] = format_nom(
                unite_legale_processed["nom"], unite_legale_processed["nom_usage"]
            )

            unite_legale_processed["dirigeants_pp"][0][
                "prenoms"
            ] = unite_legale_processed["prenom"]

        # Élus
        unite_legale_processed["liste_elus"] = create_list_names_elus(
            unite_legale_processed["colter_elus"]
        )

        unite_legale_processed[
            "est_entrepreneur_individuel"
        ] = is_entrepreneur_individuel(unite_legale["nature_juridique_unite_legale"])

        if unite_legale_processed["est_entrepreneur_individuel"]:
            unite_legale_processed["liste_dirigeants"].append(
                unite_legale_processed["nom_complet"]
            )
            unite_legale_processed["dirigeants_pp"] = []
            unite_legale_processed["dirigeants_pp"].append({})
            unite_legale_processed["dirigeants_pp"][0]["nom"] = format_nom(
                unite_legale_processed["nom"], unite_legale_processed["nom_usage"]
            )

            unite_legale_processed["dirigeants_pp"][0][
                "prenoms"
            ] = unite_legale_processed["prenom"]

        unite_legale_processed[
            "section_activite_principale"
        ] = label_section_from_activite(
            unite_legale["activite_principale_unite_legale"]
        )

        # Entrepreneur de spectacle vivant
        unite_legale_processed["est_entrepreneur_spectacle"] = sqlite_str_to_bool(
            unite_legale["est_entrepreneur_spectacle"]
        )

        # Etablissements
        unite_legale_processed["etablissements"] = format_etablissements(
            unite_legale["etablissements"]
        )

        # Siege
        unite_legale_processed["siege"] = format_siege(
            unite_legale["siege"]
        )

        list_unites_legales_processed.append(unite_legale_processed)
    return list_unites_legales_processed
