import json

from dag_datalake_sirene.data_enrichment import (
    create_list_names_elus,
    format_dirigeants_pm,
    format_dirigeants_pp,
    format_etablissements_and_complements,
    format_nom,
    format_nom_complet,
    format_slug,
    format_siege_unite_legale,
    is_entrepreneur_individuel,
    is_service_public,
    label_section_from_activite,
)
from dag_datalake_sirene.helpers.utils import sqlite_str_to_bool, str_to_list


def process_unites_legales(chunk_unites_legales_sqlite):
    list_unites_legales_processed = []
    for unite_legale in chunk_unites_legales_sqlite:
        unite_legale_processed = {}
        for field in unite_legale:
            if field in ["colter_elus"]:
                unite_legale_processed[field] = json.loads(unite_legale[field])
            else:
                unite_legale_processed[field] = unite_legale[field]
        # Statut de diffusion

        is_non_diffusible = (
            True if unite_legale["statut_diffusion_unite_legale"] != "O" else False
        )

        # Nom complet
        unite_legale_processed["nom_complet"] = format_nom_complet(
            unite_legale["nom"],
            unite_legale["nom_usage"],
            unite_legale["nom_raison_sociale"],
            unite_legale["prenom"],
        )

        # Slug Nom Complet
        unite_legale_processed["slug"] = format_slug(
            unite_legale_processed["nom_complet"],
            unite_legale["sigle"],
            unite_legale["denomination_usuelle_1_unite_legale"],
            unite_legale["denomination_usuelle_2_unite_legale"],
            unite_legale["denomination_usuelle_3_unite_legale"],
            unite_legale["siren"],
            unite_legale["statut_diffusion_unite_legale"],
        )

        # Replace missing values with 0
        unite_legale_processed["nombre_etablissements_ouverts"] = (
            0
            if unite_legale_processed["nombre_etablissements_ouverts"] is None
            else unite_legale_processed["nombre_etablissements_ouverts"]
        )

        # Bilan financier
        if unite_legale["bilan_financier"]:
            unite_legale_processed["bilan_financier"] = json.loads(
                unite_legale["bilan_financier"]
            )
        else:
            unite_legale_processed["bilan_financier"] = {}

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

        unite_legale_processed["est_service_public"] = is_service_public(
            unite_legale["nature_juridique_unite_legale"],
            unite_legale_processed["siren"],
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

        unite_legale_processed[
            "section_activite_principale"
        ] = label_section_from_activite(
            unite_legale["activite_principale_unite_legale"]
        )

        # Entrepreneur de spectacle vivant
        unite_legale_processed["est_entrepreneur_spectacle"] = sqlite_str_to_bool(
            unite_legale["est_entrepreneur_spectacle"]
        )

        # Egapro
        unite_legale_processed["egapro_renseignee"] = sqlite_str_to_bool(
            unite_legale["egapro_renseignee"]
        )

        # Etablissements
        etablissements_processed, complements = format_etablissements_and_complements(
            unite_legale["etablissements"],
            unite_legale_processed["nom_complet"],
            is_non_diffusible,
        )
        unite_legale_processed["etablissements"] = etablissements_processed

        # Complements
        for field in [
            "convention_collective_renseignee",
            "est_bio",
            "est_finess",
            "est_rge",
            "est_uai",
        ]:
            unite_legale_processed[field] = complements[field]

        # Organismes de formation
        unite_legale_processed["est_qualiopi"] = sqlite_str_to_bool(
            unite_legale_processed["est_qualiopi"]
        )
        unite_legale_processed["liste_id_organisme_formation"] = str_to_list(
            unite_legale_processed["liste_id_organisme_formation"]
        )
        unite_legale_processed["est_organisme_formation"] = (
            True if unite_legale_processed["liste_id_organisme_formation"] else False
        )

        # Siege
        unite_legale_processed["siege"] = format_siege_unite_legale(
            unite_legale["siege"], is_non_diffusible
        )

        list_unites_legales_processed.append(unite_legale_processed)
    return list_unites_legales_processed
