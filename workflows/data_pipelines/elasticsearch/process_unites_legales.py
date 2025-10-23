import json
from datetime import datetime

from dag_datalake_sirene.helpers.utils import (
    convert_date_format,
    sqlite_str_to_bool,
    str_to_list,
)
from dag_datalake_sirene.workflows.data_pipelines.elasticsearch.data_enrichment import (
    calculate_company_size_factor,
    create_list_names_elus,
    format_dirigeants_pm,
    format_etablissements_and_complements,
    format_nom,
    format_nom_complet,
    format_personnes_physiques,
    format_siege_unite_legale,
    format_slug,
    get_nom_commercial,
    is_administration_l100_3,
    is_association,
    is_entrepreneur_individuel,
    is_ess,
    is_personne_morale_insee,
    is_service_public,
    label_section_from_activite,
    map_categorie_to_number,
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
        # Statut de diffusion
        is_non_diffusible = (
            True if unite_legale["statut_diffusion_unite_legale"] != "O" else False
        )

        # Nom complet
        unite_legale_processed["nom_complet"] = format_nom_complet(
            unite_legale["nom"],
            unite_legale["nom_usage"],
            unite_legale["prenom"],
            unite_legale["nom_raison_sociale"],
            est_personne_morale_insee=is_personne_morale_insee(
                unite_legale["nature_juridique_unite_legale"]
            ),
            is_non_diffusible=is_non_diffusible,
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
        unite_legale_processed["section_activite_principale"] = (
            label_section_from_activite(
                unite_legale["activite_principale_unite_legale"]
            )
        )

        # Entrepreneur individuel
        unite_legale_processed["est_entrepreneur_individuel"] = (
            is_entrepreneur_individuel(unite_legale["nature_juridique_unite_legale"])
        )

        # Personne morale (INSEE)
        unite_legale_processed["est_personne_morale_insee"] = is_personne_morale_insee(
            unite_legale["nature_juridique_unite_legale"]
        )

        # Categorie entreprise
        unite_legale_processed["code_categorie_entreprise"] = map_categorie_to_number(
            unite_legale["categorie_entreprise"]
        )

        # Dirigeants
        unite_legale_processed["liste_dirigeants"] = []
        (
            unite_legale_processed["dirigeants_pp"],
            unite_legale_processed["liste_dirigeants"],
        ) = format_personnes_physiques(
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

            unite_legale_processed["dirigeants_pp"][0]["prenoms"] = (
                unite_legale_processed["prenom"]
            )

        # Élus
        unite_legale_processed["liste_elus"] = create_list_names_elus(
            unite_legale_processed["colter_elus"]
        )

        unite_legale_processed["est_entrepreneur_individuel"] = (
            is_entrepreneur_individuel(unite_legale["nature_juridique_unite_legale"])
        )

        unite_legale_processed["est_association"] = is_association(
            unite_legale_processed["nature_juridique_unite_legale"],
            unite_legale_processed["identifiant_association_unite_legale"],
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

            unite_legale_processed["dirigeants_pp"][0]["prenoms"] = (
                unite_legale_processed["prenom"]
            )

        unite_legale_processed["section_activite_principale"] = (
            label_section_from_activite(
                unite_legale["activite_principale_unite_legale"]
            )
        )

        # Immatriculation
        immatriculation = unite_legale.get("immatriculation")
        if immatriculation:
            immatriculation_data = json.loads(immatriculation)
            immatriculation_data["indicateur_associe_unique"] = sqlite_str_to_bool(
                immatriculation_data.get("indicateur_associe_unique", None)
            )
            immatriculation_data["capital_variable"] = sqlite_str_to_bool(
                immatriculation_data.get("capital_variable", None)
            )
            unite_legale_processed["immatriculation"] = immatriculation_data
        else:
            unite_legale_processed["immatriculation"] = {}

        # Entrepreneur de spectacle vivant
        unite_legale_processed["est_entrepreneur_spectacle"] = sqlite_str_to_bool(
            unite_legale["est_entrepreneur_spectacle"]
        )

        # ESS
        unite_legale_processed["est_ess"] = is_ess(
            sqlite_str_to_bool(unite_legale["est_ess_france"]),
            unite_legale["economie_sociale_solidaire_unite_legale"],
        )

        # Egapro
        unite_legale_processed["egapro_renseignee"] = sqlite_str_to_bool(
            unite_legale["egapro_renseignee"]
        )

        # Bilans GES
        unite_legale_processed["bilan_ges_renseigne"] = sqlite_str_to_bool(
            unite_legale["bilan_ges_renseigne"]
        )

        # Achats Responsables
        unite_legale_processed["est_achats_responsables"] = sqlite_str_to_bool(
            unite_legale["est_achats_responsables"]
        )

        # Alim'Confiance
        unite_legale_processed["est_alim_confiance"] = sqlite_str_to_bool(
            unite_legale["est_alim_confiance"]
        )

        # Marche Inclusion
        unite_legale_processed["est_siae"] = sqlite_str_to_bool(
            unite_legale["est_siae"]
        )

        # Patrimoine Vivants
        unite_legale_processed["est_patrimoine_vivant"] = sqlite_str_to_bool(
            unite_legale["est_patrimoine_vivant"]
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
        unite_legale_processed["liste_id_organisme_formation"] = str_to_list(
            unite_legale_processed["liste_id_organisme_formation"]
        )
        unite_legale_processed["est_organisme_formation"] = (
            True if unite_legale_processed["liste_id_organisme_formation"] else False
        )
        unite_legale_processed["est_qualiopi"] = sqlite_str_to_bool(
            unite_legale_processed["est_qualiopi"]
        )

        # Siege
        unite_legale_processed["siege"] = format_siege_unite_legale(
            unite_legale["siege"], is_non_diffusible
        )

        # Slug Nom Complet
        unite_legale_processed["slug"] = format_slug(
            unite_legale_processed["nom_complet"],
            unite_legale["siren"],
            unite_legale["sigle"],
            get_nom_commercial(unite_legale_processed),
            unite_legale["statut_diffusion_unite_legale"],
            unite_legale["nature_juridique_unite_legale"],
        )

        # Convention collective
        unite_legale_processed["liste_idcc_unite_legale"] = str_to_list(
            unite_legale_processed["liste_idcc_unite_legale"]
        )

        # Source de données
        unite_legale_processed["from_insee"] = sqlite_str_to_bool(
            unite_legale["from_insee"]
        )
        unite_legale_processed["from_rne"] = sqlite_str_to_bool(
            unite_legale["from_rne"]
        )

        # Get the current date and time
        unite_legale_processed["date_mise_a_jour"] = datetime.now().strftime(
            "%Y-%m-%dT%H:%M:%S"
        )
        unite_legale_processed["date_mise_a_jour_rne"] = convert_date_format(
            unite_legale["date_mise_a_jour_rne"]
        )

        # Administration
        unite_legale_processed["est_service_public"] = is_service_public(
            unite_legale["nature_juridique_unite_legale"],
            unite_legale_processed["siren"],
            unite_legale_processed["etat_administratif_unite_legale"],
        )
        unite_legale_processed["est_l100_3"] = is_administration_l100_3(
            unite_legale_processed["siren"],
            unite_legale["nature_juridique_unite_legale"],
            unite_legale_processed["est_service_public"],
        )

        # Produits catégorie/nombre étabs
        unite_legale_processed["facteur_taille_entreprise"] = (
            calculate_company_size_factor(unite_legale_processed)
        )
        # Create unité légale (structure) to be indexed
        unite_legale_to_index = {}
        unite_legale_to_index["identifiant"] = unite_legale_processed["siren"]
        unite_legale_to_index["nom_complet"] = unite_legale_processed["nom_complet"]
        unite_legale_to_index["unite_legale"] = unite_legale_processed

        list_unites_legales_processed.append(unite_legale_to_index)

    return list_unites_legales_processed
