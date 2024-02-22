import json
import logging
from slugify import slugify

from dag_datalake_sirene.workflows.data_pipelines.elasticsearch.clean_data import (
    drop_duplicates_dirigeants_pm,
    drop_duplicates_dirigeants_pp,
    unique_qualites,
)
from dag_datalake_sirene.workflows.data_pipelines.elasticsearch.es_fields import (
    get_elasticsearch_field_name,
)
from dag_datalake_sirene.helpers.utils import (
    drop_exact_duplicates,
    get_empty_string_if_none,
    str_to_bool,
    str_to_list,
)

labels_file_path = "dags/dag_datalake_sirene/helpers/labels/"


def load_file(file_name: str):
    with open(f"{labels_file_path}{file_name}") as json_file:
        file_decoded = json.load(json_file)
    return file_decoded


sections_NAF = load_file("sections_codes_naf.json")
mapping_dep_to_reg = load_file("dep_to_reg.json")
mapping_role_dirigeants = load_file("roles_dirigeants.json")
mapping_commune_to_epci = load_file("epci.json")


# Nom complet
def format_nom_complet(
    nom=None,
    nom_usage=None,
    nom_raison_sociale=None,
    prenom=None,
):
    name = None
    if prenom or nom or nom_usage:
        if nom_usage:
            formatted_name = f" {nom_usage} ({nom})" if nom else f" {nom_usage}"
        else:
            formatted_name = f" {nom}" if nom else ""

        name = f"{prenom if prenom else ''}{formatted_name}"
    if nom_raison_sociale:
        name = nom_raison_sociale
    return name.upper().strip() if name else name


# Slug
# Because we need to create sitemap and for keeping coherence
# between sitemap values and slug in API. We calculate this field
# with this function and we add it both in elasticsearch and in sitemap
def format_slug(
    nom_complet,
    sigle=None,
    denomination_usuelle_1=None,
    denomination_usuelle_2=None,
    denomination_usuelle_3=None,
    siren=None,
    statut_diffusion=None,
):
    if statut_diffusion == "P":
        if siren:
            return slugify(f"{siren}")
    all_denomination_usuelle = ""
    for item in [
        denomination_usuelle_1,
        denomination_usuelle_2,
        denomination_usuelle_3,
    ]:
        if item:
            all_denomination_usuelle += f"{item} "
    if all_denomination_usuelle:
        nom_complet = f"{nom_complet} {all_denomination_usuelle.strip()}"
    if sigle:
        nom_complet = f"{nom_complet} {sigle}"
    if siren:
        nom_complet = f"{nom_complet} {siren}"
    if nom_complet:
        return slugify(nom_complet.lower())
    # if nom_complet is null
    return ""


# Noms
def format_nom(
    nom=None,
    nom_usage=None,
):
    if nom_usage:
        formatted_name = f"{nom_usage} ({nom})" if nom else f"{nom_usage}"
    else:
        formatted_name = f"{nom}" if nom else None
    return formatted_name.upper().strip() if formatted_name else formatted_name


# Entrepreneur individuel
def is_entrepreneur_individuel(nature_juridique_unite_legale):
    if nature_juridique_unite_legale in ["1", "10", "1000"]:
        return True
    else:
        return False


# ESS
def is_ess(est_ess_france, ess_insee):
    est_ess_insee = ess_insee == "O" if ess_insee is not None else False
    return est_ess_france or est_ess_insee


# Service public
def is_service_public(nature_juridique_unite_legale, siren):
    if (
        (
            nature_juridique_unite_legale
            and nature_juridique_unite_legale.startswith(
                ("3210", "3110", "4", "71", "72", "73", "74")
            )
        )
        or siren.startswith(("1", "2"))
        or siren == "320252489"
    ):
        return True
    else:
        return False


# Association
def is_association(nature_juridique_unite_legale, identifiant_association):
    if identifiant_association:
        return True
    if nature_juridique_unite_legale and nature_juridique_unite_legale.startswith(
        ("5195", "92")
    ):
        return True
    return False


# Section activité principale
def label_section_from_activite(activite_principale_unite_legale):
    if activite_principale_unite_legale is not None:
        code_naf = activite_principale_unite_legale[:2]
        section_activite_principale = (
            sections_NAF[code_naf] if code_naf in sections_NAF else None
        )
        return section_activite_principale
    else:
        return None


# Région
def label_region_from_departement(departement):
    if departement is not None:
        region = (
            mapping_dep_to_reg[departement]
            if departement in mapping_dep_to_reg
            else None
        )
        return region
    return None


# EPCI
def label_epci_from_commune(commune):
    if not commune:
        return None
    # Modify commune if it's an arrondissement
    commune_arrondissement_mapping = {
        "751": "75056",  # Paris
        "132": "13055",  # Marseille
        "693": "69123",  # Lyon
    }
    for prefix, corresponding_commune in commune_arrondissement_mapping.items():
        if commune.startswith(prefix):
            commune = corresponding_commune
            break

    return mapping_commune_to_epci.get(commune)


# Categorie entreprise
def map_categorie_to_number(categorie):
    if categorie == "GE":
        return 3
    elif categorie == "ETI":
        return 2
    elif categorie == "PMI":
        return 1
    else:
        return 0


# Adresse complète
def format_adresse_complete(
    complement_adresse,
    numero_voie,
    indice_repetition,
    type_voie,
    libelle_voie,
    libelle_commune,
    libelle_cedex,
    distribution_speciale,
    code_postal,
    cedex,
    commune,
    libelle_commune_etranger,
    libelle_pays_etranger,
    is_non_diffusible=False,
):
    col_list = [
        complement_adresse,
        numero_voie,
        indice_repetition,
        type_voie,
        libelle_voie,
        distribution_speciale,
    ]

    if is_non_diffusible:
        adresse = (
            get_empty_string_if_none(commune)
            + " "
            + get_empty_string_if_none(libelle_commune)
        )
        return adresse.upper().strip()

    adresse = ""
    for column in col_list:
        if column:
            adresse = adresse + " " + column
    if cedex is None:
        if code_postal is None:
            adresse = adresse
        else:
            adresse = (
                adresse
                + " "
                + get_empty_string_if_none(code_postal)
                + " "
                + get_empty_string_if_none(libelle_commune)
            )
    else:
        adresse = (
            adresse
            + " "
            + get_empty_string_if_none(cedex)
            + " "
            + get_empty_string_if_none(libelle_cedex)
        )
    etranger_list = [libelle_commune_etranger, libelle_pays_etranger]
    for column in etranger_list:
        if column:
            adresse = adresse + " " + column if column != "" else ""
    return adresse.upper().strip()


# Département
def format_departement(commune):
    departement = (
        str(commune)[:3]
        if str(commune)[:2] in ["97", "98"]
        else (None if commune is None else str(commune)[:2])
    )
    return departement


# Coordonnées
def format_coordonnees(longitude, latitude):
    coordonnees = (
        None if (longitude is None) or (latitude is None) else f"{latitude},{longitude}"
    )
    return coordonnees


# Role dirigeant
def map_roles(codes):
    if codes is None:
        return None
    return [mapping_role_dirigeants.get(str(code), None) for code in codes]


# Dirigeants PP
def format_dirigeants_pp(list_dirigeants_pp_sqlite, list_all_dirigeants=[]):
    dirigeants_pp = json.loads(list_dirigeants_pp_sqlite)
    dirigeants_pp_processed = []
    for dirigeant_pp in dirigeants_pp:
        dirigeant_pp["nom"] = format_nom(dirigeant_pp["nom"], dirigeant_pp["nom_usage"])

        # Drop qualite`s exact and partial duplicates
        dirigeant_pp["role_description"] = unique_qualites(
            dirigeant_pp["role_description"]
        )

        dirigeants_pp_processed.append(
            dict(
                nom=dirigeant_pp["nom"],
                prenoms=dirigeant_pp["prenoms"],
                date_de_naissance=dirigeant_pp["date_de_naissance"],
                nationalite=dirigeant_pp["nationalite"],
                role=dirigeant_pp["role_description"],
                date_mise_a_jour=dirigeant_pp["date_mise_a_jour"],
            )
        )
        # Liste dirigeants
        if dirigeant_pp["prenoms"] and dirigeant_pp["nom"]:
            list_all_dirigeants.append(
                dirigeant_pp["prenoms"] + " " + dirigeant_pp["nom"]
            )
        elif dirigeant_pp["prenoms"] and not dirigeant_pp["nom"]:
            list_all_dirigeants.append(dirigeant_pp["prenoms"])
            logging.debug(
                f'Missing dirigeant nom for ****** {dirigeant_pp["siren"]} '
                f'****** prenoms = {dirigeant_pp["prenoms"]}'
            )
        elif dirigeant_pp["nom"] and not dirigeant_pp["prenoms"]:
            list_all_dirigeants.append(dirigeant_pp["nom"])
            logging.debug(
                f"Missing dirigeant prenoms for ****** "
                f'{dirigeant_pp["siren"]} ****** nom : {dirigeant_pp["nom"]}'
            )
        else:
            logging.debug(
                f"Missing dirigeants names for ******" f' {dirigeant_pp["siren"]}'
            )

    if dirigeants_pp_processed:
        # Case when two dirigeant have exactly the same fields/value
        dirigeants_pp_processed = drop_exact_duplicates(dirigeants_pp_processed)
        # Case when two dirigeant have partially the same fields/value
        # (eg. same name, and fistname, different date or qualities)
        dirigeants_pp_processed = drop_duplicates_dirigeants_pp(dirigeants_pp_processed)

    return dirigeants_pp_processed, list(set(list_all_dirigeants))


# Dirigeants PM
def format_dirigeants_pm(list_dirigeants_pm_sqlite, list_all_dirigeants=[]):
    dirigeants_pm = json.loads(list_dirigeants_pm_sqlite)
    dirigeants_pm_processed = []
    for dirigeant_pm in dirigeants_pm:
        if dirigeant_pm["denomination"]:
            list_all_dirigeants.append(dirigeant_pm["denomination"])
        else:
            logging.debug(
                f'Missing denomination dirigeant for ***** {dirigeant_pm["siren"]}'
            )
        dirigeant_pm["role_description"] = unique_qualites(
            dirigeant_pm["role_description"]
        )
        dirigeants_pm_processed.append(
            dict(
                siren=dirigeant_pm["siren_dirigeant"],
                denomination=dirigeant_pm["denomination"],
                role=dirigeant_pm["role_description"],
                forme_juridique=dirigeant_pm["forme_juridique"],
                date_mise_a_jour=dirigeant_pm["date_mise_a_jour"],
            )
        )

    if dirigeants_pm_processed:
        # Case when two dirigeant have exactly the same fields/value
        dirigeants_pm_processed = drop_exact_duplicates(dirigeants_pm_processed)
        # Case when two dirigeant have partially the same fields/value
        # (eg. same name, and fistname, different date or qualities)
        dirigeants_pm_processed = drop_duplicates_dirigeants_pm(dirigeants_pm_processed)

    return dirigeants_pm_processed, list(set(list_all_dirigeants))


# Élus
def create_list_names_elus(list_elus):
    list_elus_names = []
    for elu in list_elus:
        name_elu = f"{elu['nom']} {elu['prenom']}"
        list_elus_names.append(name_elu)
    return list(set(list_elus_names))


# Etablissements
def format_etablissements_and_complements(
    list_etablissements_sqlite,
    nom_complet,
    is_non_diffusible=False,
):
    etablissements = json.loads(list_etablissements_sqlite)
    etablissements_processed = []
    complements = {
        "est_uai": False,
        "est_rge": False,
        "est_finess": False,
        "est_organisme_formation": False,
        "est_bio": False,
        "convention_collective_renseignee": False,
    }
    for etablissement in etablissements:
        etablissement["nom_complet"] = nom_complet
        etablissement["adresse"] = format_adresse_complete(
            etablissement["complement_adresse"],
            etablissement["numero_voie"],
            etablissement["indice_repetition"],
            etablissement["type_voie"],
            etablissement["libelle_voie"],
            etablissement["libelle_commune"],
            etablissement["libelle_cedex"],
            etablissement["distribution_speciale"],
            etablissement["code_postal"],
            etablissement["cedex"],
            etablissement["commune"],
            etablissement["libelle_commune_etranger"],
            etablissement["libelle_pays_etranger"],
            is_non_diffusible,
        )
        etablissement["concat_enseigne_adresse_siren_siret"] = (
            get_empty_string_if_none(etablissement["enseigne_1"])
            + " "
            + get_empty_string_if_none(etablissement["enseigne_2"])
            + " "
            + get_empty_string_if_none(etablissement["enseigne_3"])
            + " "
            + get_empty_string_if_none(etablissement["adresse"])
            + " "
            + get_empty_string_if_none(etablissement["siren"])
            + " "
            + get_empty_string_if_none(etablissement["siret"])
        ).strip()
        etablissement["coordonnees"] = format_coordonnees(
            etablissement["longitude"], etablissement["latitude"]
        )
        etablissement["departement"] = format_departement(etablissement["commune"])
        etablissement["region"] = label_region_from_departement(
            etablissement["departement"]
        )
        etablissement["epci"] = label_epci_from_commune(etablissement["commune"])
        etablissement["est_siege"] = str_to_bool(etablissement["est_siege"])
        etablissement["liste_idcc"] = str_to_list(etablissement["liste_idcc"])
        etablissement["liste_rge"] = str_to_list(etablissement["liste_rge"])
        etablissement["liste_uai"] = str_to_list(etablissement["liste_uai"])
        etablissement["liste_finess"] = str_to_list(etablissement["liste_finess"])
        etablissement["liste_id_bio"] = str_to_list(etablissement["liste_id_bio"])
        etablissements_processed.append(etablissement)

        # Get complements
        for field in [
            "liste_finess",
            "liste_id_bio",
            "liste_idcc",
            "liste_rge",
            "liste_uai",
        ]:
            if etablissement[field]:
                complements[get_elasticsearch_field_name(field)] = True

    return etablissements_processed, complements


# Siege
def format_siege_unite_legale(siege, is_non_diffusible=False):
    if not siege:
        return None
    siege = json.loads(siege)
    siege["adresse"] = format_adresse_complete(
        siege["complement_adresse"],
        siege["numero_voie"],
        siege["indice_repetition"],
        siege["type_voie"],
        siege["libelle_voie"],
        siege["libelle_commune"],
        siege["libelle_cedex"],
        siege["distribution_speciale"],
        siege["code_postal"],
        siege["cedex"],
        siege["commune"],
        siege["libelle_commune_etranger"],
        siege["libelle_pays_etranger"],
        is_non_diffusible,
    )
    siege["coordonnees"] = format_coordonnees(siege["longitude"], siege["latitude"])
    siege["departement"] = format_departement(siege["commune"])
    siege["region"] = label_region_from_departement(siege["departement"])
    siege["epci"] = label_epci_from_commune(siege["commune"])
    siege["est_siege"] = str_to_bool(siege["est_siege"])
    siege["liste_idcc"] = str_to_list(siege["liste_idcc"])
    siege["liste_rge"] = str_to_list(siege["liste_rge"])
    siege["liste_uai"] = str_to_list(siege["liste_uai"])
    siege["liste_finess"] = str_to_list(siege["liste_finess"])
    siege["liste_id_bio"] = str_to_list(siege["liste_id_bio"])

    return siege
