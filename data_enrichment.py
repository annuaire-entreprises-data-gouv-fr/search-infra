import json
import logging
from slugify import slugify

from dag_datalake_sirene.helpers.clean_dirigeants import (
    drop_duplicates_dirigeants_pm,
    drop_duplicates_dirigeants_pp,
    unique_qualites,
)
from dag_datalake_sirene.helpers.es_fields import get_elasticsearch_field_name
from dag_datalake_sirene.helpers.utils import (
    drop_exact_duplicates,
    get_empty_string_if_none,
    normalize_date,
    str_to_bool,
    str_to_list,
)

labels_file_path = "dags/dag_datalake_sirene/labels/"


def load_file(file_name: str):
    with open(f"{labels_file_path}{file_name}") as json_file:
        file_decoded = json.load(json_file)
    return file_decoded


sections_NAF = load_file("sections_codes_naf.json")


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


# Slug Nom complet
def format_slug_nom_complet(
    nom_complet,
    sigle=None,
    denomination_usuelle_1=None,
    denomination_usuelle_2=None,
    denomination_usuelle_3=None,
    siren=None,
):
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


# Service public
def is_service_public(nature_juridique_unite_legale, siren):
    if (
        nature_juridique_unite_legale
        and nature_juridique_unite_legale.startswith(
            ("3210", "3110", "4", "71", "72", "73", "74")
        )
    ) or siren.startswith(("1", "2")):
        return True
    else:
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
    libelle_commune_etranger,
    libelle_pays_etranger,
):
    col_list = [
        complement_adresse,
        numero_voie,
        indice_repetition,
        type_voie,
        libelle_voie,
        distribution_speciale,
    ]
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
        if str(commune)[:2] == "97"
        else (None if commune is None else str(commune)[:2])
    )
    return departement


# Coordonnées
def format_coordonnees(longitude, latitude):
    coordonnees = (
        None if (longitude is None) or (latitude is None) else f"{latitude},{longitude}"
    )
    return coordonnees


# Dirigeants PP
def format_dirigeants_pp(list_dirigeants_pp_sqlite, list_all_dirigeants=[]):
    dirigeants_pp = json.loads(list_dirigeants_pp_sqlite)
    dirigeants_pp_processed = []
    for dirigeant_pp in dirigeants_pp:
        dirigeant_pp["nom"] = format_nom(
            dirigeant_pp["nom_patronymique"], dirigeant_pp["nom_usage"]
        )
        dirigeant_pp["date_naissance"] = normalize_date(dirigeant_pp["date_naissance"])

        # Drop qualite`s exact and partial duplicates
        dirigeant_pp["qualite"] = unique_qualites(dirigeant_pp["qualite"])

        dirigeants_pp_processed.append(
            dict(
                nom=dirigeant_pp["nom"],
                prenoms=dirigeant_pp["prenoms"],
                date_naissance=dirigeant_pp["date_naissance"],
                ville_naissance=dirigeant_pp["ville_naissance"],
                pays_naissance=dirigeant_pp["pays_naissance"],
                qualite=dirigeant_pp["qualite"],
            )
        )
        # Liste dirigeants
        if dirigeant_pp["prenoms"] and dirigeant_pp["nom"]:
            list_all_dirigeants.append(
                dirigeant_pp["prenoms"] + " " + dirigeant_pp["nom"]
            )
        elif dirigeant_pp["prenoms"] and not dirigeant_pp["nom"]:
            list_all_dirigeants.append(dirigeant_pp["prenoms"])
            logging.info(
                f'Missing dirigeant nom for ****** {dirigeant_pp["siren"]} '
                f'****** prenoms = {dirigeant_pp["prenoms"]}'
            )
        elif dirigeant_pp["nom"] and not dirigeant_pp["prenoms"]:
            list_all_dirigeants.append(dirigeant_pp["nom"])
            logging.info(
                f"Missing dirigeant prenoms for ****** "
                f'{dirigeant_pp["siren"]} ****** nom : {dirigeant_pp["nom"]}'
            )
        else:
            logging.info(f'Missing dirigeants names for ****** {dirigeant_pp["siren"]}')

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
            logging.info(
                f'Missing denomination dirigeant for ***** {dirigeant_pm["siren"]}'
            )
        dirigeant_pm["qualite"] = unique_qualites(dirigeant_pm["qualite"])
        dirigeants_pm_processed.append(
            dict(
                siren=dirigeant_pm["siren_pm"],
                denomination=dirigeant_pm["denomination"],
                sigle=dirigeant_pm["sigle"],
                qualite=dirigeant_pm["qualite"],
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
def format_etablissements_and_complements(list_etablissements_sqlite, nom_complet):
    etablissements = json.loads(list_etablissements_sqlite)
    etablissements_processed = []
    complements = {
        "est_uai": False,
        "est_rge": False,
        "est_finess": False,
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
            etablissement["libelle_commune_etranger"],
            etablissement["libelle_pays_etranger"],
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
def format_siege_unite_legale(siege):
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
        siege["libelle_commune_etranger"],
        siege["libelle_pays_etranger"],
    )
    siege["coordonnees"] = format_coordonnees(siege["longitude"], siege["latitude"])
    siege["departement"] = format_departement(siege["commune"])
    siege["est_siege"] = str_to_bool(siege["est_siege"])
    siege["liste_idcc"] = str_to_list(siege["liste_idcc"])
    siege["liste_rge"] = str_to_list(siege["liste_rge"])
    siege["liste_uai"] = str_to_list(siege["liste_uai"])
    siege["liste_finess"] = str_to_list(siege["liste_finess"])
    siege["liste_id_bio"] = str_to_list(siege["liste_id_bio"])

    return siege
