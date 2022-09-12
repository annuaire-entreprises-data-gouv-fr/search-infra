import json
import logging
import itertools

from dag_datalake_sirene.helpers.utils import (
    drop_duplicates,
    get_empty_string_if_none,
    normalize_date,
    normalize_string,
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
    sigle=None,
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

    if sigle:
        name = f"{name} ({sigle})"
    return name.lower().strip() if name else name


# Noms
def format_nom(
    nom=None,
    nom_usage=None,
):
    if nom_usage:
        formatted_name = f"{nom_usage} ({nom})" if nom else f"{nom_usage}"
    else:
        formatted_name = f"{nom}" if nom else None
    return formatted_name.lower().strip() if formatted_name else formatted_name


# Entrepreneur individuel
def is_entrepreneur_individuel(nature_juridique_unite_legale):
    if nature_juridique_unite_legale in ["1", "10", "1000"]:
        return "true"
    else:
        return "false"


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
    commune,
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
        if commune is None:
            adresse = adresse
        else:
            adresse = (
                adresse
                + " "
                + get_empty_string_if_none(commune)
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
    return adresse.strip()


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
        dirigeant_pp["nom"] = format_nom(dirigeant_pp["nom_patronymique"], dirigeant_pp["nom_usage"])
        dirigeant_pp["date_naissance"] = normalize_date(dirigeant_pp["date_naissance"])
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
            logging.info(f'Missing dirigeant nom for ****** {dirigeant_pp["siren"]} '
                         f'****** prenoms = {dirigeant_pp["prenoms"]}')
        elif dirigeant_pp["nom"] and not dirigeant_pp["prenoms"]:
            list_all_dirigeants.append(dirigeant_pp["nom"])
            logging.info(f'Missing dirigeant prenoms for ****** '
                         f'{dirigeant_pp["siren"]} ****** nom : {dirigeant_pp["nom"]}')
        else:
            logging.info(f'Missing dirigeants names for ****** {dirigeant_pp["siren"]}')

    dirigeants_pp_processed = drop_duplicates(dirigeants_pp_processed)
    return dirigeants_pp_processed, list(set(list_all_dirigeants))


# Dirigeants PM
def format_dirigeants_pm(list_dirigeants_pm_sqlite, list_all_dirigeants=[]):
    dirigeants_pm = json.loads(list_dirigeants_pm_sqlite)
    dirigeants_pm_processed = []
    for dirigeant_pm in dirigeants_pm:
        if dirigeant_pm["denomination"]:
            list_all_dirigeants.append(dirigeant_pm["denomination"])
        else:
            logging.info(f'Missing denomination dirigeant for ***** {dirigeant_pm["siren"]}')

        dirigeants_pm_processed.append(
            dict(
                siren=dirigeant_pm["siren_pm"],
                denomination=dirigeant_pm["denomination"],
                sigle=dirigeant_pm["sigle"],
                qualite=dirigeant_pm["qualite"],
            )
        )
    dirigeants_pm_processed = drop_duplicates(dirigeants_pm_processed)

    return dirigeants_pm_processed, list(set(list_all_dirigeants))
