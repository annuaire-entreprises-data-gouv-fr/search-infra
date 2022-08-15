import json
from dag_datalake_sirene.helpers.single_dispatch_funcs import get_string

labels_file_path = "dags/dag_datalake_sirene/labels/"


def load_file(file_name: str):
    with open(f"{labels_file_path}{file_name}") as json_file:
        file_decoded = json.load(json_file)
    return file_decoded


sections_NAF = load_file("sections_codes_naf.json")


# Nom complet
def create_nom_complet(nature_juridique_unite_legale=None, nom=None, nom_usage=None,
                       nom_raison_sociale=None, sigle=None, prenom=None):
    is_auto_entrepreneur = nature_juridique_unite_legale == "1000"

    if is_auto_entrepreneur:
        formatted_nom_usage = " " + nom_usage if nom_usage else ""
        formatted_sigle = sigle if sigle else ""

        if prenom is None and nom is None:
            return None
        if nom_usage is None:
            return f"{prenom} {nom} ({formatted_sigle})".lower().replace(" ()", "")
        else:
            return f"{prenom} {formatted_nom_usage}({nom}, {formatted_sigle})".lower(

            ).replace(" ()", "")

    else:
        if nom_raison_sociale is None and sigle is None:
            return None
        else:
            formatted_sigle = f" ({sigle})" if sigle else ""
            return f"{nom_raison_sociale}{formatted_sigle}".lower()


# Entrepreneur individuel
def create_entrepreneur_individuel(nature_juridique_unite_legale):
    if nature_juridique_unite_legale in ["1", "10", "1000"]:
        return 'true'
    else:
        return 'false'


# Section activité principale
def create_section(activite_principale_unite_legale):
    if activite_principale_unite_legale is not None:
        code_naf = activite_principale_unite_legale[:2]
        section_activite_principale = sections_NAF[
            code_naf] if code_naf in sections_NAF else None
        return section_activite_principale
    else:
        return None


# Adresse complète
def create_adresse_complete(complement_adresse, numero_voie, indice_repetition,
                            type_voie, libelle_voie, libelle_commune, libelle_cedex,
                            distribution_speciale, commune, cedex,
                            libelle_commune_etranger, libelle_pays_etranger):
    col_list = [complement_adresse, numero_voie, indice_repetition, type_voie,
                libelle_voie, distribution_speciale]
    adresse = ""
    for column in col_list:
        if column:
            adresse = adresse + " " + column
    if cedex is None:
        if commune is None:
            adresse = adresse
        else:
            adresse = adresse + " " + get_string(commune) + " " + \
                      get_string(libelle_commune)
    else:
        adresse = adresse + " " + get_string(cedex) + " " + get_string(libelle_cedex)
    etranger_list = [libelle_commune_etranger, libelle_pays_etranger]
    for column in etranger_list:
        if column:
            adresse = adresse + " " + column if column != "" else ""
    return adresse.strip()


# Département
def create_departement(commune):
    departement = str(commune)[:3] if str(commune)[:2] == "97" else (
        None if commune is None else str(commune)[:2])
    return departement


# Coordonnées
def create_coordonnees(longitude, latitude):
    coordonnees = None if (longitude is None) or (
                latitude is None) else f"{latitude},{longitude}"
    return coordonnees
