import json
import logging

from slugify import slugify

from dag_datalake_sirene.helpers.geolocalisation import (
    transform_coordinates,
)
from dag_datalake_sirene.helpers.utils import (
    drop_exact_duplicates,
    get_empty_string_if_none,
    load_file,
    sqlite_str_to_bool,
    str_to_bool,
    str_to_list,
)
from dag_datalake_sirene.workflows.data_pipelines.elasticsearch.clean_data import (
    drop_duplicates_dirigeants_pm,
    drop_duplicates_personnes_physiques,
    unique_qualites,
)
from dag_datalake_sirene.workflows.data_pipelines.elasticsearch.es_fields import (
    get_elasticsearch_field_name,
)

sections_NAF = load_file("sections_codes_naf.json")
mapping_dep_to_reg = load_file("dep_to_reg.json")
mapping_role_dirigeants = load_file("roles_dirigeants.json")
mapping_commune_to_epci = load_file("epci.json")
nature_juridique_service_public = set(load_file("administration_nature_juridique.json"))
service_public_whitelist = set(load_file("administration_siren_whitelist.json"))
service_public_blacklist = set(load_file("administration_siren_blacklist.json"))
L100_3_siren_whitelist = set(load_file("administration_L100-3_siren_whitelist.json"))
excluded_nature_juridique_L100_3 = set(
    load_file("administration_L100-3_nature_juridique_exclue.json")
)


# Nom complet
def format_nom_complet(
    nom=None,
    nom_usage=None,
    prenom=None,
    nom_raison_sociale=None,
    est_personne_morale_insee: bool = False,
    is_non_diffusible: bool = False,
):
    """Build `nom_complet` from identity parts.

    Returns `nom_raison_sociale` for personne morale, or builds personne physique name
    in format: "PRENOM NOM_USAGE (NOM_NAISSANCE)".

    Returns None if no data provided.
    """
    # Personne morale and non diffusible: keep only raison sociale when available
    if est_personne_morale_insee and is_non_diffusible and nom_raison_sociale:
        return nom_raison_sociale.upper().strip()

    # Raison sociale takes precedence for legal entities
    if nom_raison_sociale:
        return nom_raison_sociale.upper().strip()

    # Build natural person name
    if not (prenom or nom or nom_usage):
        return None

    parts = []
    if prenom:
        parts.append(prenom)

    if nom_usage:
        if nom:
            parts.append(f"{nom_usage} ({nom})")
        else:
            parts.append(nom_usage)
    elif nom:
        parts.append(nom)

    return " ".join(parts).upper().strip()


def get_nom_commercial(unite_legale):
    siege = unite_legale.get("siege", None)
    if siege is not None:
        nom_commercial = siege.get("nom_commercial", None)
        return nom_commercial
    return None


# Slug
# Because we need to create sitemap and to ensure coherence
# between sitemap values and slug in API. We calculate this field
# with this function and we add it both in elasticsearch and in sitemap


def format_slug(
    nom_complet,
    siren,
    sigle=None,
    nom_commercial_siege=None,
    statut_diffusion=None,
    nature_juridique_unite_legale=None,
):
    """Generate a slug based on company information."""
    # If a personne physique company is non-diffusible, use only the SIREN as the slug
    if statut_diffusion == "P" and not is_personne_morale_insee(
        nature_juridique_unite_legale
    ):
        return slugify(siren)

    # Initialize slug parts with the nom_complet
    slug_parts = [nom_complet]

    # Handle the case of nom_commercial_siege
    if nom_commercial_siege:
        slug_parts.append(nom_commercial_siege)

    # Sigle is not a diffusible field for non-diffusible personnes morales
    if sigle and statut_diffusion == "O":
        slug_parts.append(sigle)
    if siren:
        slug_parts.append(siren)

    # Join parts to form the full name, and slugify it
    full_name = " ".join(filter(None, slug_parts)).lower()
    return slugify(full_name) if full_name else ""


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


def is_personne_morale_insee(nature_juridique_unite_legale):
    """Return True if INSEE legal nature indicates personne morale.

    Rules (personne physique are codes 1000 and  les unités non dotées de la personnalité morale start with '2') https://www.insee.fr/fr/information/7456564:
    - Return False for personne physique and les unités non dotées de la personnalité morale
    - Return True otherwise
    """
    if nature_juridique_unite_legale is None:
        return True
    nature = str(nature_juridique_unite_legale)
    return not (nature == "1000" or nature.startswith("2"))


# ESS
def is_ess(est_ess_france, ess_insee):
    est_ess_insee = ess_insee == "O" if ess_insee is not None else False
    return est_ess_france or est_ess_insee


# Administration
def is_service_public(
    nature_juridique_unite_legale: str, siren: str, etat_administratif: str
) -> bool:
    """
    Determine if a given entity is classified as a public service.

    Args:
        nature_juridique_unite_legale (str): The legal nature code of the entity
        siren (str): The SIREN identification number of the entity
        etat_administratif (str): Administrative status of the entity ('A' for active, 'C' for closed)

    Returns:
        bool: True if the entity is classified as a public service, False otherwise

    Notes:
        - Entities in the service_public_blacklist are never considered public services
        - Closed entities (etat_administratif == 'C') are never considered public services
        - Entities are considered public services if either:
          1. Their legal nature code is in nature_juridique_service_public, or
          2. Their SIREN is in service_public_whitelist
    """
    # Check blacklist first
    if siren in service_public_blacklist:
        return False

    # Closed entities are not considered public services
    if etat_administratif == "C":
        return False

    # Check if entity is in whitelist or has a public service legal nature code
    return (
        (nature_juridique_unite_legale in nature_juridique_service_public)
        if nature_juridique_unite_legale is not None
        else False
    ) or siren in service_public_whitelist


def is_administration_l100_3(
    siren: str, nature_juridique: str, is_service_public: bool = False
) -> bool:
    """
    Determine if a given entity is classified as `administration au sens L100-3`
    """
    if is_service_public is False:
        return False
    else:
        return (
            True
            if siren in L100_3_siren_whitelist
            else False
            if nature_juridique in excluded_nature_juridique_L100_3
            else True
        )


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


# Perosnnes physiques (dirigeants pp)
def format_personnes_physiques(list_personnes_physiques_sqlite, list_all_personnes=[]):
    personnes_physiques = json.loads(list_personnes_physiques_sqlite)
    personnes_physiques_processed = []

    for personne_physique in personnes_physiques:
        personne_physique["nom"] = format_nom(
            personne_physique["nom"], personne_physique["nom_usage"]
        )

        # Drop qualites exact and partial duplicates
        personne_physique["role_description"] = unique_qualites(
            personne_physique["role_description"]
        )

        personnes_physiques_processed.append(
            dict(
                nom=personne_physique["nom"],
                prenoms=personne_physique["prenoms"],
                date_de_naissance=personne_physique["date_de_naissance"],
                nationalite=personne_physique["nationalite"],
                role=personne_physique["role_description"],
                date_mise_a_jour=personne_physique["date_mise_a_jour"],
            )
        )

        # Liste personnes
        if personne_physique["prenoms"] and personne_physique["nom"]:
            list_all_personnes.append(
                personne_physique["prenoms"] + " " + personne_physique["nom"]
            )
        elif personne_physique["prenoms"] and not personne_physique["nom"]:
            list_all_personnes.append(personne_physique["prenoms"])
            logging.debug(
                f"Missing personne_physique nom for ****** {personne_physique['siren']}"
                f" ***** prenoms = {personne_physique['prenoms']}"
            )
        elif personne_physique["nom"] and not personne_physique["prenoms"]:
            list_all_personnes.append(personne_physique["nom"])
            logging.debug(
                f"Missing personne_physique prenoms for ****** "
                f"{personne_physique['siren']} ****** nom : {personne_physique['nom']}"
            )
        else:
            logging.debug(
                f"Missing personne_physique names for **** {personne_physique['siren']}"
            )

    if personnes_physiques_processed:
        # Case when two personne_physique have exactly the same fields/values
        personnes_physiques_processed = drop_exact_duplicates(
            personnes_physiques_processed
        )
        # Case when two personne_physique have partially the same fields/values
        # (eg. same name, and firstname, different date or qualities)
        personnes_physiques_processed = drop_duplicates_personnes_physiques(
            personnes_physiques_processed
        )

    return personnes_physiques_processed, list(set(list_all_personnes))


# Dirigeants PM
def format_dirigeants_pm(list_dirigeants_pm_sqlite, list_all_dirigeants=[]):
    dirigeants_pm = json.loads(list_dirigeants_pm_sqlite)
    dirigeants_pm_processed = []
    for dirigeant_pm in dirigeants_pm:
        if dirigeant_pm["denomination"]:
            list_all_dirigeants.append(dirigeant_pm["denomination"])
        else:
            logging.debug(
                f"Missing denomination dirigeant for ***** {dirigeant_pm['siren']}"
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
        etablissement["departement"] = format_departement(etablissement["commune"])
        etablissement["region"] = label_region_from_departement(
            etablissement["departement"]
        )
        if etablissement["latitude"] is None or etablissement["longitude"] is None:
            (
                etablissement["latitude"],
                etablissement["longitude"],
            ) = transform_coordinates(
                etablissement["departement"],
                etablissement["x"],
                etablissement["y"],
            )
        etablissement["ancien_siege"] = sqlite_str_to_bool(
            etablissement["ancien_siege"]
        )
        etablissement["coordonnees"] = format_coordonnees(
            etablissement["longitude"], etablissement["latitude"]
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
    siege["departement"] = format_departement(siege["commune"])
    if siege["latitude"] is None or siege["longitude"] is None:
        siege["latitude"], siege["longitude"] = transform_coordinates(
            siege["departement"],
            siege["x"],
            siege["y"],
        )
    siege["coordonnees"] = format_coordonnees(siege["longitude"], siege["latitude"])
    siege["region"] = label_region_from_departement(siege["departement"])
    siege["epci"] = label_epci_from_commune(siege["commune"])
    siege["est_siege"] = str_to_bool(siege["est_siege"])
    siege["liste_idcc"] = str_to_list(siege["liste_idcc"])
    siege["liste_rge"] = str_to_list(siege["liste_rge"])
    siege["liste_uai"] = str_to_list(siege["liste_uai"])
    siege["liste_finess"] = str_to_list(siege["liste_finess"])
    siege["liste_id_bio"] = str_to_list(siege["liste_id_bio"])

    return siege


def calculate_company_size_factor(unite_legale):
    """
    Calculate the size factor of an enterprise based on its category
    code and the number of open establishments.
    """
    code_categorie_entreprise = unite_legale["code_categorie_entreprise"]
    nombre_etablissements = unite_legale["nombre_etablissements_ouverts"]

    if code_categorie_entreprise is None or nombre_etablissements is None:
        return None

    size_factor = code_categorie_entreprise * nombre_etablissements

    return size_factor
