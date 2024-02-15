import logging
from dag_datalake_sirene.workflows.data_pipelines.rne.database.rne_model import (
    RNECompany,
)
from dag_datalake_sirene.workflows.data_pipelines.rne.database.ul_model import (
    Adresse,
    DirigeantsPP,
    DirigeantsPM,
    UniteLegale,
    Siege,
)


def map_rne_company_to_ul(rne_company: RNECompany, unite_legale: UniteLegale):
    unite_legale.siren = rne_company.siren
    unite_legale.date_mise_a_jour = rne_company.updatedAt
    unite_legale.statut_diffusion = rne_company.formality.diffusionINSEE
    unite_legale.date_creation = get_date_creation(rne_company)
    unite_legale.forme_exercice_activite_principale = (
        rne_company.formality.content.formeExerciceActivitePrincipale
    )
    unite_legale.etat_administratif = (
        rne_company.formality.content.natureCessationEntreprise.etatAdministratifInsee
    )
    cessation = get_detail_cessation(rne_company)
    if cessation:
        unite_legale.date_radiation = cessation.dateRadiation

    identite = get_identite(rne_company)
    if identite:
        unite_legale.denomination = identite.denomination
        unite_legale.nom_commercial = identite.nomCommercial
        unite_legale.nature_juridique = identite.formeJuridique
        unite_legale.date_immatriculation = identite.dateImmat
        unite_legale.tranche_effectif_salarie = identite.effectifSalarie
    else:
        logging.warning(f"++++++++ Unite legale has no identite : {unite_legale.siren}")

    company_address = get_adresse(rne_company)
    unite_legale.adresse = map_address_rne_to_ul(company_address)

    company_dirigeants = get_dirigeants(rne_company)
    unite_legale.dirigeants = map_dirigeants_rne_to_dirigeants_list_ul(
        company_dirigeants
    )

    unite_legale = get_denomination_personne_physique(rne_company, unite_legale)

    siege = get_siege(rne_company)
    if siege:
        unite_legale.siege = map_rne_siege_to_ul(siege)
    else:
        logging.warning(f"+++++++++++ Unite legale has no siege : {unite_legale.siren}")

    return unite_legale


def get_date_creation(rne_company: RNECompany):
    return (
        rne_company.createdAt
        if rne_company.createdAt
        else rne_company.formality.content.natureCreation.dateCreation
    )


def get_denomination_personne_physique(
    rne_company: RNECompany, unite_legale: UniteLegale
):
    if rne_company.is_personne_physique():
        dirigeant = unite_legale.dirigeants[0] if unite_legale.dirigeants else None
        if dirigeant:
            unite_legale.nom = dirigeant.nom
            unite_legale.nom_usage = dirigeant.nom_usage
            unite_legale.prenom = dirigeant.prenoms
    return unite_legale


def get_value(rne_company: RNECompany, key: str, default=None):
    content = rne_company.formality.content

    if rne_company.is_personne_morale():
        return getattr(content.personneMorale, key, default)
    elif rne_company.is_exploitation():
        return getattr(content.exploitation, key, default)
    elif rne_company.is_personne_physique():
        return getattr(content.personnePhysique, key, default)
    else:
        return default


def get_identite(rne_company: RNECompany):
    identite = get_value(rne_company, "identite")

    if identite and hasattr(identite, "entreprise"):
        return identite.entreprise
    else:
        return None


def get_detail_cessation(rne_company: RNECompany):
    return get_value(rne_company, "detailCessationEntreprise")


def get_adresse(rne_company: RNECompany):
    adresse_entreprise = get_value(rne_company, "adresseEntreprise")
    if adresse_entreprise and hasattr(adresse_entreprise, "adresse"):
        return adresse_entreprise.adresse
    else:
        return {}


def get_siege(rne_company: RNECompany):
    return get_value(rne_company, "etablissementPrincipal", default={})


def get_dirigeants(rne_company: RNECompany):
    # Method to map dirigeants based on the logic in RNECompany
    composition = get_value(rne_company, "composition")

    if rne_company.is_personne_physique():
        identite = get_value(rne_company, "identite")
        return [identite.entrepreneur] if identite and identite.entrepreneur else []

    return composition.pouvoirs if composition else []


def map_address_rne_to_ul(address_rne):
    address_ul = Adresse()
    address_ul.pays = address_rne.pays
    address_ul.code_pays = address_rne.codePays
    address_ul.commune = address_rne.commune
    address_ul.code_postal = address_rne.codePostal
    address_ul.code_commune = address_rne.codeInseeCommune
    address_ul.voie = address_rne.voie
    address_ul.num_voie = address_rne.numVoie
    address_ul.type_voie = address_rne.typeVoie
    address_ul.indice_repetition = address_rne.indiceRepetition
    address_ul.distribution_speciale = address_rne.distributionSpeciale
    address_ul.complement_localisation = address_rne.complementLocalisation
    return address_ul


def map_rne_dirigeant_pp_to_ul(dirigeant_pp_rne):
    dirigeant_pp_ul = DirigeantsPP()
    dirigeant_pp_ul.date_de_naissance = dirigeant_pp_rne.dateDeNaissance
    dirigeant_pp_ul.nom = dirigeant_pp_rne.nom
    dirigeant_pp_ul.nom_usage = dirigeant_pp_rne.nomUsage
    if isinstance(dirigeant_pp_rne.prenoms, list):
        dirigeant_pp_ul.prenoms = " ".join(dirigeant_pp_rne.prenoms)
    else:
        dirigeant_pp_ul.prenoms = dirigeant_pp_rne.prenoms
    dirigeant_pp_ul.genre = dirigeant_pp_rne.genre
    dirigeant_pp_ul.role = dirigeant_pp_rne.role
    dirigeant_pp_ul.nationalite = dirigeant_pp_rne.nationalite
    dirigeant_pp_ul.situation_matrimoniale = dirigeant_pp_rne.situationMatrimoniale
    return dirigeant_pp_ul


def map_rne_dirigeant_pm_to_ul(dirigeant_pm_rne):
    dirigeant_pm_ul = DirigeantsPM()
    dirigeant_pm_ul.siren = dirigeant_pm_rne.siren
    dirigeant_pm_ul.denomination = dirigeant_pm_rne.denomination
    dirigeant_pm_ul.role = dirigeant_pm_rne.role
    dirigeant_pm_ul.pays = dirigeant_pm_rne.pays
    dirigeant_pm_ul.forme_juridique = dirigeant_pm_rne.formeJuridique
    return dirigeant_pm_ul


def map_rne_siege_to_ul(siege_rne):
    siege = Siege()
    description_siege = siege_rne.descriptionEtablissement
    adresse_siege = siege_rne.adresse

    siege.siret = description_siege.siret
    siege.nom_commercial = description_siege.nomCommercial
    siege.enseigne = description_siege.enseigne

    siege.adresse.pays = adresse_siege.pays
    siege.adresse.code_pays = adresse_siege.codePays
    siege.adresse.commune = adresse_siege.commune
    siege.adresse.code_postal = adresse_siege.codePostal
    siege.adresse.voie = siege_rne.adresse.voie
    siege.adresse.num_voie = adresse_siege.numVoie
    siege.adresse.type_voie = adresse_siege.typeVoie
    siege.adresse.indice_repetition = adresse_siege.indiceRepetition
    siege.adresse.complement_localisation = adresse_siege.complementLocalisation
    siege.adresse.distribution_speciale = adresse_siege.distributionSpeciale
    siege.adresse.pays = adresse_siege.pays

    return siege


def map_dirigeants_rne_to_dirigeants_list_ul(dirigeants_rne):
    list_dirigeants = []
    # Cas personne morale et exploitation
    for dirigeant in dirigeants_rne:
        if hasattr(dirigeant, "typeDePersonne"):
            if dirigeant.typeDePersonne == "INDIVIDU":
                list_dirigeants.append(
                    map_rne_dirigeant_pp_to_ul(dirigeant.individu.descriptionPersonne)
                )
            elif dirigeant.typeDePersonne == "ENTREPRISE":
                list_dirigeants.append(map_rne_dirigeant_pm_to_ul(dirigeant.entreprise))
        # Cas personne physique
        else:
            list_dirigeants.append(
                map_rne_dirigeant_pp_to_ul(dirigeant.descriptionPersonne)
            )

    return list_dirigeants
