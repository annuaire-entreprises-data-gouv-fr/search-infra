import logging

from dag_datalake_sirene.helpers.utils import (
    remove_spaces,
)
from dag_datalake_sirene.workflows.data_pipelines.rne.database.rne_model import (
    RNECompany,
)
from dag_datalake_sirene.workflows.data_pipelines.rne.database.ul_model import (
    Activite,
    Adresse,
    DirigeantsPM,
    DirigeantsPP,
    Etablissement,
    Siege,
    UniteLegale,
)


def map_rne_company_to_ul(rne_company: RNECompany, unite_legale: UniteLegale):
    unite_legale.siren = rne_company.siren
    unite_legale.date_mise_a_jour = rne_company.updatedAt
    unite_legale.statut_diffusion = rne_company.formality.diffusionINSEE
    unite_legale.nature_juridique = get_forme_juridique(rne_company)
    unite_legale.date_creation = get_date_creation(rne_company)
    unite_legale.forme_exercice_activite_principale = (
        rne_company.formality.content.formeExerciceActivitePrincipale
    )
    unite_legale.etat_administratif = (
        rne_company.formality.content.natureCessationEntreprise.etatAdministratifInsee
    )

    cessation = get_detail_cessation(rne_company)
    if cessation:
        unite_legale.immatriculation.date_radiation = (
            cessation.dateRadiation
            or cessation.dateEffet
            or cessation.dateCessationTotaleActivite
            or None
        )

    identite_entr = get_identite_entreprise(rne_company)
    if identite_entr:
        unite_legale.denomination = identite_entr.denomination
        unite_legale.nom_commercial = identite_entr.nomCommercial
        unite_legale.immatriculation.date_immatriculation = identite_entr.dateImmat
        unite_legale.tranche_effectif_salarie = identite_entr.effectifSalarie
        unite_legale.immatriculation.indicateur_associe_unique = (
            identite_entr.indicateurAssocieUnique
        )
        unite_legale.immatriculation.date_debut_activite = identite_entr.dateDebutActiv
    else:
        logging.warning(f"Unite legale has no identite : {unite_legale.siren}")

    identite_descr = get_identite_description(rne_company)
    if identite_descr:
        unite_legale.immatriculation.capital_social = identite_descr.montantCapital
        unite_legale.immatriculation.date_cloture_exercice = (
            identite_descr.dateClotureExerciceSocial
        )
        unite_legale.immatriculation.capital_variable = identite_descr.capitalVariable
        unite_legale.immatriculation.devise_capital = identite_descr.deviseCapital
        unite_legale.immatriculation.duree_personne_morale = identite_descr.duree

    unite_legale.immatriculation.nature_entreprise = get_nature_entreprise_list(
        rne_company
    )

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
        logging.warning(f"Unite legale has no siege : {unite_legale.siren}")

    # Add mapping for etablissements
    etablissements = get_etablissements(rne_company)
    if etablissements:
        unite_legale.etablissements = map_rne_etablissements_to_ul(etablissements)
    else:
        unite_legale.etablissements = []

    return unite_legale


def get_value(rne_company: RNECompany, key: str, default=None):
    content = rne_company.formality.content

    if rne_company.is_personne_morale():
        return getattr(content.personneMorale, key, default)
    if rne_company.is_exploitation():
        return getattr(content.exploitation, key, default)
    if rne_company.is_personne_physique():
        return getattr(content.personnePhysique, key, default)
    return default


def get_nature_entreprise_list(rne_company: "RNECompany") -> list[str] | None:
    nature_entreprise = set()

    # Add the main activity form if it exists
    forme_exercice_activite_principale = getattr(
        rne_company.formality.content, "formeExerciceActivitePrincipale", None
    )
    if forme_exercice_activite_principale:
        nature_entreprise.add(forme_exercice_activite_principale)

    # Check the main establishment
    siege = get_siege(rne_company)
    if siege and siege.activites:
        for activite in siege.activites:
            if getattr(activite, "indicateurPrincipal", None):
                forme_exercice = getattr(activite, "formeExercice", None)
                if forme_exercice:
                    nature_entreprise.add(forme_exercice)

    # Check other establishments
    etablissements = get_etablissements(rne_company)
    if etablissements:
        for etablissement in etablissements:
            for activite in getattr(etablissement, "activites", []):
                if getattr(activite, "indicateurPrincipal", None):
                    forme_exercice = getattr(activite, "formeExercice", None)
                    if forme_exercice:
                        nature_entreprise.add(forme_exercice)

    # Return the list of nature_entreprise or None if it's empty
    return list(nature_entreprise) if nature_entreprise else None


def get_etablissements(rne_company: "RNECompany"):
    return get_value(rne_company, "autresEtablissements", default=[])


def get_forme_juridique(rne_company: RNECompany) -> str | None:
    forme_juridique = rne_company.formality.formeJuridique
    if forme_juridique:
        return forme_juridique

    content = rne_company.formality.content
    if content.natureCreation:
        return content.natureCreation.formeJuridique

    identite = get_identite_entreprise(rne_company)
    if identite:
        return identite.formeJuridique or identite.formeJuridiqueInsee

    return None


def get_date_creation(rne_company: RNECompany):
    return (
        rne_company.createdAt
        or rne_company.formality.content.natureCreation.dateCreation
    )


def get_denomination_personne_physique(
    rne_company: RNECompany, unite_legale: UniteLegale
):
    if rne_company.is_personne_physique() and unite_legale.dirigeants:
        dirigeant = unite_legale.dirigeants[0]
        unite_legale.nom = dirigeant.nom
        unite_legale.nom_usage = dirigeant.nom_usage
        unite_legale.prenom = dirigeant.prenoms
    return unite_legale


def get_identite_entreprise(rne_company: RNECompany):
    identite = get_value(rne_company, "identite")
    return getattr(identite, "entreprise", None) if identite else None


def get_identite_description(rne_company: RNECompany):
    identite = get_value(rne_company, "identite")
    return getattr(identite, "description", None) if identite else None


def get_detail_cessation(rne_company: RNECompany):
    return get_value(rne_company, "detailCessationEntreprise")


def get_adresse(rne_company: RNECompany):
    adresse_entreprise = get_value(rne_company, "adresseEntreprise")
    if adresse_entreprise and hasattr(adresse_entreprise, "adresse"):
        return adresse_entreprise.adresse
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


def map_rne_dirigeant_pp_to_ul(dirigeant_pp_rne, role_entreprise):
    dirigeant_pp_ul = DirigeantsPP()
    dirigeant_pp_ul.date_de_naissance = dirigeant_pp_rne.dateDeNaissance
    dirigeant_pp_ul.nom = dirigeant_pp_rne.nom
    dirigeant_pp_ul.nom_usage = dirigeant_pp_rne.nomUsage
    if isinstance(dirigeant_pp_rne.prenoms, list):
        dirigeant_pp_ul.prenoms = " ".join(dirigeant_pp_rne.prenoms)
    else:
        dirigeant_pp_ul.prenoms = dirigeant_pp_rne.prenoms
    dirigeant_pp_ul.genre = dirigeant_pp_rne.genre
    dirigeant_pp_ul.role = role_entreprise
    dirigeant_pp_ul.nationalite = dirigeant_pp_rne.nationalite
    dirigeant_pp_ul.situation_matrimoniale = dirigeant_pp_rne.situationMatrimoniale
    return dirigeant_pp_ul


def map_rne_dirigeant_pm_to_ul(dirigeant_pm_rne):
    dirigeant_pm_ul = DirigeantsPM()
    dirigeant_pm_ul.siren = remove_spaces(dirigeant_pm_rne.siren)
    dirigeant_pm_ul.denomination = dirigeant_pm_rne.denomination
    dirigeant_pm_ul.role = dirigeant_pm_rne.roleEntreprise
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

    # Map activites if they exist
    if hasattr(siege_rne, "activites") and siege_rne.activites:
        siege.activites = map_rne_activites_to_ul(siege_rne.activites)

    return siege


def map_dirigeants_rne_to_dirigeants_list_ul(dirigeants_rne):
    list_dirigeants = []
    # Cas personne morale et exploitation
    for dirigeant in dirigeants_rne:
        if hasattr(dirigeant, "typeDePersonne"):
            if dirigeant.typeDePersonne == "INDIVIDU":
                list_dirigeants.append(
                    map_rne_dirigeant_pp_to_ul(
                        dirigeant.individu.descriptionPersonne, dirigeant.roleEntreprise
                    )
                )
            elif dirigeant.typeDePersonne == "ENTREPRISE":
                list_dirigeants.append(map_rne_dirigeant_pm_to_ul(dirigeant.entreprise))
        # Cas personne physique
        else:
            list_dirigeants.append(
                map_rne_dirigeant_pp_to_ul(dirigeant.descriptionPersonne, None)
            )

    return list_dirigeants


def map_rne_etablissements_to_ul(etablissements_rne):
    """Maps RNE etablissements to UL etablissements"""
    etablissements_ul = []

    if not etablissements_rne:
        return etablissements_ul

    for etablissement_rne in etablissements_rne:
        etablissement_ul = Etablissement()
        description = etablissement_rne.descriptionEtablissement

        if description:
            etablissement_ul.siret = description.siret

        # Map activites if they exist
        if hasattr(etablissement_rne, "activites") and etablissement_rne.activites:
            etablissement_ul.activites = map_rne_activites_to_ul(
                etablissement_rne.activites
            )

        etablissements_ul.append(etablissement_ul)

    return etablissements_ul


def map_rne_activites_to_ul(activites_rne):
    """Maps RNE activites to UL activites"""
    activites_ul = []

    for activite_rne in activites_rne:
        try:
            activite_ul = Activite()

            activite_ul.code_category = getattr(activite_rne, "categoryCode", None)
            activite_ul.indicateur_principal = getattr(
                activite_rne, "indicateurPrincipal", None
            )
            activite_ul.indicateur_prolongement = getattr(
                activite_rne, "indicateurProlongement", None
            )
            activite_ul.date_debut = getattr(activite_rne, "dateDebut", None)
            activite_ul.form_exercice = getattr(activite_rne, "formeExercice", None)
            activite_ul.categorisation_activite1 = getattr(
                activite_rne, "categorisationActivite1", None
            )
            activite_ul.categorisation_activite2 = getattr(
                activite_rne, "categorisationActivite2", None
            )
            activite_ul.categorisation_activite3 = getattr(
                activite_rne, "categorisationActivite3", None
            )
            activite_ul.indicateur_activitee_ape = getattr(
                activite_rne, "indicateurActiviteeApe", None
            )
            activite_ul.code_ape = getattr(activite_rne, "codeApe", None)
            activite_ul.activite_rattachee_eirl = getattr(
                activite_rne, "activiteRattacheeEirl", None
            )

            activites_ul.append(activite_ul)
        except Exception as e:
            logging.error(f"Error mapping activite: {e}")
            logging.error(f"Activite RNE data: {activite_rne}")
    return activites_ul
