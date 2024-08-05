import logging
from dag_datalake_sirene.helpers.utils import (
    remove_spaces,
)
from dag_datalake_sirene.workflows.data_pipelines.rne.database.rne_model import (
    RNECompany,
)
from dag_datalake_sirene.workflows.data_pipelines.rne.database.ul_model import (
    Adresse,
    Beneficiaire,
    DirigeantsPP,
    DirigeantsPM,
    UniteLegale,
    Siege,
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
    unite_legale.immatriculation.description_detaillee = (
        get_description_detaillee_activite_list(rne_company)
    )

    unite_legale.micro_entreprise = (
        rne_company.formality.content.natureCreation.microEntreprise
    )
    unite_legale.eirl = rne_company.formality.content.natureCreation.eirl
    unite_legale.micro_entreprise = get_regime_micro_social(rne_company)
    unite_legale.indicateur_artiste_auteur = get_indicateur_artiste_auteur(rne_company)
    unite_legale.indicateur_marin_professionnel = get_indicateur_marin_professionnel(
        rne_company
    )

    company_address = get_adresse(rne_company)
    unite_legale.adresse = map_address_rne_to_ul(company_address)

    company_dirigeants = get_dirigeants(rne_company)
    unite_legale.dirigeants = map_dirigeants_rne_to_dirigeants_list_ul(
        company_dirigeants
    )

    company_beneficiaires = get_beneficiaires(rne_company)
    unite_legale.beneficiaires = map_beneficiaire_rne_to_beneficiaire_ul(
        company_beneficiaires
    )

    unite_legale = get_denomination_personne_physique(rne_company, unite_legale)

    siege = get_siege(rne_company)
    if siege:
        unite_legale.siege = map_rne_siege_to_ul(siege)
    else:
        logging.warning(f"Unite legale has no siege : {unite_legale.siren}")

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


def get_indicateur_status(rne_company: "RNECompany", indicateur_name: str) -> bool:
    def has_indicateur(etablissement):
        if hasattr(etablissement, "activites"):
            return any(
                getattr(activite, indicateur_name, False)
                for activite in etablissement.activites
            )
        return False

    etablissements = [get_siege(rne_company)] + (get_etablissements(rne_company) or [])
    return any(has_indicateur(etablissement) for etablissement in etablissements)


def get_indicateur_artiste_auteur(rne_company: "RNECompany") -> bool:
    return get_indicateur_status(rne_company, "indicateurArtisteAuteur")


def get_indicateur_marin_professionnel(rne_company: "RNECompany") -> bool:
    return get_indicateur_status(rne_company, "indicateurMarinProfessionnel")


def get_description_detaillee_activite_list(
    rne_company: "RNECompany",
) -> list[str] | None:
    description_detaillee = set()

    def extract_descriptions(etablissement):
        if hasattr(etablissement, "activites"):
            for activite in etablissement.activites:
                description = getattr(activite, "descriptionDetaillee", None)
                if description:
                    description_detaillee.add(description)

    # Check the main establishment
    extract_descriptions(get_siege(rne_company))

    # Check other establishments
    for etablissement in get_etablissements(rne_company) or []:
        extract_descriptions(etablissement)

    return list(description_detaillee) or None


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


def get_beneficiaires(rne_company: RNECompany):
    beneficiaires = get_value(rne_company, "beneficiairesEffectifs", None)
    return beneficiaires


def get_regime_micro_social(rne_company: RNECompany):
    if rne_company.is_personne_physique():
        identite = getattr(rne_company, "identite", None)
        if identite:
            entrepreneur = getattr(identite, "entrepreneur", None)
            if entrepreneur:
                regime_micro_social = getattr(entrepreneur, "regimeMicroSocial", None)
                if regime_micro_social:
                    return getattr(regime_micro_social, "optionMicroSocial", None)
    return None


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


def map_beneficiaire_rne_to_beneficiaire_ul(beneficiaires):
    if not beneficiaires:
        return None
    list_beneficiaires = []

    for beneficiaire in beneficiaires:
        beneficiaire_ul = Beneficiaire()
        beneficiaire_ul.actif = beneficiaire.actif

        personne = beneficiaire.beneficiaire.descriptionPersonne
        beneficiaire_ul.date_de_naissance = personne.dateDeNaissance
        beneficiaire_ul.nom = personne.nom
        beneficiaire_ul.nom_usage = personne.nomUsage

        beneficiaire_ul.prenoms = (
            " ".join(personne.prenoms)
            if isinstance(personne.prenoms, list)
            else personne.prenoms
        )
        beneficiaire_ul.genre = personne.genre
        beneficiaire_ul.role = personne.role
        beneficiaire_ul.nationalite = personne.nationalite
        beneficiaire_ul.situation_matrimoniale = personne.situationMatrimoniale

        list_beneficiaires.append(beneficiaire_ul)
    return list_beneficiaires
