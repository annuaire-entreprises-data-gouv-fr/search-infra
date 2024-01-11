from dag_datalake_sirene.workflows.data_pipelines.rne.database.rne_model import (
    RNECompany,
)
from dag_datalake_sirene.workflows.data_pipelines.rne.database.company_model import (
    Adresse,
    DirigeantsPP,
    DirigeantsPM,
    UniteLegale,
    Siege,
)


def map_rne_company_to_ul(rne_company: RNECompany, unite_legale: UniteLegale):
    unite_legale.siren = rne_company.siren
    unite_legale.date_creation = rne_company.createdAt
    unite_legale.date_mise_a_jour = rne_company.updatedAt
    unite_legale.statut_diffusion = rne_company.formality.diffusionINSEE
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

    company_address = get_adresse(rne_company)
    unite_legale.adresse = map_address_rne_to_ul(company_address)

    company_dirigeants = get_dirigeants(rne_company)
    unite_legale.dirigeants = map_dirigeants_rne_to_dirigeant_ul(company_dirigeants)

    siege = get_siege(rne_company)
    if siege:
        unite_legale.siege = map_rne_siege_to_ul(siege)

    return unite_legale


def get_identite(rne_company: RNECompany):
    if rne_company.is_personne_morale():
        return rne_company.formality.content.personneMorale.identite.entreprise
    elif rne_company.is_exploitation():
        return rne_company.formality.content.exploitation.identite.entreprise
    else:
        return None


def get_detail_cessation(rne_company: RNECompany):
    if rne_company.is_personne_morale():
        return rne_company.formality.content.personneMorale.detailCessationEntreprise
    elif rne_company.is_exploitation():
        return rne_company.formality.content.exploitation.detailCessationEntreprise
    elif rne_company.is_personne_physique():
        return rne_company.formality.content.personnePhysique.detailCessationEntreprise
    else:
        return None


def get_adresse(rne_company: RNECompany):
    if rne_company.is_personne_morale():
        return rne_company.formality.content.personneMorale.adresseEntreprise.adresse
    elif rne_company.is_exploitation():
        return rne_company.formality.content.exploitation.adresseEntreprise.adresse
    elif rne_company.is_personne_physique():
        return rne_company.formality.content.personnePhysique.adresseEntreprise.adresse
    else:
        return {}


def get_siege(rne_company: RNECompany):
    if rne_company.is_personne_morale():
        return rne_company.formality.content.personneMorale.etablissementPrincipal
    elif rne_company.is_exploitation():
        return rne_company.formality.content.exploitation.etablissementPrincipal
    elif rne_company.is_personne_physique():
        return rne_company.formality.content.personnePhysique.etablissementPrincipal
    else:
        return {}


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


def get_dirigeants(rne_company: RNECompany):
    # Method to map dirigeants based on the logic in RNECompany
    if rne_company.is_personne_morale():
        if rne_company.formality.content.personneMorale.composition:
            return rne_company.formality.content.personneMorale.composition.pouvoirs
    elif rne_company.is_exploitation():
        if rne_company.formality.content.exploitation.composition:
            return rne_company.formality.content.exploitation.composition.pouvoirs
    elif rne_company.is_personne_physique():
        if (
            rne_company.formality.content.personnePhysique
            and rne_company.formality.content.personnePhysique.identite
        ):
            return [
                rne_company.formality.content.personnePhysique.identite.entrepreneur
            ]
    return []


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


def map_dirigeants_rne_to_dirigeant_ul(dirigeants_rne):
    list_dirigeants = []
    for dirigeant in dirigeants_rne:
        # Cas personne morale et exploitation
        if hasattr(dirigeant, "typeDePersonne"):
            if dirigeant.typeDePersonne == "INDIVIDU":
                list_dirigeants.append(
                    map_rne_dirigeant_pp_to_ul(dirigeant.individu.descriptionPersonne)
                )

            elif dirigeant.typeDePersonne == "ENTREPRISE":
                list_dirigeants.append(map_rne_dirigeant_pm_to_ul(dirigeant.entreprise))
        else:
            # Cas personne physique
            list_dirigeants.append(
                map_rne_dirigeant_pp_to_ul(dirigeant.descriptionPersonne)
            )
    return list_dirigeants
