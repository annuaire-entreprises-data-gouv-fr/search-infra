from dag_datalake_sirene.helpers.utils import (
    remove_spaces,
)
from dag_datalake_sirene.workflows.data_pipelines.rne.database.rne_model import (
    RNECompany,
)
from dag_datalake_sirene.workflows.data_pipelines.rne.database.ul_model import (
    Activite,
    Adresse,
    BeneficiaireEffectif,
    DirigeantsPM,
    DirigeantsPP,
    Etablissement,
    Historique,
    InscriptionOffice,
    Mention,
    Modalite,
    Siege,
    UniteLegale,
)


def map_rne_company_to_ul(rne_company: RNECompany, unite_legale: UniteLegale):
    """Map RNE company data to UniteLegale model"""

    # Basic company data
    unite_legale.siren = rne_company.siren
    unite_legale.siren_doublons = rne_company.sirenDoublons
    unite_legale.date_mise_a_jour_formalite = rne_company.formality.updated
    unite_legale.date_mise_a_jour_rne = rne_company.updatedAt
    unite_legale.nombre_representants_actifs = rne_company.nombreRepresentantsActifs
    unite_legale.nombre_etablissements_ouverts = rne_company.nombreEtablissementsOuverts

    # Identité
    identite_entr = get_identite_entreprise(rne_company)
    if identite_entr:
        unite_legale.denomination = identite_entr.denomination
        unite_legale.nom_commercial = identite_entr.nomCommercial
        unite_legale.nom_exploitation = identite_entr.nomExploitation
        unite_legale.code_ape = identite_entr.codeApe
        unite_legale.tranche_effectif_salarie = identite_entr.effectifSalarie
        unite_legale.activite_principale = identite_entr.codeApe

    # Nature Creation
    nature_creation = rne_company.formality.content.natureCreation
    if nature_creation:
        unite_legale.date_creation = nature_creation.dateCreation
        unite_legale.societe_etrangere = nature_creation.societeEtrangere
        unite_legale.entreprise_agricole = nature_creation.entrepriseAgricole
        unite_legale.reliee_entreprise_agricole = (
            nature_creation.relieeEntrepriseAgricole
        )
        unite_legale.eirl = nature_creation.eirl
        unite_legale.forme_juridique = nature_creation.formeJuridique
        unite_legale.forme_juridique_insee = nature_creation.formeJuridiqueInsee

    # Statut administratif
    unite_legale.statut_diffusion = rne_company.formality.diffusionINSEE
    unite_legale.forme_exercice_activite_principale = (
        rne_company.formality.content.formeExerciceActivitePrincipale
    )

    # Cessation
    cessation = get_detail_cessation(rne_company)
    if cessation:
        unite_legale.date_radiation = cessation.dateRadiation
        unite_legale.date_sommeil = cessation.dateMiseEnSommeil
        unite_legale.date_fin_existence = cessation.dateCessationTotaleActivite
    # Etat administratif INSEE (comes from natureCessationEntreprise)
    nature_cessation_entreprise = (
        rne_company.formality.content.natureCessationEntreprise
    )
    if nature_cessation_entreprise:
        unite_legale.etat_administratif_insee = (
            nature_cessation_entreprise.etatAdministratifInsee
        )

    # Description identité
    identite_descr = get_identite_description(rne_company)
    if identite_descr:
        unite_legale.indicateur_associe_unique = identite_descr.indicateurAssocieUnique
        unite_legale.indicateur_ess = identite_descr.ess
        unite_legale.montant_capital = identite_descr.montantCapital
        unite_legale.capital_variable = identite_descr.capitalVariable
        unite_legale.devise_capital = identite_descr.deviseCapital
        unite_legale.societe_mission = identite_descr.societeMission
        unite_legale.date_cloture_exercice_social = (
            identite_descr.dateClotureExerciceSocial
        )
        unite_legale.duree_personne_morale = identite_descr.duree
        unite_legale.date_fin_existence = identite_descr.dateFinExistence

    # Adresse
    company_address = get_adresse(rne_company)
    unite_legale.adresse = map_address_rne_to_ul(company_address)

    # Domiciliation
    adresse_entreprise = get_value(rne_company, "adresseEntreprise")
    if adresse_entreprise and adresse_entreprise.entrepriseDomiciliataire:
        domic = adresse_entreprise.entrepriseDomiciliataire
        unite_legale.entreprise_domiciliataire_siren = domic.siren
        unite_legale.entreprise_domiciliataire_denomination = domic.denomination
        unite_legale.entreprise_domiciliataire_nom_commercial = domic.nomCommercial

    # Personne physique
    if rne_company.is_personne_physique():
        entrepreneur = get_entrepreneur(rne_company)
        if entrepreneur:
            unite_legale.nom = entrepreneur.descriptionPersonne.nom
            unite_legale.nom_usage = entrepreneur.descriptionPersonne.nomUsage
            unite_legale.prenoms = (
                " ".join(entrepreneur.descriptionPersonne.prenoms)
                if entrepreneur.descriptionPersonne.prenoms
                else None
            )
            unite_legale.genre = entrepreneur.descriptionPersonne.genre
            unite_legale.date_naissance = (
                entrepreneur.descriptionPersonne.dateDeNaissance
            )
            unite_legale.lieu_naissance = (
                entrepreneur.descriptionPersonne.lieuDeNaissance
            )
            unite_legale.pays_naissance = entrepreneur.descriptionPersonne.paysNaissance
            unite_legale.nationalite = entrepreneur.descriptionPersonne.nationalite
            unite_legale.qualite_artisan = entrepreneur.qualiteArtisan
            unite_legale.mail = (
                entrepreneur.contact.mail if entrepreneur.contact else None
            )
            unite_legale.telephone = (
                entrepreneur.contact.telephone if entrepreneur.contact else None
            )
            unite_legale.adresse = (
                map_address_rne_to_ul(entrepreneur.adresseDomicile)
                if entrepreneur.adresseDomicile
                else None
            )

            # Conjoint collaborateur
            if entrepreneur.conjoint:
                unite_legale.role_conjoint = entrepreneur.roleConjoint
                conjoint = entrepreneur.conjoint
                if conjoint.descriptionPersonne:
                    unite_legale.nom_conjoint = conjoint.descriptionPersonne.nom
                    unite_legale.nom_usage_conjoint = (
                        conjoint.descriptionPersonne.nomUsage
                    )
                    unite_legale.prenoms_conjoint = (
                        " ".join(conjoint.descriptionPersonne.prenoms)
                        if conjoint.descriptionPersonne.prenoms
                        else None
                    )
                    unite_legale.genre_conjoint = conjoint.descriptionPersonne.genre
                    unite_legale.date_naissance_conjoint = (
                        conjoint.descriptionPersonne.dateDeNaissance
                    )
                    unite_legale.lieu_naissance_conjoint = (
                        conjoint.descriptionPersonne.lieuDeNaissance
                    )
                    unite_legale.pays_naissance_conjoint = (
                        conjoint.descriptionPersonne.paysNaissance
                    )
                    unite_legale.nationalite_conjoint = (
                        conjoint.descriptionPersonne.nationalite
                    )
                unite_legale.mail_conjoint = (
                    conjoint.contact.mail if conjoint.contact else None
                )
                unite_legale.telephone_conjoint = (
                    conjoint.contact.telephone if conjoint.contact else None
                )
                unite_legale.adresse_conjoint = (
                    map_address_rne_to_ul(conjoint.adresseDomicile)
                    if conjoint.adresseDomicile
                    else None
                )
                # Not stored on UniteLegale: situation_matrimoniale

    # Dirigeants
    company_dirigeants = get_dirigeants(rne_company)
    unite_legale.dirigeants = map_dirigeants_rne_to_dirigeants_list_ul(
        company_dirigeants
    )

    # Siege
    siege = get_siege(rne_company)
    if siege:
        unite_legale.siege = map_rne_siege_to_ul(siege)
    # else:
    # logging.warning(f"%%%%%%%%%%%%%Unite legale has no siege : {unite_legale.siren}")

    # Etablissements
    etablissements = get_etablissements(rne_company)
    if etablissements:
        unite_legale.etablissements = map_rne_etablissements_to_ul(etablissements)
    else:
        unite_legale.etablissements = []

    # Beneficiaires effectifs
    beneficiaires = get_value(rne_company, "beneficiairesEffectifs")
    if beneficiaires:
        unite_legale.beneficiaires_effectifs = map_beneficiaires_rne_to_ul(
            beneficiaires
        )

    # Historique is a list: map all entries via helper
    historiques_src = rne_company.formality.historique
    if historiques_src:
        unite_legale.historique = map_rne_historique_to_ul(historiques_src)

    # Inscription offices
    inscriptions = rne_company.formality.content.inscriptionsOffices
    if inscriptions:
        unite_legale.inscriptions_offices = map_inscriptions_rne_to_ul(inscriptions)

    # Immatriculation
    unite_legale.immatriculation = map_immatriculation_rne_to_ul(rne_company)

    return unite_legale


def get_value(rne_company: RNECompany, key: str, default=None):
    """Get value from RNE company based on type"""
    content = rne_company.formality.content

    if rne_company.is_personne_morale():
        return getattr(content.personneMorale, key, default)
    if rne_company.is_exploitation():
        return getattr(content.exploitation, key, default)
    if rne_company.is_personne_physique():
        return getattr(content.personnePhysique, key, default)
    return default


def get_entrepreneur(rne_company: RNECompany):
    """Get entrepreneur info for physical persons"""
    if rne_company.is_personne_physique():
        identite = get_value(rne_company, "identite")
        return identite.entrepreneur if identite else None
    return None


def get_etablissements(rne_company: RNECompany):
    return get_value(rne_company, "autresEtablissements", default=[])


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
    return None


def get_siege(rne_company: RNECompany):
    return get_value(rne_company, "etablissementPrincipal", default=None)


def get_dirigeants(rne_company: RNECompany):
    """Get dirigeants based on company type"""
    composition = get_value(rne_company, "composition")

    if rne_company.is_personne_physique():
        identite = get_value(rne_company, "identite")
        return [identite.entrepreneur] if identite and identite.entrepreneur else []

    return composition.pouvoirs if composition else []


def map_address_rne_to_ul(address_rne):
    """Map RNE address to UL address"""
    if not address_rne:
        return None

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
    """Map RNE dirigeant personne physique to UL"""
    dirigeant_pp_ul = DirigeantsPP()

    # Basic info from the dirigeant level
    dirigeant_pp_ul.role = getattr(dirigeant_pp_rne, "roleEntreprise", None)
    dirigeant_pp_ul.actif = getattr(dirigeant_pp_rne, "actif", None)
    dirigeant_pp_ul.date_debut = getattr(dirigeant_pp_rne, "dateDebut", None)
    dirigeant_pp_ul.date_fin = getattr(dirigeant_pp_rne, "dateFin", None)
    dirigeant_pp_ul.qualite_artisan = getattr(dirigeant_pp_rne, "qualiteArtisan", None)

    # Person info from individu
    if hasattr(dirigeant_pp_rne, "individu") and dirigeant_pp_rne.individu:
        individu = dirigeant_pp_rne.individu
        if hasattr(individu, "descriptionPersonne") and individu.descriptionPersonne:
            desc = individu.descriptionPersonne
            dirigeant_pp_ul.nom = desc.nom
            dirigeant_pp_ul.nom_usage = desc.nomUsage
            dirigeant_pp_ul.prenoms = " ".join(desc.prenoms) if desc.prenoms else None
            dirigeant_pp_ul.genre = desc.genre
            dirigeant_pp_ul.date_naissance = desc.dateDeNaissance
            dirigeant_pp_ul.lieu_naissance = desc.lieuDeNaissance
            dirigeant_pp_ul.pays_naissance = desc.paysNaissance
            dirigeant_pp_ul.nationalite = desc.nationalite
            dirigeant_pp_ul.situation_matrimoniale = desc.situationMatrimoniale

        # Contact info
        if hasattr(individu, "contact") and individu.contact:
            dirigeant_pp_ul.mail = individu.contact.mail
            dirigeant_pp_ul.telephone = individu.contact.telephone

        # Adresse individu
        if hasattr(individu, "adresseDomicile") and individu.adresseDomicile:
            dirigeant_pp_ul.adresse = map_address_rne_to_ul(individu.adresseDomicile)

        # Conjoint collaborateur
        if hasattr(individu, "conjoint") and individu.conjoint:
            conjoint = individu.conjoint
            dirigeant_pp_ul.role_conjoint = individu.roleConjoint
            if conjoint.descriptionPersonne:
                dirigeant_pp_ul.nom_conjoint = conjoint.descriptionPersonne.nom
                dirigeant_pp_ul.nom_usage_conjoint = (
                    conjoint.descriptionPersonne.nomUsage
                )
                dirigeant_pp_ul.prenoms_conjoint = (
                    " ".join(conjoint.descriptionPersonne.prenoms)
                    if conjoint.descriptionPersonne.prenoms
                    else None
                )
                dirigeant_pp_ul.genre_conjoint = conjoint.descriptionPersonne.genre
                dirigeant_pp_ul.date_naissance_conjoint = (
                    conjoint.descriptionPersonne.dateDeNaissance
                )
                dirigeant_pp_ul.lieu_naissance_conjoint = (
                    conjoint.descriptionPersonne.lieuDeNaissance
                )
                dirigeant_pp_ul.pays_naissance_conjoint = (
                    conjoint.descriptionPersonne.paysNaissance
                )
                dirigeant_pp_ul.nationalite_conjoint = (
                    conjoint.descriptionPersonne.nationalite
                )
            if conjoint.contact:
                dirigeant_pp_ul.mail_conjoint = conjoint.contact.mail
                dirigeant_pp_ul.telephone_conjoint = conjoint.contact.telephone
            if conjoint.adresseDomicile:
                dirigeant_pp_ul.adresse_conjoint = map_address_rne_to_ul(
                    conjoint.adresseDomicile
                )

    return dirigeant_pp_ul


def map_rne_dirigeant_pm_to_ul(dirigeant_pm_rne):
    """Map RNE dirigeant personne morale to UL"""
    dirigeant_pm_ul = DirigeantsPM()

    # Basic info from the dirigeant level
    dirigeant_pm_ul.actif = dirigeant_pm_rne.actif
    dirigeant_pm_ul.date_debut = dirigeant_pm_rne.dateDebut
    dirigeant_pm_ul.date_fin = dirigeant_pm_rne.dateFin
    dirigeant_pm_ul.qualite_artisan = dirigeant_pm_rne.qualiteArtisan

    # Roles from the dirigeant level
    dirigeant_pm_ul.libelle_role = dirigeant_pm_rne.libelleRoleEntreprise
    dirigeant_pm_ul.autre_role = dirigeant_pm_rne.autreRoleEntreprise
    dirigeant_pm_ul.second_role = dirigeant_pm_rne.secondRoleEntreprise
    dirigeant_pm_ul.libelle_second_role = dirigeant_pm_rne.libelleSecondRoleEntreprise
    dirigeant_pm_ul.role = dirigeant_pm_rne.roleEntreprise

    # Enterprise info from entreprise
    if hasattr(dirigeant_pm_rne, "entreprise") and dirigeant_pm_rne.entreprise:
        entreprise = dirigeant_pm_rne.entreprise
        dirigeant_pm_ul.siren = remove_spaces(entreprise.siren)
        dirigeant_pm_ul.denomination = entreprise.denomination
        dirigeant_pm_ul.pays = entreprise.pays
        dirigeant_pm_ul.forme_juridique = entreprise.formeJuridique
        dirigeant_pm_ul.lieu_registre = entreprise.lieuRegistre

        # Individu Representant from entreprise
        if entreprise.individuRepresentant:
            indiv = entreprise.individuRepresentant
            dirigeant_pm_ul.nom_individu_representant = indiv.descriptionPersonne.nom
            dirigeant_pm_ul.nom_usage_individu_representant = (
                indiv.descriptionPersonne.nomUsage
            )
            dirigeant_pm_ul.prenoms_individu_representant = (
                " ".join(indiv.descriptionPersonne.prenoms)
                if indiv.descriptionPersonne.prenoms
                else None
            )
            dirigeant_pm_ul.genre_individu_representant = (
                indiv.descriptionPersonne.genre
            )
            dirigeant_pm_ul.date_naissance_individu_representant = (
                indiv.descriptionPersonne.dateDeNaissance
            )
            dirigeant_pm_ul.lieu_naissance_individu_representant = (
                indiv.descriptionPersonne.lieuDeNaissance
            )
            dirigeant_pm_ul.pays_naissance_individu_representant = (
                indiv.descriptionPersonne.paysNaissance
            )
            dirigeant_pm_ul.nationalite_individu_representant = (
                indiv.descriptionPersonne.nationalite
            )
            dirigeant_pm_ul.adresse_individu_representant = (
                map_address_rne_to_ul(indiv.adresseDomicile)
                if indiv.adresseDomicile
                else None
            )

    return dirigeant_pm_ul


def map_rne_siege_to_ul(siege_rne):
    """Map RNE siege to UL siege"""
    siege = Siege()

    if siege_rne.descriptionEtablissement:
        desc = siege_rne.descriptionEtablissement
        siege.siret = desc.siret
        siege.statut = desc.statutOuvertFerme
        siege.type = desc.rolePourEntreprise
        siege.date_ouverture = desc.dateEffet
        siege.date_fermeture = desc.dateEffetFermeture
        siege.enseigne = desc.enseigne
        siege.nom_commercial = desc.nomCommercial
        siege.code_ape = desc.codeApe

    if siege_rne.adresse:
        siege.adresse = map_address_rne_to_ul(siege_rne.adresse)

    # Map activites if they exist
    if hasattr(siege_rne, "activites") and siege_rne.activites:
        siege.activites = map_rne_activites_to_ul(siege_rne.activites)

    return siege


def map_dirigeants_rne_to_dirigeants_list_ul(dirigeants_rne):
    """Map RNE dirigeants to UL dirigeants list"""
    list_dirigeants = []

    for dirigeant in dirigeants_rne:
        # Cas personne morale et exploitation
        if hasattr(dirigeant, "typeDePersonne"):
            if dirigeant.typeDePersonne == "INDIVIDU":
                list_dirigeants.append(map_rne_dirigeant_pp_to_ul(dirigeant))
            elif dirigeant.typeDePersonne == "ENTREPRISE":
                list_dirigeants.append(map_rne_dirigeant_pm_to_ul(dirigeant))
        else:
            # Cas personne physique
            list_dirigeants.append(map_rne_dirigeant_pp_to_ul(dirigeant))

    return list_dirigeants


def map_rne_etablissements_to_ul(etablissements_rne):
    """Maps RNE etablissements to UL etablissements"""
    etablissements_ul = []

    if not etablissements_rne:
        return etablissements_ul

    for etablissement_rne in etablissements_rne:
        etablissement_ul = Etablissement()

        if etablissement_rne.descriptionEtablissement:
            desc = etablissement_rne.descriptionEtablissement
            etablissement_ul.siret = desc.siret
            etablissement_ul.statut = desc.statutOuvertFerme
            etablissement_ul.type = desc.rolePourEntreprise
            etablissement_ul.date_ouverture = desc.dateEffet
            etablissement_ul.date_fermeture = desc.dateEffetFermeture
            etablissement_ul.enseigne = desc.enseigne
            etablissement_ul.nom_commercial = desc.nomCommercial
            etablissement_ul.code_ape = desc.codeApe

        if etablissement_rne.adresse:
            etablissement_ul.adresse = map_address_rne_to_ul(etablissement_rne.adresse)

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
        activite_ul = Activite()

        activite_ul.siret = getattr(activite_rne, "siret", None)
        activite_ul.indicateur_principal = getattr(
            activite_rne, "indicateurPrincipal", None
        )
        activite_ul.activite_reguliere = getattr(
            activite_rne, "activiteReguliere", None
        )
        activite_ul.qualite_non_sedentaire = getattr(
            activite_rne, "qualiteNonSedentaire", None
        )
        activite_ul.indicateur_non_sedentaire = getattr(
            activite_rne, "indicateurNonSedentaire", None
        )
        activite_ul.indicateur_artiste_auteur = getattr(
            activite_rne, "indicateurArtisteAuteur", None
        )
        activite_ul.indicateur_marin_professionnel = getattr(
            activite_rne, "indicateurMarinProfessionnel", None
        )
        activite_ul.indicateur_prolongement = getattr(
            activite_rne, "indicateurProlongement", None
        )
        activite_ul.description_detaillee = getattr(
            activite_rne, "descriptionDetaillee", None
        )
        activite_ul.forme_exercice = getattr(activite_rne, "formeExercice", None)
        activite_ul.code_aprm = getattr(activite_rne, "codeAprm", None)
        activite_ul.metier_art = getattr(activite_rne, "metierArt", None)
        activite_ul.date_debut = getattr(activite_rne, "dateDebut", None)
        activite_ul.date_fin = getattr(activite_rne, "dateFin", None)
        activite_ul.categorisation_activite_1 = getattr(
            activite_rne, "categorisationActivite1", None
        )
        activite_ul.categorisation_activite_2 = getattr(
            activite_rne, "categorisationActivite2", None
        )
        activite_ul.categorisation_activite_3 = getattr(
            activite_rne, "categorisationActivite3", None
        )
        activite_ul.categorisation_activite_4 = getattr(
            activite_rne, "categorisationActivite4", None
        )
        activite_ul.type_origine_fond = getattr(activite_rne, "typeOrigine", None)
        activite_ul.code_category = getattr(activite_rne, "categoryCode", None)
        activite_ul.code_ape = getattr(activite_rne, "codeApe", None)
        activite_ul.activite_rattachee_eirl = getattr(
            activite_rne, "activiteRattacheeEirl", None
        )

        activites_ul.append(activite_ul)
    return activites_ul


def map_beneficiaires_rne_to_ul(beneficiaires_rne):
    """Map RNE beneficiaires effectifs to UL"""
    beneficiaires_ul = []

    for beneficiaire_rne in beneficiaires_rne:
        beneficiaire_ul = BeneficiaireEffectif()

        if (
            beneficiaire_rne.beneficiaire
            and beneficiaire_rne.beneficiaire.descriptionPersonne
        ):
            desc = beneficiaire_rne.beneficiaire.descriptionPersonne
            beneficiaire_ul.nom = desc.nom
            beneficiaire_ul.nom_usage = desc.nomUsage
            beneficiaire_ul.prenoms = " ".join(desc.prenoms) if desc.prenoms else None
            beneficiaire_ul.genre = desc.genre
            beneficiaire_ul.date_naissance = desc.dateDeNaissance
            beneficiaire_ul.lieu_naissance = desc.lieuDeNaissance
            beneficiaire_ul.pays_naissance = desc.paysNaissance

        if (
            beneficiaire_rne.beneficiaire
            and beneficiaire_rne.beneficiaire.adresseDomicile
        ):
            beneficiaire_ul.adresse = map_address_rne_to_ul(
                beneficiaire_rne.beneficiaire.adresseDomicile
            )

        if beneficiaire_rne.modalite:
            modalite_ul = Modalite()
            modalite_ul.date_effet = beneficiaire_rne.modalite.dateEffet
            modalite_ul.detention_part_directe = (
                beneficiaire_rne.modalite.detentionPartDirecte
            )
            modalite_ul.modalites_de_controle = (
                beneficiaire_rne.modalite.modalitesDeControle
            )
            modalite_ul.detention_25p_capital = (
                beneficiaire_rne.modalite.detention25pCapital
            )
            modalite_ul.detention_25p_droit_vote = (
                beneficiaire_rne.modalite.detention25pDroitVote
            )
            beneficiaire_ul.modalite = modalite_ul

        beneficiaires_ul.append(beneficiaire_ul)

    return beneficiaires_ul


def map_rne_historique_to_ul(historiques_rne):
    """Map list of RNE historiques to UL historiques list"""
    historiques_ul = []
    if not historiques_rne:
        return historiques_ul
    for h in historiques_rne:
        if not h:
            continue
        historique_ul = Historique()
        historique_ul.date_integration = getattr(h, "dateIntegration", None)
        historique_ul.code_evenement = getattr(h, "codeEvenement", None)
        historique_ul.libelle_evenement = getattr(h, "libelleEvenement", None)
        historique_ul.numero_liasse = getattr(h, "numeroLiasse", None)
        historique_ul.patch_id = getattr(h, "patchId", None)
        historique_ul.date_effet = getattr(h, "dateEffet", None)
        historique_ul.chemin_date_effet = getattr(h, "cheminDateEffet", None)
        historique_ul.chemin_date_effet_id = getattr(h, "cheminDateEffetId", None)
        historiques_ul.append(historique_ul)
    return historiques_ul


def map_inscriptions_rne_to_ul(inscriptions_rne):
    """Map RNE inscriptions office to UL"""
    inscriptions_ul = []

    for inscription_rne in inscriptions_rne:
        inscription_ul = InscriptionOffice()
        inscription_ul.event = inscription_rne.event
        inscription_ul.date_effet = inscription_rne.dateEffet
        inscription_ul.observation = inscription_rne.observation
        inscription_ul.partner_center = inscription_rne.partnerCenter
        inscription_ul.partner_code = inscription_rne.partnerCode
        inscription_ul.observation_complementaire = (
            inscription_rne.observationComplementaire
        )

        if inscription_rne.mentions:
            mentions_ul = []
            for mention_rne in inscription_rne.mentions:
                mention_ul = Mention()
                mention_ul.num_observation = mention_rne.numObservation
                mention_ul.date_effet = mention_rne.dateEffet
                mention_ul.texte = mention_rne.texte
                mentions_ul.append(mention_ul)
            inscription_ul.mentions = mentions_ul

        inscriptions_ul.append(inscription_ul)

    return inscriptions_ul


def map_immatriculation_rne_to_ul(rne_company: RNECompany):
    """Map RNE immatriculation to UL"""
    from dag_datalake_sirene.workflows.data_pipelines.rne.database.ul_model import (
        Immatriculation,
    )

    immatriculation = Immatriculation()

    # Get basic immatriculation info
    identite_entr = get_identite_entreprise(rne_company)
    if identite_entr:
        immatriculation.date_immatriculation = identite_entr.dateImmat
        immatriculation.date_debut_activite = identite_entr.dateDebutActiv

    # Get description info
    identite_descr = get_identite_description(rne_company)
    if identite_descr:
        immatriculation.capital_social = identite_descr.montantCapital
        immatriculation.date_cloture_exercice = identite_descr.dateClotureExerciceSocial
        immatriculation.duree_personne_morale = identite_descr.duree
        immatriculation.capital_variable = identite_descr.capitalVariable
        immatriculation.devise_capital = identite_descr.deviseCapital

    # Get cessation info
    cessation = get_detail_cessation(rne_company)
    if cessation:
        immatriculation.date_radiation = cessation.dateRadiation

    # Get nature of enterprise
    nature_creation = rne_company.formality.content.natureCreation
    if nature_creation:
        immatriculation.nature_entreprise = nature_creation.typeExploitation

    return immatriculation
