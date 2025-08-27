from datetime import date, datetime

from pydantic import BaseModel


class Contact(BaseModel):
    mail: str | None = None
    telephone: str | None = None


class Adresse(BaseModel):
    pays: str | None = None
    codePays: str | None = None
    commune: str | None = None
    codePostal: str | None = None
    codeInseeCommune: str | None = None
    voie: str | None = None
    numVoie: str | None = None
    typeVoie: str | None = None
    indiceRepetition: str | None = None
    complementLocalisation: str | None = None
    distributionSpeciale: str | None = None


class EntrepriseDomiciliataire(BaseModel):
    siren: str | None = None
    denomination: str | None = None
    nomCommercial: str | None = None
    formeJuridique: str | None = None


class AdresseEntreprise(BaseModel):
    adresse: Adresse | None = None
    entrepriseDomiciliataire: EntrepriseDomiciliataire | None = None


class Description(BaseModel):
    indicateurAssocieUnique: bool | None = None
    ess: bool | None = None
    montantCapital: float | None = None
    capitalVariable: bool | None = None
    deviseCapital: str | None = None
    societeMission: bool | None = None
    dateClotureExerciceSocial: str | None = None
    duree: int | None = None
    dateFinExistence: str | None = None
    formeCooperative: bool | None = None


class DescriptionPersonne(BaseModel):
    role: str | None = None
    nom: str | None = None
    prenoms: list[str] | None = None
    genre: str | None = None
    titre: str | None = None
    nomUsage: str | None = None
    dateDeNaissance: str | None = None
    paysNaissance: str | None = None
    lieuDeNaissance: str | None = None
    nationalite: str | None = None
    situationMatrimoniale: str | None = None


class Conjoint(BaseModel):
    contact: Contact | None = None
    descriptionPersonne: DescriptionPersonne | None = None
    adresseDomicile: Adresse | None = None


class Modalite(BaseModel):
    dateEffet: str | None = None
    detentionPartDirecte: bool | None = None
    modalitesDeControle: list[str] | None = None
    detention25pCapital: bool | None = None
    detention25pDroitVote: bool | None = None


class Beneficiaire(BaseModel):
    descriptionPersonne: DescriptionPersonne | None = None
    adresseDomicile: Adresse | None = None


class BeneficiaireEffectif(BaseModel):
    beneficiaire: Beneficiaire | None = None
    modalite: Modalite | None = None


class PouvoirIndividu(BaseModel):
    descriptionPersonne: DescriptionPersonne | None = None
    contact: Contact | None = None
    adresseDomicile: Adresse | None = None
    roleConjoint: str | None = None
    conjoint: Conjoint | None = None


class PouvoirEntreprise(BaseModel):
    siren: str | None = None
    formeJuridique: str | None = None
    denomination: str | None = None
    nomCommercial: str | None = None
    roleEntreprise: str | None = None
    pays: str | None = None
    lieuRegistre: str | None = None
    individuRepresentant: PouvoirIndividu | None = None


class Pouvoir(BaseModel):
    individu: PouvoirIndividu | None = None
    entreprise: PouvoirEntreprise | None = None
    qualiteArtisan: str | None = None
    dateDebut: str | None = None
    dateFin: str | None = None
    actif: bool | None = None
    roleEntreprise: str | None = None
    autreRoleEntreprise: str | None = None
    libelleRoleEntreprise: str | None = None
    typeDePersonne: str | None = None
    adresseEntreprise: Adresse | None = None
    secondRoleEntreprise: str | None = None
    libelleSecondRoleEntreprise: str | None = None


class Composition(BaseModel):
    pouvoirs: list[Pouvoir] | None = None


class DescriptionEtablissement(BaseModel):
    siret: str | None = None
    enseigne: str | None = None
    nomCommercial: str | None = None
    dateEffet: str | None = None
    dateEffetFermeture: str | None = None
    nomEtablissement: str | None = None
    rolePourEntreprise: str | None = None  # siège, principal ou secondaire
    codeApe: str | None = None
    statutOuvertFerme: str | None = None
    indicateurEtablissementPrincipal: bool | None = None


class Origine(BaseModel):
    typeOrigine: str | None = None


class Activite(BaseModel):
    indicateurPrincipal: bool | None = None
    indicateurArtisteAuteur: bool | None = None
    indicateurMarinProfessionnel: bool | None = None
    metierArt: bool | None = None
    activiteMicro: bool | None = None
    indicateurProlongement: bool | None = None
    activiteReguliere: str | None = None
    dateDebut: str | None = None
    dateFin: str | None = None
    codeAprm: str | None = None
    descriptionDetaillee: str | None = None
    formeExercice: str | None = None
    categorisationActivite1: str | None = None
    categorisationActivite2: str | None = None
    categorisationActivite3: str | None = None
    categorisationActivite4: str | None = None
    categoryCode: str | None = None
    codeApe: str | None = None
    qualiteNonSedentaire: str | None = None
    indicateurNonSedentaire: bool | None = None
    statutPraticien: str | None = None  # médecin
    precisionActivite: str | None = None
    numPraticien: str | None = None
    origine: Origine | None = None


class Domaine(BaseModel):
    nomDomaine: str | None = None


class DetailCessation(BaseModel):
    dateRadiation: str | None = None
    detailMotifCessation: str | None = None
    motifCessation: str | None = None
    dateCessationActiviteSalariee: str | None = None
    dateCessationTotaleActivite: str | None = None
    dateMiseEnSommeil: str | None = None
    indicateurDissolution: bool | None = None
    typeDissolution: str | None = None
    indicateurCessationTemporaire: bool | None = None
    indicateurDisparitionPM: bool | None = None


class Etablissement(BaseModel):
    descriptionEtablissement: DescriptionEtablissement | None = None
    adresse: Adresse | None = None
    activites: list[Activite] | None = None
    nomsDeDomaine: list[Domaine] | None = None
    detailCessationEtablissement: DetailCessation | None = None
    isPrincipal: bool | None = None


class Entrepreneur(BaseModel):
    roleConjoint: str | None = None
    conjoint: Conjoint | None = None
    descriptionPersonne: DescriptionPersonne | None = None
    adresseDomicile: Adresse | None = None
    qualiteArtisan: str | None = None
    contact: Contact | None = None


class Entreprise(BaseModel):
    formeJuridique: str | None = None
    codeApe: str | None = None
    denomination: str | None = None
    nomCommercial: str | None = None
    nomExploitation: str | None = None
    effectifSalarie: str | None = None
    effectifApprenti: str | None = None
    dateImmat: str | None = None
    dateDebutActiv: str | None = None


class Identite(BaseModel):
    entreprise: Entreprise | None = None
    entrepreneur: Entrepreneur | None = None  # Personne Physique uniquement
    description: Description | None = None  # Personne Morale uniquement


class Exploitation(BaseModel):
    identite: Identite | None = None
    adresseEntreprise: AdresseEntreprise | None = None
    etablissementPrincipal: Etablissement | None = None
    autresEtablissements: list[Etablissement] | None = None
    detailCessationEntreprise: DetailCessation | None = None
    beneficiairesEffectifs: list[BeneficiaireEffectif] | None = None
    composition: Composition | None = None


class PersonneMorale(BaseModel):
    identite: Identite | None = None
    adresseEntreprise: AdresseEntreprise | None = None
    etablissementPrincipal: Etablissement | None = None
    autresEtablissements: list[Etablissement] | None = None
    detailCessationEntreprise: DetailCessation | None = None
    beneficiairesEffectifs: list[BeneficiaireEffectif] | None = None
    composition: Composition | None = None


class PersonnePhysique(BaseModel):
    identite: Identite | None = None
    adresseEntreprise: AdresseEntreprise | None = None
    etablissementPrincipal: Etablissement | None = None
    autresEtablissements: list[Etablissement] | None = None
    detailCessationEntreprise: DetailCessation | None = None
    beneficiairesEffectifs: list[BeneficiaireEffectif] | None = None
    composition: Composition | None = None


class RAA(BaseModel):
    estPresent: bool | None = None
    dateDebut: str | None = None
    dateFin: str | None = None


class RNM(BaseModel):
    estPresent: bool | None = None
    dateDebut: str | None = None
    dateFin: str | None = None


class RNCS(BaseModel):
    estPresent: bool | None = None
    dateDebut: str | None = None
    dateFin: str | None = None


class RegistreAnterieur(BaseModel):
    raa: RAA | None = None
    rnm: RNM | None = None
    rncs: RNCS | None = None


class Mention(BaseModel):
    numObservation: str | None = None
    dateEffet: str | None = None
    texte: str | None = None


class InscriptionOffice(BaseModel):
    event: str | None = None
    dateEffet: str | None = None
    observation: str | None = None
    partnerCenter: str | None = None
    partnerCode: str | None = None
    observationComplementaire: str | None = None
    mentions: list[Mention] | None = None  # schéma privé


class NatureCreation(BaseModel):
    dateCreation: str | None = None
    formeJuridique: str | None = None
    formeJuridiqueInsee: str | None = None
    societeEtrangere: bool | None = None
    entrepriseAgricole: bool | None = None
    relieeEntrepriseAgricole: bool | None = None
    eirl: bool | None = None
    typeExploitation: str | None = None
    microEntreprise: bool | None = None


class NatureCessationEntreprise(BaseModel):
    dateRadiation: date | None = None
    etatAdministratifInsee: str | None = None


class Content(BaseModel):
    evenementCessation: str | None = None
    natureCessation: str | None = None
    natureCessationEntreprise: NatureCessationEntreprise | None = None
    succursaleOuFiliale: str | None = None
    formeExerciceActivitePrincipale: str | None = None
    natureCreation: NatureCreation | None = None
    personnePhysique: PersonnePhysique | None = None
    personneMorale: PersonneMorale | None = None
    exploitation: Exploitation | None = None
    registreAnterieur: RegistreAnterieur | None = None
    inscriptionsOffices: list[InscriptionOffice] | None = None


class Historique(BaseModel):
    dateIntegration: str | None = None
    codeEvenement: str | None = None
    libelleEvenement: str | None = None
    numeroLiasse: str | None = None
    patchId: str | None = None
    dateEffet: str | None = None
    cheminDateEffet: str | None = None
    cheminDateEffetId: str | None = None


class Formality(BaseModel):
    siren: str
    content: Content
    diffusionINSEE: str | None = None
    typePersonne: str | None = None
    codeAPE: str | None = None  # schéma privé
    diffusionCommerciale: bool | None = None
    historique: list[Historique] | None = None
    formeJuridique: str | None = None
    numRna: str | None = None
    created: datetime | None = None
    updated: datetime | None = None  # date màj formalité


class RNECompany(BaseModel):
    createdAt: datetime | None = None
    updatedAt: datetime | None = None  # date màj RNE
    nombreEtablissementsOuverts: int | None = None
    nombreRepresentantsActifs: int | None = None
    formality: Formality
    siren: str
    sirenDoublons: str | None = None

    def is_personne_morale(self) -> bool:
        return isinstance(self.formality.content.personneMorale, PersonneMorale)

    def is_exploitation(self) -> bool:
        return isinstance(self.formality.content.exploitation, Exploitation)

    def is_personne_physique(self) -> bool:
        return isinstance(self.formality.content.personnePhysique, PersonnePhysique)
