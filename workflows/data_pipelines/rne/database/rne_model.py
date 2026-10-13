from datetime import date, datetime

from pydantic import BaseModel


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


class Description(BaseModel):
    montantCapital: float | None = None
    capitalVariable: bool | None = None
    deviseCapital: str | None = None
    dateClotureExerciceSocial: str | None = None
    duree: int | None = None
    dateFinExistence: str | None = None


class DescriptionPersonne(BaseModel):
    dateDeNaissance: str | None = None
    nom: str | None = None
    nomUsage: str | None = None
    prenoms: list[str] | None = None
    genre: str | None = None
    titre: str | None = None
    role: str | None = None
    nationalite: str | None = None
    situationMatrimoniale: str | None = None


class PouvoirIndividu(BaseModel):
    descriptionPersonne: DescriptionPersonne | None = DescriptionPersonne()
    adresseDomicile: Adresse | None = Adresse()
    qualiteArtisan: str | None = None


class PouvoirEntreprise(BaseModel):
    pays: str | None = None
    siren: str | None = None
    denomination: str | None = None
    roleEntreprise: str | None = None
    formeJuridique: str | None = None
    autreIdentifiantEtranger: str | None = None
    nicSiege: str | None = None
    nomCommercial: str | None = None


class Entrepreneur(BaseModel):
    descriptionPersonne: DescriptionPersonne | None = DescriptionPersonne()
    adresseDomicile: Adresse | None = Adresse()


class Pouvoir(BaseModel):
    roleEntreprise: str | None = None
    autreRoleEntreprise: str | None = None
    secondRoleEntreprise: str | None = None
    libelleSecondRoleEntreprise: str | None = None
    representantId: str | None = None
    qualiteArtisan: str | None = None
    representant: PouvoirIndividu | None = None
    mentionDemissionOrdre: bool | None = None
    dateMentionDemissionOrdre: date | None = None
    libelleRoleEntreprise: str | None = None
    typeDePersonne: str | None = None
    individu: PouvoirIndividu | None = PouvoirIndividu()
    entreprise: PouvoirEntreprise | None = PouvoirEntreprise()
    adresseEntreprise: Adresse | None = Adresse()


class Composition(BaseModel):
    pouvoirs: list[Pouvoir] | None = []


class Entreprise(BaseModel):
    denomination: str | None = None
    formeJuridique: str | None = None
    nomCommercial: str | None = None
    effectifSalarie: str | None = None
    dateImmat: str | None = None
    codeApe: str | None = None
    indicateurAssocieUnique: bool | None = None
    dateDebutActiv: str | None = None


class NomDeDomaine(BaseModel):
    nomDomaine: str | None = None
    dateEffet: date | None = None


class Identite(BaseModel):
    entreprise: Entreprise | None = Entreprise()
    entrepreneur: Entrepreneur | None = Entrepreneur()
    description: Description | None = Description()
    nomsDeDomaine: list[NomDeDomaine] | None = []


class Caracteristiques(BaseModel):
    domiciliataire: bool | None = None


class EntrepriseDomiciliataire(BaseModel):
    siren: str | None = None
    denomination: str | None = None


class AdresseEntreprise(BaseModel):
    caracteristiques: Caracteristiques | None = None
    adresse: Adresse | None = Adresse()
    entrepriseDomiciliataire: EntrepriseDomiciliataire | None = None


class DescriptionEtablissement(BaseModel):
    siret: str | None = None
    enseigne: str | None = None
    nomCommercial: str | None = None


class Activite(BaseModel):
    categoryCode: str | None = None
    indicateurPrincipal: bool | None = None
    indicateurProlongement: bool | None = None
    dateDebut: date | None = None
    formeExercice: str | None = None
    categorisationActivite1: str | None = None
    categorisationActivite2: str | None = None
    categorisationActivite3: str | None = None
    categorisationActivite4: str | None = None
    dateFin: date | None = None
    exerciceActivite: str | None = None
    descriptionDetaillee: str | None = None
    precisionActivite: str | None = None
    precisionAutre: str | None = None
    codeAprm: str | None = None
    indicateurActiviteeApe: bool | None = None
    codeApe: str | None = None
    activiteRattacheeEirl: bool | None = None


class EtablissementPrincipal(BaseModel):
    descriptionEtablissement: DescriptionEtablissement | None = (
        DescriptionEtablissement()
    )
    adresse: Adresse | None = Adresse()
    activites: list[Activite] | None = []
    nomsDeDomaine: list[NomDeDomaine] | None = []


class DetailCessationEntreprise(BaseModel):
    dateCessationTotaleActivite: date | None = None
    dateEffet: date | None = None
    dateRadiation: date | None = None


class NatureCessationEntreprise(BaseModel):
    etatAdministratifInsee: str | None = None


class Exploitation(BaseModel):
    identite: Identite | None = Identite()
    composition: Composition | None = None
    adresseEntreprise: AdresseEntreprise | None = AdresseEntreprise()
    etablissementPrincipal: EtablissementPrincipal | None = EtablissementPrincipal()
    autresEtablissements: list[EtablissementPrincipal] | None = [
        EtablissementPrincipal()
    ]
    detailCessationEntreprise: DetailCessationEntreprise | None = (
        DetailCessationEntreprise()
    )


class PersonneMorale(BaseModel):
    identite: Identite | None = Identite()
    composition: Composition | None = None
    adresseEntreprise: AdresseEntreprise | None = AdresseEntreprise()
    etablissementPrincipal: EtablissementPrincipal | None = EtablissementPrincipal()
    autresEtablissements: list[EtablissementPrincipal] | None = [
        EtablissementPrincipal()
    ]
    detailCessationEntreprise: DetailCessationEntreprise | None = (
        DetailCessationEntreprise()
    )


class PersonnePhysique(BaseModel):
    identite: Identite | None = Identite()
    composition: Composition | None = None
    adresseEntreprise: AdresseEntreprise | None = AdresseEntreprise()
    etablissementPrincipal: EtablissementPrincipal | None = EtablissementPrincipal()
    autresEtablissements: list[EtablissementPrincipal] | None = [
        EtablissementPrincipal()
    ]
    detailCessationEntreprise: DetailCessationEntreprise | None = (
        DetailCessationEntreprise()
    )


class NatureCreation(BaseModel):
    dateCreation: str | None = None
    formeJuridique: str | None = None
    formeJuridiqueInsee: str | None = None
    societeEtrangere: bool | None = None
    entrepriseAgricole: bool | None = None


class Content(BaseModel):
    formeExerciceActivitePrincipale: str | None = None
    personnePhysique: PersonnePhysique | None = None
    personneMorale: PersonneMorale | None = None
    exploitation: Exploitation | None = None
    natureCessationEntreprise: NatureCessationEntreprise | None = (
        NatureCessationEntreprise()
    )
    natureCreation: NatureCreation | None = NatureCreation()


class Formality(BaseModel):
    siren: str
    companyName: str | None = None
    content: Content
    typePersonne: str | None = None
    formeJuridique: str | None = None
    diffusionINSEE: str | None = None
    diffusionCommerciale: bool | None = None
    codeAPE: str | None = None
    created: datetime | None = None
    updated: datetime | None = None


class RNECompany(BaseModel):
    updatedAt: datetime | None = None
    createdAt: datetime | None = None
    formality: Formality
    siren: str
    origin: str | None = None

    def is_personne_morale(self) -> bool:
        return isinstance(self.formality.content.personneMorale, PersonneMorale)

    def is_exploitation(self) -> bool:
        return isinstance(self.formality.content.exploitation, Exploitation)

    def is_personne_physique(self) -> bool:
        return isinstance(self.formality.content.personnePhysique, PersonnePhysique)
