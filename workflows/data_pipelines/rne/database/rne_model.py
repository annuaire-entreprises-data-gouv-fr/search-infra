from datetime import datetime, date
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


class PouvoirEntreprise(BaseModel):
    pays: str | None = None
    siren: str | None = None
    denomination: str | None = None
    role: str | None = None
    formeJuridique: str | None = None
    autreIdentifiantEtranger: str | None = None
    nicSiege: str | None = None
    nomCommercial: str | None = None


class Entrepreneur(BaseModel):
    descriptionPersonne: DescriptionPersonne | None = DescriptionPersonne()
    adresseDomicile: Adresse | None = Adresse()


class Pouvoir(BaseModel):
    typeDePersonne: str | None = None
    individu: PouvoirIndividu | None = PouvoirIndividu()
    entreprise: PouvoirEntreprise | None = PouvoirEntreprise()
    adresseEntreprise: Adresse | None = Adresse()


class Composition(BaseModel):
    pouvoirs: list[Pouvoir] | None = Pouvoir()


class Entrerpise(BaseModel):
    denomination: str | None = None
    formeJuridique: str | None = None
    nomCommercial: str | None = None
    effectifSalarie: str | None = None
    dateImmat: datetime | None = None
    codeApe: str | None = None


class Identite(BaseModel):
    entreprise: Entrerpise | None = Entrerpise()
    entrepreneur: Entrepreneur | None = Entrepreneur()


class AdresseEntreprise(BaseModel):
    caracteristiques: dict | None = None
    adresse: Adresse | None = Adresse()
    entrepriseDomiciliataire: dict | None = None


class DescriptionEtablissement(BaseModel):
    siret: str | None = None
    enseigne: str | None = None
    nomCommercial: str | None = None


class EtablissementPrincipal(BaseModel):
    descriptionEtablissement: DescriptionEtablissement | None = (
        DescriptionEtablissement()
    )
    adresse: Adresse | None = Adresse()


class DetailCessationEntreprise(BaseModel):
    dateRadiation: date | None = None


class NatureCessationEntreprise(BaseModel):
    etatAdministratifInsee: str | None = None


class Exploitation(BaseModel):
    identite: Identite | None = Identite()
    composition: Composition | None = None
    adresseEntreprise: AdresseEntreprise | None = AdresseEntreprise()
    etablissementPrincipal: EtablissementPrincipal | None = EtablissementPrincipal()
    detailCessationEntreprise: DetailCessationEntreprise | None = None


class PersonneMorale(BaseModel):
    identite: Identite | None = Identite()
    composition: Composition | None = None
    adresseEntreprise: AdresseEntreprise | None = AdresseEntreprise()
    etablissementPrincipal: EtablissementPrincipal | None = EtablissementPrincipal()
    detailCessationEntreprise: DetailCessationEntreprise | None = None


class PersonnePhysique(BaseModel):
    identite: Identite | None = None
    composition: Composition | None = None
    adresseEntreprise: AdresseEntreprise | None = AdresseEntreprise()
    etablissementPrincipal: EtablissementPrincipal | None = EtablissementPrincipal()
    detailCessationEntreprise: DetailCessationEntreprise | None = None


class Content(BaseModel):
    formeExerciceActivitePrincipale: str | None = None
    personnePhysique: PersonnePhysique | None = None
    personneMorale: PersonneMorale | None = None
    exploitation: Exploitation | None = None
    natureCessationEntreprise: NatureCessationEntreprise | None = (
        NatureCessationEntreprise()
    )


class Formality(BaseModel):
    siren: str
    companyName: str | None = None
    content: Content
    typePersonne: str | None = None
    formeJuridique: str | None = None
    diffusionINSEE: str | None = None
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
