from datetime import datetime
from pydantic import BaseModel


class Adresse(BaseModel):
    pays: str | None = None
    codePays: str | None = None
    commune: str | None = None
    codePostal: str | None = None
    codeInseeCommune: str | None = None
    voie: str | None = None


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
    pouvoirs: list[Pouvoir] | None = None


class Identite(BaseModel):
    entreprise: dict | None = None
    entrepreneur: Entrepreneur | None = Entrepreneur()


class Exploitation(BaseModel):
    identite: dict | None = None
    composition: Composition | None = None

    @property
    def dirigeants(self):
        return self.composition.pouvoirs if self.composition else []


class PersonneMorale(BaseModel):
    identite: dict | None = None
    composition: Composition | None = None

    @property
    def dirigeants(self):
        return self.composition.pouvoirs if self.composition else []


class PersonnePhysique(BaseModel):
    identite: Identite | None = None
    composition: Composition | None = None

    @property
    def dirigeants(self):
        return [self.identite.entrepreneur if self.identite else None]


class Content(BaseModel):
    personnePhysique: PersonnePhysique | None = None
    personneMorale: PersonneMorale | None = None
    exploitation: Exploitation | None = None


class Formality(BaseModel):
    siren: str
    content: Content
    typePersonne: str | None = None
    formeJuridique: str | None = None
    created: datetime | None = None
    updated: datetime | None = None


class RNECompany(BaseModel):
    updatedAt: datetime
    formality: Formality
    siren: str

    @property
    def dirigeants(self):
        if self.is_personne_morale():
            return self.formality.content.personneMorale.dirigeants
        elif self.is_exploitation():
            return self.formality.content.exploitation.dirigeants
        elif self.is_personne_physique():
            return self.formality.content.personnePhysique.dirigeants
        else:
            return []

    def is_personne_morale(self) -> bool:
        return isinstance(self.formality.content.personneMorale, PersonneMorale)

    def is_exploitation(self) -> bool:
        return isinstance(self.formality.content.exploitation, Exploitation)

    def is_personne_physique(self) -> bool:
        return isinstance(self.formality.content.personnePhysique, PersonnePhysique)
