from dataclasses import dataclass
from datetime import datetime


@dataclass
class Entreprise:
    siren: str | None = None
    forme_juridique: str | None = None
    forme_juridique_code: str | None = None
    nom_commercial: str | None = None
    raison_sociale: str | None = None
    nom: str | None = None
    prenom: str | None = None


@dataclass
class Demandeur:
    siret: str | None = None
    siege_social: bool | None = None
    entreprise: Entreprise | None = None


@dataclass
class Traitement:
    state: str
    email_agent_traitant: str
    date_traitement: datetime | None
    motivation: str | None


@dataclass
class Champ:
    id: str
    champ_descriptor_id: str
    type_name: str
    label: str
    string_value: str
    updated_at: datetime
    prefilled: bool


@dataclass
class Dossier:
    id: str
    number: int
    archived: bool
    state: str
    date_derniere_modification: datetime
    date_depot: datetime
    date_passage_en_construction: datetime
    date_passage_en_instruction: datetime
    date_traitement: datetime | None
    date_expiration: datetime | None
    date_suppression_par_usager: datetime | None
    demandeur: Demandeur
    traitements: list[Traitement]
    champs: list[Champ]
    usager_email: str


@dataclass
class PageInfo:
    has_next_page: bool
    has_previous_page: bool
    start_cursor: str
    end_cursor: str


@dataclass
class DemarcheResponse:
    id: str
    number: int
    title: str
    state: str
    date_creation: datetime
    dossiers: list[Dossier]
    page_info: PageInfo
