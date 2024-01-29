from datetime import datetime
from pydantic import BaseModel
from typing import Literal
from dag_datalake_sirene.workflows.data_pipelines.elasticsearch.data_enrichment import (
    format_adresse_complete,
)


class Adresse(BaseModel):
    pays: str | None = None
    code_pays: str | None = None
    commune: str | None = None
    code_postal: str | None = None
    code_commune: str | None = None
    voie: str | None = None
    num_voie: str | None = None
    type_voie: str | None = None
    indice_repetition: str | None = None
    complement_localisation: str | None = None
    distribution_speciale: str | None = None


class DirigeantsPP(BaseModel):
    nom: str | None = None
    nom_usage: str | None = None
    prenoms: str | None = None
    genre: str | None = None
    date_de_naissance: str | None = None
    role: str | None = None
    nationalite: str | None = None
    situation_matrimoniale: str | None = None
    type_dirigeant: Literal["personne physique"] = "personne physique"


class DirigeantsPM(BaseModel):
    siren: str | None = None
    denomination: str | None = None
    role: str | None = None
    pays: str | None = None
    forme_juridique: str | None = None
    type_dirigeant: Literal["personne morale"] = "personne morale"


class Siege(BaseModel):
    siret: str | None = None
    adresse: Adresse | None = Adresse()
    enseigne: str | None = None
    nom_commercial: str | None = None


class UniteLegale(BaseModel):
    siren: str | None = None
    denomination: str | None = None
    nom_commercial: str | None = None
    date_creation: datetime | None = None
    date_mise_a_jour: datetime | None = None
    date_immatriculation: datetime | None = None
    date_radiation: datetime | None = None
    activite_principale: str | None = None
    tranche_effectif_salarie: str | None = None
    nature_juridique: str | None = None
    etat_administratif: str | None = None
    forme_exercice_activite_principale: str | None = None
    statut_diffusion: str | None = None
    adresse: Adresse | None = Adresse()
    dirigeants: list[DirigeantsPP | DirigeantsPM] | None = None
    siege: Siege | None = Siege()

    def get_dirigeants_list(self):
        dirigeants_pp_list = []
        dirigeants_pm_list = []

        if self.dirigeants:
            for dirigeant in self.dirigeants:
                if isinstance(dirigeant, DirigeantsPP):
                    dirigeants_pp_list.append(dirigeant)
                elif isinstance(dirigeant, DirigeantsPM):
                    dirigeants_pm_list.append(dirigeant)
        return dirigeants_pp_list, dirigeants_pm_list

    def format_address(self):
        if self.adresse:
            addr = self.adresse
            return format_adresse_complete(
                addr.complement_localisation,
                addr.num_voie,
                addr.indice_repetition,
                addr.type_voie,
                addr.voie,
                addr.commune,
                None,  # No libelle_cedex in the provided Adresse class
                addr.distribution_speciale,
                addr.code_postal,
                None,  # No cedex in the provided Adresse class
                addr.commune,
                None,  # No libelle_commune_etranger in the provided Adresse class
                None,  # No libelle_pays_etranger in the provided Adresse class
            )
        return ""
