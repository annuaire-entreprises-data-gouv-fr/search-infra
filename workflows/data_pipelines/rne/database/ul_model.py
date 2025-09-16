from datetime import date
from typing import Literal

from pydantic import BaseModel


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


class Immatriculation(BaseModel):
    date_immatriculation: date | None = None
    date_radiation: date | None = None
    capital_social: float | None = None
    date_cloture_exercice: str | None = None
    duree_personne_morale: int | None = None
    nature_entreprise: str | None = None
    date_debut_activite: str | None = None
    capital_variable: bool | None = None
    devise_capital: str | None = None


class Mention(BaseModel):
    num_observation: str | None = None
    date_effet: str | None = None
    texte: str | None = None


class InscriptionOffice(BaseModel):
    event: str | None = None
    date_effet: str | None = None
    observation: str | None = None
    partner_center: str | None = None
    partner_code: str | None = None
    observation_complementaire: str | None = None
    mentions: list[Mention] | None = None  # schéma privé


class Historique(BaseModel):
    date_integration: str | None = None
    code_evenement: str | None = None
    libelle_evenement: str | None = None
    numero_liasse: str | None = None
    patch_id: str | None = None
    date_effet: str | None = None
    chemin_date_effet: str | None = None
    chemin_date_effet_id: str | None = None


class Modalite(BaseModel):
    date_effet: str | None = None
    detention_part_directe: bool | None = None
    modalites_de_controle: list[str] | None = None
    detention_25p_capital: bool | None = None
    detention_25p_droit_vote: bool | None = None


class BeneficiaireEffectif(BaseModel):
    nom: str | None = None  # new
    nom_usage: str | None = None  # new
    prenoms: str | None = None  # new
    genre: str | None = None  # new
    date_naissance: str | None = None  # new
    lieu_naissance: str | None = None  # new
    pays_naissance: str | None = None  # new
    adresse: Adresse | None = None  # new
    modalite: Modalite | None = None  # new


class DirigeantsPP(BaseModel):
    type_dirigeant: Literal["personne physique"] = "personne physique"
    date_debut: str | None = None  # new
    date_fin: str | None = None  # new
    actif: bool | None = None  # new
    role: str | None = None
    libelle_role: str | None = None  # new
    autre_role: str | None = None  # new
    second_role: str | None = None  # new
    libelle_second_role: str | None = None  # new
    qualite_artisan: str | None = None  # new
    nom: str | None = None
    nom_usage: str | None = None
    prenoms: str | None = None
    genre: str | None = None
    date_naissance: str | None = None  # new
    lieu_naissance: str | None = None  # new
    pays_naissance: str | None = None  # new
    nationalite: str | None = None
    adresse: Adresse | None = None  # new
    mail: str | None = None  # new
    telephone: str | None = None  # new
    role_conjoint: str | None = None  # new
    nom_conjoint: str | None = None  # new
    nom_usage_conjoint: str | None = None  # new
    prenoms_conjoint: str | None = None  # new
    genre_conjoint: str | None = None  # new
    date_naissance_conjoint: str | None = None  # new
    lieu_naissance_conjoint: str | None = None  # new
    pays_naissance_conjoint: str | None = None  # new
    nationalite_conjoint: str | None = None  # new
    adresse_conjoint: Adresse | None = None
    mail_conjoint: str | None = None  # new
    telephone_conjoint: str | None = None  # new
    situation_matrimoniale: str | None = None


class DirigeantsPM(BaseModel):
    type_dirigeant: Literal["personne morale"] = "personne morale"
    date_debut: str | None = None  # new
    date_fin: str | None = None  # new
    actif: bool | None = None  # new
    role: str | None = None
    libelle_role: str | None = None  # new
    autre_role: str | None = None  # new
    second_role: str | None = None  # new
    libelle_second_role: str | None = None  # new
    qualite_artisan: str | None = None  # new
    siren: str | None = None
    forme_juridique: str | None = None
    denomination: str | None = None
    pays: str | None = None
    lieu_registre: str | None = None  # new
    nom_individu_representant: str | None = None  # new
    nom_usage_individu_representant: str | None = None  # new
    prenoms_individu_representant: str | None = None  # new
    genre_individu_representant: str | None = None  # new
    date_naissance_individu_representant: str | None = None  # new
    lieu_naissance_individu_representant: str | None = None  # new
    pays_naissance_individu_representant: str | None = None  # new
    nationalite_individu_representant: str | None = None  # new
    adresse_individu_representant: Adresse | None = None  # new


class Activite(BaseModel):
    siret: str | None = None
    indicateur_principal: str | None = None
    activite_reguliere: str | None = None
    qualite_non_sedentaire: str | None = None
    indicateur_non_sedentaire: bool | None = None
    indicateur_artiste_auteur: bool | None = None
    indicateur_marin_professionnel: bool | None = None
    indicateur_prolongement: bool | None = None
    description_detaillee: str | None = None
    forme_exercice: str | None = None
    code_aprm: str | None = None
    metier_art: bool | None = None
    date_debut: date | None = None
    date_fin: str | None = None
    categorisation_activite_1: str | None = None
    categorisation_activite_2: str | None = None
    categorisation_activite_3: str | None = None
    categorisation_activite_4: str | None = None
    type_origine_fond: str | None = None
    ### to check
    code_category: str | None = None
    indicateur_activitee_ape: bool | None = None  # not demanded
    code_ape: str | None = None  # not demanded
    activite_rattachee_eirl: bool | None = None  # not demanded


## to check
class Siege(BaseModel):
    siret: str | None = None
    statut: str | None = None  # new
    type: str | None = None  # new
    date_ouverture: str | None = None  # new
    date_fermeture: str | None = None  # new
    adresse: Adresse | None = None  # new
    enseigne: str | None = None
    nom_commercial: str | None = None
    activites: list[Activite] | None = None
    code_ape: str | None = None  # new


class Etablissement(BaseModel):
    siret: str | None = None
    statut: str | None = None  # new
    type: str | None = None  # new
    date_ouverture: str | None = None  # new
    date_fermeture: str | None = None  # new
    enseigne: str | None = None  # new
    nom_commercial: str | None = None  # new
    adresse: Adresse | None = None  # new
    code_ape: str | None = None  # new
    activites: list[Activite] | None = None  ### check


class UniteLegale(BaseModel):
    siren: str | None = None
    code_ape: str | None = None  # new
    date_mise_a_jour_formalite: str | None = None  # new
    date_mise_a_jour_rne: str | None = None  # new
    siren_doublons: str | None = None  # new
    nombre_representants_actifs: int | None = None  # new
    nombre_etablissements_ouverts: int | None = None  # new
    date_creation: str | None = None  # new
    societe_etrangere: bool | None = None  # new
    forme_juridique: str | None = None  # new
    forme_juridique_insee: str | None = None  # new
    indicateur_associe_unique: bool | None = None  # new
    indicateur_ess: bool | None = None  # new
    eirl: bool | None = None  # new
    entreprise_agricole: bool | None = None  # new
    reliee_entreprise_agricole: bool | None = None  # new
    date_sommeil: str | None = None  # new
    date_radiation: str | None = None
    statut: str | None = None  # new
    denomination: str | None = None
    nom_commercial: str | None = None
    nom_exploitation: str | None = None  # new
    adresse: Adresse | None = None  # new
    date_cloture_exercice_social: str | None = None  # new
    date_fin_existence: str | None = None  # new
    duree_personne_morale: int | None = None  # new
    montant_capital: float | None = None  # new
    capital_variable: bool | None = None  # new
    devise_capital: str | None = None  # new
    societe_mission: bool | None = None  # new
    entreprise_domiciliataire_siren: str | None = None  # new
    entreprise_domiciliataire_denomination: str | None = None  # new
    entreprise_domiciliataire_nom_commercial: str | None = None  # new

    ## pour personne physique identite
    nom: str | None = None
    nom_usage: str | None = None
    prenoms: str | None = None
    genre: str | None = None  # new
    date_naissance: str | None = None  # new
    lieu_naissance: str | None = None  # new
    pays_naissance: str | None = None  # new
    nationalite: str | None = None  # new
    qualite_artisan: str | None = None  # new
    mail: str | None = None  # new
    telephone: str | None = None  # new
    role_conjoint: str | None = None  # new
    nom_conjoint: str | None = None  # new
    nom_usage_conjoint: str | None = None  # new
    prenoms_conjoint: str | None = None  # new
    genre_conjoint: str | None = None  # new
    date_naissance_conjoint: str | None = None  # new
    lieu_naissance_conjoint: str | None = None  # new
    pays_naissance_conjoint: str | None = None  # new
    nationalite_conjoint: str | None = None  # new
    adresse_conjoint: Adresse | None = None
    mail_conjoint: str | None = None  # new
    telephone_conjoint: str | None = None  # new

    etat_administratif_insee: str | None = None  # new
    forme_exercice_activite_principale: str | None = None  # new
    statut_diffusion: str | None = None  # new
    activite_principale: str | None = None
    tranche_effectif_salarie: str | None = None
    nature_juridique: str | None = None
    dirigeants: list[DirigeantsPP | DirigeantsPM] | None = None
    siege: Siege | None = None
    immatriculation: Immatriculation | None = None
    etablissements: list[Etablissement] | None = None
    beneficiaires_effectifs: list[BeneficiaireEffectif] | None = None  # new
    historique: list[Historique] | None = None  # new
    inscriptions_offices: list[InscriptionOffice] | None = None  # new

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
