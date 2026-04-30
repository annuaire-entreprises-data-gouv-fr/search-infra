import json
import logging
import re

import pandas as pd

from data_pipelines_annuaire.helpers.utils import parse_json_safe


def parse_date_bodacc(date_str: str) -> str:
    """
    Parse une date BODACC et retourne au format YYYY-MM-DD.
    Gère les formats :
        - big-endian : YYYY-MM-DD
        - little-endian : JJ/MM/AAAA
        - texte : 27 novembre 2008 ou 1er janvier 2020
    """

    MOIS_FR = {
        "janvier": "01",
        "février": "02",
        "fevrier": "02",
        "mars": "03",
        "avril": "04",
        "mai": "05",
        "juin": "06",
        "juillet": "07",
        "août": "08",
        "aout": "08",
        "septembre": "09",
        "octobre": "10",
        "novembre": "11",
        "décembre": "12",
        "decembre": "12",
    }

    if pd.isna(date_str) or not date_str:
        return ""

    date_str = str(date_str).strip()

    # Format ISO : YYYY-MM-DD
    if re.match(r"^\d{4}-\d{2}-\d{2}$", date_str):
        return date_str

    # Format numérique : JJ/MM/AAAA
    match = re.match(r"^(\d{2})/(\d{2})/(\d{4})$", date_str)
    if match:
        jour, mois, annee = match.groups()
        return f"{annee}-{mois}-{jour}"

    # Remplacer les espaces insécables par des espaces normaux
    date_str = date_str.replace("\xa0", " ")

    # Format français : "27 novembre 2008" ou "1er janvier 2020"
    match = re.match(r"(\d{1,2})(?:er)?\s+(\w+)\s+(\d{4})", date_str)
    if match:
        jour, mois, annee = match.groups()
        mois_num = MOIS_FR.get(mois.lower())
        if mois_num:
            return f"{annee}-{mois_num}-{jour.zfill(2)}"

    return ""


def parse_radiation_json(radiation_str: str) -> str:
    """Extrait la date de cessation depuis le champ radiationaurcs."""
    if pd.isna(radiation_str) or not radiation_str:
        return ""
    data = json.loads(radiation_str)
    # Format acutel PP
    if "dateCessationActivitePP" in data:
        return parse_date_bodacc(data["dateCessationActivitePP"])
    # Format obsolète PP
    if "radiationPP" in data and isinstance(data["radiationPP"], dict):
        return parse_date_bodacc(data["radiationPP"].get("dateCessationActivitePP", ""))
    # Les PM n'ont pas de date de disponible
    return ""


def parse_jugement_json(jugement_str: str) -> dict:
    """Parse le champs Json jugement et en extraire famille, nature et date."""
    if pd.isna(jugement_str) or not jugement_str:
        return {"famille": "", "nature": "", "date": ""}
    try:
        data = json.loads(jugement_str)
        return {
            "famille": data.get("famille", ""),
            "nature": data.get("nature", ""),
            "date": parse_date_bodacc(data.get("date", "")),
        }
    except json.JSONDecodeError:
        return {"famille": "", "nature": "", "date": ""}


def is_procedure_en_cours(nature: str) -> bool:
    """Vérifie si la nature de jugement correspond à une procédure en cours."""
    NATURES_CLOTURE = [
        "clôture",
        "cloture",
        "clotûre",
        "plan arrêté",
        "plan arrete",
        "arrêtant le plan",
        "arretant le plan",
    ]
    if pd.isna(nature) or not nature:
        return False
    nature_lower = nature.lower()
    return not any(kw in nature_lower for kw in NATURES_CLOTURE)


def is_cloture(famille: str) -> bool:
    """Vérifie si la famille correspond à une clôture de procédure."""
    # Familles de jugement indiquant une clôture
    FAMILLES_CLOTURE = [
        "jugement de clôture",
        "jugement de clÃ´ture",  # Encodage UTF-8 mal interprété
    ]
    if pd.isna(famille) or not famille:
        return False
    famille_lower = famille.lower()
    return any(kw in famille_lower for kw in FAMILLES_CLOTURE)


def is_retractation(famille: str) -> bool:
    """Vérifie si la famille correspond à une rétractation (équivalent annulation)."""
    # Familles équivalentes à une annulation
    FAMILLES_ANNULATION = [
        "rétractation sur tierce opposition",
        # "retractation sur tierce opposition",
    ]
    if pd.isna(famille) or not famille:
        return False
    famille_lower = famille.lower()
    return any(kw in famille_lower for kw in FAMILLES_ANNULATION)


def is_rapport_de_radiation_doffice(radiationaurcs_str: str) -> bool:
    """
    Vérifie si le champ radiationaurcs indique un rapport de radiation d'office.
    Un rapport de radiation d'office correspond à l'annulation d'une radiation.
    """
    data = parse_json_safe(radiationaurcs_str)
    if not data:
        return False
    return data.get("commentaire", "") == "Rapport de radiation d'office"


def build_bodacc_id(
    code_publication: str, numero_parution: str | int, numero_annonce: str | int
) -> str | None:
    if code_publication and numero_parution and numero_annonce:
        lettre = code_publication.split()[-1] if code_publication else ""
        return f"{lettre}{numero_parution}{numero_annonce}"
    return None


def extract_id_from_avis_precedent(avis_precedent_str: str) -> str | None:
    """Extrait l'ID de l'annonce annulée ou rectifiée depuis le champs Json parutionavisprecedent."""
    avis = parse_json_safe(avis_precedent_str)
    if not avis:
        return None
    return build_bodacc_id(
        code_publication=avis.get("nomPublication", ""),
        numero_parution=avis.get("numeroParution", ""),
        numero_annonce=avis.get("numeroAnnonce", ""),
    )


def get_previous_ids_to_discard(df: pd.DataFrame) -> set:
    """Extrait les IDs des annonces précédentes annulées, rectifiées ou rétractées."""
    discarded_ids = set()

    # Annulation explicite de l'avis précédent
    annulations = df[df["typeavis"] == "annulation"]
    if not annulations.empty:
        ids_from_annulations = annulations["parutionavisprecedent"].apply(
            extract_id_from_avis_precedent
        )
        discarded_ids.update(ids_from_annulations.dropna())

    # Rectificatif (remplacement) de l'annonce précédente
    rectificatifs = df[df["typeavis"] == "rectificatif"]
    if not rectificatifs.empty:
        ids_from_rectificatifs = rectificatifs["parutionavisprecedent"].apply(
            extract_id_from_avis_precedent
        )
        discarded_ids.update(ids_from_rectificatifs.dropna())

    # Rétractations sur tierce opposition
    # S'applique aux procédures collectives et non aux radiations
    # Le filtre sur jugement permet d'identifier les procédures collectives des radiations
    if "jugement" in df.columns:
        familles = df["jugement"].apply(
            lambda x: (
                parse_json_safe(x).get("famille", "") if parse_json_safe(x) else ""
            )
        )
        retractations = df[familles.apply(is_retractation)]
        if not retractations.empty:
            ids_from_retractations = retractations["parutionavisprecedent"].apply(
                extract_id_from_avis_precedent
            )
            discarded_ids.update(ids_from_retractations.dropna())

    return discarded_ids


def get_processed_ids_to_discard(df: pd.DataFrame) -> set:
    """
    Extrait les IDs des :
       - avis d'annulation,
       - rectificatifs avec rapport de radiation d'office,
       - jugements de rétractations.
    """
    ids_to_discard = set()

    # Avis d'annulations
    annulations = df[df["typeavis"] == "annulation"]
    ids_to_discard.update(annulations["id"].dropna())

    # Rectificatifs de type "radiation d'office" (équivalent annulation)
    if "radiationaurcs" in df.columns:
        is_rectificatif = df["typeavis"] == "rectificatif"
        is_rad_doffice = df["radiationaurcs"].apply(is_rapport_de_radiation_doffice)
        ids_to_discard.update(df.loc[is_rectificatif & is_rad_doffice, "id"].dropna())

    # Rétractations sur tierce opposition
    # Le filtre sur jugement permet d'identifier les procédures collectives des radiations
    if "jugement" in df.columns:
        familles = df["jugement"].apply(
            lambda x: (
                parse_json_safe(x).get("famille", "") if parse_json_safe(x) else ""
            )
        )
        retractations = df[familles.apply(is_retractation)]
        ids_to_discard.update(retractations["id"].dropna())

    return ids_to_discard


def process_discarded_announcements(df: pd.DataFrame) -> pd.DataFrame:
    """
    Exclut :
        - les annonces annulées,
        - les annonces rectifiées,
        - les avis d'annulations,
        - les rétractations.
    Les rectificatifs sont conservés car ils portent la valeur à jour.
    """

    logging.info("Supprime les annonces annulées ou remplacées")
    discarded_ids = get_previous_ids_to_discard(df)
    df = df[~df["id"].isin(discarded_ids)]

    logging.info("Supprime les annonces qui annulent les annonces précédentes")
    ids_to_discard = get_processed_ids_to_discard(df)
    df = df[~df["id"].isin(ids_to_discard)]

    return df


def extract_siren_from_registre(df: pd.DataFrame) -> pd.DataFrame:
    """
    Extrait le Siren depuis la colonne registre.
    Exemple :
        - formats d'input existants :
            - "123 456 789,123456789"
            - "123456789,123 456 789"
        - output: "123456789"
    """
    df = df.copy()
    df["siren"] = df["registre"].str.split(",").str[0].str.strip()
    # Retire les Siren manquants avant clean_sirent_column afin de ne logger
    # que les Siren avec un mauvais format
    df = df[df["siren"].notna() & (df["siren"] != "")]
    return df
