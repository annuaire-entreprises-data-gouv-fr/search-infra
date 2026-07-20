import logging
import re

import pandas as pd

from data_pipelines_annuaire.helpers.utils import keep_only_numbers, parse_json_safe


def fix_mojibake(text: str) -> str:
    """Répare les chaînes UTF-8 mal décodées en Latin-1 (ex: 'clÃ´ture' -> 'clôture')."""
    if not text:
        return text
    try:
        return text.encode("latin-1").decode("utf-8")
    except (UnicodeDecodeError, UnicodeEncodeError):
        return text


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


def _is_jugement_retractation(famille: str) -> bool:
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


def _is_radiationaurcs_radiation_doffice(radiationaurcs_str: str) -> bool:
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
    """Extrait l'ID de l'annonce annulée ou rectifiée depuis le champ json parutionavisprecedent."""
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
        retractations = df[familles.apply(_is_jugement_retractation)]
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
        is_rad_doffice = df["radiationaurcs"].apply(
            _is_radiationaurcs_radiation_doffice
        )
        ids_to_discard.update(df.loc[is_rectificatif & is_rad_doffice, "id"].dropna())

    # Rétractations sur tierce opposition
    # Le filtre sur jugement permet d'identifier les procédures collectives des radiations
    if "jugement" in df.columns:
        familles = df["jugement"].apply(
            lambda x: (
                parse_json_safe(x).get("famille", "") if parse_json_safe(x) else ""
            )
        )
        retractations = df[familles.apply(_is_jugement_retractation)]
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


def _extract_sirens_from_personne(listepersonnes: str) -> list[str]:
    """
    Extract all unique siren from the listepersonnes field.
    If there is multiple entries, "listepersonnes" looks like:
        ```json
        {
          "personne": [
            {"numeroImmatriculation": {"numeroIdentification": "123 456 789",[...]},[...]},
            {"inscriptionRM": {"numeroIdentificationRM": "424 557 189",[...]},[...]},
            [...]
          ]
        }
        ```
    But if there is only one entry, "listepersonnes" looks like:
        ```json
        {
          "personne": {"numeroImmatriculation": {"numeroIdentification": "123 456 789",[...]},[...]},
        }
        ```
    Or:
        ```json
        {
          "personne": {"inscriptionRM": {"numeroIdentificationRM": "123 456 789",[...]},[...]},
        }
        ```

    """
    data = parse_json_safe(listepersonnes)
    if not data:
        return []

    personnes = data.get("personne", [])
    if not isinstance(personnes, list):
        personnes = [personnes]

    sirens = []
    for personne in personnes:
        if not isinstance(personne, dict):
            continue
        siren = None
        immat_rcs = personne.get("numeroImmatriculation")
        if isinstance(immat_rcs, dict):
            siren = immat_rcs.get("numeroIdentification")
        if not siren:
            immat_rm = personne.get("inscriptionRM")
            if isinstance(immat_rm, dict):
                siren = immat_rm.get("numeroIdentificationRM")
        siren = keep_only_numbers(siren)
        if siren and siren not in sirens:
            sirens.append(siren)
    return sirens


def extract_sirens_from_listepersonnes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Unnest listepersonnes to produce one row per unique siren.
    All columns keep the same values, only the siren one changes.
    """
    df = df.copy()
    df["_sirens"] = df["listepersonnes"].apply(_extract_sirens_from_personne)
    df = df[df["_sirens"].apply(len) > 0]
    df = df.explode("_sirens", ignore_index=True)
    df = df.rename(columns={"_sirens": "siren"})
    return df
