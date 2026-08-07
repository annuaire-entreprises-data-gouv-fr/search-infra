import json
import logging

import pandas as pd

from data_pipelines_annuaire.helpers.data_quality import clean_sirent_column
from data_pipelines_annuaire.workflows.data_pipelines.bodacc.utils import (
    extract_sirens_from_listepersonnes,
    parse_date_bodacc,
    process_discarded_announcements,
)

_INPUT_COLUMNS = [
    "id",
    "listepersonnes",
    "dateparution",
    "typeavis",
    "radiationaurcs",
    "parutionavisprecedent",
]

_OUTPUT_COLUMNS = [
    "siren",
    "id_annonce",
    "est_radie",
    "date",
    "date_publication",
]


def _parse_radiation_json(radiation_str: str) -> str:
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


def _process_radiation_chunk(chunk: pd.DataFrame) -> pd.DataFrame:
    """Traiter un chunk de données radiations."""
    # Un avis de radiation ne concerne à chaque fois qu'une seule "personne"
    # Quelques rares cas ont cependant plusieurs objets "personne"
    # Mais ils partagent à chaque fois le même siren, ainsi après déduplication
    # on se retrouve tout de même avec une seule "personne" par annonce
    chunk = extract_sirens_from_listepersonnes(chunk)

    # Nettoyer et valider les SIREN
    chunk = clean_sirent_column(
        chunk,
        column_type="siren",
        column_name="siren",
        add_leading_zeros=True,
        max_removal_percentage=0.1,
    )

    if chunk.empty:
        return chunk

    # Parser le JSON radiationaurcs pour extraire la date
    chunk["date"] = chunk["radiationaurcs"].apply(_parse_radiation_json)

    # Marquer comme radié (présent dans le fichier = radié)
    chunk["est_radie"] = 1

    # Garder uniquement les colonnes nécessaires
    chunk["date_publication"] = chunk["dateparution"]
    chunk["id_annonce"] = chunk["id"]
    return chunk[_OUTPUT_COLUMNS]


def process_radiations(raw_file_path: str, chunk_size: int) -> pd.DataFrame:
    logging.info("Processing radiations...")
    chunks_processed = []

    # Charger le fichier complet pour gérer les annulations
    df_full = pd.read_csv(
        raw_file_path,
        dtype=str,
        sep=";",
        usecols=_INPUT_COLUMNS,
    )
    logging.info(f"Loaded {len(df_full)} radiation rows")

    # Filtrer les annulations
    df_full = process_discarded_announcements(df_full)
    logging.info(f"After filtering cancellations: {len(df_full)} rows")

    # Traiter par chunks pour la mémoire
    for i in range(0, len(df_full), chunk_size):
        chunk = df_full.iloc[i : i + chunk_size]
        processed = _process_radiation_chunk(chunk)
        if not processed.empty:
            chunks_processed.append(processed)

    if not chunks_processed:
        logging.warning("No radiations found")
        return pd.DataFrame(columns=_OUTPUT_COLUMNS)

    # Concaténer tous les chunks
    df = pd.concat(chunks_processed, ignore_index=True)

    # Environ 109 dates de radiations avec des dates absurdes comme 1213-10-03 ou 5019-06-01
    # Ces "erreurs" sont ignorées lors des conversions en datetime
    df["date"] = pd.to_datetime(df["date"], errors="coerce", format="%Y-%m-%d")
    df["date_publication"] = pd.to_datetime(
        df["date_publication"], errors="coerce", format="%Y-%m-%d"
    )

    # Dédupliquer par Siren en gardant la radiation la plus récente
    df = df.sort_values(
        ["date", "date_publication"],
        ascending=[False, False],
    )
    duplicated_mask = df.duplicated(subset=["siren"], keep="first")
    n_duplicates = duplicated_mask.sum()
    if n_duplicates > 0:
        sample_sirens = df.loc[duplicated_mask, "siren"].head(5).tolist()
        logging.info(
            f"Radiations: {n_duplicates} duplicate Siren, sample:\n{sample_sirens}"
        )
    df = df.drop_duplicates(subset=["siren"], keep="first")

    return df[_OUTPUT_COLUMNS]
