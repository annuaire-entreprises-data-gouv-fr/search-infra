import logging

import pandas as pd

from data_pipelines_annuaire.helpers.data_quality import clean_sirent_column
from data_pipelines_annuaire.helpers.utils import parse_json_safe
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
    "acte",
    "parutionavisprecedent",
]

_OUTPUT_COLUMNS = [
    "siren",
    "id_annonce",
    "date",
    "date_publication",
]


def _parse_creation_date(acte_str: str) -> str:
    """Extrait la date de création depuis le champ json acte.

    On privilégie la date d'immatriculation au RCS, avec repli sur la date de
    commencement d'activité quand elle est absente.
    """
    data = parse_json_safe(acte_str)
    if not data:
        return ""
    date = data.get("dateImmatriculation", "") or data.get(
        "dateCommencementActivite", ""
    )
    return parse_date_bodacc(date)


def _process_creation_chunk(chunk: pd.DataFrame) -> pd.DataFrame:
    """Traiter un chunk de données créations."""
    chunk = extract_sirens_from_listepersonnes(chunk)

    chunk = clean_sirent_column(
        chunk,
        column_type="siren",
        column_name="siren",
        add_leading_zeros=True,
        max_removal_percentage=0.1,
    )

    if chunk.empty:
        return chunk

    chunk["date"] = chunk["acte"].apply(_parse_creation_date)
    chunk["date_publication"] = chunk["dateparution"]
    chunk["id_annonce"] = chunk["id"]
    return chunk[_OUTPUT_COLUMNS]


def process_creations(raw_file_path: str, chunk_size: int) -> pd.DataFrame:
    logging.info("Processing creations...")
    chunks_processed = []

    # Charger le fichier complet pour gérer les annulations
    df_full = pd.read_csv(
        raw_file_path,
        dtype=str,
        sep=";",
        usecols=_INPUT_COLUMNS,
    )
    logging.info(f"Loaded {len(df_full)} creation rows")

    # Filtrer les annulations et rectificatifs
    df_full = process_discarded_announcements(df_full)
    logging.info(f"After filtering cancellations: {len(df_full)} rows")

    # Traiter par chunks pour la mémoire
    for i in range(0, len(df_full), chunk_size):
        chunk = df_full.iloc[i : i + chunk_size]
        processed = _process_creation_chunk(chunk)
        if not processed.empty:
            chunks_processed.append(processed)

    if not chunks_processed:
        logging.warning("No creations found")
        return pd.DataFrame(columns=_OUTPUT_COLUMNS)

    # Concaténer tous les chunks. On conserve toutes les créations (une entité
    # peut être immatriculée plusieurs fois) : pas de déduplication par SIREN.
    df = pd.concat(chunks_processed, ignore_index=True)

    df["date"] = pd.to_datetime(df["date"], errors="coerce", format="%Y-%m-%d")
    df["date_publication"] = pd.to_datetime(
        df["date_publication"], errors="coerce", format="%Y-%m-%d"
    )

    return df[_OUTPUT_COLUMNS]
