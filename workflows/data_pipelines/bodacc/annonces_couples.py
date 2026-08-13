import logging

import pandas as pd

from data_pipelines_annuaire.workflows.data_pipelines.bodacc.utils import (
    extract_sirens_greffes_from_listepersonnes,
)

logger = logging.getLogger(__name__)

_INPUT_COLUMNS = [
    "listepersonnes",
    "dateparution",
]

_OUTPUT_COLUMNS = [
    "siren",
    "greffe",
    "date_publication",
]


def _keep_latest_couple(dfs: list[pd.DataFrame]) -> pd.DataFrame:
    """Ne conserver que la parution la plus récente par couple (siren, greffe)."""
    df = pd.concat(dfs, ignore_index=True)
    return df.groupby(["siren", "greffe"], as_index=False)["date_publication"].max()


def _process_annonces_chunk(chunk: pd.DataFrame) -> pd.DataFrame:
    chunk = extract_sirens_greffes_from_listepersonnes(chunk)
    # La colonne greffe est obligatoire autrement l'avis ne sera pas utile
    # pour discriminer une radiation
    chunk = chunk[chunk["greffe"].notna() & chunk["siren"].notna()]
    if chunk.empty:
        return chunk
    chunk["date_publication"] = chunk["dateparution"]
    return chunk[_OUTPUT_COLUMNS]


def process_annonces_couples(raw_file_path: str, chunk_size: int) -> pd.DataFrame:
    """
    Réduit le flux d'annonces à la dernière parution par couple (siren, greffe).
    Cette agrégation suffit à décider si une radiation est un transfert ou non.
    """
    logger.info("Processing annonces couples...")
    couples: list[pd.DataFrame] = []
    n_rows = 0

    reader = pd.read_csv(
        raw_file_path,
        dtype=str,
        sep=";",
        usecols=_INPUT_COLUMNS,
        chunksize=chunk_size,
    )
    # Pour chaque chunk on ne garde que le couple (siren, greffe) le plus récent
    # en réintégrant à chaque fois les couples déjà trouvés à partir des chunks précédents
    for _, chunk in enumerate(reader, start=1):
        n_rows += len(chunk)
        chunk_couples = _process_annonces_chunk(chunk)
        if not chunk_couples.empty:
            couples.append(chunk_couples)
        if len(couples) > 1:
            couples = [_keep_latest_couple(couples)]
            logger.info(f"{n_rows} annonces lues, {len(couples[0])} couples retenus")

    latest_couples = _keep_latest_couple(couples)
    latest_couples["date_publication"] = pd.to_datetime(
        latest_couples["date_publication"], errors="coerce", format="%Y-%m-%d"
    )
    logger.info(f"Annonces: {n_rows} lignes réduites à {len(latest_couples)} couples")
    return latest_couples[_OUTPUT_COLUMNS]
