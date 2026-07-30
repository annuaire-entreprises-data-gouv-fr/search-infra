import logging
import shutil
from collections.abc import Iterable
from pathlib import Path

import pandas as pd
import requests

from data_pipelines_annuaire.config import AIRFLOW_ETL_DATA_DIR, CURRENT_MONTH
from data_pipelines_annuaire.workflows.data_pipelines.etl.task_functions.determine_sirene_date import (
    get_sirene_processing_month,
)
from data_pipelines_annuaire.workflows.data_pipelines.sirene.stock.config import (
    STOCK_SIRENE_CONFIG,
)

logger = logging.getLogger(__name__)


def download_succession():
    year_month = get_sirene_processing_month()

    filename_template = STOCK_SIRENE_CONFIG.files_to_download["liens_succession"][
        "destination"
    ].split("/")[-1]
    filename = filename_template.replace(CURRENT_MONTH, year_month)

    url = STOCK_SIRENE_CONFIG.url_object_storage + filename

    data_path = Path(AIRFLOW_ETL_DATA_DIR)
    zip_path = data_path / "StockEtablissementLiensSuccession_utf8.zip"

    logger.info(f"Downloading and unpacking {url}")

    with requests.get(url, allow_redirects=True, stream=True) as r:
        r.raise_for_status()

        with open(zip_path, "wb") as f_out:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f_out.write(chunk)

    shutil.unpack_archive(zip_path, data_path)

    csv_path = data_path / "StockEtablissementLiensSuccession_utf8.csv"

    df_iterator = pd.read_csv(
        csv_path,
        chunksize=100000,
        dtype=str,
    )

    return df_iterator


def preprocess_succession_df() -> Iterable[pd.DataFrame]:
    df_iterator = download_succession()

    for _, df_liens in enumerate(df_iterator):
        df_liens = df_liens.rename(
            columns={
                "siretEtablissementPredecesseur": "siret_predecesseur",
                "siretEtablissementSuccesseur": "siret_successeur",
                "dateLienSuccession": "date_lien_succession",
                "transfertSiege": "transfert_siege",
                "continuiteEconomique": "continuite_economique",
            }
        )
        yield df_liens[
            [
                "siret_predecesseur",
                "siret_successeur",
                "date_lien_succession",
                "transfert_siege",
                "continuite_economique",
            ]
        ]
