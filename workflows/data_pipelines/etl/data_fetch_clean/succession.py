import logging
import shutil
from typing import Iterable

import pandas as pd
import requests

from data_pipelines_annuaire.config import AIRFLOW_ETL_DATA_DIR, CURRENT_MONTH
from data_pipelines_annuaire.workflows.data_pipelines.etl.task_functions.determine_sirene_date import (
    get_sirene_processing_month,
)
from data_pipelines_annuaire.workflows.data_pipelines.sirene.stock.config import (
    STOCK_SIRENE_CONFIG,
)


def download_succession():
    year_month = get_sirene_processing_month()
    filename_template = STOCK_SIRENE_CONFIG.files_to_download["liens_succession"][
        "destination"
    ].split("/")[-1]
    filename = filename_template.replace(CURRENT_MONTH, year_month)
    url = STOCK_SIRENE_CONFIG.url_object_storage + filename

    logging.info(f"Downloading and unpacking {url}")
    r = requests.get(url, allow_redirects=True)
    open(
        AIRFLOW_ETL_DATA_DIR + "StockEtablissementLiensSuccession_utf8.zip", "wb"
    ).write(r.content)
    shutil.unpack_archive(
        AIRFLOW_ETL_DATA_DIR + "StockEtablissementLiensSuccession_utf8.zip",
        AIRFLOW_ETL_DATA_DIR,
    )
    df_iterator = pd.read_csv(
        f"{AIRFLOW_ETL_DATA_DIR}StockEtablissementLiensSuccession_utf8.csv",
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
