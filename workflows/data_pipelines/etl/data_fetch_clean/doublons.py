import logging
import shutil
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


def download_doublons():
    year_month = get_sirene_processing_month()

    filename_template = STOCK_SIRENE_CONFIG.files_to_download["doublons"][
        "destination"
    ].split("/")[-1]
    filename = filename_template.replace(CURRENT_MONTH, year_month)

    url = STOCK_SIRENE_CONFIG.url_object_storage + filename

    data_path = Path(AIRFLOW_ETL_DATA_DIR)
    zip_path = data_path / "StockDoublons_utf8.zip"

    logger.info(f"Downloading and unpacking {url}")

    with requests.get(url, allow_redirects=True, stream=True) as r:
        r.raise_for_status()

        with open(zip_path, "wb") as f_out:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f_out.write(chunk)

    shutil.unpack_archive(zip_path, data_path)

    return pd.read_csv(data_path / "StockDoublons_utf8.csv", dtype=str)
