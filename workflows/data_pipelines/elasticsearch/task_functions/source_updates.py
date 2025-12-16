import logging

from data_pipelines_annuaire.config import (
    MINIO_DATA_SOURCE_UPDATE_DATES_FILE,
)
from data_pipelines_annuaire.helpers.minio_helpers import MinIOClient


def sync_data_source_updates():
    logging.info("Copying data sources' last update JSON file!!!")

    MinIOClient().copy_file(
        f"metadata/updates/new/{MINIO_DATA_SOURCE_UPDATE_DATES_FILE}",
        f"metadata/updates/latest/{MINIO_DATA_SOURCE_UPDATE_DATES_FILE}",
    )
