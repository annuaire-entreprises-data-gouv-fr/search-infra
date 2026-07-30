import logging

from airflow.sdk import task

from data_pipelines_annuaire.config import (
    OBJECT_STORAGE_DATA_SOURCE_UPDATE_DATES_FILE,
)
from data_pipelines_annuaire.helpers.object_storage import ObjectStorageClient

logger = logging.getLogger(__name__)


@task
def sync_data_source_updates_file():
    logger.info("Copying data sources' last update JSON file!!!")

    ObjectStorageClient().copy_file(
        f"metadata/updates/new/{OBJECT_STORAGE_DATA_SOURCE_UPDATE_DATES_FILE}",
        f"metadata/updates/latest/{OBJECT_STORAGE_DATA_SOURCE_UPDATE_DATES_FILE}",
    )
