from datetime import datetime

from airflow.sdk import task

from data_pipelines_annuaire.config import (
    SIRENE_OBJECT_STORAGE_DATA_PATH,
)
from data_pipelines_annuaire.helpers.object_storage import ObjectStorageClient


@task
def upload_db_to_object_storage(database_file_path: str) -> None:
    current_date = datetime.now().date()
    ObjectStorageClient().upload_compressed_file(
        source_file_path=database_file_path,
        object_storage_path=SIRENE_OBJECT_STORAGE_DATA_PATH,
        dest_name=f"sirene_{current_date}.db.gz",
        content_type="application/vnd.sqlite3",
    )
