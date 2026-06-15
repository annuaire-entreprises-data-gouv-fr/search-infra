from dataclasses import dataclass

from airflow.sdk import task

from data_pipelines_annuaire.config import (
    SHARE_PUBLIC_DATASET_DATA_DIR,
    SIRENE_OBJECT_STORAGE_DATA_PATH,
)
from data_pipelines_annuaire.helpers import (
    DataProcessor,
    Notification,
    ObjectStorageClient,
    SqliteClient,
)

DATABASE_LOCATION = f"{SHARE_PUBLIC_DATASET_DATA_DIR}sirene.db"
EXPORT_OBJECT_STORAGE_PATH = "export/"


@dataclass(frozen=True)
class ExportFile:
    file_name: str
    query: str


@task
def get_latest_sirene_database():
    ObjectStorageClient().get_latest_database(
        SIRENE_OBJECT_STORAGE_DATA_PATH,
        DATABASE_LOCATION,
    )


@task
def share_file(export_file: ExportFile) -> None:
    """Génère un fichier CSV depuis la base SIRENE et l'envoie sur l'Object Storage."""
    csv_file = SqliteClient(DATABASE_LOCATION).to_csv(
        export_file.query, f"{SHARE_PUBLIC_DATASET_DATA_DIR}{export_file.file_name}"
    )

    csv_file.upload_to_object_storage(
        object_storage_path=EXPORT_OBJECT_STORAGE_PATH,
        content_type="text/csv",
    )

    DataProcessor.push_message(
        Notification.notification_xcom_key,
        description=f"File {export_file.file_name} ready with {csv_file.lines_count()} lines.",
    )
