from dataclasses import dataclass
from datetime import datetime

from airflow.sdk import task

from data_pipelines_annuaire.config import (
    EXPORT_DATA_DIR,
    SIRENE_OBJECT_STORAGE_DATA_PATH,
)
from data_pipelines_annuaire.helpers import (
    DataProcessor,
    Notification,
    ObjectStorageClient,
    SqliteClient,
)

DATABASE_LOCATION = f"{EXPORT_DATA_DIR}sirene.db"
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
def export_file(export_file: ExportFile) -> None:
    """Génère un fichier CSV depuis la base SIRENE et l'envoie sur l'Object Storage."""
    csv_file = SqliteClient(DATABASE_LOCATION).to_csv(
        export_file.query, f"{EXPORT_DATA_DIR}{export_file.file_name}"
    )

    base_name = export_file.file_name.removesuffix(".csv")
    date_str = datetime.now().strftime("%Y-%m-%d")

    # The latest file will be overwritten each run
    # Removal of old files is handled in the object storage cleaning DAG
    for object_storage_filename in (
        f"{base_name}_{date_str}.csv",
        f"{base_name}_latest.csv",
    ):
        csv_file.upload_to_object_storage(
            object_storage_path=EXPORT_OBJECT_STORAGE_PATH,
            object_storage_filename=object_storage_filename,
            content_type="text/csv",
        )

    DataProcessor.push_message(
        Notification.notification_xcom_key,
        description=f"File {export_file.file_name} ready with {csv_file.lines_count()} lines.",
    )
