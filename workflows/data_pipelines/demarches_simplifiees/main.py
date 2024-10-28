import logging
from config import (
    DS_AUTH,
    DS_API_URL,
    DS_TMP_FOLDER,
    DS_MINIO_DATA_PATH,
    DEMARCHE_NUMBER,
    DS_INSTRUCTEUR_ID,
)
from client import (
    DemarcheSimplifieeClient,
)
from demarches_simplifiees import (
    repository,
)
from storage import (
    StorageManager,
)
from helpers.minio_helpers import minio_client


class DemarcheSimplifieeProcessor:
    def __init__(self):
        self.storage_manager = StorageManager(
            minio_client=minio_client,
            tmp_folder=DS_TMP_FOLDER,
            minio_data_path=DS_MINIO_DATA_PATH,
        )
        self.client = DemarcheSimplifieeClient(api_url=DS_API_URL, auth_token=DS_AUTH)

    def process(self):
        try:
            # Get or create database
            db_path = self.storage_manager.fetch_latest_database()
            if not db_path:
                db_path = self.storage_manager.get_database_path()

            # Initialize database
            repo = repository.DatabaseRepository(db_path)
            repo.initialize_database()

            # Fetch and save data
            demarche = self.client.get_demarche(DEMARCHE_NUMBER)
            repo.save_demarche(demarche)

            # Archive dossiers
            for dossier in demarche.dossiers:
                self.client.archive_dossier(
                    dossier_id=dossier.id, instructeur_id=DS_INSTRUCTEUR_ID
                )

            # Upload to Minio
            self.storage_manager.save_to_minio(db_path)

        except Exception as e:
            logging.error(f"Error processing demarche: {e}")
            raise


def run_processor():
    processor = DemarcheSimplifieeProcessor()
    processor.process()
