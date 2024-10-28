import os
import gzip
import shutil
import logging
from typing import Optional
from minio.error import S3Error


class StorageManager:
    def __init__(self, minio_client, tmp_folder: str, minio_data_path: str):
        self.minio_client = minio_client
        self.tmp_folder = tmp_folder
        self.minio_data_path = minio_data_path

    def get_database_path(self) -> str:
        return os.path.join(self.tmp_folder, "demarches_simplifiees.db")

    def get_compressed_database_path(self) -> str:
        return f"{self.get_database_path()}.gz"

    def fetch_latest_database(self) -> Optional[str]:
        """Fetches the latest database from Minio and returns the
        path to the uncompressed database."""
        try:
            # Download compressed database
            self.minio_client.get_files(
                list_files=[
                    {
                        "source_path": self.minio_data_path,
                        "source_name": "demarches_simplifiees.db.gz",
                        "dest_path": self.tmp_folder,
                        "dest_name": "demarches_simplifiees.db.gz",
                    }
                ]
            )

            # Decompress database
            db_path = self.get_database_path()
            with gzip.open(self.get_compressed_database_path(), "rb") as f_in:
                with open(db_path, "wb") as f_out:
                    shutil.copyfileobj(f_in, f_out)

            # Clean up compressed file
            os.remove(self.get_compressed_database_path())

            return db_path

        except S3Error as err:
            if err.code == "NoSuchKey":
                logging.warning(f"No database found in MinIO: {err}")
                return None
            raise

    def save_to_minio(self, db_path: str):
        """Compresses and uploads the database to Minio."""
        compressed_path = self.get_compressed_database_path()

        # Compress database
        with open(db_path, "rb") as f_in:
            with gzip.open(compressed_path, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)

        # Upload to Minio
        self.minio_client.send_files(
            [
                {
                    "source_path": self.tmp_folder,
                    "source_name": "demarches_simplifiees.db.gz",
                    "dest_path": self.minio_data_path,
                    "dest_name": "demarches_simplifiees.db.gz",
                }
            ]
        )

        # Clean up local files
        os.remove(db_path)
        os.remove(compressed_path)
