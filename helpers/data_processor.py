from abc import ABC, abstractmethod
import logging
from dag_datalake_sirene.helpers.minio_helpers import minio_client, File
from dag_datalake_sirene.helpers.tchap import send_message
from dag_datalake_sirene.helpers.utils import fetch_and_store_last_modified_metadata
from dag_datalake_sirene.config import DataSourceConfig


class DataProcessor(ABC):
    def __init__(self, config: DataSourceConfig):
        self.config = config
        self.minio_client = minio_client

    @abstractmethod
    def preprocess_data(self):
        pass

    def save_date_last_modified(self):
        if self.config.resource_id:
            fetch_and_store_last_modified_metadata(
                self.config.resource_id, self.config.tmp_folder
            )
        else:
            logging.warning("No resource_id provided for last modified date.")

    def send_file_to_minio(self):
        self.minio_client.send_files(
            list_files=[
                File(
                    source_path=f"{self.config.tmp_folder}/",
                    source_name=f"{self.config.file_name}.csv",
                    dest_path=f"{self.config.minio_path}/new/",
                    dest_name=f"{self.config.file_name}.csv",
                ),
                File(
                    source_path=f"{self.config.tmp_folder}/",
                    source_name="metadata.json",
                    dest_path=f"{self.config.minio_path}/new/",
                    dest_name="metadata.json",
                ),
            ],
        )

    def compare_files_minio(self):
        is_same = self.minio_client.compare_files(
            file_path_1=f"{self.config.minio_path}/new/",
            file_name_2=f"{self.config.file_name}.csv",
            file_path_2=f"{self.config.minio_path}/latest/",
            file_name_1=f"{self.config.file_name}.csv",
        )
        if not is_same:
            self.minio_client.send_files(
                list_files=[
                    File(
                        source_path=f"{self.config.tmp_folder}/",
                        source_name=f"{self.config.file_name}.csv",
                        dest_path=f"{self.config.minio_path}/latest/",
                        dest_name=f"{self.config.file_name}.csv",
                    ),
                    File(
                        source_path=f"{self.config.tmp_folder}/",
                        source_name="metadata.json",
                        dest_path=f"{self.config.minio_path}/latest/",
                        dest_name="metadata.json",
                    ),
                ],
            )
        return not is_same

    def send_notification(self, message):
        send_message(message)
