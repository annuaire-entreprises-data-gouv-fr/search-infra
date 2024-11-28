from abc import ABC, abstractmethod
from airflow.operators.python import get_current_context
import logging
import requests
from dag_datalake_sirene.helpers.minio_helpers import minio_client, File
from dag_datalake_sirene.helpers.tchap import send_message
from dag_datalake_sirene.helpers.utils import fetch_and_store_last_modified_metadata
from dag_datalake_sirene.config import DataSourceConfig


class DataProcessor(ABC):
    """Abstract base class for processing data.

    This class provides methods for preprocessing data, saving metadata,
    sending files to MinIO, comparing files in MinIO, and sending notifications.
    """

    def __init__(self, config: DataSourceConfig):
        self.config = config
        self.minio_client = minio_client

    def download_data(self, destination):
        """
        Downloads data from the specified URL and saves it to the destination path.

        Args:
            url (str): The URL to download data from.
            destination (str): The file path where the downloaded data will be saved.
        """
        try:
            r = requests.get(self.config.url)
            r.raise_for_status()
            with open(destination, "wb") as f:
                for chunk in r.iter_content(1024):
                    f.write(chunk)
            logging.info(
                f"Data downloaded successfully from {self.config.url} to {destination}."
            )
        except requests.exceptions.RequestException as e:
            logging.error(f"Error downloading data from {self.config.url}: {e}")

    @abstractmethod
    def preprocess_data(self):
        """
        This method must be implemented by subclasses.
        """
        pass

    def _push_unique_count(self, df, column_name, xcom_key):
        """
        Counts unique values in the specified column and pushes the count to XCom.

        Args:
            df (pd.DataFrame): The DataFrame to analyze.
            column_name (str): The name of the column to count unique values.
            xcom_key (str): The key to use for pushing the count to XCom.
        """
        unique_count = df[column_name].nunique()
        ti = get_current_context()["ti"]
        ti.xcom_push(key=xcom_key, value=str(unique_count))
        logging.info(
            f"Processed {unique_count} unique values in column '{column_name}'."
        )

    def save_date_last_modified(self):
        if self.config.resource_id:
            fetch_and_store_last_modified_metadata(
                self.config.resource_id, self.config.tmp_folder
            )
        else:
            logging.warning("No resource_id provided for last modified date.")

    def send_file_to_minio(self):
        """
        Sends the CSV file and metadata JSON to the specified MinIO path.
        """
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
        """Compares files in MinIO.

        Checks if the current file is the same as the latest file in MinIO.
        If not, it sends the current file to the latest path.

        Returns:
            bool: True if the files are different, False if they are the same.
        """
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
