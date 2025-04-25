import logging
import os
from abc import ABC
from datetime import datetime

import requests
from airflow.operators.python import get_current_context

from dag_datalake_sirene.config import DataSourceConfig
from dag_datalake_sirene.helpers.datagouv import (
    fetch_last_modified_date,
    fetch_last_resource_from_dataset,
)
from dag_datalake_sirene.helpers.minio_helpers import File, MinIOClient
from dag_datalake_sirene.helpers.notification import Notification, monitoring_logger
from dag_datalake_sirene.helpers.utils import (
    download_file,
    fetch_hyperlink_from_page,
    get_date_last_modified,
    save_to_metadata,
)


class DataProcessor(ABC):
    """Abstract base class for processing data.

    This class provides methods for preprocessing data, saving metadata,
    sending files to MinIO, comparing files in MinIO, and sending notifications.
    """

    def __init__(self, config: DataSourceConfig) -> None:
        self.config = config
        self.minio_client = MinIOClient()

    def download_data(self) -> None:
        """
        Downloads data from the specified URL and saves it to the destination path.

        Configurations:
            - self.config.files_to_download (dict): The dictionary of resources to download.
              Args of each file dictionary:
            - url (str): The URL of the file or dataset.
            - destination (str): The local path to save the file.
            - dataset_id (optional, str): Indicates the resource has to be fetched from the dataset.
            - pattern (optional, str): The pattern to search for in the URL page to find the actual file link.
        """
        for name, params in self.config.files_to_download.items():
            try:
                if "dataset_id" in params:
                    _, url = fetch_last_resource_from_dataset(params["url"])
                elif "pattern" in params:
                    url = params["url"]

                    placeholders = {
                        "%%current_year%%": str(datetime.now().year),
                        "%%current_month%%": str(datetime.now().strftime("%Y-%m")),
                        "%%current_day%%": str(datetime.now().strftime("%Y-%m-%d")),
                    }
                    search_text = params["pattern"]
                    for key, value in placeholders.items():
                        search_text = search_text.replace(key, value)

                    url = fetch_hyperlink_from_page(url, search_text)
                else:
                    url = params["url"]

                download_file(url, params["destination"])

            except (requests.exceptions.RequestException, ValueError):
                error_message = f"Failed to download {name} from {params['url']}"
                ti = get_current_context()["ti"]
                ti.xcom_push(
                    key=Notification.notification_xcom_key, value=error_message
                )
                logging.error(error_message)
                raise

    def preprocess_data(self):
        """
        This method must be implemented by subclasses.
        """
        pass

    @staticmethod
    def push_message(xcom_key, column=None, description: str = ""):
        """
        Pushes a message to XCom.
        If a column is provided, it sends the unique count of values in that column as a message.
        Otherwise, it sends the provided description directly.

        Args:
            xcom_key (str): XCom key to use.
            column (pd.Series | None, optional): The DataFrame column to count unique values from. Defaults to None.
            description (str, optional): Description to append next to the count or the message to send if no column is provided. Defaults to an empty string.

        Returns:
            None
        """
        if column is None and not description:
            raise ValueError(
                "DataProcessor.push_message() requires at least a Dataframe column or a non empty description as argument."
            )

        if column is not None:
            metric = column.nunique()
            description = column.name if description is None else description
            description = f"{metric} {description}"
            monitoring_logger(key=column.name, value=metric)

        ti = get_current_context()["ti"]
        ti.xcom_push(key=xcom_key, value=description)
        logging.info(f"Pushed notification: {description}")

    def save_date_last_modified(self) -> None:
        """Saves the last modified date for a resource or URL to a metadata file.

        Depending on the configuration, the method fetches and stores the last
        modified timestamp metadata from all data.gouv resources or URLs.
        If no last modified date is available the current datetime is used.

        Configurations:
            - self.config.files_to_download (list[dict], optional): The list
            of resources to download.
              Args of each file dictionary:
                - url (str): The URL of the file or dataset.
                - destination (str): The local path to save the file.
                - dataset_id (optional): Indicates the resource has to be
                fetched from the dataset.
            - `self.config.tmp_folder` (str): The path to the local tmp folder.

        Usage:
            my_processor = DataProcessor()
            @task
            def save_date_last_modified():
                return my_processor.save_date_last_modified()

        """
        date_list = []
        metadata_path = os.path.join(self.config.tmp_folder, "metadata.json")

        if self.config.files_to_download:
            for name, params in self.config.files_to_download.items():
                if "resource_id" in params:
                    date_list.append(fetch_last_modified_date(params["resource_id"]))
                elif "dataset_id" in params:
                    resource_id, _ = fetch_last_resource_from_dataset(params["url"])
                    date_list.append(fetch_last_modified_date(resource_id))
                elif "url" in params:
                    url_date = get_date_last_modified(url=params["url"])
                    if url_date:
                        date_list.append(url_date)
                else:
                    logging.warning(
                        f"Resource type not handled in {self.__class__.__name__}.save_date_last_modified():\n{name}:\n{params}"
                    )

        logging.info(date_list)

        if date_list:
            # Convert date strings to datetime objects and keep the most recent (Remove timezone offset)
            try:
                date_list_dt = [
                    datetime.fromisoformat(date.split("+")[0]) for date in date_list
                ]
            except ValueError as e:
                error_message = f"A date can't be parsed properly: {date_list}"
                ti = get_current_context()["ti"]
                ti.xcom_push(
                    key=Notification.notification_xcom_key, value=error_message
                )
                logging.warning(f"{error_message}\n{e}")
            date_last_modified = max(date_list_dt).strftime("%Y-%m-%dT%H:%M:%S.%f")
        else:
            date_last_modified = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f")
            logging.warning("No resource_id nor URL provided in the configuration.")

        save_to_metadata(metadata_path, "last_modified", date_last_modified)
        logging.info(
            f"Last modified date ({date_last_modified}) saved successfully to {metadata_path}"
        )

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
                    content_type=None,
                ),
                File(
                    source_path=f"{self.config.tmp_folder}/",
                    source_name="metadata.json",
                    dest_path=f"{self.config.minio_path}/new/",
                    dest_name="metadata.json",
                    content_type=None,
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
                        content_type=None,
                    ),
                    File(
                        source_path=f"{self.config.tmp_folder}/",
                        source_name="metadata.json",
                        dest_path=f"{self.config.minio_path}/latest/",
                        dest_name="metadata.json",
                        content_type=None,
                    ),
                ],
            )
        return not is_same
