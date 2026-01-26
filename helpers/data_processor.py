import logging
import os
from abc import ABC
from datetime import datetime

import requests
from airflow.sdk import get_current_context
from dateutil.relativedelta import relativedelta

from data_pipelines_annuaire.config import DataSourceConfig
from data_pipelines_annuaire.helpers.data_quality import validate_file
from data_pipelines_annuaire.helpers.datagouv import (
    fetch_last_modified_date,
    fetch_last_resource_from_dataset,
)
from data_pipelines_annuaire.helpers.notification import Notification, monitoring_logger
from data_pipelines_annuaire.helpers.object_storage import File, ObjectStorageClient
from data_pipelines_annuaire.helpers.utils import (
    download_file,
    fetch_hyperlink_from_page,
    get_date_last_modified,
    save_to_metadata,
)


class DataProcessor(ABC):
    """Abstract base class for processing data.

    This class provides methods for preprocessing data, saving metadata,
    sending files to the object storage, comparing files in the object storage, and sending notifications.
    """

    def __init__(self, config: DataSourceConfig) -> None:
        self.config = config
        self.object_storage_client = ObjectStorageClient()

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
        ti = get_current_context()["ti"]

        for name, params in self.config.files_to_download.items():
            try:
                if "dataset_id" in params:
                    _, file_url = fetch_last_resource_from_dataset(params["url"])
                elif "pattern" in params:
                    url = params["url"]

                    # In case a placeholder is found in the URL, replace it with the corresponding value.
                    # For placeholders with fallback logic, this will be handled only if the first option
                    # returns no result.
                    placeholders = {
                        "%%current_year%%": str(datetime.now().year),
                        "%%current_month%%": str(datetime.now().strftime("%Y-%m")),
                        "%%current_day%%": str(datetime.now().strftime("%Y-%m-%d")),
                        "%%current_or_previous_year%%": str(datetime.now().year),
                        "%%current_or_previous_month%%": str(
                            datetime.now().strftime("%Y-%m")
                        ),
                        "%%current_or_previous_day%%": str(
                            datetime.now().strftime("%Y-%m-%d")
                        ),
                    }
                    search_text = params["pattern"]
                    for key, value in placeholders.items():
                        search_text = search_text.replace(key, value)

                    try:
                        file_url = fetch_hyperlink_from_page(url, search_text)
                    except ValueError as e:
                        logging.info(
                            "Failed to find the URL. Looking for fallback options.."
                        )
                        # First try did not succeed to find the URL
                        # so trying a second time with fallback options.
                        placeholders.update(
                            {
                                "%%current_or_previous_year%%": str(
                                    datetime.now().year - 1
                                ),
                                "%%current_or_previous_month%%": str(
                                    (datetime.now() - relativedelta(months=1)).strftime(
                                        "%Y-%m"
                                    )
                                ),
                                "%%current_or_previous_day%%": str(
                                    (datetime.now() - relativedelta(days=1)).strftime(
                                        "%Y-%m-%d"
                                    )
                                ),
                            }
                        )
                        search_text = params["pattern"]
                        for key, value in placeholders.items():
                            search_text = search_text.replace(key, value)

                        try:
                            file_url = fetch_hyperlink_from_page(url, search_text)
                            # Add information message to the Mattermost notification
                            # that only the previous file was found.
                            ti.xcom_push(
                                key=Notification.notification_xcom_key,
                                value=f"Only previous file found: {search_text}",
                            )
                        except ValueError as new_e:
                            raise new_e from e

                else:
                    file_url = params["url"]

                download_file(file_url, params["destination"])

                try:
                    # Validate if the downloaded file contains at least 2 lines
                    validate_file(
                        params["destination"],
                        csv_encoding=params.get("encoding", "utf-8"),
                    )
                except ValueError as e:
                    error_message = f"File validation failed for {name}: {str(e)}"
                    ti.xcom_push(
                        key=Notification.notification_xcom_key, value=error_message
                    )
                    logging.error(error_message)
                    raise

            except requests.exceptions.RequestException:
                error_message = f"Failed to download {name} from {params['url']}"
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

        logging.info(f"Available metadata dates: {date_list}")

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
            logging.info(
                "No resource_id nor URL provided in the configuration: last modified date defaults to now."
            )

        save_to_metadata(metadata_path, "last_modified", date_last_modified)
        logging.info(
            f"Last modified date ({date_last_modified}) saved successfully to {metadata_path}"
        )

    def send_file_to_object_storage(self):
        """
        Sends the CSV file and metadata JSON to the specified object storage path.
        """
        self.object_storage_client.send_files(
            list_files=[
                File(
                    source_path=f"{self.config.tmp_folder}/",
                    source_name=f"{self.config.file_name}.csv",
                    dest_path=f"{self.config.object_storage_path}/new/",
                    dest_name=f"{self.config.file_name}.csv",
                    content_type=None,
                ),
                File(
                    source_path=f"{self.config.tmp_folder}/",
                    source_name="metadata.json",
                    dest_path=f"{self.config.object_storage_path}/new/",
                    dest_name="metadata.json",
                    content_type=None,
                ),
            ],
        )

    def compare_files_object_storage(self):
        """Compares files in object storage.

        Checks if the current file is the same as the latest file in object storage.
        If not, it sends the current file to the latest path.

        Returns:
            bool: True if the files are different, False if they are the same.
        """
        is_same = self.object_storage_client.compare_files(
            file_path_1=f"{self.config.object_storage_path}/new/",
            file_name_2=f"{self.config.file_name}.csv",
            file_path_2=f"{self.config.object_storage_path}/latest/",
            file_name_1=f"{self.config.file_name}.csv",
        )
        if not is_same:
            self.object_storage_client.send_files(
                list_files=[
                    File(
                        source_path=f"{self.config.tmp_folder}/",
                        source_name=f"{self.config.file_name}.csv",
                        dest_path=f"{self.config.object_storage_path}/latest/",
                        dest_name=f"{self.config.file_name}.csv",
                        content_type=None,
                    ),
                    File(
                        source_path=f"{self.config.tmp_folder}/",
                        source_name="metadata.json",
                        dest_path=f"{self.config.object_storage_path}/latest/",
                        dest_name="metadata.json",
                        content_type=None,
                    ),
                ],
            )
        return not is_same
