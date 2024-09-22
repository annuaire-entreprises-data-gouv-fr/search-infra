import boto3
import botocore
from datetime import datetime
import logging
from minio import Minio, S3Error
from minio.commonconfig import CopySource
from typing import List, TypedDict, Optional
import os
from helpers.settings import Settings

class File(TypedDict):
    source_path: str
    source_name: str
    dest_path: str
    dest_name: str
    content_type: Optional[str]


class MinIOClient:
    def __init__(self):
        self.url = Settings.MINIO_URL
        self.user = Settings.MINIO_USER
        self.password = Settings.MINIO_PASSWORD
        self.bucket = Settings.MINIO_BUCKET
        self.client = Minio(
            self.url if not self.url.startswith("http") or self.url.startswith("https") else self.url.replace("http://", "").replace("https://", ""),
            access_key=self.user,
            secret_key=self.password,
            secure=False,
        )
        self.bucket_exists = self.client.bucket_exists(self.bucket)
        if not self.bucket_exists:
            raise ValueError(f"Bucket '{self.bucket}' does not exist.")

    def get_root_dirpath(self) -> str:
        return f"ae/{Settings.AIRFLOW_ENV}"

    def send_files(
        self,
        list_files: List[File],
    ):
        """Send list of file to Minio bucket

        Args:
            list_files (List[File]): List of Dictionnaries containing for each
            `source_path` and `source_name` : local file location ;
            `dest_path` and `dest_name` : minio location (inside bucket specified) ;

        Raises:
            Exception: when specified local file does not exists
            Exception: when specified bucket does not exist
        """
        for file in list_files:
            is_file = os.path.isfile(
                os.path.join(file["source_path"], file["source_name"])
            )
            logging.info("Sending ", file["source_name"])
            if is_file:
                self.client.fput_object(
                    self.bucket,
                    f"ae/{Settings.AIRFLOW_ENV}/{file['dest_path']}{file['dest_name']}",
                    os.path.join(file["source_path"], file["source_name"]),
                    content_type=(
                        file["content_type"] if "content_type" in file else None
                    ),
                )
            else:
                raise Exception(
                    f"file {file['source_path']}{file['source_name']} " "does not exist"
                )

    def get_files_from_prefix(self, prefix: str):
        """Retrieve only the list of files in a Minio pattern

        Args:
            prefix: (str): prefix to search files
        """
        list_objects = []
        objects = self.client.list_objects(
            self.bucket, prefix=f"ae/{Settings.AIRFLOW_ENV}/{prefix}"
        )
        for obj in objects:
            if not obj.object_name.endswith("/"):  # Exclude folders
                logging.info(obj.object_name)
                list_objects.append(obj.object_name.replace(f"ae/{Settings.AIRFLOW_ENV}/", ""))
        return list_objects

    def get_files(self, list_files: List[File]):
        """Retrieve list of files from Minio

        Args:
            list_files (List[File]): List of Dictionnaries containing for each
            `source_path` and `source_name` : Minio location inside specified bucket ;
            `dest_path` and `dest_name` : local file destination ;
        """
        for file in list_files:
            self.client.fget_object(
                self.bucket,
                f"ae/{Settings.AIRFLOW_ENV}/{file['source_path']}{file['source_name']}",
                f"{file['dest_path']}{file['dest_name']}",
            )

    def get_object_minio(
        self,
        minio_path: str,
        filename: str,
        local_path: str,
    ) -> None:
        self.client.fget_object(
            self.bucket,
            f"{minio_path}{filename}",
            local_path,
        )

    def put_object_minio(
        self,
        filename: str,
        minio_path: str,
        local_path: str,
        content_type: str = "application/octet-stream",
    ) -> None:
        self.client.fput_object(
            bucket_name=self.bucket,
            object_name=minio_path,
            file_path=local_path + filename,
            content_type=content_type,
        )

    def get_latest_file_minio(
        self,
        minio_path: str,
        local_path: str,
    ) -> None:
        """
        Download the latest .db.gz file from Minio to the local file system.

        Args:
            minio_path (str): The path within the Minio bucket where compressed .db
            files are located.
            local_path (str): The local directory where the downloaded file
            will be saved.
        Note:
            This function assumes that the .db files in Minio have filenames
            in the format:
            'filename_YYYY-MM-DD.db', where YYYY-MM-DD represents the date.
        """

        objects = self.client.list_objects(
            self.bucket, prefix=minio_path, recursive=True
        )

        # Filter and sort .gz files based on their names and the date in the filename
        db_files = [
            obj.object_name for obj in objects if obj.object_name.endswith(".gz")
        ]

        sorted_db_files = sorted(
            db_files,
            key=lambda x: datetime.strptime(x.split("_")[-1].split(".")[0], "%Y-%m-%d"),
            reverse=True,
        )

        if sorted_db_files:
            latest_db_file = sorted_db_files[0]
            logging.info(f"Latest dirigeants database: {latest_db_file}")

            self.client.fget_object(
                self.bucket,
                latest_db_file,
                local_path,
            )
        else:
            logging.warning("No .gz files found in the specified path.")

    def delete_file(self, file_path: str):
        """/!\ USE WITH CAUTION"""
        try:
            self.client.stat_object(self.bucket, file_path)
            self.client.remove_object(self.bucket, f"{file_path}")
            logging.info(f"File '{file_path}' deleted successfully.")
        except S3Error as e:
            logging.error(e)

    def get_files_and_last_modified(self, prefix: str):
        """
        Get a list of files and their last modified timestamps from MinIO.

        Args:
            prefix (str): Prefix of the files to retrieve.

        Returns:
            list: List of tuples containing file name and last modified timestamp.
        """
        objects = self.client.list_objects(self.bucket, prefix=prefix, recursive=True)
        file_info_list = []

        for obj in objects:
            file_name = obj.object_name
            last_modified = obj.last_modified
            file_info_list.append((file_name, last_modified))
        logging.info(f"*****List of files: {file_info_list}")
        return file_info_list

    def compare_files(
        self,
        file_path_1: str,
        file_path_2: str,
        file_name_1: str,
        file_name_2: str,
    ):
        """Compare two minio files

        Args:
            both path and name from files to compare

        """
        if self.bucket is None:
            raise AttributeError("A bucket has to be specified.")
        s3 = boto3.client(
            "s3",
            endpoint_url=self.url,
            aws_access_key_id=self.user,
            aws_secret_access_key=self.password,
        )

        try:
            logging.info(f"ae/{Settings.AIRFLOW_ENV}/{file_path_1}{file_name_1}")
            logging.info(f"ae/{Settings.AIRFLOW_ENV}/{file_path_2}{file_name_2}")
            file_1 = s3.head_object(
                    Bucket=self.bucket, Key=f"ae/{Settings.AIRFLOW_ENV}/{file_path_1}{file_name_1}"
            )
            file_2 = s3.head_object(
                Bucket=self.bucket, Key=f"ae/{Settings.AIRFLOW_ENV}/{file_path_2}{file_name_2}"
            )
            logging.info(f"Hash file 1 : {file_1['ETag']}")
            logging.info(f"Hash file 2 : {file_2['ETag']}")
            logging.info(bool(file_1["ETag"] == file_2["ETag"]))

            return bool(file_1["ETag"] == file_2["ETag"])

        except botocore.exceptions.ClientError as e:
            logging.error("Error loading files:", e)
            return None

    def rename_folder(self, old_folder_suffix: str, new_folder_suffix: str):
        """
        Rename a folder in MinIO under the default path 'ae/{env}/'.

        Args:
            old_folder_suffix (str): The folder suffix to rename (after 'ae/{env}/').
            new_folder_suffix (str): The new folder suffix (after 'ae/{env}/').
        """
        old_folder = f"ae/{Settings.AIRFLOW_ENV}/{old_folder_suffix}"
        new_folder = f"ae/{Settings.AIRFLOW_ENV}/{new_folder_suffix}"

        # Ensure folder suffixes end with '/'
        if not old_folder.endswith("/"):
            old_folder += "/"
        if not new_folder.endswith("/"):
            new_folder += "/"

        objects = self.client.list_objects(
            self.bucket, prefix=old_folder, recursive=True
        )

        for obj in objects:
            old_object_name = obj.object_name
            new_object_name = old_object_name.replace(old_folder, new_folder, 1)

            # Copy object to the new location using CopySource
            self.client.copy_object(
                self.bucket,
                new_object_name,
                CopySource(self.bucket, old_object_name),
            )
            logging.info(f"Copied {old_object_name} to {new_object_name}")

            # Delete the old object
            self.client.remove_object(self.bucket, old_object_name)
            logging.info(f"Deleted {old_object_name}")

        logging.info(f"Folder '{old_folder}' renamed to '{new_folder}'")


minio_client = MinIOClient()
