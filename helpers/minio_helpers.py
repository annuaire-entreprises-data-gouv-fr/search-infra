from __future__ import annotations

import logging
import os
from datetime import datetime
from pathlib import Path
from typing import TypedDict

import boto3
import botocore
from minio import Minio, S3Error
from minio.commonconfig import CopySource

import dag_datalake_sirene.helpers.filesystem as filesystem
from dag_datalake_sirene.config import (
    AIRFLOW_ENV,
    MINIO_BUCKET,
    MINIO_PASSWORD,
    MINIO_URL,
    MINIO_USER,
)


# Use and enrich MinIOFile instead
class File(TypedDict):
    source_path: str
    source_name: str
    dest_path: str
    dest_name: str
    content_type: str | None


class MinIOClient:
    def __init__(self):
        self.url = MINIO_URL
        self.user = MINIO_USER
        self.password = MINIO_PASSWORD
        self.bucket = MINIO_BUCKET
        self.client = Minio(
            self.url,
            access_key=self.user,
            secret_key=self.password,
            secure=True,
        )
        self.bucket_exists = self.client.bucket_exists(self.bucket)
        if not self.bucket_exists:
            raise ValueError(f"Bucket '{self.bucket}' does not exist.")

    def get_root_dirpath(self) -> str:
        return f"ae/{AIRFLOW_ENV}"

    def send_files(
        self,
        list_files: list[File],
    ):
        """Send list of file to Minio bucket

        Args:
            list_files (list[File]): List of Dictionnaries containing for each
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
            logging.info(f"Sending {file['source_name']}")
            if is_file:
                self.client.fput_object(
                    self.bucket,
                    f"ae/{AIRFLOW_ENV}/{file['dest_path']}{file['dest_name']}",
                    os.path.join(file["source_path"], file["source_name"]),
                    content_type=(
                        file["content_type"] if "content_type" in file else None
                    ),
                )
            else:
                raise Exception(
                    f"file {file['source_path']}{file['source_name']} does not exist"
                )

    def get_files_from_prefix(self, prefix: str):
        """Retrieve only the list of files in a Minio pattern

        Args:
            prefix: (str): prefix to search files
        """
        list_objects = []
        objects = self.client.list_objects(
            self.bucket, prefix=f"ae/{AIRFLOW_ENV}/{prefix}"
        )
        for obj in objects:
            if not obj.object_name.endswith("/"):  # Exclude folders
                logging.info(obj.object_name)
                list_objects.append(obj.object_name.replace(f"ae/{AIRFLOW_ENV}/", ""))
        return list_objects

    def get_files(self, list_files: list[File]):
        """Retrieve list of files from Minio

        Args:
            list_files (list[File]): List of Dictionnaries containing for each
            `source_path` and `source_name` : Minio location inside specified bucket ;
            `dest_path` and `dest_name` : local file destination ;
        """
        for file in list_files:
            self.client.fget_object(
                self.bucket,
                f"ae/{AIRFLOW_ENV}/{file['source_path']}{file['source_name']}",
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

    def get_latest_file(
        self,
        minio_path: str,
        local_path: str,
    ):
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

            # Extract the date from the filename
            latest_file_date_str = latest_db_file.split("_")[-1].split(".")[0]
            latest_file_date = datetime.strptime(
                latest_file_date_str, "%Y-%m-%d"
            ).strftime("%Y-%m-%dT%H:%M:%S")
            logging.info(f"Date of latest file: {latest_file_date}")

            self.client.fget_object(
                self.bucket,
                latest_db_file,
                local_path,
            )
            return latest_file_date
        else:
            logging.warning("No .gz files found in the specified path.")

    def delete_file(self, file_path: str):
        """!! USE WITH CAUTION !!"""
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
            endpoint_url=f"https://{self.url}",
            aws_access_key_id=self.user,
            aws_secret_access_key=self.password,
        )

        try:
            logging.info(f"ae/{AIRFLOW_ENV}/{file_path_1}{file_name_1}")
            logging.info(f"ae/{AIRFLOW_ENV}/{file_path_2}{file_name_2}")
            file_1 = s3.head_object(
                Bucket=self.bucket, Key=f"ae/{AIRFLOW_ENV}/{file_path_1}{file_name_1}"
            )
            file_2 = s3.head_object(
                Bucket=self.bucket, Key=f"ae/{AIRFLOW_ENV}/{file_path_2}{file_name_2}"
            )
            logging.info(f"Hash file 1 : {file_1['ETag']}")
            logging.info(f"Hash file 2 : {file_2['ETag']}")
            logging.info(bool(file_1["ETag"] == file_2["ETag"]))

            return bool(file_1["ETag"] == file_2["ETag"])

        except botocore.exceptions.ClientError as e:
            logging.error(f"Error loading files: {e}")
            return None

    def rename_folder(self, old_folder_suffix: str, new_folder_suffix: str):
        """
        Rename a folder in MinIO under the default path 'ae/{env}/'.

        Args:
            old_folder_suffix (str): The folder suffix to rename (after 'ae/{env}/').
            new_folder_suffix (str): The new folder suffix (after 'ae/{env}/').
        """
        old_folder = f"ae/{AIRFLOW_ENV}/{old_folder_suffix}"
        new_folder = f"ae/{AIRFLOW_ENV}/{new_folder_suffix}"

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

    def get_date_last_modified(self, file_path: str) -> str | None:
        """
        Get the last modified date of a specific file in MinIO in ISO 8601 format.

        Args:
            file_path (str): The path of the file in MinIO.

        Returns:
            str: The last modified date of the file in ISO 8601 format,
            or None if an error occurs.
        """
        try:
            stat = self.client.stat_object(self.bucket, f"ae/{AIRFLOW_ENV}/{file_path}")
            # Format the datetime object to ISO 8601 format
            last_modified_str = stat.last_modified.strftime("%Y-%m-%dT%H:%M:%S")
            logging.info(f"Last modified date of '{file_path}': {last_modified_str}")
            return last_modified_str
        except S3Error as e:
            logging.error(f"Error retrieving file metadata for {file_path}: {e}")
            return None

    def copy_file(self, source_path: str, dest_path: str):
        """
        Copy a file from one MinIO path to another.

        Args:
            source_path (str): The MinIO path of the source file.
            dest_path (str): The MinIO path where the file should be copied.
        """
        try:
            # Define the full MinIO path with environment prefix
            source_full_path = f"ae/{AIRFLOW_ENV}/{source_path}"
            dest_full_path = f"ae/{AIRFLOW_ENV}/{dest_path}"

            # Copy object to the new location using CopySource
            self.client.copy_object(
                self.bucket,
                dest_full_path,
                CopySource(self.bucket, source_full_path),
            )
            logging.info(f"Copied {source_full_path} to {dest_full_path}")
        except S3Error as e:
            logging.error(f"Error copying file from {source_path} to {dest_path}: {e}")


class MinIOFile:
    def __init__(
        self,
        path: str,
    ) -> None:
        """
        Initialize a MinIOFile object.

        Args:
            path (str): The full path to file.

        Example:
            > remote_file = MinIOFile("/integration/content.csv")
            > local_file = file.download_to("/tmp/test.csv")
            > ...
            > remote_file.replace_with(local_file)
        """
        if not self.does_exist(path):
            raise FileNotFoundError(f"File '{path}' does not exist in MinIO.")

        self._path = Path(path)
        self.path = path
        self.filepath = str(self._path.parent) + "/"
        self.filename = self._path.name
        self.client = MinIOClient()

    @classmethod
    def does_exist(cls, path: str) -> bool:
        try:
            client = MinIOClient()
            client.client.stat_object(
                client.bucket,
                os.path.join(client.get_root_dirpath(), path),
            )
            return True
        except S3Error as _:
            return False

    def download_to(
        self,
        local_path: str,
    ) -> filesystem.LocalFile:
        if os.path.isdir(local_path):
            local_path = os.path.join(local_path, self.filename)
        self.client.get_object_minio(
            os.path.join(self.client.get_root_dirpath(), self.filepath),
            self.filename,
            local_path,
        )
        return filesystem.LocalFile(local_path)

    def replace_with(
        self,
        local_file: filesystem.LocalFile,
    ) -> None:
        self.client.send_files(
            [
                File(
                    source_path=str(local_file.path.parent),
                    source_name=local_file.path.name,
                    dest_path=self.filepath,
                    dest_name=self.filename,
                    content_type=None,
                )
            ]
        )
