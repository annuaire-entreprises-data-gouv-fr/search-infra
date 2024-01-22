from datetime import datetime
import logging
from minio import Minio, S3Error
from typing import List, TypedDict, Optional
import os
from dag_datalake_sirene.config import (
    AIRFLOW_ENV,
    MINIO_BUCKET,
    MINIO_URL,
    MINIO_USER,
    MINIO_PASSWORD,
)


class File(TypedDict):
    source_path: str
    source_name: str
    dest_path: str
    dest_name: str
    content_type: Optional[str]


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
                    f"ae/{AIRFLOW_ENV}/{file['dest_path']}{file['dest_name']}",
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
            self.bucket, prefix=f"ae/{AIRFLOW_ENV}/{prefix}"
        )
        for obj in objects:
            logging.info(obj.object_name)
            list_objects.append(obj.object_name.replace(f"ae/{AIRFLOW_ENV}/", ""))
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
    ) -> None:
        self.client.fput_object(
            bucket_name=self.bucket,
            object_name=minio_path,
            file_path=local_path + filename,
        )

    def get_latest_file_minio(
        self,
        minio_path: str,
        local_path: str,
    ) -> None:
        """
        Download the latest .db file from Minio to the local file system.

        Args:
            minio_path (str): The path within the Minio bucket where .db files
            are located.
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

        # Filter and sort .db files based on their names and the date in the filename
        db_files = [
            obj.object_name for obj in objects if obj.object_name.endswith(".db")
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
            logging.warning("No .db files found in the specified path.")

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


minio_client = MinIOClient()
