import boto3
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError
from datetime import datetime
import logging
from typing import List, TypedDict, Optional
import os
from dag_datalake_sirene.config import (
    AIRFLOW_ENV,
    S3_REGION,
    S3_BUCKET,
    S3_URL,
    S3_ACCESS_KEY,
    S3_SECRET_KEY,
)


class File(TypedDict):
    source_path: str
    source_name: str
    dest_path: str
    dest_name: str
    content_type: Optional[str]


class S3Client:
    def __init__(self):
        self.bucket = S3_BUCKET
        self.client = boto3.client(
            "s3",
            aws_access_key_id=S3_ACCESS_KEY,
            aws_secret_access_key=S3_SECRET_KEY,
            endpoint_url=S3_URL,
            region_name=S3_REGION,
        )
        self.transfer_config = TransferConfig(multipart_threshold=4 * (1024**3))

        try:
            self.bucket_exists = (
                self.client.head_bucket(Bucket=self.bucket)["ResponseMetadata"][
                    "HTTPStatusCode"
                ]
                == 200
            )
        except ClientError as e:
            logging.error(e)
            self.bucket_exists = False

        if not self.bucket_exists:
            raise ValueError(f"Bucket '{self.bucket}' does not exist.")

    def get_root_dirpath(self) -> str:
        return f"ae/{AIRFLOW_ENV}"

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
                self.client.upload_file(
                    os.path.join(file["source_path"], file["source_name"]),
                    self.bucket,
                    f"ae/{AIRFLOW_ENV}/{file['dest_path']}{file['dest_name']}",
                    ExtraArgs={
                        "ContentType": (
                            file["content_type"] if "content_type" in file else None
                        )
                    },
                    Config=self.transfer_config,
                )
            else:
                raise Exception(
                    f"file {file['source_path']}{file['source_name']} " "does not exist"
                )

    def get_files_from_prefix(self, prefix: str) -> List[str]:
        """Retrieve only the list of files in a Minio pattern

        Args:
            prefix: (str): prefix to search files
        """
        filenames = [
            file["Key"]
            for page in self.client.get_paginator("list_objects_v2").paginate(
                Bucket=self.bucket, Prefix=f"ae/{AIRFLOW_ENV}/{prefix}"
            )
            for file in (page["Contents"] if "Contents" in page else [])
        ]

        for filename in filenames:
            logging.info(filename)

        filenames = [
            filename.replace(f"ae/{AIRFLOW_ENV}/", "") for filename in filenames
        ]

        return filenames

    def get_files(self, list_files: List[File]):
        """Retrieve list of files from Minio

        Args:
            list_files (List[File]): List of Dictionnaries containing for each
            `source_path` and `source_name` : Minio location inside specified bucket ;
            `dest_path` and `dest_name` : local file destination ;
        """
        for file in list_files:
            self.client.download_file(
                Bucket=self.bucket,
                Key=f"ae/{AIRFLOW_ENV}/{file['source_path']}{file['source_name']}",
                Filename=f"{file['dest_path']}{file['dest_name']}",
                Config=self.transfer_config,
            )

    def get_object_minio(
        self,
        s3_path: str,
        filename: str,
        local_path: str,
    ) -> None:
        self.client.download_file(
            Bucket=self.bucket,
            Key=f"{s3_path}{filename}",
            Filename=local_path,
            Config=self.transfer_config,
        )

    def put_object_minio(
        self,
        filename: str,
        minio_path: str,
        local_path: str,
        content_type: str = "application/octet-stream",
    ) -> None:
        self.client.upload_file(
            local_path + filename,
            self.bucket,
            minio_path,
            ExtraArgs={"ContentType": content_type},
            Config=self.transfer_config,
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

        db_files = [
            file["Key"]
            for page in self.client.get_paginator("list_objects_v2").paginate(
                Bucket=self.bucket, Prefix=minio_path
            )
            for file in (page["Contents"] if "Contents" in page else [])
            if file["Key"].endswith(".gz")
        ]

        sorted_db_files = sorted(
            db_files,
            key=lambda x: datetime.strptime(x.split("_")[-1].split(".")[0], "%Y-%m-%d"),
            reverse=True,
        )

        if sorted_db_files:
            latest_db_file = sorted_db_files[0]
            logging.info(f"Latest dirigeants database: {latest_db_file}")

            self.client.download_file(
                Bucket=self.bucket,
                Key=latest_db_file,
                Filename=local_path,
                Config=self.transfer_config,
            )
            print("downloaded")
        else:
            logging.warning("No .gz files found in the specified path.")

    def delete_file(self, file_path: str):
        """/!\ USE WITH CAUTION"""
        try:
            self.client.head_object(Bucket=self.bucket, Key=file_path)
            self.client.delete_object(Bucket=self.bucket, Key=file_path)
            logging.info(f"File '{file_path}' deleted successfully.")
        except ClientError as e:
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

        except ClientError as e:
            logging.error("Error loading files:", e)
            return None


s3_client = S3Client()
