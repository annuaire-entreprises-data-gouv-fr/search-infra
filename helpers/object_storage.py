import logging
import os
from datetime import datetime
from pathlib import Path
from typing import TypedDict

import boto3
from botocore.exceptions import ClientError

import data_pipelines_annuaire.helpers.filesystem as filesystem
from data_pipelines_annuaire.config import (
    AIRFLOW_ENV,
    OBJECT_STORAGE_ACCESS_KEY,
    OBJECT_STORAGE_BUCKET,
    OBJECT_STORAGE_SECRET_KEY,
    OBJECT_STORAGE_URL,
)


# Use and enrich ObjectStorageFile instead
class File(TypedDict):
    source_path: str
    source_name: str
    dest_path: str
    dest_name: str
    content_type: str | None


class ObjectStorageClient:
    def __init__(self):
        self.url = OBJECT_STORAGE_URL
        self.access_key = OBJECT_STORAGE_ACCESS_KEY
        self.secret_key = OBJECT_STORAGE_SECRET_KEY
        self.bucket = OBJECT_STORAGE_BUCKET

        # Create boto3 S3 client
        self.client = boto3.client(
            "s3",
            endpoint_url=self.url,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
        )

        # Check if bucket exists
        self.bucket_exists = self._check_bucket_exists()
        if not self.bucket_exists:
            raise ValueError(f"Bucket '{self.bucket}' does not exist.")

    def _check_bucket_exists(self) -> bool:
        """Check if the bucket exists using boto3"""
        try:
            self.client.head_bucket(Bucket=self.bucket)
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return False
            else:
                # Other errors (like access denied) should be raised
                raise

    def get_root_dirpath(self) -> str:
        return f"ae/{AIRFLOW_ENV}"

    def send_files(
        self,
        list_files: list[File],
        is_public: bool = True,
    ):
        """Send list of file to S3 bucket

        Args:
            list_files (list[File]): List of Dictionnaries containing for each
            `source_path` and `source_name` : local file location ;
            `dest_path` and `dest_name` : S3 location (inside bucket specified) ;

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
                object_key = f"ae/{AIRFLOW_ENV}/{file['dest_path']}{file['dest_name']}"
                local_file_path = os.path.join(file["source_path"], file["source_name"])

                extra_args = {}
                if is_public:
                    extra_args["ACL"] = "public-read"
                # Set extra args for content type if provided
                if "content_type" in file and file["content_type"]:
                    extra_args["ContentType"] = file["content_type"]

                self.client.upload_file(
                    local_file_path, self.bucket, object_key, ExtraArgs=extra_args
                )
            else:
                raise Exception(
                    f"file {file['source_path']}{file['source_name']} does not exist"
                )

    def get_files_from_prefix(self, prefix: str):
        """Retrieve only the list of files in a S3 pattern

        Args:
            prefix: (str): prefix to search files
        """
        list_objects = []

        # Use list_objects_v2 which is the recommended method
        paginator = self.client.get_paginator("list_objects_v2")
        page_iterator = paginator.paginate(
            Bucket=self.bucket, Prefix=f"ae/{AIRFLOW_ENV}/{prefix}"
        )

        for page in page_iterator:
            if "Contents" in page:
                for obj in page["Contents"]:
                    object_name = obj["Key"]
                    if not object_name.endswith("/"):  # Exclude folders
                        logging.info(object_name)
                        list_objects.append(
                            object_name.replace(f"ae/{AIRFLOW_ENV}/", "")
                        )
        return list_objects

    def get_files(self, list_files: list[File]):
        """Retrieve list of files from S3

        Args:
            list_files (list[File]): List of Dictionnaries containing for each
            `source_path` and `source_name` : S3 location inside specified bucket ;
            `dest_path` and `dest_name` : local file destination ;
        """
        for file in list_files:
            object_key = f"ae/{AIRFLOW_ENV}/{file['source_path']}{file['source_name']}"
            local_path = f"{file['dest_path']}{file['dest_name']}"

            # Ensure destination directory exists
            os.makedirs(os.path.dirname(local_path), exist_ok=True)

            self.client.download_file(self.bucket, object_key, local_path)

    def get_object_object_storage(
        self,
        object_storage_path: str,
        filename: str,
        local_path: str,
    ) -> None:
        object_key = f"{object_storage_path}{filename}"

        # Ensure destination directory exists
        os.makedirs(os.path.dirname(local_path), exist_ok=True)

        self.client.download_file(self.bucket, object_key, local_path)

    def put_object_object_storage(
        self,
        filename: str,
        object_storage_path: str,
        local_path: str,
        content_type: str = "application/octet-stream",
        is_public: bool = True,
    ) -> None:
        file_path = local_path + filename

        # Set extra args for content type
        extra_args = {
            "ContentType": content_type,
        }
        if is_public:
            extra_args["ACL"] = "public-read"

        self.client.upload_file(
            file_path, self.bucket, object_storage_path, ExtraArgs=extra_args
        )

    def get_latest_file(
        self,
        object_storage_path: str,
        local_path: str,
    ):
        """
        Download the latest .db.gz file from S3 to the local file system.

        Args:
            object_storage_path (str): The path within the S3 bucket where compressed .db
            files are located.
            local_path (str): The local directory where the downloaded file
            will be saved.
        Note:
            This function assumes that the .db files in S3 have filenames
            in the format:
            'filename_YYYY-MM-DD.db', where YYYY-MM-DD represents the date.
        """

        # Use list_objects_v2 which is the recommended method
        paginator = self.client.get_paginator("list_objects_v2")
        page_iterator = paginator.paginate(
            Bucket=self.bucket, Prefix=object_storage_path
        )

        # Filter and sort .gz files based on their names and the date in the filename
        db_files = []
        for page in page_iterator:
            if "Contents" in page:
                for obj in page["Contents"]:
                    if obj["Key"].endswith(".gz"):
                        db_files.append(obj["Key"])

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

            # Ensure destination directory exists
            os.makedirs(os.path.dirname(local_path), exist_ok=True)

            self.client.download_file(
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
            # Check if file exists first
            self.client.head_object(Bucket=self.bucket, Key=file_path)
            self.client.delete_object(Bucket=self.bucket, Key=file_path)
            logging.info(f"File '{file_path}' deleted successfully.")
        except ClientError as e:
            logging.error(e)

    def get_files_and_last_modified(self, prefix: str):
        """
        Get a list of files and their last modified timestamps from S3.

        Args:
            prefix (str): Prefix of the files to retrieve.

        Returns:
            list: List of tuples containing file name and last modified timestamp.
        """
        # Use list_objects_v2 which is the recommended method
        paginator = self.client.get_paginator("list_objects_v2")
        page_iterator = paginator.paginate(Bucket=self.bucket, Prefix=prefix)

        file_info_list = []

        for page in page_iterator:
            if "Contents" in page:
                for obj in page["Contents"]:
                    file_name = obj["Key"]
                    last_modified = obj["LastModified"]
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
        """Compare two S3 files

        Args:
            both path and name from files to compare

        """
        if self.bucket is None:
            raise AttributeError("A bucket has to be specified.")

        try:
            file_1_key = f"ae/{AIRFLOW_ENV}/{file_path_1}{file_name_1}"
            file_2_key = f"ae/{AIRFLOW_ENV}/{file_path_2}{file_name_2}"

            logging.info(file_1_key)
            logging.info(file_2_key)

            file_1_stat = self.client.head_object(Bucket=self.bucket, Key=file_1_key)
            file_2_stat = self.client.head_object(Bucket=self.bucket, Key=file_2_key)

            file_1_etag = file_1_stat["ETag"]
            file_2_etag = file_2_stat["ETag"]

            logging.info(f"Hash file 1 : {file_1_etag}")
            logging.info(f"Hash file 2 : {file_2_etag}")
            logging.info(bool(file_1_etag == file_2_etag))

            return bool(file_1_etag == file_2_etag)

        except ClientError as e:
            logging.error(f"Error loading files: {e}")
            return None

    def rename_folder(self, old_folder_suffix: str, new_folder_suffix: str):
        """
        Rename a folder in S3 under the default path 'ae/{env}/'.

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

        # Use list_objects_v2 which is the recommended method
        paginator = self.client.get_paginator("list_objects_v2")
        page_iterator = paginator.paginate(Bucket=self.bucket, Prefix=old_folder)

        for page in page_iterator:
            if "Contents" in page:
                for obj in page["Contents"]:
                    old_object_name = obj["Key"]
                    new_object_name = old_object_name.replace(old_folder, new_folder, 1)

                    # Copy object to the new location
                    copy_source = {"Bucket": self.bucket, "Key": old_object_name}
                    self.client.copy_object(
                        Bucket=self.bucket, Key=new_object_name, CopySource=copy_source
                    )
                    logging.info(f"Copied {old_object_name} to {new_object_name}")

                    # Delete the old object
                    self.client.delete_object(Bucket=self.bucket, Key=old_object_name)
                    logging.info(f"Deleted {old_object_name}")

        logging.info(f"Folder '{old_folder}' renamed to '{new_folder}'")

    def get_date_last_modified(self, file_path: str) -> str | None:
        """
        Get the last modified date of a specific file in S3 in ISO 8601 format.

        Args:
            file_path (str): The path of the file in S3.

        Returns:
            str: The last modified date of the file in ISO 8601 format,
            or None if an error occurs.
        """
        try:
            stat = self.client.head_object(
                Bucket=self.bucket, Key=f"ae/{AIRFLOW_ENV}/{file_path}"
            )
            # Format the datetime object to ISO 8601 format
            last_modified_str = stat["LastModified"].strftime("%Y-%m-%dT%H:%M:%S")
            logging.info(f"Last modified date of '{file_path}': {last_modified_str}")
            return last_modified_str
        except ClientError as e:
            logging.error(f"Error retrieving file metadata for {file_path}: {e}")
            return None

    def copy_file(self, source_path: str, dest_path: str):
        """
        Copy a file from one S3 path to another.

        Args:
            source_path (str): The S3 path of the source file.
            dest_path (str): The S3 path where the file should be copied.
        """
        try:
            # Define the full S3 path with environment prefix
            source_full_path = f"ae/{AIRFLOW_ENV}/{source_path}"
            dest_full_path = f"ae/{AIRFLOW_ENV}/{dest_path}"

            # Copy object to the new location
            copy_source = {"Bucket": self.bucket, "Key": source_full_path}
            self.client.copy_object(
                Bucket=self.bucket, Key=dest_full_path, CopySource=copy_source
            )
            logging.info(f"Copied {source_full_path} to {dest_full_path}")
        except ClientError as e:
            logging.error(f"Error copying file from {source_path} to {dest_path}: {e}")


class ObjectStorageFile:
    def __init__(
        self,
        path: str,
    ) -> None:
        """
        Initialize a ObjectStorageFile object.

        Args:
            path (str): The full path to file.

        Example:
            > remote_file = ObjectStorageFile("/integration/content.csv")
            > local_file = file.download_to("/tmp/test.csv")
            > ...
            > remote_file.replace_with(local_file)
        """
        if not self.does_exist(path):
            raise FileNotFoundError(f"File '{path}' does not exist in object storage.")

        self._path = Path(path)
        self.path = path
        self.filepath = str(self._path.parent) + "/"
        self.filename = self._path.name
        self.client = ObjectStorageClient()

    @classmethod
    def does_exist(cls, path: str) -> bool:
        try:
            client = ObjectStorageClient()
            client.client.head_object(
                Bucket=client.bucket,
                Key=os.path.join(client.get_root_dirpath(), path),
            )
            return True
        except ClientError as _:
            return False

    def download_to(
        self,
        local_path: str,
    ) -> filesystem.LocalFile:
        if os.path.isdir(local_path):
            local_path = os.path.join(local_path, self.filename)
        self.client.get_object_object_storage(
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
