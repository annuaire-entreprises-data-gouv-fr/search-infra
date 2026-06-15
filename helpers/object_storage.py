import gzip
import logging
import os
import re
import shutil
from datetime import datetime
from pathlib import Path
from typing import TypedDict

import boto3
from botocore.exceptions import ClientError

import data_pipelines_annuaire.helpers.filesystem as filesystem
from data_pipelines_annuaire.config import (
    OBJECT_STORAGE_ACCESS_KEY,
    OBJECT_STORAGE_BUCKET,
    OBJECT_STORAGE_ENV_PATH,
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
                object_key = (
                    f"{OBJECT_STORAGE_ENV_PATH}{file['dest_path']}{file['dest_name']}"
                )
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
            Bucket=self.bucket, Prefix=f"{OBJECT_STORAGE_ENV_PATH}{prefix}"
        )

        for page in page_iterator:
            if "Contents" in page:
                for obj in page["Contents"]:
                    object_name = obj["Key"]
                    if not object_name.endswith("/"):  # Exclude folders
                        logging.info(object_name)
                        list_objects.append(
                            object_name.replace(f"{OBJECT_STORAGE_ENV_PATH}", "")
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
            object_key = (
                f"{OBJECT_STORAGE_ENV_PATH}{file['source_path']}{file['source_name']}"
            )
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

    def get_latest_database(
        self,
        object_storage_path: str,
        local_path: str,
    ) -> str:
        """
        Download and decompress the latest .db.gz file from object storage
        to the local file system.

        Args:
            object_storage_path (str): The path within the bucket where compressed .db
            files are located.
            local_path (str): The local path where the decompressed .db file
            will be saved.

        Note:
            This function assumes that the .db files in object storage have filenames
            in the format:
            'filename_YYYY-MM-DD.db.gz', where YYYY-MM-DD represents the date.
            Files that do not match this pattern are ignored.
        """
        prefix = f"{OBJECT_STORAGE_ENV_PATH}{object_storage_path}"

        # Use list_objects_v2 which is the recommended method
        paginator = self.client.get_paginator("list_objects_v2")
        page_iterator = paginator.paginate(Bucket=self.bucket, Prefix=prefix)

        # Keep only dated 'blabla_YYYY-MM-DD.db.gz' files, pairing each with its date
        db_file_date_pattern = re.compile(r"_(\d{4}-\d{2}-\d{2})\.db\.gz$")
        dated_db_files = []
        for page in page_iterator:
            if "Contents" in page:
                for obj in page["Contents"]:
                    match = db_file_date_pattern.search(obj["Key"])
                    if match:
                        dated_db_files.append((match.group(1), obj["Key"]))

        if not dated_db_files:
            raise Exception(f"No database file was found in: {prefix}")

        latest_file_date_str, latest_db_file = max(dated_db_files)
        logging.info(f"Latest database: {latest_db_file}")

        latest_file_date = datetime.strptime(latest_file_date_str, "%Y-%m-%d").strftime(
            "%Y-%m-%dT%H:%M:%S"
        )
        logging.info(f"Date of latest file: {latest_file_date}")

        # Ensure destination directory exists
        os.makedirs(os.path.dirname(local_path), exist_ok=True)

        compressed_path = f"{local_path}.gz"
        self.client.download_file(self.bucket, latest_db_file, compressed_path)

        # Decompress the downloaded database file
        with gzip.open(compressed_path, "rb") as f_in:
            with open(local_path, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)
        os.remove(compressed_path)

        return latest_file_date

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
            file_1_key = f"{OBJECT_STORAGE_ENV_PATH}{file_path_1}{file_name_1}"
            file_2_key = f"{OBJECT_STORAGE_ENV_PATH}{file_path_2}{file_name_2}"

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
        Rename a folder in object storage under the default path.

        Args:
            old_folder_suffix (str): The folder suffix to rename.
            new_folder_suffix (str): The new folder suffix.
        """
        old_folder = f"{OBJECT_STORAGE_ENV_PATH}{old_folder_suffix}"
        new_folder = f"{OBJECT_STORAGE_ENV_PATH}{new_folder_suffix}"

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
                Bucket=self.bucket, Key=f"{OBJECT_STORAGE_ENV_PATH}{file_path}"
            )
            # Format the datetime object to ISO 8601 format
            last_modified_str = stat["LastModified"].strftime("%Y-%m-%dT%H:%M:%S")
            logging.info(f"Last modified date of '{file_path}': {last_modified_str}")
            return last_modified_str
        except ClientError as e:
            logging.error(f"Error retrieving file metadata for {file_path}: {e}")
            return None

    def copy_file(self, source_path: str, dest_path: str, is_public: bool = True):
        """
        Copy a file from one S3 path to another.

        Args:
            source_path (str): The S3 path of the source file.
            dest_path (str): The S3 path where the file should be copied.
            is_public (bool): Whether the copied file should be publicly readable.
                Defaults to True.
        """
        try:
            # Define the full S3 path with environment prefix
            source_full_path = f"{OBJECT_STORAGE_ENV_PATH}{source_path}"
            dest_full_path = f"{OBJECT_STORAGE_ENV_PATH}{dest_path}"

            # Copy object to the new location
            copy_source = {"Bucket": self.bucket, "Key": source_full_path}
            extra_args = {}
            if is_public:
                extra_args["ACL"] = "public-read"
            self.client.copy_object(
                Bucket=self.bucket,
                Key=dest_full_path,
                CopySource=copy_source,
                **extra_args,
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
                Key=os.path.join(OBJECT_STORAGE_ENV_PATH, path),
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
            os.path.join(OBJECT_STORAGE_ENV_PATH, self.filepath),
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
