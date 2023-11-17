from datetime import datetime
import logging
from minio import Minio
from typing import List, TypedDict, Optional
import os
from dag_datalake_sirene.config import AIRFLOW_ENV

from dag_datalake_sirene.task_functions.global_variables import (
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


def send_files(
    MINIO_URL: str,
    MINIO_BUCKET: str,
    MINIO_USER: str,
    MINIO_PASSWORD: str,
    list_files: List[File],
):
    """Send list of file to Minio bucket

    Args:
        MINIO_URL (str): Minio endpoint
        MINIO_BUCKET (str): bucket
        MINIO_USER (str): user
        MINIO_PASSWORD (str): password
        list_files (List[File]): List of Dictionnaries containing for each
        `source_path` and `source_name` : local file location ;
        `dest_path` and `dest_name` : minio location (inside bucket specified) ;

    Raises:
        Exception: when specified local file does not exists
        Exception: when specified bucket does not exist
    """
    client = Minio(
        MINIO_URL,
        access_key=MINIO_USER,
        secret_key=MINIO_PASSWORD,
        secure=True,
    )
    found = client.bucket_exists(MINIO_BUCKET)
    if found:
        for file in list_files:
            is_file = os.path.isfile(
                os.path.join(file["source_path"], file["source_name"])
            )
            print("Sending ", file["source_name"])
            if is_file:
                client.fput_object(
                    MINIO_BUCKET,
                    f"ae/{AIRFLOW_ENV}/{file['dest_path']}{file['dest_name']}",
                    os.path.join(file["source_path"], file["source_name"]),
                    content_type=file["content_type"]
                    if "content_type" in file
                    else None,
                )
            else:
                raise Exception(
                    f"file {file['source_path']}{file['source_name']} "
                    "does not exists"
                )
    else:
        raise Exception(f"Bucket {MINIO_BUCKET} does not exists")


def get_files_from_prefix(
    MINIO_URL: str,
    MINIO_BUCKET: str,
    MINIO_USER: str,
    MINIO_PASSWORD: str,
    prefix: str,
):
    """Retrieve only the list of files in a Minio pattern

    Args:
        MINIO_URL (str): Minio endpoint
        MINIO_BUCKET (str): bucket
        MINIO_USER (str): user
        MINIO_PASSWORD (str): password
        prefix: (str): prefix to search files

    Raises:
        Exception: _description_
    """
    client = Minio(
        MINIO_URL,
        access_key=MINIO_USER,
        secret_key=MINIO_PASSWORD,
        secure=True,
    )
    found = client.bucket_exists(MINIO_BUCKET)
    if found:
        list_objects = []
        objects = client.list_objects(MINIO_BUCKET, prefix=f"ae/{AIRFLOW_ENV}/{prefix}")
        for obj in objects:
            print(obj.object_name)
            list_objects.append(obj.object_name.replace(f"ae/{AIRFLOW_ENV}/", ""))
        return list_objects
    else:
        raise Exception(f"Bucket {MINIO_BUCKET} does not exists")


def get_files(
    MINIO_URL: str,
    MINIO_BUCKET: str,
    MINIO_USER: str,
    MINIO_PASSWORD: str,
    list_files: List[File],
):
    """Retrieve list of files from Minio

    Args:
        MINIO_URL (str): Minio endpoint
        MINIO_BUCKET (str): bucket
        MINIO_USER (str): user
        MINIO_PASSWORD (str): password
        list_files (List[File]): List of Dictionnaries containing for each
        `source_path` and `source_name` : Minio location inside specified bucket ;
        `dest_path` and `dest_name` : local file destination ;

    Raises:
        Exception: _description_
    """
    client = Minio(
        MINIO_URL,
        access_key=MINIO_USER,
        secret_key=MINIO_PASSWORD,
        secure=True,
    )
    found = client.bucket_exists(MINIO_BUCKET)
    if found:
        for file in list_files:
            client.fget_object(
                MINIO_BUCKET,
                f"ae/{AIRFLOW_ENV}/{file['source_path']}{file['source_name']}",
                f"{file['dest_path']}{file['dest_name']}",
            )
    else:
        raise Exception(f"Bucket {MINIO_BUCKET} does not exists")


def get_object_minio(
    filename: str,
    minio_path: str,
    local_path: str,
    minio_bucket: str,
) -> None:
    minio_url = MINIO_URL
    minio_bucket = minio_bucket
    minio_user = MINIO_USER
    minio_password = MINIO_PASSWORD

    client = Minio(
        minio_url,
        access_key=minio_user,
        secret_key=minio_password,
        secure=True,
    )
    client.fget_object(
        minio_bucket,
        f"{minio_path}{filename}",
        local_path,
    )


def put_object_minio(
    filename: str,
    minio_path: str,
    local_path: str,
    minio_bucket: str,
):
    minio_url = MINIO_URL
    minio_bucket = minio_bucket
    minio_user = MINIO_USER
    minio_password = MINIO_PASSWORD

    # Start client
    client = Minio(
        minio_url,
        access_key=minio_user,
        secret_key=minio_password,
        secure=True,
    )

    # Check if bucket exists
    found = client.bucket_exists(minio_bucket)
    if found:
        client.fput_object(
            bucket_name=minio_bucket,
            object_name=minio_path,
            file_path=local_path + filename,
        )


def get_latest_file_minio(
    minio_path: str,
    local_path: str,
    minio_bucket: str,
):
    """
    Download the latest .db file from Minio to the local file system.

    Args:
        minio_path (str): The path within the Minio bucket where .db files are located.
        local_path (str): The local directory where the downloaded file will be saved.
        minio_bucket (str): The name of the Minio bucket.

    Returns:
        None: The function downloads the latest .db file and does not return any value.

    Note:
        This function assumes that the .db files in Minio have filenames in the format:
        'filename_YYYY-MM-DD.db', where YYYY-MM-DD represents the date.
    """
    minio_url = MINIO_URL
    minio_bucket = minio_bucket
    minio_user = MINIO_USER
    minio_password = MINIO_PASSWORD

    logging.info("Connecting to Minio...")
    client = Minio(
        minio_url,
        access_key=minio_user,
        secret_key=minio_password,
        secure=True,
    )

    objects = client.list_objects(minio_bucket, prefix=minio_path, recursive=True)

    # Filter and sort .db files based on their names and the date in the filename
    db_files = [obj.object_name for obj in objects if obj.object_name.endswith(".db")]

    sorted_db_files = sorted(
        db_files,
        key=lambda x: datetime.strptime(x.split("_")[-1].split(".")[0], "%Y-%m-%d"),
        reverse=True,
    )

    if sorted_db_files:
        latest_db_file = sorted_db_files[0]
        logging.info(f"Latest dirigeants database: {latest_db_file}")

        client.fget_object(
            minio_bucket,
            latest_db_file,
            local_path,
        )
    else:
        logging.warning("No .db files found in the specified path.")
