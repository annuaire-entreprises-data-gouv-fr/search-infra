from minio import Minio
from typing import List, TypedDict, Optional
import os
from dag_datalake_sirene.config import AIRFLOW_ENV


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
