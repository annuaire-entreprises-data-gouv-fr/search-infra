from minio import Minio
from datetime import datetime
import logging


from dag_datalake_sirene.task_functions.global_variables import (
    MINIO_URL,
    MINIO_USER,
    MINIO_PASSWORD,
)


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
