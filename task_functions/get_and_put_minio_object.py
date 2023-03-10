from minio import Minio


from dag_datalake_sirene.task_functions.global_variables import (
    MINIO_URL,
    MINIO_BUCKET,
    MINIO_USER,
    MINIO_PASSWORD,
)


def get_object_minio(
    filename: str,
    minio_path: str,
    local_path: str,
) -> None:
    print(filename, minio_path, local_path)
    minio_url = MINIO_URL
    minio_bucket = MINIO_BUCKET
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
):
    minio_url = MINIO_URL
    minio_bucket = MINIO_BUCKET
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
