from minio import Minio

from dag_datalake_sirene.task_functions.global_variables import (
    ENV,
    DATA_DIR,
    MINIO_URL,
    MINIO_BUCKET,
    MINIO_USER,
    MINIO_PASSWORD,
)


def update_sitemap():
    minio_filepath = "ae/sitemap-" + ENV + ".csv"
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
            object_name=minio_filepath,
            file_path=DATA_DIR + "sitemap-" + ENV + ".csv",
            content_type="text/csv",
        )
