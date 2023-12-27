from minio import Minio

from dag_datalake_sirene.config import (
    AIRFLOW_ENV,
    AIRFLOW_PREPROCESSING_DATA_DIR,
    MINIO_URL,
    MINIO_BUCKET,
    MINIO_USER,
    MINIO_PASSWORD,
)


def update_sitemap():
    minio_filepath = "ae/sitemap-" + AIRFLOW_ENV + ".csv"
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
            file_path=AIRFLOW_PREPROCESSING_DATA_DIR
            + "sitemap-"
            + AIRFLOW_ENV
            + ".csv",
            content_type="text/csv",
        )
