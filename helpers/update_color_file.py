import json
import logging

from minio import Minio
from dag_datalake_sirene.config import (
    AIRFLOW_ENV,
    MINIO_URL,
    MINIO_BUCKET,
    MINIO_USER,
    MINIO_PASSWORD,
)


def update_color_file(**kwargs):
    next_color = kwargs["ti"].xcom_pull(key="next_color", task_ids="get_colors")
    current_color = kwargs["ti"].xcom_pull(key="current_color", task_ids="get_colors")
    colors = {"CURRENT_COLOR": next_color, "NEXT_COLOR": current_color}
    logging.info(f"******************** Next color configuration: {colors}")

    with open("colors.json", "w") as write_file:
        json.dump(colors, write_file)
    minio_filepath = f"ae/colors-{AIRFLOW_ENV}.json"
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
            file_path="colors.json",
            content_type="application/json",
        )
