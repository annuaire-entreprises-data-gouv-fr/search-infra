import os
import zipfile
import logging
from helpers.tchap import send_message
from helpers.minio_helpers import minio_client
from config import (
    RNE_STOCK_ZIP_FILE_PATH,
    RNE_STOCK_EXTRACTED_FILES_PATH,
)


def unzip_files_and_upload_minio(**kwargs):
    with zipfile.ZipFile(RNE_STOCK_ZIP_FILE_PATH, mode="r") as z:
        sent_files = 0
        for file_info in z.infolist():
            # Extract each file one by one
            z.extract(file_info, path=RNE_STOCK_EXTRACTED_FILES_PATH)

            logging.info(f"Saving file {file_info.filename} in MinIO.....")
            minio_client.send_files(
                list_files=[
                    {
                        "source_path": RNE_STOCK_EXTRACTED_FILES_PATH,
                        "source_name": file_info.filename,
                        "dest_path": "rne/stock/",
                        "dest_name": file_info.filename,
                    },
                ],
            )
            sent_files += 1
            # Delete the extracted file
            extracted_file_path = os.path.join(
                RNE_STOCK_EXTRACTED_FILES_PATH, file_info.filename
            )
            os.remove(extracted_file_path)

    kwargs["ti"].xcom_push(key="stock_files_rne_count", value=sent_files)


def send_notification_success_tchap(**kwargs):
    count_files_rne_stock = kwargs["ti"].xcom_pull(
        key="stock_files_rne_count", task_ids="unzip_files_and_upload_minio"
    )
    send_message(
        f"\U0001F7E2 Données :"
        f"\nDonnées stock RNE mises à jour."
        f"\n - Nombre de fichiers stock : {count_files_rne_stock}."
    )


def send_notification_failure_tchap(context):
    send_message("\U0001F534 Données :" "\nFail DAG stock RNE!!!!")
