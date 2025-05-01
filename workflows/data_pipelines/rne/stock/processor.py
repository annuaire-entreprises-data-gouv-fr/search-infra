import logging
import os
import subprocess
import zipfile

from dag_datalake_sirene.config import AIRFLOW_DAG_HOME
from dag_datalake_sirene.helpers import DataProcessor
from dag_datalake_sirene.helpers.minio_helpers import File
from dag_datalake_sirene.workflows.data_pipelines.rne.stock.config import (
    RNE_STOCK_CONFIG,
)


class RneStockProcessor(DataProcessor):
    def __init__(self):
        super().__init__(RNE_STOCK_CONFIG)

    def download_stock(self, ftp_url: str) -> None:
        """Downloads the stock file from FTP server"""

        logging.info(f"+++++++++++++{RNE_STOCK_CONFIG.files_to_download}")

        script_path = os.path.join(
            AIRFLOW_DAG_HOME,
            "dag_datalake_sirene/workflows/data_pipelines/rne/stock/get_stock.sh",
        )

        # Ensure tmp folder exists
        os.makedirs(self.config.tmp_folder, exist_ok=True)

        try:
            subprocess.run(
                [script_path, self.config.tmp_folder, ftp_url],
                check=True,
                capture_output=True,
                text=True,
            )
            logging.info("Stock file downloaded successfully")
        except subprocess.CalledProcessError as e:
            error_msg = f"Failed to download stock file: {e.stderr}"
            logging.error(error_msg)
            raise RuntimeError(error_msg)

    def send_stock_to_minio(self) -> int:
        """Sends the stock files from the zip to MinIO"""
        sent_files = 0
        zip_path = f"{self.config.tmp_folder}/stock_rne.zip"

        with zipfile.ZipFile(zip_path, mode="r") as z:
            for file_info in z.infolist():
                # Extract each file one by one
                z.extract(file_info, path=self.config.tmp_folder)

                logging.info(f"Saving file {file_info.filename} in MinIO.....")
                self.minio_client.send_files(
                    list_files=[
                        File(
                            source_path=f"{self.config.tmp_folder}/",
                            source_name=file_info.filename,
                            dest_path=f"{self.config.minio_path}/",
                            dest_name=file_info.filename,
                        ),
                    ],
                )
                sent_files += 1
                # Delete the extracted file
                extracted_file_path = os.path.join(
                    self.config.tmp_folder, file_info.filename
                )
                os.remove(extracted_file_path)
        os.remove(zip_path)

        return sent_files
