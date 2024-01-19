import os
import json
import re
import logging
from dag_datalake_sirene.helpers.minio_helpers import minio_client
from dag_datalake_sirene.config import (
    RNE_DB_TMP_FOLDER,
    RNE_MINIO_FLUX_DATA_PATH,
)


def clean_flux_json_files(**kwargs):
    start_date = kwargs["ti"].xcom_pull(key="start_date", task_ids="get_start_date")

    json_daily_flux_files = minio_client.get_files_from_prefix(
        prefix=RNE_MINIO_FLUX_DATA_PATH,
    )

    if not json_daily_flux_files:
        return None

    if start_date is None:
        start_date = "0000-00-00"

    for file_path in sorted(json_daily_flux_files, reverse=False):
        date_match = re.search(r"rne_flux_(\d{4}-\d{2}-\d{2})", file_path)
        if date_match:
            file_date = date_match.group(1)
            if file_date >= start_date:
                logging.info(f"Processing file {file_path} with date {file_date}")
                minio_client.get_files(
                    list_files=[
                        {
                            "source_path": RNE_MINIO_FLUX_DATA_PATH,
                            "source_name": f"rne_flux_{file_date}.json",
                            "dest_path": RNE_DB_TMP_FOLDER,
                            "dest_name": f"{file_date}_rne_flux.json",
                        }
                    ],
                )
                json_path = f"{RNE_DB_TMP_FOLDER}{file_date}_rne_flux.json"
                new_json_path = f"{RNE_DB_TMP_FOLDER}rne_flux_{file_date}.json"

                with open(json_path, "r") as file:
                    for obj in file:
                        array = json.loads(obj)
                        for company in array:
                            if company:
                                with open(new_json_path, "a+") as output_file:
                                    json.dump(company, output_file)
                                    output_file.write("\n")
                if os.path.exists(new_json_path):
                    minio_client.send_files(
                        list_files=[
                            {
                                "source_path": "",
                                "source_name": f"{new_json_path}",
                                "dest_path": RNE_MINIO_FLUX_DATA_PATH,
                                "dest_name": f"rne_flux_{file_date}.json",
                            },
                        ],
                    )
                logging.info(f"File {file_date} processed and sent to minio.")
                os.remove(json_path)
                os.remove(new_json_path)
