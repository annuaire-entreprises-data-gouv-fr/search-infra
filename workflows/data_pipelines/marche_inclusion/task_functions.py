import csv
import requests
from datetime import datetime
import logging
import os
from dag_datalake_sirene.helpers.minio_helpers import minio_client
from dag_datalake_sirene.helpers.utils import save_to_metadata
from dag_datalake_sirene.config import (
    MARCHE_INCLUSION_API_URL,
    MARCHE_INCLUSION_TMP_FOLDER,
    SECRET_TOKEN_MARCHE_INCLUSION,
)


def call_api_marche_inclusion(number_of_strctures):
    query_params = f"token={SECRET_TOKEN_MARCHE_INCLUSION}&limit={number_of_strctures}"

    endpoint = f"{MARCHE_INCLUSION_API_URL}{query_params}"

    response = requests.get(endpoint)
    data = response.json()
    return data


def save_siae_to_csv(data, file_path):
    csv_data = [[result["siret"], result["kind"]] for result in data.get("results", [])]
    with open(file_path, "w", newline="") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(["siret", "kind"])
        writer.writerows(csv_data)


def get_structures_siae():
    number_of_structures = 20000
    response_data = call_api_marche_inclusion(number_of_structures)
    actual_number_of_structures = response_data.get("count", 0)
    logging.info(f"Number of structures: {actual_number_of_structures}")

    if actual_number_of_structures > number_of_structures:
        number_of_structures = actual_number_of_structures
        response_data = call_api_marche_inclusion(number_of_structures)

    file_path = f"{MARCHE_INCLUSION_TMP_FOLDER}marche_inclusion.csv"
    save_siae_to_csv(response_data, file_path)


def save_date_last_modified():
    date_last_modified = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f")
    metadata_path = os.path.join(MARCHE_INCLUSION_TMP_FOLDER, "metadata.json")

    # Save the 'last_modified' date to the metadata file
    save_to_metadata(metadata_path, "last_modified", date_last_modified)

    logging.info(f"Last modified date saved successfully to {metadata_path}")


def send_file_minio():
    minio_client.send_files(
        list_files=[
            {
                "source_path": MARCHE_INCLUSION_TMP_FOLDER,
                "source_name": "marche_inclusion.csv",
                "dest_path": "marche_inclusion/",
                "dest_name": "stock_marche_inclusion.csv",
            },
            {
                "source_path": MARCHE_INCLUSION_TMP_FOLDER,
                "source_name": "metadata.json",
                "dest_path": "marche_inclusion/",
                "dest_name": "metadata.json",
            },
        ],
    )
