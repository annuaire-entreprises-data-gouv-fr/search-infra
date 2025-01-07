import logging
import os
import json
import requests

from dag_datalake_sirene.config import (
    AIRFLOW_ETL_DATA_DIR,
    MINIO_DATA_SOURCE_UPDATE_DATES_FILE,
    URL_MINIO_BILANS_FINANCIERS_METADATA,
    URL_MINIO_CONVENTION_COLLECTIVE_METADATA,
)
from dag_datalake_sirene.workflows.data_pipelines.egapro.config import EGAPRO_CONFIG
from dag_datalake_sirene.workflows.data_pipelines.finess.config import FINESS_CONFIG
from dag_datalake_sirene.workflows.data_pipelines.agence_bio.agence_bio_config import (
    AGENCE_BIO_CONFIG,
)
from dag_datalake_sirene.workflows.data_pipelines.ess_france.config import (
    ESS_CONFIG,
)
from dag_datalake_sirene.workflows.data_pipelines.rge.config import (
    RGE_CONFIG,
)
from dag_datalake_sirene.workflows.data_pipelines.spectacle.spectacle_config import (
    SPECTACLE_CONFIG,
)
from dag_datalake_sirene.workflows.data_pipelines.formation.formation_config import (
    FORMATION_CONFIG,
)
from dag_datalake_sirene.workflows.data_pipelines.uai.uai_config import (
    UAI_CONFIG,
)
from dag_datalake_sirene.workflows.data_pipelines.colter.colter_config import (
    COLTER_CONFIG,
    ELUS_CONFIG,
)
from dag_datalake_sirene.workflows.data_pipelines.sirene.flux.config import (
    FLUX_SIRENE_CONFIG,
)
from dag_datalake_sirene.helpers.minio_helpers import minio_client
from dag_datalake_sirene.helpers.utils import simplify_date


def create_data_source_last_modified_file(**kwargs):
    metadata_dict = {}

    metadata_url_to_datasource = {
        ESS_CONFIG.url_minio_metadata: "ess_france",
        RGE_CONFIG.url_minio_metadata: "rge",
        UAI_CONFIG.url_minio_metadata: "uai",
        COLTER_CONFIG.url_minio_metadata: "collectivite_territoriale",
        ELUS_CONFIG.url_minio_metadata: "collectivite_territoriale_elus",
        FLUX_SIRENE_CONFIG.url_minio_metadata: "sirene",
        EGAPRO_CONFIG.url_minio_metadata: "egapro",
        AGENCE_BIO_CONFIG.url_minio_metadata: "agence_bio",
        SPECTACLE_CONFIG.url_minio_metadata: "entrepreneur_spectacle",
        FINESS_CONFIG.url_minio_metadata: "finess",
        URL_MINIO_BILANS_FINANCIERS_METADATA: "bilan_financier",
        FORMATION_CONFIG.url_minio_metadata: "organisme_formation",
        URL_MINIO_CONVENTION_COLLECTIVE_METADATA: "convention_collective",
    }

    json_file_path = os.path.join(
        AIRFLOW_ETL_DATA_DIR, MINIO_DATA_SOURCE_UPDATE_DATES_FILE
    )

    # Fetch metadata from each URL
    for url, datasource in metadata_url_to_datasource.items():
        try:
            response = requests.get(url)
            response.raise_for_status()  # If the response code is not 200, raise an HTTPError
            json_data = response.json()  # Parse the JSON response

            # Get the 'last_modified' key
            last_modified = simplify_date(json_data.get("last_modified", None))
            metadata_dict[datasource] = (
                last_modified  # Set value (can be None if not present)
            )

        except requests.exceptions.HTTPError as e:
            # Handle the case where the URL is not reachable (e.g., 404, 403 errors)
            logging.error(f"HTTP error for {url}: {e}")
            metadata_dict[datasource] = None  # Assign None if the URL doesn't exist

        except requests.RequestException as e:
            # Handle other network-related errors
            logging.error(f"Error fetching data from {url}: {e}")
            metadata_dict[datasource] = None  # Assign None if any request error occurs

    # Fetch RNE metadata
    rne_last_modified_date = kwargs["ti"].xcom_pull(
        key="rne_last_modified", task_ids="get_rne_database"
    )

    metadata_dict["rne"] = simplify_date(rne_last_modified_date)

    # Save the metadata_dict to a JSON file
    with open(json_file_path, "w") as json_file:
        json.dump(metadata_dict, json_file, indent=4)

    # Send the updated JSON file to Minio
    minio_client.send_files(
        [
            {
                "source_path": AIRFLOW_ETL_DATA_DIR,
                "source_name": MINIO_DATA_SOURCE_UPDATE_DATES_FILE,
                "dest_path": "metadata/updates/new/",
                "dest_name": MINIO_DATA_SOURCE_UPDATE_DATES_FILE,
            }
        ]
    )
    return None
