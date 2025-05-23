from datetime import datetime

from dag_datalake_sirene.config import CURRENT_MONTH
from dag_datalake_sirene.helpers import DataProcessor
from dag_datalake_sirene.helpers.datagouv import get_dataset_or_resource_metadata
from dag_datalake_sirene.helpers.minio_helpers import File
from dag_datalake_sirene.workflows.data_pipelines.sirene.stock.config import (
    STOCK_SIRENE_CONFIG,
)

MONTH_MAPPING = {
    "janvier": "01",
    "fevrier": "02",
    "mars": "03",
    "avril": "04",
    "mai": "05",
    "juin": "06",
    "juillet": "07",
    "aout": "08",
    "septembre": "09",
    "octobre": "10",
    "novembre": "11",
    "decembre": "12",
}


class SireneStockProcessor(DataProcessor):
    def __init__(self):
        super().__init__(STOCK_SIRENE_CONFIG)

    def stock_filename(self, original_filename: str, resource_id: str) -> str:
        metadata = get_dataset_or_resource_metadata(resource_id=resource_id)
        if metadata.get("message") == "error":
            raise Exception(f"Error while retrieving the {resource_id} metadata")

        title = (
            metadata["resource"]["title"].lower().replace("é", "e").replace("û", "u")
        )

        year_month = ""
        for month in MONTH_MAPPING:
            if month in title:
                year_month = f"{datetime.now().year}-{MONTH_MAPPING[month]}"
                break
        if not year_month:
            raise Exception(f"Month not found in the title of {resource_id}")

        return original_filename.replace(CURRENT_MONTH, year_month)

    def send_stock_to_minio(self):
        self.minio_client.send_files(
            [
                File(
                    source_path=f"{self.config.tmp_folder}",
                    source_name=file["destination"].split("/")[-1],
                    dest_path=f"{self.config.minio_path}",
                    dest_name=self.stock_filename(
                        file["destination"].split("/")[-1], file["resource_id"]
                    ),
                    content_type=None,
                )
                for file in self.config.files_to_download.values()
            ],
        )
