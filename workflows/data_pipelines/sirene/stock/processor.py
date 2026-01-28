from datetime import datetime

from data_pipelines_annuaire.config import CURRENT_MONTH
from data_pipelines_annuaire.helpers import DataProcessor
from data_pipelines_annuaire.helpers.datagouv import get_dataset_or_resource_metadata
from data_pipelines_annuaire.helpers.object_storage import File
from data_pipelines_annuaire.workflows.data_pipelines.sirene.stock.config import (
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

        current_month = datetime.now().month
        current_year = datetime.now().year

        year_month = ""
        for month in MONTH_MAPPING:
            if month in title:
                month_num = MONTH_MAPPING[month]
                # If the month in the datagouv title is December while the
                # current month is January, it means the stock file
                # is still about the previous year
                if month_num == "12" and current_month == 1:
                    year = current_year - 1
                else:
                    year = current_year
                year_month = f"{year}-{month_num}"
                break
        if not year_month:
            raise Exception(f"Month not found in the title of {resource_id}")

        return original_filename.replace(CURRENT_MONTH, year_month)

    def send_stock_to_object_storage(self):
        self.object_storage_client.send_files(
            [
                File(
                    source_path=f"{self.config.tmp_folder}",
                    source_name=file["destination"].split("/")[-1],
                    dest_path=f"{self.config.object_storage_path}",
                    dest_name=self.stock_filename(
                        file["destination"].split("/")[-1], file["resource_id"]
                    ),
                    content_type=None,
                )
                for file in self.config.files_to_download.values()
            ],
        )
