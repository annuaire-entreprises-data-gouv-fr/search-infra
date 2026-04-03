import logging
import shutil
import zipfile
from datetime import datetime

import pandas as pd

from data_pipelines_annuaire.config import CURRENT_MONTH
from data_pipelines_annuaire.helpers import DataProcessor
from data_pipelines_annuaire.helpers.datagouv import get_dataset_or_resource_metadata
from data_pipelines_annuaire.helpers.geolocalisation import (
    convert_dataframe_lambert_to_gps,
)
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

    def convert_stock_etablissement_coordinates(self):
        stock_etab_config = self.config.files_to_download["stock_etablissement"]
        zip_path = stock_etab_config["destination"]
        zip_filename = zip_path.split("/")[-1]
        output_zip_filename = zip_filename.replace(".zip", "_with_gps.zip")
        input_csv = f"{self.config.tmp_folder}StockEtablissement_utf8.csv"
        output_csv = f"{self.config.tmp_folder}StockEtablissement_utf8_with_gps.csv"

        logging.info(f"Extracting {zip_filename}...")
        shutil.unpack_archive(zip_path, self.config.tmp_folder)

        logging.info("Converting Lambert coordinates to GPS (processing by chunks)...")
        chunk_size = 500_000
        first_chunk = True

        for chunk in pd.read_csv(input_csv, dtype=str, chunksize=chunk_size):
            chunk = convert_dataframe_lambert_to_gps(
                chunk,
                x_col="coordonneeLambertAbscisseEtablissement",
                y_col="coordonneeLambertOrdonneeEtablissement",
                code_postal_col="codePostalEtablissement",
                code_commune_col="codeCommuneEtablissement",
            )

            if first_chunk:
                chunk.to_csv(output_csv, mode="w", header=True, index=False)
                first_chunk = False
            else:
                chunk.to_csv(output_csv, mode="a", header=False, index=False)

            logging.info(f"Processed {chunk_size} rows...")

        logging.info(
            f"Compressing output file {output_csv} to {output_zip_filename}..."
        )
        with zipfile.ZipFile(
            f"{self.config.tmp_folder}{output_zip_filename}", "w"
        ) as zipf:
            # by default zip is conserving the whole directory structure (e.g. /tmp/sirene/stock..)
            # arcname allow to overwrite this
            zipf.write(output_csv, arcname="StockEtablissement_utf8_with_gps.csv")

        logging.info("Stock etablissement coordinates conversion completed.")

    def send_stock_to_object_storage(self):

        files_to_send = []

        for key, file_config in self.config.files_to_download.items():
            original_filename = file_config["destination"].split("/")[-1]

            if key == "stock_etablissement":
                source_name = original_filename.replace(".zip", "_with_gps.zip")
                dest_name = self.stock_filename(source_name, file_config["resource_id"])
            else:
                source_name = original_filename
                dest_name = self.stock_filename(
                    original_filename, file_config["resource_id"]
                )

            files_to_send.append(
                File(
                    source_path=f"{self.config.tmp_folder}",
                    source_name=source_name,
                    dest_path=f"{self.config.object_storage_path}",
                    dest_name=dest_name,
                    content_type=None,
                )
            )

        self.object_storage_client.send_files(files_to_send)
