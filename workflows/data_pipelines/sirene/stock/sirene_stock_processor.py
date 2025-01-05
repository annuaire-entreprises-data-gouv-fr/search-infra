from dag_datalake_sirene.helpers import DataProcessor

from dag_datalake_sirene.workflows.data_pipelines.sirene.stock.config import (
    STOCK_SIRENE_CONFIG,
)
from dag_datalake_sirene.helpers.minio_helpers import File


class SireneStockProcessor(DataProcessor):
    def __init__(self):
        super().__init__(STOCK_SIRENE_CONFIG)

    def send_stock_to_minio(self):
        self.minio_client.send_files(
            list_files=[
                File(
                    source_path=f"{self.config.tmp_folder}",
                    source_name="StockUniteLegale_utf8.zip",
                    dest_path=f"{self.config.minio_path}",
                    dest_name="StockUniteLegale_utf8.zip",
                ),
                File(
                    source_path=f"{self.config.tmp_folder}",
                    source_name="StockUniteLegaleHistorique_utf8.zip",
                    dest_path=f"{self.config.minio_path}",
                    dest_name="StockUniteLegaleHistorique_utf8.zip",
                ),
                File(
                    source_path=f"{self.config.tmp_folder}",
                    source_name="StockEtablissementHistorique_utf8.zip",
                    dest_path=f"{self.config.minio_path}",
                    dest_name="StockEtablissementHistorique_utf8.zip",
                ),
            ],
        )
