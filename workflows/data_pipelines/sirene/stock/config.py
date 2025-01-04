from dag_datalake_sirene.config import (
    DataSourceConfig,
    MINIO_BASE_URL,
)


STOCK_SIRENE_CONFIG = DataSourceConfig(
    name="stock_sirene",
    tmp_folder=f"{DataSourceConfig.base_tmp_folder}/sirene/stock/",
    files_to_download={
        "stock_ul": {
            "url": "https://files.data.gouv.fr/insee-sirene/StockUniteLegale_utf8.zip",
            "destination": f"{DataSourceConfig.base_tmp_folder}/sirene/stock/StockUniteLegale_utf8.zip",
        },
        "historique_ul": {
            "url": "https://files.data.gouv.fr/insee-sirene/StockUniteLegaleHistorique_utf8.zip",
            "destination": f"{DataSourceConfig.base_tmp_folder}/sirene/stock/StockUniteLegaleHistorique_utf8.zip",
        },
    },
    minio_path="insee/stock/",
    url_minio=f"{MINIO_BASE_URL}insee/stock/",
)
