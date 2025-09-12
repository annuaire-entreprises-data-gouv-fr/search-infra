from dag_datalake_sirene.config import (
    CURRENT_MONTH,
    MINIO_BASE_URL,
    DataSourceConfig,
)

STOCK_SIRENE_CONFIG = DataSourceConfig(
    name="stock_sirene",
    tmp_folder=f"{DataSourceConfig.base_tmp_folder}/sirene/stock/",
    files_to_download={
        "stock_unite_legale": {
            "resource_id": "825f4199-cadd-486c-ac46-a65a8ea1a047",
            "url": "https://www.data.gouv.fr/datasets/r/825f4199-cadd-486c-ac46-a65a8ea1a047",
            "destination": f"{DataSourceConfig.base_tmp_folder}/sirene/stock/StockUniteLegale_{CURRENT_MONTH}.zip",
        },
        "historique_unite_legale": {
            "resource_id": "0835cd60-2c2a-497b-bc64-404de704ce89",
            "url": "https://www.data.gouv.fr/datasets/r/0835cd60-2c2a-497b-bc64-404de704ce89",
            "destination": f"{DataSourceConfig.base_tmp_folder}/sirene/stock/StockUniteLegaleHistorique_{CURRENT_MONTH}.zip",
        },
        "historique_etablissement": {
            "resource_id": "88fbb6b4-0320-443e-b739-b4376a012c32",
            "url": "https://www.data.gouv.fr/datasets/r/88fbb6b4-0320-443e-b739-b4376a012c32",
            "destination": f"{DataSourceConfig.base_tmp_folder}/sirene/stock/StockEtablissementHistorique_{CURRENT_MONTH}.zip",
        },
    },
    minio_path="insee/stock/",
    url_minio=f"{MINIO_BASE_URL}insee/stock/",
)
