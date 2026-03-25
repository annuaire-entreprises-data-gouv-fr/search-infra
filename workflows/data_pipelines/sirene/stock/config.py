from data_pipelines_annuaire.config import (
    CURRENT_MONTH,
    OBJECT_STORAGE_BASE_URL,
    DataSourceConfig,
)

STOCK_SIRENE_CONFIG = DataSourceConfig(
    name="sirene_stock",
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
        "stock_etablissement": {
            "resource_id": "0651fb76-bcf3-4f6a-a38d-bc04fa708576",
            "url": "https://www.data.gouv.fr/datasets/r/0651fb76-bcf3-4f6a-a38d-bc04fa708576",
            "destination": f"{DataSourceConfig.base_tmp_folder}/sirene/stock/StockEtablissement_{CURRENT_MONTH}.zip",
        },
        "historique_etablissement": {
            "resource_id": "88fbb6b4-0320-443e-b739-b4376a012c32",
            "url": "https://www.data.gouv.fr/datasets/r/88fbb6b4-0320-443e-b739-b4376a012c32",
            "destination": f"{DataSourceConfig.base_tmp_folder}/sirene/stock/StockEtablissementHistorique_{CURRENT_MONTH}.zip",
        },
        "geo_stats": {
            "resource_id": "ba6a4e4c-aac6-4764-bbd2-f80ae345afc5",
            "url": "https://www.data.gouv.fr/datasets/r/ba6a4e4c-aac6-4764-bbd2-f80ae345afc5",
            "destination": f"{DataSourceConfig.base_tmp_folder}/sirene/stock/etablissement_geo_stats_{CURRENT_MONTH}.zip",
        },
    },
    object_storage_path="insee/sirene_stock/",
    url_object_storage=f"{OBJECT_STORAGE_BASE_URL}insee/sirene_stock/",
)
