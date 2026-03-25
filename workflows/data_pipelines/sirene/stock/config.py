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
            "resource_id": "350182c9-148a-46e0-8389-76c2ec1374a3",
            "url": "https://www.data.gouv.fr/datasets/r/350182c9-148a-46e0-8389-76c2ec1374a3",
            "destination": f"{DataSourceConfig.base_tmp_folder}/sirene/stock/StockUniteLegale_{CURRENT_MONTH}.parquet",
        },
        "historique_unite_legale": {
            "resource_id": "1b9290ed-d0bc-461f-ba31-0250a99cc140",
            "url": "https://www.data.gouv.fr/datasets/r/1b9290ed-d0bc-461f-ba31-0250a99cc140",
            "destination": f"{DataSourceConfig.base_tmp_folder}/sirene/stock/StockUniteLegaleHistorique_{CURRENT_MONTH}.parquet",
        },
        "stock_etablissement": {
            "resource_id": "0651fb76-bcf3-4f6a-a38d-bc04fa708576",
            "url": "https://www.data.gouv.fr/datasets/r/0651fb76-bcf3-4f6a-a38d-bc04fa708576",
            "destination": f"{DataSourceConfig.base_tmp_folder}/sirene/stock/StockEtablissement_{CURRENT_MONTH}.zip",
        },
        "historique_etablissement": {
            "resource_id": "2b3a0c79-f97b-46b8-ac02-8be6c1f01a8c",
            "url": "https://www.data.gouv.fr/datasets/r/2b3a0c79-f97b-46b8-ac02-8be6c1f01a8c",
            "destination": f"{DataSourceConfig.base_tmp_folder}/sirene/stock/StockEtablissementHistorique_{CURRENT_MONTH}.parquet",
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
