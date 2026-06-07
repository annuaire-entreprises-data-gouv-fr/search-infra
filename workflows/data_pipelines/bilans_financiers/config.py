from data_pipelines_annuaire.config import (
    OBJECT_STORAGE_BASE_URL,
    DataSourceConfig,
)

BILANS_FINANCIERS_CONFIG = DataSourceConfig(
    name="bilan_financier",
    tmp_folder=f"{DataSourceConfig.base_tmp_folder}/bilans_financiers",
    object_storage_path="bilans_financiers",
    file_name="synthese_bilans",
    files_to_download={
        "bilans_financiers": {
            "url": "https://data.economie.gouv.fr/api/explore/v2.1/catalog/datasets/ratios_inpi_bce/exports/parquet?use_labels=true",
            "resource_id": "9d213815-1649-4527-9eb4-427146ef2e5b",
            "destination": f"{DataSourceConfig.base_tmp_folder}/bilans_financiers/bilans-financiers-download.parquet",
        }
    },
    url_object_storage=f"{OBJECT_STORAGE_BASE_URL}bilans_financiers/latest/synthese_bilans.csv",
    url_object_storage_metadata=f"{OBJECT_STORAGE_BASE_URL}bilans_financiers/latest/metadata.json",
    file_output=f"{DataSourceConfig.base_tmp_folder}/bilans_financiers/synthese_bilans.csv",
    table_ddl="""
        BEGIN;
        CREATE TABLE IF NOT EXISTS bilan_financier
        (
            siren TEXT PRIMARY KEY,
            ca REAL,
            resultat_net REAL,
            date_cloture_exercice TEXT,
            annee_cloture_exercice TEXT
        );
        COMMIT;
     """,
)
