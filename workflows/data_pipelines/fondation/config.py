from data_pipelines_annuaire.config import (
    DATAGOUV_URL,
    OBJECT_STORAGE_BASE_URL,
    DataSourceConfig,
)

FONDATION_CONFIG = DataSourceConfig(
    name="fondation",
    tmp_folder=f"{DataSourceConfig.base_tmp_folder}/fondation",
    object_storage_path="fondation",
    file_name="fondation",
    files_to_download={
        "fondation": {
            # A new resource is added every month
            "url": f"{DATAGOUV_URL}/api/1/datasets/6a04750602ec619fc638a075",
            "dataset_id": "6a04750602ec619fc638a075",
            "resource_extension": "zip",
            "title_prefix": "opendata - Données fonds/fondations (csv/zip)",
            "destination": f"{DataSourceConfig.base_tmp_folder}/fondation/fondation-download.zip",
        },
    },
    url_object_storage=f"{OBJECT_STORAGE_BASE_URL}fondation/latest/fondation.csv",
    url_object_storage_metadata=f"{OBJECT_STORAGE_BASE_URL}fondation/latest/metadata.json",
    file_output=f"{DataSourceConfig.base_tmp_folder}/fondation/fondation.csv",
    table_ddl="""
        BEGIN;
        CREATE TABLE IF NOT EXISTS fondation
        (
            siret TEXT,
            siren TEXT,
            denomination TEXT,
            date_creation DATE,
            numero_rnf TEXT,
            adresse TEXT,
            code_postal TEXT,
            ville TEXT,
            type_organisme TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_fondation_siret ON fondation (siret);
        CREATE INDEX IF NOT EXISTS idx_fondation_siren ON fondation (siren);
        COMMIT;
    """,
)
