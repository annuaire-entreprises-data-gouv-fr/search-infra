from data_pipelines_annuaire.config import (
    DATA_GOUV_BASE_URL,
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
            "url": f"{DATA_GOUV_BASE_URL}45d450f1-b0d4-42b7-bb16-7b46895eb83a",
            "resource_id": "45d450f1-b0d4-42b7-bb16-7b46895eb83a",
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
            titre TEXT,
            date_creation DATE,
            numero_rnf TEXT,
            adresse TEXT,
            code_postal TEXT,
            ville TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_fondation_siret ON fondation (siret);
        CREATE INDEX IF NOT EXISTS idx_fondation_siren ON fondation (siren);
        COMMIT;
    """,
)
