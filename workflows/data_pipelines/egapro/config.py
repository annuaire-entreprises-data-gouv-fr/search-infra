from dag_datalake_sirene.config import (
    DATA_GOUV_BASE_URL,
    MINIO_BASE_URL,
    DataSourceConfig,
)

EGAPRO_CONFIG = DataSourceConfig(
    name="egapro",
    tmp_folder=f"{DataSourceConfig.base_tmp_folder}/egapro",
    minio_path="egapro",
    file_name="egapro",
    files_to_download={
        "egapro": {
            "url": f"{DATA_GOUV_BASE_URL}d434859f-8d3b-4381-bcdb-ec9200653ae6",
            "resource_id": "d434859f-8d3b-4381-bcdb-ec9200653ae6",
        }
    },
    url_minio=f"{MINIO_BASE_URL}egapro/latest/egapro.csv",
    url_minio_metadata=f"{MINIO_BASE_URL}egapro/latest/metadata.json",
    table_ddl="""
        BEGIN;
        CREATE TABLE IF NOT EXISTS egapro
        (
            siren TEXT PRIMARY KEY,
            egapro_renseignee INTEGER
        );
        COMMIT;
    """,
)
