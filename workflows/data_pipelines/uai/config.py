from dag_datalake_sirene.config import (
    DATA_GOUV_BASE_URL,
    DATAGOUV_URL,
    MINIO_BASE_URL,
    DataSourceConfig,
)

UAI_CONFIG = DataSourceConfig(
    name="uai",
    tmp_folder=f"{DataSourceConfig.base_tmp_folder}/uai",
    minio_path="uai",
    file_name="uai",
    files_to_download={
        "mesr": {
            "url": f"{DATA_GOUV_BASE_URL}bcc3229a-beb2-4077-a8d8-50a065dfbbfa",
            "resource_id": "bcc3229a-beb2-4077-a8d8-50a065dfbbfa",
            "destination": f"{DataSourceConfig.base_tmp_folder}/uai/uai-mesr-download.csv",
        },
        "menj": {
            "url": f"{DATAGOUV_URL}/api/1/datasets/5889d03fa3a72974cbf0d5b1",
            "dataset_id": "5889d03fa3a72974cbf0d5b1",
            "destination": f"{DataSourceConfig.base_tmp_folder}/uai/uai-menj-download.csv",
        },
        "onisep": {
            "url": f"{DATAGOUV_URL}/api/1/datasets/5fa5e386afdaa6152360f323",
            "dataset_id": "5fa5e386afdaa6152360f323",
            "destination": f"{DataSourceConfig.base_tmp_folder}/uai/uai-onisep-download.csv",
        },
    },
    url_minio=f"{MINIO_BASE_URL}uai/latest/uai.csv",
    url_minio_metadata=f"{MINIO_BASE_URL}uai/latest/metadata.json",
    file_output=f"{DataSourceConfig.base_tmp_folder}/uai/uai.csv",
    table_ddl="""
        BEGIN;
        CREATE TABLE IF NOT EXISTS uai
        (
            siret TEXT PRIMARY KEY,
            liste_uai TEXT
        );
        COMMIT;
    """,
)
