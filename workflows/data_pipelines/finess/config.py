from dag_datalake_sirene.config import (
    DataSourceConfig,
    DATA_GOUV_BASE_URL,
    MINIO_BASE_URL,
)

FINESS_CONFIG = DataSourceConfig(
    name="finess",
    tmp_folder=f"{DataSourceConfig.base_tmp_folder}/finess",
    minio_path="finess",
    file_name="finess",
    resource_id="2ce43ade-8d2c-4d1d-81da-ca06c82abc68",
    url=f"{DATA_GOUV_BASE_URL}2ce43ade-8d2c-4d1d-81da-ca06c82abc68",
    url_minio=f"{MINIO_BASE_URL}finess/latest/finess.csv",
    url_minio_metadata=f"{MINIO_BASE_URL}finess/latest/metadata.json",
)
