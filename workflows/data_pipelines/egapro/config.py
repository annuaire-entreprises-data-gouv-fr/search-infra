from dag_datalake_sirene.config import (
    DataSourceConfig,
    BASE_TMP_FOLDER,
    DATA_GOUV_BASE_URL,
    MINIO_BASE_URL,
)


EGAPRO_CONFIG = DataSourceConfig(
    name="egapro",
    tmp_folder=f"{BASE_TMP_FOLDER}/egapro",
    minio_path="egapro",
    file_name="egapro",
    resource_id="d434859f-8d3b-4381-bcdb-ec9200653ae6",
    url=f"{DATA_GOUV_BASE_URL}d434859f-8d3b-4381-bcdb-ec9200653ae6",
    url_minio=f"{MINIO_BASE_URL}egapro/latest/egapro.csv",
    url_minio_metadata=f"{MINIO_BASE_URL}egapro/latest/metadata.json",
)