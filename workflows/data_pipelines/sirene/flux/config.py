from airflow.models import Variable

from data_pipelines_annuaire.config import (
    OBJECT_STORAGE_BASE_URL,
    DataSourceConfig,
)

FLUX_SIRENE_CONFIG = DataSourceConfig(
    name="flux_api_sirene",
    tmp_folder=f"{DataSourceConfig.base_tmp_folder}/sirene/flux/",
    object_storage_path="insee/flux/",
    url_object_storage=f"{OBJECT_STORAGE_BASE_URL}insee/flux/",
    url_object_storage_metadata=f"{OBJECT_STORAGE_BASE_URL}insee/flux/metadata.json",
    url_api="https://api.insee.fr/api-sirene/3.11/",
    auth_api=Variable.get("SECRET_BEARER_INSEE", None),
)
