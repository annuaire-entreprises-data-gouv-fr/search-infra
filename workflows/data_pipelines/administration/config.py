from data_pipelines_annuaire.config import (
    DataSourceConfig,
)

ADMINISTRATION_CONFIG = DataSourceConfig(
    name="administration",
    tmp_folder=f"{DataSourceConfig.base_tmp_folder}/administration",
    object_storage_path="administration",
    file_name="administration",
)
