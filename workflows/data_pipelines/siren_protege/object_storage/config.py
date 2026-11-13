from data_pipelines_annuaire.config import DataSourceConfig

SIREN_PROTEGE_DAG_NAME = "data_processing_siren_protege"

SIREN_PROTEGE_CONFIG = DataSourceConfig(
    name="siren_protege",
    tmp_folder=f"{DataSourceConfig.base_tmp_folder}/siren_protege",
    object_storage_path="siren_protege",
    file_name="siren_protege",
    # Names and birth dates of people who opposed the publication of their data
    is_public=False,
)
