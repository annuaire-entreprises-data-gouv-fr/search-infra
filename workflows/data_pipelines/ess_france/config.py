from data_pipelines_annuaire.config import (
    OBJECT_STORAGE_BASE_URL,
    DataSourceConfig,
)

ESS_FRANCE_PAGE_URL = "https://www.ess-france.org/fr/la-liste-des-entreprises-de-less"

ESS_CONFIG = DataSourceConfig(
    name="ess_france",
    tmp_folder=f"{DataSourceConfig.base_tmp_folder}/ess",
    object_storage_path="ess",
    file_name="ess",
    url_object_storage=f"{OBJECT_STORAGE_BASE_URL}ess/latest/ess.csv",
    url_object_storage_metadata=f"{OBJECT_STORAGE_BASE_URL}ess/latest/metadata.json",
    table_ddl="""
        BEGIN;
        CREATE TABLE IF NOT EXISTS ess_france
        (
            siren TEXT PRIMARY KEY,
            est_ess_france INTEGER
        );
        COMMIT;
    """,
)
