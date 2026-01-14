from data_pipelines_annuaire.config import OBJECT_STORAGE_BASE_URL, DataSourceConfig

FORMATION_CONFIG = DataSourceConfig(
    name="organisme_formation",
    tmp_folder=f"{DataSourceConfig.base_tmp_folder}/formation",
    object_storage_path="formation",
    file_name="formation",
    files_to_download={
        "formation": {
            "url": "https://dgefp.opendatasoft.com/api/explore/v2.1/catalog/datasets/liste"
            "-publique-des-of-v2/exports/csv?lang=fr&timezone=Europe%2FBerlin&use_labels"
            "=true&delimiter=%3B",
            "destination": f"{DataSourceConfig.base_tmp_folder}/formation/qualiopi-download.csv",
        }
    },
    url_object_storage=f"{OBJECT_STORAGE_BASE_URL}formation/latest/formation.csv",
    url_object_storage_metadata=f"{OBJECT_STORAGE_BASE_URL}formation/latest/metadata.json",
    file_output=f"{DataSourceConfig.base_tmp_folder}/formation/formation.csv",
    table_ddl="""
        BEGIN;
        CREATE TABLE IF NOT EXISTS organisme_formation
        (
            siren TEXT PRIMARY KEY,
            liste_id_organisme_formation TEXT,
            est_qualiopi INTEGER
        );
        COMMIT;
    """,
)
