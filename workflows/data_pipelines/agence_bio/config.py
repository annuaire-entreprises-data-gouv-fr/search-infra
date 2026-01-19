from data_pipelines_annuaire.config import OBJECT_STORAGE_BASE_URL, DataSourceConfig

AGENCE_BIO_CONFIG = DataSourceConfig(
    name="agence_bio",
    tmp_folder=f"{DataSourceConfig.base_tmp_folder}/agence_bio",
    object_storage_path="agence_bio",
    file_name="agence_bio_certifications",
    files_to_download={},
    url_object_storage=f"{OBJECT_STORAGE_BASE_URL}agence_bio/latest/agence_bio_certifications.csv",
    url_object_storage_metadata=f"{OBJECT_STORAGE_BASE_URL}agence_bio/latest/metadata.json",
    file_output=f"{DataSourceConfig.base_tmp_folder}/agence_bio/agence_bio.csv",
    table_ddl="""
        BEGIN;
        CREATE TABLE IF NOT EXISTS agence_bio
        (
            siret PRIMARY KEY,
            siren,
            liste_id_bio,
            statut_bio
        );
        CREATE INDEX idx_siren_agence_bio ON agence_bio (siren);
        COMMIT;
    """,
)
