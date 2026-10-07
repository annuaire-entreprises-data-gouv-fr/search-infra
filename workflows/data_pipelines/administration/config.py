from data_pipelines_annuaire.config import (
    OBJECT_STORAGE_BASE_URL,
    DataSourceConfig,
)

ADMINISTRATION_CONFIG = DataSourceConfig(
    name="administration",
    tmp_folder=f"{DataSourceConfig.base_tmp_folder}/administration",
    object_storage_path="administration",
    file_name="administration",
)

# The lists are edited by hand in Grist and may contain duplicated or malformed
# SIREN: indexes rather than primary keys, so that the ETL does not fail on them.
ADMINISTRATION_CODES_JURIDIQUES_CONFIG = DataSourceConfig(
    name="administration_codes_juridiques",
    tmp_folder=ADMINISTRATION_CONFIG.tmp_folder,
    object_storage_path=ADMINISTRATION_CONFIG.object_storage_path,
    file_name="administration_codes_juridiques",
    url_object_storage=f"{OBJECT_STORAGE_BASE_URL}administration/latest/administration_codes_juridiques.csv",
    table_ddl="""
        BEGIN;
        CREATE TABLE IF NOT EXISTS administration_codes_juridiques
        (
            code_juridique TEXT,
            libelle TEXT,
            mission_de_service_public_administratif TEXT,
            administration_de_l_etat_services_centraux_deconcentres_et_criteres_de_regie_ou_quasi_regie_ TEXT,
            collectivites TEXT
        );
        CREATE INDEX idx_code_juridique_administration_codes_juridiques
            ON administration_codes_juridiques (code_juridique);
        COMMIT;
    """,
)

ADMINISTRATION_WHITELIST_CONFIG = DataSourceConfig(
    name="administration_whitelist_siren",
    tmp_folder=ADMINISTRATION_CONFIG.tmp_folder,
    object_storage_path=ADMINISTRATION_CONFIG.object_storage_path,
    file_name="administration_whitelist_siren",
    url_object_storage=f"{OBJECT_STORAGE_BASE_URL}administration/latest/administration_whitelist_siren.csv",
    table_ddl="""
        BEGIN;
        CREATE TABLE IF NOT EXISTS administration_whitelist_siren
        (
            siren TEXT,
            denomination TEXT,
            datapass TEXT,
            motivation TEXT,
            administration_de_l_etat_services_centraux_deconcentres_et_criteres_de_regie_ou_quasi_regie_ TEXT,
            date_d_inscription TEXT
        );
        CREATE INDEX idx_siren_administration_whitelist_siren
            ON administration_whitelist_siren (siren);
        COMMIT;
    """,
)

ADMINISTRATION_BLACKLIST_CONFIG = DataSourceConfig(
    name="administration_blacklist_siren",
    tmp_folder=ADMINISTRATION_CONFIG.tmp_folder,
    object_storage_path=ADMINISTRATION_CONFIG.object_storage_path,
    file_name="administration_blacklist_siren",
    url_object_storage=f"{OBJECT_STORAGE_BASE_URL}administration/latest/administration_blacklist_siren.csv",
    table_ddl="""
        BEGIN;
        CREATE TABLE IF NOT EXISTS administration_blacklist_siren
        (
            siren TEXT,
            denomination TEXT
        );
        CREATE INDEX idx_siren_administration_blacklist_siren
            ON administration_blacklist_siren (siren);
        COMMIT;
    """,
)
