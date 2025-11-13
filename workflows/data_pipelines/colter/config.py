from dag_datalake_sirene.config import (
    DATA_GOUV_BASE_URL,
    MINIO_BASE_URL,
    DataSourceConfig,
)

COLTER_CONFIG = DataSourceConfig(
    name="colter",
    tmp_folder=f"{DataSourceConfig.base_tmp_folder}/colter",
    minio_path="colter",
    file_name="colter",
    files_to_download={
        "colter_regions": {
            "url": f"{DATA_GOUV_BASE_URL}619ee62e-8f9e-4c62-b166-abc6f2b86201",
            "resource_id": "619ee62e-8f9e-4c62-b166-abc6f2b86201",
            "destination": f"{DataSourceConfig.base_tmp_folder}/colter/regions-download.csv",
        },
        "colter_deps": {
            "url": f"{DATA_GOUV_BASE_URL}2f4f901d-e3ce-4760-b122-56a311340fc4",
            "resource_id": "2f4f901d-e3ce-4760-b122-56a311340fc4",
            "destination": f"{DataSourceConfig.base_tmp_folder}/colter/deps-download.csv",
        },
        "colter_epci": {
            "url": "https://www.collectivites-locales.gouv.fr/etudes-et-statistiques/acces-par-thematique/perimetre-des-intercommunalites/liste-et-composition-des-epci-fiscalite-propre",
            "pattern": "Liste des EPCI au 1er janvier %%current_year%% (xls)",
            "destination": f"{DataSourceConfig.base_tmp_folder}/colter/epci-download.xlsx",
        },
        "colter_communes": {
            "url": f"{DATA_GOUV_BASE_URL}1e3493b3-7fc0-4005-8aa7-240bdf17e617",
            "resource_id": "1e3493b3-7fc0-4005-8aa7-240bdf17e617",
            "destination": f"{DataSourceConfig.base_tmp_folder}/colter/communes-download.csv",
        },
    },
    url_minio=f"{MINIO_BASE_URL}colter/latest/colter.csv",
    url_minio_metadata=f"{MINIO_BASE_URL}colter/latest/metadata.json",
    file_output=f"{DataSourceConfig.base_tmp_folder}/colter/colter.csv",
    # No unique key in the data
    table_ddl="""
        BEGIN;
        CREATE TABLE IF NOT EXISTS colter
        (
            siren TEXT,
            colter_code TEXT,
            colter_code_insee TEXT,
            colter_niveau TEXT
        );
        CREATE INDEX idx_siren_colter ON colter (siren);
        COMMIT;
    """,
)


ELUS_CONFIG = DataSourceConfig(
    name="elus",
    tmp_folder=f"{DataSourceConfig.base_tmp_folder}/colter_elus",
    minio_path="colter_elus",
    file_name="elus",
    files_to_download={
        "assemblee_col_statut_particulier": {
            "url": f"{DATA_GOUV_BASE_URL}a595be27-cfab-4810-b9d4-22e193bffe35",
            "resource_id": "a595be27-cfab-4810-b9d4-22e193bffe35",
        },
        "conseillers_regionaux": {
            "url": f"{DATA_GOUV_BASE_URL}430e13f9-834b-4411-a1a8-da0b4b6e715c",
            "resource_id": "430e13f9-834b-4411-a1a8-da0b4b6e715c",
        },
        "conseillers_departementaux": {
            "url": f"{DATA_GOUV_BASE_URL}601ef073-d986-4582-8e1a-ed14dc857fba",
            "resource_id": "601ef073-d986-4582-8e1a-ed14dc857fba",
        },
        "conseillers_municipaux": {
            "url": f"{DATA_GOUV_BASE_URL}d5f400de-ae3f-4966-8cb6-a85c70c6c24a",
            "resource_id": "d5f400de-ae3f-4966-8cb6-a85c70c6c24a",
        },
        "elus_epci": {
            "url": f"{DATA_GOUV_BASE_URL}41d95d7d-b172-4636-ac44-32656367cdc7",
            "resource_id": "41d95d7d-b172-4636-ac44-32656367cdc7",
        },
    },
    url_minio=f"{MINIO_BASE_URL}colter_elus/latest/elus.csv",
    url_minio_metadata=f"{MINIO_BASE_URL}colter_elus/latest/metadata.json",
    file_output=f"{DataSourceConfig.base_tmp_folder}/colter_elus/elus.csv",
    # No unique key in the data
    table_ddl="""
        BEGIN;
        CREATE TABLE IF NOT EXISTS elus
        (
            siren TEXT,
            nom TEXT,
            prenom TEXT,
            date_naissance TEXT,
            sexe TEXT,
            fonction TEXT
        );
        CREATE INDEX idx_siren_elus ON elus (siren);
        COMMIT;
    """,
)
