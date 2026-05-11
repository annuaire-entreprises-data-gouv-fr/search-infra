from data_pipelines_annuaire.config import (
    OBJECT_STORAGE_BASE_URL,
    DataSourceConfig,
)

BODACC_CONFIG = DataSourceConfig(
    name="bodacc",
    tmp_folder=f"{DataSourceConfig.base_tmp_folder}/bodacc",
    object_storage_path="bodacc",
    file_name="bodacc",
    files_to_download={
        "procedures_collectives": {
            "huwise": True,
            "url": "https://bodacc-datadila.opendatasoft.com/api/explore/v2.1/catalog/datasets/annonces-commerciales/exports/csv?lang=fr&refine=familleavis_lib%3A%22Proc%C3%A9dures%20collectives%22&timezone=Europe%2FParis&use_labels=true&delimiter=%3B",
            "destination": f"{DataSourceConfig.base_tmp_folder}/bodacc/procedures-collectives-raw.csv",
        },
        "radiations": {
            "huwise": True,
            "url": "https://bodacc-datadila.opendatasoft.com/api/explore/v2.1/catalog/datasets/annonces-commerciales/exports/csv?lang=fr&refine=familleavis%3A%22radiation%22&timezone=Europe%2FBerlin&use_labels=true&delimiter=%3B",
            "destination": f"{DataSourceConfig.base_tmp_folder}/bodacc/radiations-raw.csv",
        },
    },
    url_object_storage=f"{OBJECT_STORAGE_BASE_URL}bodacc/latest/bodacc.csv",
    url_object_storage_metadata=f"{OBJECT_STORAGE_BASE_URL}bodacc/latest/metadata.json",
    table_ddl="""
        BEGIN;
        CREATE TABLE IF NOT EXISTS bodacc
        (
            siren TEXT PRIMARY KEY,
            radiation_rcs_id TEXT,
            radiation_rcs INTEGER,
            radiation_rcs_date DATE,
            radiation_rcs_date_publication DATE,
            procedure_collective_id TEXT,
            procedure_collective_nature TEXT,
            procedure_collective_date_jugement DATE,
            procedure_collective_date_publication DATE,

            procedure_collective_cloturee_nature TEXT
        );
        COMMIT;
    """,
)
