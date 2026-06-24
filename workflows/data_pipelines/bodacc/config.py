from data_pipelines_annuaire.config import (
    OBJECT_STORAGE_BASE_URL,
    DataSourceConfig,
)

BODACC_CONFIG = DataSourceConfig(
    name="bodacc",
    tmp_folder=f"{DataSourceConfig.base_tmp_folder}/bodacc",
    object_storage_path="bodacc",
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
        "creations": {
            "huwise": True,
            "url": "https://bodacc-datadila.opendatasoft.com/api/explore/v2.1/catalog/datasets/annonces-commerciales/exports/csv?lang=fr&refine=familleavis%3A%22creation%22&timezone=Europe%2FBerlin&use_labels=true&delimiter=%3B",
            "destination": f"{DataSourceConfig.base_tmp_folder}/bodacc/creations-raw.csv",
        },
    },
)

RADIATIONS_CONFIG = DataSourceConfig(
    name="bodacc_radiations",
    tmp_folder=BODACC_CONFIG.tmp_folder,
    object_storage_path=f"{BODACC_CONFIG.object_storage_path}/radiations",
    file_name="radiations",
    url_object_storage=f"{OBJECT_STORAGE_BASE_URL}bodacc/radiations/latest/radiations.csv",
    url_object_storage_metadata=f"{OBJECT_STORAGE_BASE_URL}bodacc/radiations/latest/metadata.json",
    table_ddl="""
        BEGIN;
        CREATE TABLE IF NOT EXISTS bodacc_radiations
        (
            siren TEXT PRIMARY KEY,
            id_annonce TEXT,
            est_radie INTEGER,
            date DATE,
            date_publication DATE,

            visibility INTEGER DEFAULT 1,
            visibility_reason TEXT DEFAULT 'visible_by_default'
        );
        COMMIT;
    """,
    post_processing_queries=[
        """
            UPDATE bodacc_radiations
            SET visibility = 0,
                visibility_reason = 'ei_active_on_sirene'
            WHERE bodacc_radiations.est_radie
            AND siren IN (
                SELECT ul.siren
                FROM unite_legale AS ul
                INNER JOIN etablissement AS e ON e.siren = ul.siren
                WHERE bodacc_radiations.siren = ul.siren
                AND ul.nature_juridique_unite_legale = '1000'
                AND ul.etat_administratif_unite_legale = 'A'
                AND e.etat_administratif_etablissement = 'A'
            )
            """,
        # radiation_visibility et radiation_visibility_reason sont alimentés par la
        # task de postprocessing dans le DAG d'ETL pour masquer les radiations correspondantes
        """
            UPDATE bodacc_radiations
            SET visibility = 0,
                visibility_reason = 'ei_with_new_etab_since_radiation'
            WHERE bodacc_radiations.est_radie
            AND siren IN (
                SELECT ul.siren
                FROM unite_legale AS ul
                INNER JOIN etablissement AS e ON e.siren = ul.siren
                left join immatriculation AS i on i.siren = ul.siren
                WHERE bodacc_radiations.siren = ul.siren
                AND ul.nature_juridique_unite_legale = '1000'
                AND (
                    e.date_creation >= bodacc_radiations.date
                 OR e.date_debut_activite >= bodacc_radiations.date
                 OR i.date_immatriculation >= bodacc_radiations.date
                 )
            )
            """,
        # Une création publiée au BODACC postérieure à la radiation indique que
        # l'entité est de nouveau active : on masque la radiation correspondante.
        # Seules les PP portent une date de radiation, les PM (date nulle) ne
        # matchent donc jamais la comparaison.
        """
            UPDATE bodacc_radiations
            SET visibility = 0,
                visibility_reason = 'ei_with_bodacc_creation_since_radiation'
            WHERE bodacc_radiations.est_radiee
            AND EXISTS (
                SELECT 1
                FROM bodacc_creations AS c
                WHERE c.siren = bodacc_radiations.siren
                AND c.date >= bodacc_radiations.date
            )
            """,
    ],
)

CREATIONS_CONFIG = DataSourceConfig(
    name="bodacc_creations",
    tmp_folder=BODACC_CONFIG.tmp_folder,
    object_storage_path=f"{BODACC_CONFIG.object_storage_path}/creations",
    file_name="creations",
    url_object_storage=f"{OBJECT_STORAGE_BASE_URL}bodacc/creations/latest/creations.csv",
    url_object_storage_metadata=f"{OBJECT_STORAGE_BASE_URL}bodacc/creations/latest/metadata.json",
    table_ddl="""
        BEGIN;
        CREATE TABLE IF NOT EXISTS bodacc_creations
        (
            siren TEXT,
            id_annonce TEXT,
            date DATE,
            date_publication DATE
        );
        CREATE INDEX IF NOT EXISTS idx_bodacc_creations_siren
            ON bodacc_creations (siren);
        COMMIT;
    """,
)

PROCEDURES_COLLECTIVES_CONFIG = DataSourceConfig(
    name="bodacc_procedures_collectives",
    tmp_folder=BODACC_CONFIG.tmp_folder,
    object_storage_path=f"{BODACC_CONFIG.object_storage_path}/procedures_collectives",
    file_name="procedures_collectives",
    url_object_storage=f"{OBJECT_STORAGE_BASE_URL}bodacc/procedures_collectives/latest/procedures_collectives.csv",
    url_object_storage_metadata=f"{OBJECT_STORAGE_BASE_URL}bodacc/procedures_collectives/latest/metadata.json",
    table_ddl="""
        BEGIN;
        CREATE TABLE IF NOT EXISTS bodacc_procedures_collectives
        (
            siren TEXT PRIMARY KEY,
            id_annonce TEXT,
            statut TEXT,
            nature TEXT,
            complement TEXT,
            date DATE,
            date_publication DATE,
            cloturee_nature TEXT
        );
        COMMIT;
    """,
)
