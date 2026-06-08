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
    # radiation_visibility et radiation_visibility_reason sont alimentés par la
    # task de postprocessing dans le DAG d'ETL pour masquer certaines radiations
    table_ddl="""
        BEGIN;
        CREATE TABLE IF NOT EXISTS bodacc
        (
            siren TEXT PRIMARY KEY,
            radiation_id_annonce TEXT,
            radiation_est_radie INTEGER,
            radiation_date DATE,
            radiation_date_publication DATE,
            procedure_collective_id_annonce TEXT,
            procedure_collective_statut TEXT,
            procedure_collective_nature TEXT,
            procedure_collective_complement TEXT,
            procedure_collective_date DATE,
            procedure_collective_date_publication DATE,

            procedure_collective_cloturee_nature TEXT,

            radiation_visibility INTEGER DEFAULT 1,
            radiation_visibility_reason TEXT DEFAULT 'visible_by_default'
        );
        COMMIT;
    """,
    post_processing_queries=[
        """
            UPDATE bodacc
            SET radiation_visibility = 0,
                radiation_visibility_reason = 'ei_active_on_sirene'
            WHERE bodacc.radiation_est_radie
            AND siren IN (
                SELECT ul.siren
                FROM unite_legale AS ul
                INNER JOIN etablissement AS e ON e.siren = ul.siren
                WHERE bodacc.siren = ul.siren
                AND ul.nature_juridique_unite_legale = '1000'
                AND ul.etat_administratif_unite_legale = 'A'
                AND e.etat_administratif_etablissement = 'A'
            )
            """,
        """
            UPDATE bodacc
            SET radiation_visibility = 0,
                radiation_visibility_reason = 'ei_with_new_etab_since_radiation'
            WHERE bodacc.radiation_est_radie
            AND siren IN (
                SELECT ul.siren
                FROM unite_legale AS ul
                INNER JOIN etablissement AS e ON e.siren = ul.siren
                left join immatriculation AS i on i.siren = ul.siren
                WHERE bodacc.siren = ul.siren
                AND ul.nature_juridique_unite_legale = '1000'
                AND (
                    e.date_creation >= bodacc.radiation_date
                 OR e.date_debut_activite >= bodacc.radiation_date
                 OR i.date_immatriculation >= bodacc.radiation_date
                 )
            )
            """,
    ],
)
