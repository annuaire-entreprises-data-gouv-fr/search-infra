import logging
from dag_datalake_sirene.helpers.sqlite_client import SqliteClient

# fmt: off
from dag_datalake_sirene.workflows.data_pipelines.etl.data_fetch_clean.unite_legale\
    import (
    preprocess_unite_legale_data,
)
from dag_datalake_sirene.workflows.data_pipelines.etl.sqlite.queries.unite_legale\
    import (
    create_table_flux_unite_legale_query,
    create_table_unite_legale_query,
    replace_table_unite_legale_query,
)
# fmt: on
from dag_datalake_sirene.workflows.data_pipelines.etl.sqlite.helpers import (
    create_table_model,
    create_unique_index,
    get_table_count,
    replace_table_model,
)
from dag_datalake_sirene.config import AIRFLOW_ETL_DATA_DIR
from dag_datalake_sirene.config import (
    SIRENE_DATABASE_LOCATION,
    RNE_DATABASE_LOCATION,
)


def create_table(query, table_name, index, sirene_file_type):
    sqlite_client = create_table_model(
        table_name=table_name,
        create_table_query=query,
        create_index_func=create_unique_index,
        index_name=index,
        index_column="siren",
    )
    for df_unite_legale in preprocess_unite_legale_data(
        AIRFLOW_ETL_DATA_DIR, sirene_file_type
    ):
        df_unite_legale.to_sql(
            table_name, sqlite_client.db_conn, if_exists="append", index=False
        )
        for row in sqlite_client.execute(get_table_count(table_name)):
            logging.debug(
                f"************ {row} total records have been added "
                f"to the {table_name} table!"
            )

    del df_unite_legale

    for count_unites_legales in sqlite_client.execute(get_table_count(table_name)):
        logging.info(
            f"************ {count_unites_legales} total records have been added to the "
            f"{table_name} table!"
        )
    sqlite_client.commit_and_close_conn()
    return count_unites_legales[0]


def create_unite_legale_table(**kwargs):
    counts = create_table(
        create_table_unite_legale_query,
        "unite_legale",
        "index_siren",
        "stock",
    )
    kwargs["ti"].xcom_push(key="count_unites_legales", value=counts)


def create_flux_unite_legale_table(**kwargs):
    counts = create_table(
        create_table_flux_unite_legale_query,
        "flux_unite_legale",
        "index_flux_siren",
        "flux",
    )
    kwargs["ti"].xcom_push(key="count_flux_unites_legales", value=counts)


def replace_unite_legale_table():
    sqlite_client = replace_table_model(
        replace_table_query=replace_table_unite_legale_query,
    )
    sqlite_client.commit_and_close_conn()


def add_rne_siren_data_to_unite_legale_table(**kwargs):
    # Connect to the first database
    sqlite_client_siren = SqliteClient(SIRENE_DATABASE_LOCATION)

    # Attach the second database
    sqlite_client_siren.execute(f"ATTACH DATABASE '{RNE_DATABASE_LOCATION}' AS db_rne")

    # Add a new column 'from_rne' to table1 if not exists
    sqlite_client_siren.execute("PRAGMA foreign_keys=off")
    sqlite_client_siren.execute("BEGIN TRANSACTION")
    sqlite_client_siren.execute(
        "ALTER TABLE unite_legale ADD COLUMN from_rne BOOLEAN DEFAULT FALSE"
    )
    sqlite_client_siren.execute("COMMIT")
    sqlite_client_siren.execute("PRAGMA foreign_keys=on")

    # Update existing rows in table1 based on siren from table2
    update_query = """
        UPDATE unite_legale
        SET from_rne = TRUE
        WHERE siren IN (SELECT siren FROM db_rne.unites_legales)
    """
    sqlite_client_siren.execute(update_query)

    # Insert new rows from table2 into table1
    insert_query = """
        INSERT INTO unite_legale
        SELECT DISTINCT
            siren,
            date_creation AS date_creation_unite_legale,
            NULL AS sigle,
            NULL AS prenom,
            NULL AS identifiant_association_unite_legale,
            tranche_effectif_salarie AS tranche_effectif_salarie_unite_legale,
            date_mise_a_jour AS date_mise_a_jour_unite_legale,
            NULL AS categorie_entreprise,
            etat_administratif AS etat_administratif_unite_legale,
            NULL AS nom,
            NULL AS nom_usage,
            NULL AS nom_raison_sociale,
            NULL AS denomination_usuelle_1,
            NULL AS denomination_usuelle_2,
            NULL AS denomination_usuelle_3,
            nature_juridique AS nature_juridique_unite_legale,
            activite_principale AS activite_principale_unite_legale,
            NULL AS economie_sociale_solidaire_unite_legale,
            statut_diffusion AS statut_diffusion_unite_legale,
            NULL AS est_societe_mission,
            NULL AS annee_categorie_entreprise,
            NULL AS annee_tranche_effectif_salarie,
            NULL AS caractere_employeur,
            FALSE AS from_insee,
            TRUE AS from_rne
        FROM db_rne.unites_legales
        WHERE siren NOT IN (SELECT siren FROM unite_legale)
    """
    sqlite_client_siren.execute(insert_query)

    sqlite_client_siren.commit_and_close_conn()
