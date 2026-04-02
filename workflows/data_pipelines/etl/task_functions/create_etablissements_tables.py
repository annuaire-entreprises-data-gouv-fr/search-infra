import logging

from airflow.sdk import get_current_context, task

from data_pipelines_annuaire.config import (
    AIRFLOW_ETL_DATA_DIR,
    SIRENE_DATABASE_LOCATION,
)
from data_pipelines_annuaire.helpers.data_processor import Notification
from data_pipelines_annuaire.helpers.sqlite_client import SqliteClient
from data_pipelines_annuaire.workflows.data_pipelines.etl.data_fetch_clean.etablissements import (
    preprocess_flux_etablissement_data,
    preprocess_flux_periodes_data,
    preprocess_geo_stats_data,
    preprocess_historique_etablissement_data,
    preprocess_stock_etablissement_data,
)
from data_pipelines_annuaire.workflows.data_pipelines.etl.sqlite.helpers import (
    create_index,
    create_table_model,
    create_unique_index,
    execute_query,
    get_table_count,
)
from data_pipelines_annuaire.workflows.data_pipelines.etl.sqlite.queries.etablissements import (
    count_nombre_etablissement_ouvert_query,
    count_nombre_etablissement_query,
    create_table_count_etablissement_ouvert_query,
    create_table_count_etablissement_query,
    create_table_date_fermeture_etablissement_query,
    create_table_etablissement_query,
    create_table_flux_etablissement_query,
    create_table_geo_stats_query,
    create_table_historique_etablissement_query,
    insert_date_fermeture_etablissement_query,
    replace_table_etablissement_query,
    update_etablissement_coordinates_from_geo_stats_query,
)


@task
def create_etablissement_table():
    sqlite_client = create_table_model(
        table_name="etablissement",
        create_table_query=create_table_etablissement_query,
        create_index_func=create_index,
        index_name="index_etablissement",
        index_column="siren",
    )

    for df_chunk in preprocess_stock_etablissement_data():
        df_chunk["coord_source"] = df_chunk.apply(
            lambda row: (
                "stock"
                if row["latitude"] is not None and row["longitude"] is not None
                else None
            ),
            axis=1,
        )
        df_chunk.to_sql(
            "etablissement", sqlite_client.db_conn, if_exists="append", index=False
        )
        for row in sqlite_client.execute(get_table_count("etablissement")):
            logging.debug(
                f"************ {row} records have been added to the "
                f"`établissements` table!"
            )
        del df_chunk

    for count_etablissement in sqlite_client.execute(get_table_count("etablissement")):
        logging.info(
            f"************ {count_etablissement} total records have been added to the "
            f"siret table!"
        )
    sqlite_client.commit_and_close_conn()


@task
def create_flux_etablissement_table():
    sqlite_client = create_table_model(
        table_name="flux_etablissement",
        create_table_query=create_table_flux_etablissement_query,
        create_index_func=create_index,
        index_name="index_flux_etablissement",
        index_column="siren",
    )
    for df_chunk in preprocess_flux_etablissement_data(AIRFLOW_ETL_DATA_DIR):
        df_chunk["coord_source"] = "flux"
        df_chunk.to_sql(
            "flux_etablissement",
            sqlite_client.db_conn,
            if_exists="append",
            index=False,
        )
    for row in sqlite_client.execute(get_table_count("flux_etablissement")):
        logging.info(
            f"************ {row} total records have been added to the "
            f"`flux établissements` table!"
        )
    sqlite_client.commit_and_close_conn()


@task
def replace_etablissement_table():
    execute_query(
        query=replace_table_etablissement_query,
    )


@task
def count_nombre_etablissement():
    sqlite_client = create_table_model(
        table_name="count_etablissement",
        create_table_query=create_table_count_etablissement_query,
        create_index_func=create_unique_index,
        index_name="index_count_siren",
        index_column="siren",
    )

    sqlite_client.execute(count_nombre_etablissement_query)
    sqlite_client.commit_and_close_conn()


@task
def count_nombre_etablissement_ouvert():
    sqlite_client = create_table_model(
        table_name="count_etablissement_ouvert",
        create_table_query=create_table_count_etablissement_ouvert_query,
        create_index_func=create_unique_index,
        index_name="index_count_ouvert_siren",
        index_column="siren",
    )
    sqlite_client.execute(count_nombre_etablissement_ouvert_query)
    sqlite_client.commit_and_close_conn()


@task
def create_historique_etablissement_table(ti):
    table_name = "historique_etablissement"
    sqlite_client = create_table_model(
        table_name=table_name,
        create_table_query=create_table_historique_etablissement_query,
        create_index_func=create_index,
        index_name="index_historique_siret",
        index_column="siret",
    )

    # Process stock periodes
    for df_hist_etablissement in preprocess_historique_etablissement_data(
        AIRFLOW_ETL_DATA_DIR,
    ):
        df_hist_etablissement.to_sql(
            table_name, sqlite_client.db_conn, if_exists="append", index=False
        )
        for row in sqlite_client.execute(get_table_count(table_name)):
            logging.debug(
                f"************ {row} total records have been added "
                f"to the {table_name} table!"
            )

    df_flux_periodes = preprocess_flux_periodes_data(AIRFLOW_ETL_DATA_DIR)

    if not df_flux_periodes.empty:
        flux_sirets = set(df_flux_periodes["siret"].unique())

        # Delete existing stock periodes for sirets present in flux
        if flux_sirets:
            flux_sirets_list = list(flux_sirets)
            batch_size = 10_000
            # Batch deletes to avoid SQLite "too many SQL variables" error
            for i in range(0, len(flux_sirets_list), batch_size):
                batch = flux_sirets_list[i : i + batch_size]
                placeholders = ",".join(["?"] * len(batch))
                delete_query = (
                    f"DELETE FROM {table_name} WHERE siret IN ({placeholders})"
                )

                sqlite_client.execute(delete_query, batch)
            logging.info(
                f"Deleted stock periodes for {len(flux_sirets)} sirets present in flux"
            )

        # Insert all the periodes of the etablissement retrieved from the flux
        df_flux_periodes.to_sql(
            table_name, sqlite_client.db_conn, if_exists="append", index=False
        )
        logging.info(
            f"Added {len(df_flux_periodes)} flux periodes to {table_name} table"
        )

    del df_hist_etablissement

    for count_etablissement in sqlite_client.execute(get_table_count(table_name)):
        logging.info(
            f"************ {count_etablissement} total records have been added to the "
            f"{table_name} table!"
        )
    sqlite_client.commit_and_close_conn()
    ti.xcom_push(key="count_historique_etablissement", value=count_etablissement)


@task
def create_date_fermeture_etablissement_table(ti):
    table_name = "date_fermeture_etablissement"
    sqlite_client = create_table_model(
        table_name=table_name,
        create_table_query=create_table_date_fermeture_etablissement_query,
        create_index_func=create_unique_index,
        index_name="index_date_fermeture_siret",
        index_column="siret",
    )

    for count_etablissement in sqlite_client.execute(get_table_count(table_name)):
        logging.info(
            f"************ {count_etablissement} total records have been added to the "
            f"{table_name} table!"
        )
    sqlite_client.commit_and_close_conn()
    ti.xcom_push(key="count_date_fermeture_etablissement", value=count_etablissement)


@task
def create_geo_stats_table():
    sqlite_client = create_table_model(
        table_name="geo_stats",
        create_table_query=create_table_geo_stats_query,
        create_index_func=create_unique_index,
        index_name="index_geo_stats_siret",
        index_column="siret",
    )

    for df_chunk in preprocess_geo_stats_data(AIRFLOW_ETL_DATA_DIR):
        df_chunk.to_sql(
            "geo_stats", sqlite_client.db_conn, if_exists="append", index=False
        )
        for row in sqlite_client.execute(get_table_count("geo_stats")):
            logging.debug(
                f"************ {row} records have been added to the `geo_stats` table!"
            )
        del df_chunk

    for count_geo_stats in sqlite_client.execute(get_table_count("geo_stats")):
        logging.info(
            f"************ {count_geo_stats} total records have been added to the "
            f"geo_stats table!"
        )
    sqlite_client.commit_and_close_conn()


@task
def insert_date_fermeture_etablissement():
    sqlite_client = SqliteClient(SIRENE_DATABASE_LOCATION)
    sqlite_client.execute(insert_date_fermeture_etablissement_query)
    sqlite_client.commit_and_close_conn()


@task
def apply_geo_stats_coordinates():
    sqlite_client = SqliteClient(SIRENE_DATABASE_LOCATION)
    sqlite_client.execute(update_etablissement_coordinates_from_geo_stats_query)
    logging.info("Applied geo_stats coordinates to etablissement table")

    stats = {}

    for row in sqlite_client.execute(
        "SELECT COUNT(*) FROM etablissement WHERE coord_source = 'geo_stats'"
    ):
        stats["geo_stats"] = row[0]

    for row in sqlite_client.execute(
        "SELECT COUNT(*) FROM etablissement WHERE coord_source = 'flux'"
    ):
        stats["flux"] = row[0]

    for row in sqlite_client.execute(
        "SELECT COUNT(*) FROM etablissement WHERE coord_source = 'stock'"
    ):
        stats["stock"] = row[0]

    for row in sqlite_client.execute(
        "SELECT COUNT(*) FROM etablissement WHERE coord_source IS NULL"
    ):
        stats["null"] = row[0]

    for row in sqlite_client.execute("SELECT COUNT(*) FROM etablissement"):
        stats["total"] = row[0]

    sqlite_client.commit_and_close_conn()

    message = (
        f"📍 Statistiques de géocodage:"
        f"<ul><li>geo_stats: {stats['geo_stats']:,}</li>"
        f"<li>flux: {stats['flux']:,}</li>"
        f"<li>stock: {stats['stock']:,}</li>"
        f"<li>sans coordonnées: {stats['null']:,}</li>"
        f"<li>total: {stats['total']:,}</li></ul>"
    )
    logging.info(message)

    ti = get_current_context()["ti"]
    ti.xcom_push(key=Notification.notification_xcom_key, value=message)
