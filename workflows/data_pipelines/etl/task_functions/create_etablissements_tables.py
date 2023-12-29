import logging

from dag_datalake_sirene.helpers.labels.departements import all_deps

# fmt: off
from dag_datalake_sirene.workflows.data_pipelines.etl.data_fetch_clean.etablissements\
    import preprocess_etablissements_data
from dag_datalake_sirene.workflows.data_pipelines.etl.sqlite.helpers import (
    get_table_count,
    create_index,
    replace_table_model,
    create_table_model,
    create_unique_index,
)
from dag_datalake_sirene.workflows.data_pipelines.etl.sqlite.queries.etablissements\
    import (
    create_table_flux_etablissements_query,
    create_table_etablissements_query,
    create_table_siret_siege_query,
    populate_table_siret_siege_query,
    replace_table_etablissement_query,
    replace_table_siret_siege_query,
    create_table_count_etablissements_query,
    count_nombre_etablissements_query,
    create_table_count_etablissements_ouverts_query,
    count_nombre_etablissements_ouverts_query,
)
# fmt: on
from dag_datalake_sirene.config import AIRFLOW_ETL_DATA_DIR


def create_etablissements_table():
    sqlite_client = create_table_model(
        table_name="siret",
        create_table_query=create_table_etablissements_query,
        create_index_func=create_index,
        index_name="index_siret",
        index_column="siren",
    )
    # Upload geo data by departement
    for dep in all_deps:
        df_dep = preprocess_etablissements_data("stock", dep, None)
        df_dep.to_sql("siret", sqlite_client.db_conn, if_exists="append", index=False)
        for row in sqlite_client.execute(get_table_count("siret")):
            logging.debug(
                f"************ {row} total records have been added to the "
                f"`établissements` table!"
            )
    del df_dep
    sqlite_client.commit_and_close_conn()


def create_flux_etablissements_table():
    sqlite_client = create_table_model(
        table_name="flux_siret",
        create_table_query=create_table_flux_etablissements_query,
        create_index_func=create_index,
        index_name="index_flux_siret",
        index_column="siren",
    )
    # Upload flux data
    df_siret = preprocess_etablissements_data("flux", None, AIRFLOW_ETL_DATA_DIR)
    df_siret.to_sql(
        "flux_siret",
        sqlite_client.db_conn,
        if_exists="append",
        index=False,
    )
    for row in sqlite_client.execute(get_table_count("flux_siret")):
        logging.info(
            f"************ {row} total records have been added to the "
            f"`flux établissements` table!"
        )
    del df_siret
    sqlite_client.commit_and_close_conn()


def create_siege_only_table(**kwargs):
    sqlite_client = create_table_model(
        table_name="siretsiege",
        create_table_query=create_table_siret_siege_query,
        create_index_func=create_index,
        index_name="index_siret_siren",
        index_column="siren",
    )
    sqlite_client.execute(populate_table_siret_siege_query)
    for count_sieges in sqlite_client.execute(get_table_count("siretsiege")):
        logging.info(
            f"************ {count_sieges} total records have been added to the "
            f"unité légale table!"
        )
    kwargs["ti"].xcom_push(key="count_sieges", value=count_sieges[0])
    sqlite_client.commit_and_close_conn()


def replace_etablissements_table():
    sqlite_client = replace_table_model(
        replace_table_query=replace_table_etablissement_query,
    )
    sqlite_client.commit_and_close_conn()


def replace_siege_only_table():
    sqlite_client = replace_table_model(
        replace_table_query=replace_table_siret_siege_query,
    )
    sqlite_client.commit_and_close_conn()


def count_nombre_etablissements():
    sqlite_client = create_table_model(
        table_name="count_etab",
        create_table_query=create_table_count_etablissements_query,
        create_index_func=create_unique_index,
        index_name="index_count_siren",
        index_column="siren",
    )

    sqlite_client.execute(count_nombre_etablissements_query)
    sqlite_client.commit_and_close_conn()


def count_nombre_etablissements_ouverts():
    sqlite_client = create_table_model(
        table_name="count_etab_ouvert",
        create_table_query=create_table_count_etablissements_ouverts_query,
        create_index_func=create_unique_index,
        index_name="index_count_ouvert_siren",
        index_column="siren",
    )
    sqlite_client.execute(count_nombre_etablissements_ouverts_query)
    sqlite_client.commit_and_close_conn()
