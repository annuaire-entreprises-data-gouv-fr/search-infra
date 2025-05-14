import gzip
import logging
import os
import shutil

from airflow.decorators import task

from dag_datalake_sirene.config import (
    AIRFLOW_ENV,
    RNE_DATABASE_LOCATION,
    SIRENE_DATABASE_LOCATION,
)
from dag_datalake_sirene.helpers.minio_helpers import MinIOClient

# fmt: on
from dag_datalake_sirene.helpers.sqlite_client import SqliteClient

# fmt: off
from dag_datalake_sirene.workflows.data_pipelines.etl.data_fetch_clean.dirigeants import (
    preprocess_dirigeant_pm,
    preprocess_personne_physique,
)
from dag_datalake_sirene.workflows.data_pipelines.etl.sqlite.helpers import (
    create_index,
    drop_table,
    get_distinct_column_count,
)
from dag_datalake_sirene.workflows.data_pipelines.etl.sqlite.queries.dirigeants import (
    create_table_dirigeant_pm_query,
    create_table_dirigeant_pp_query,
    get_chunk_dirig_pm_from_db_query,
    get_chunk_dirig_pp_from_db_query,
)


@task
def get_rne_database(**kwargs):
    latest_file_date = MinIOClient().get_latest_file(
        f"ae/{AIRFLOW_ENV}/rne/database/",
        f"{RNE_DATABASE_LOCATION}.gz",
    )
    kwargs["ti"].xcom_push(key="rne_last_modified", value=latest_file_date)

    logging.info(f"******* Getting file : {RNE_DATABASE_LOCATION}.gz")
    # Unzip database file
    with gzip.open(f"{RNE_DATABASE_LOCATION}.gz", "rb") as f_in:
        with open(RNE_DATABASE_LOCATION, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)

    os.remove(f"{RNE_DATABASE_LOCATION}.gz")


@task
def create_dirig_pp_table():
    sqlite_client_siren = SqliteClient(SIRENE_DATABASE_LOCATION)
    sqlite_client_dirig = SqliteClient(RNE_DATABASE_LOCATION)
    chunk_size = int(100000)
    for row in sqlite_client_dirig.execute(
        get_distinct_column_count("dirigeant_pp", "siren")
    ):
        nb_iter = int(int(row[0]) / chunk_size) + 1
    sqlite_client_siren.execute(drop_table("dirigeant_pp"))
    sqlite_client_siren.execute(create_table_dirigeant_pp_query)
    sqlite_client_siren.execute(create_index("siren_pp", "dirigeant_pp", "siren"))
    for i in range(nb_iter):
        query = sqlite_client_dirig.execute(
            get_chunk_dirig_pp_from_db_query(chunk_size, i)
        )
        dir_pp_clean = preprocess_personne_physique(query)
        dir_pp_clean.to_sql(
            "dirigeant_pp",
            sqlite_client_siren.db_conn,
            if_exists="append",
            index=False,
        )
        logging.info(f"Iter: {i}")

    del dir_pp_clean
    sqlite_client_siren.commit_and_close_conn()
    sqlite_client_dirig.commit_and_close_conn()


@task
def create_dirig_pm_table():
    sqlite_client_siren = SqliteClient(SIRENE_DATABASE_LOCATION)
    sqlite_client_dirig = SqliteClient(RNE_DATABASE_LOCATION)

    chunk_size = int(100000)
    for row in sqlite_client_dirig.execute(
        get_distinct_column_count("dirigeant_pm", "siren")
    ):
        nb_iter = int(int(row[0]) / chunk_size) + 1

    # Create table dirigeant_pm in siren database
    sqlite_client_siren.execute(drop_table("dirigeant_pm"))
    sqlite_client_siren.execute(create_table_dirigeant_pm_query)
    sqlite_client_siren.execute(create_index("siren_pm", "dirigeant_pm", "siren"))
    for i in range(nb_iter):
        query = sqlite_client_dirig.execute(
            get_chunk_dirig_pm_from_db_query(chunk_size, i)
        )
        dir_pm_clean = preprocess_dirigeant_pm(query)
        dir_pm_clean.to_sql(
            "dirigeant_pm",
            sqlite_client_siren.db_conn,
            if_exists="append",
            index=False,
        )
        logging.info(f"Iter: {i}")
    del dir_pm_clean
    sqlite_client_siren.commit_and_close_conn()
    sqlite_client_dirig.commit_and_close_conn()
