import logging

from dag_datalake_sirene.data_pipelines.etl.data_fetch_clean.dirigeants import (
    preprocess_dirigeant_pm,
    preprocess_dirigeants_pp,
)

from dag_datalake_sirene.data_pipelines.etl.sqlite.sqlite_client import SqliteClient

from dag_datalake_sirene.data_pipelines.etl.sqlite.helpers import (
    drop_table,
    get_distinct_column_count,
    create_index,
)
from dag_datalake_sirene.data_pipelines.etl.sqlite.queries.dirigeants import (
    create_table_dirigeant_pp_query,
    create_table_dirigeant_pm_query,
    get_chunk_dirig_pp_from_db_query,
    get_chunk_dirig_pm_from_db_query,
)

from dag_datalake_sirene.config import (
    SIRENE_DATABASE_LOCATION,
    DIRIG_DATABASE_LOCATION,
)


def create_dirig_pp_table():
    sqlite_client_siren = SqliteClient(SIRENE_DATABASE_LOCATION)
    sqlite_client_dirig = SqliteClient(DIRIG_DATABASE_LOCATION)
    chunk_size = int(100000)
    for row in sqlite_client_dirig.execute(
        get_distinct_column_count("dirigeants_pp", "siren")
    ):
        nb_iter = int(int(row[0]) / chunk_size) + 1
    sqlite_client_siren.execute(drop_table("dirigeants_pp"))
    sqlite_client_siren.execute(create_table_dirigeant_pp_query)
    sqlite_client_siren.execute(create_index("siren_pp", "dirigeants_pp", "siren"))
    for i in range(nb_iter):
        query = sqlite_client_dirig.execute(
            get_chunk_dirig_pp_from_db_query(chunk_size, i)
        )
        dir_pp_clean = preprocess_dirigeants_pp(query)
        dir_pp_clean.to_sql(
            "dirigeants_pp",
            sqlite_client_siren.db_conn,
            if_exists="append",
            index=False,
        )
        logging.info(f"Iter: {i}")

    del dir_pp_clean
    sqlite_client_siren.commit_and_close_conn()
    sqlite_client_dirig.commit_and_close_conn()


def create_dirig_pm_table():
    sqlite_client_siren = SqliteClient(SIRENE_DATABASE_LOCATION)
    sqlite_client_dirig = SqliteClient(DIRIG_DATABASE_LOCATION)

    chunk_size = int(100000)
    for row in sqlite_client_dirig.execute(
        get_distinct_column_count("dirigeants_pm", "siren")
    ):
        nb_iter = int(int(row[0]) / chunk_size) + 1

    # Create table dirigeants_pm in siren database
    sqlite_client_siren.execute(drop_table("dirigeants_pm"))
    sqlite_client_siren.execute(create_table_dirigeant_pm_query)
    sqlite_client_siren.execute(create_index("siren_pm", "dirigeants_pm", "siren"))
    for i in range(nb_iter):
        query = sqlite_client_dirig.execute(
            get_chunk_dirig_pm_from_db_query(chunk_size, i)
        )
        dir_pm_clean = preprocess_dirigeant_pm(query)
        dir_pm_clean.to_sql(
            "dirigeants_pm",
            sqlite_client_siren.db_conn,
            if_exists="append",
            index=False,
        )
        logging.info(f"Iter: {i}")
    del dir_pm_clean
    sqlite_client_siren.commit_and_close_conn()
    sqlite_client_dirig.commit_and_close_conn()
