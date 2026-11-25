import json
import logging
import os

import pandas as pd
from airflow.sdk import get_current_context, task

from data_pipelines_annuaire.config import (
    RNE_DATABASE_LOCATION,
    RNE_OBJECT_STORAGE_DATA_PATH,
    SIRENE_DATABASE_LOCATION,
)
from data_pipelines_annuaire.helpers.object_storage import (
    ObjectStorageClient,
    ObjectStorageFile,
)
from data_pipelines_annuaire.helpers.sqlite_client import SqliteClient
from data_pipelines_annuaire.workflows.data_pipelines.etl.data_fetch_clean.dirigeants import (
    match_protected_dirigeants,
    preprocess_dirigeant_pm,
    preprocess_dirigeant_pp,
)
from data_pipelines_annuaire.workflows.data_pipelines.etl.sqlite.helpers import (
    create_index,
    drop_table,
    get_distinct_column_count,
)
from data_pipelines_annuaire.workflows.data_pipelines.etl.sqlite.queries.dirigeants import (
    add_boolean_column_query,
    create_table_dirigeant_pm_query,
    create_table_dirigeant_pp_query,
    get_chunk_dirig_pm_from_db_query,
    get_chunk_dirig_pp_from_db_query,
    get_dirigeant_pp_from_sirens_query,
    update_est_protege_in_dirigeant_pp_query,
    update_est_protege_legacy_unite_legale_query,
    update_est_protege_unite_legale_query,
)
from data_pipelines_annuaire.workflows.data_pipelines.siren_protege.config import (
    SIREN_PROTEGE_CONFIG,
)

logger = logging.getLogger(__name__)


@task
def get_latest_rne_database():
    latest_file_date = ObjectStorageClient().get_latest_database(
        RNE_OBJECT_STORAGE_DATA_PATH,
        RNE_DATABASE_LOCATION,
    )
    ti = get_current_context()["ti"]
    ti.xcom_push(key="rne_last_modified", value=latest_file_date)


@task
def create_dirig_pp_table():
    sqlite_client_siren = SqliteClient(SIRENE_DATABASE_LOCATION)
    sqlite_client_dirig = SqliteClient(RNE_DATABASE_LOCATION)
    chunk_size = 100000
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
        dir_pp_clean = preprocess_dirigeant_pp(query)
        dir_pp_clean.to_sql(
            "dirigeant_pp",
            sqlite_client_siren.db_conn,
            if_exists="append",
            index=False,
        )
        logger.info(f"Iter: {i}")

    del dir_pp_clean
    sqlite_client_siren.commit_and_close_conn()
    sqlite_client_dirig.commit_and_close_conn()


@task
def create_dirig_pm_table():
    sqlite_client_siren = SqliteClient(SIRENE_DATABASE_LOCATION)
    sqlite_client_dirig = SqliteClient(RNE_DATABASE_LOCATION)

    chunk_size = 100000
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
        logger.info(f"Iter: {i}")
    del dir_pm_clean
    sqlite_client_siren.commit_and_close_conn()
    sqlite_client_dirig.commit_and_close_conn()


@task
def add_protege_flags():
    """
    Flag the dirigeants of the protected list with est_protege on dirigeant_pp
    and their unités légales with est_protege.
    Flag the SIREN of the legacy rows with est_protege_legacy on unite_legale even
    if no dirigeant matches, since the legacy protection also covers the address.
    """
    protected_file = ObjectStorageFile(
        f"{SIREN_PROTEGE_CONFIG.object_storage_path}/latest/"
        f"{SIREN_PROTEGE_CONFIG.file_name}.csv"
    ).download_to(os.path.dirname(SIRENE_DATABASE_LOCATION))
    protected_list = pd.read_csv(
        protected_file.path,
        dtype=str,
        # Avoid a potential "NAN", "NA" or "NULL" name to be read as a missing value
        keep_default_na=False,
        usecols=["siren", "nom", "prenoms", "date_de_naissance", "legacy"],
    )
    logger.info(f"{len(protected_list)} rows in the protected list")

    with SqliteClient(SIRENE_DATABASE_LOCATION) as sqlite_client:
        sqlite_client.execute(add_boolean_column_query("dirigeant_pp", "est_protege"))
        for column in ("est_protege", "est_protege_legacy"):
            sqlite_client.execute(add_boolean_column_query("unite_legale", column))
        dirigeants = pd.read_sql(
            get_dirigeant_pp_from_sirens_query,
            sqlite_client.db_conn,
            params=[json.dumps(protected_list["siren"].unique().tolist())],
        )
        protected_dirigeants, unmatched = match_protected_dirigeants(
            dirigeants, protected_list
        )
        sqlite_client.execute(
            update_est_protege_in_dirigeant_pp_query,
            [json.dumps(protected_dirigeants["rowid"].tolist())],
        )
        protected_count = sqlite_client.execute(
            update_est_protege_unite_legale_query,
            [json.dumps(protected_dirigeants["siren"].unique().tolist())],
        ).rowcount
        legacy_sirens = protected_list.loc[protected_list["legacy"] == "true", "siren"]
        legacy_count = sqlite_client.execute(
            update_est_protege_legacy_unite_legale_query,
            [json.dumps(legacy_sirens.unique().tolist())],
        ).rowcount

    logger.info(
        f"{len(protected_dirigeants)} protected dirigeants, "
        f"{protected_count} unités légales with est_protege, "
        f"{legacy_count} with est_protege_legacy"
    )
    if not unmatched.empty:
        logger.warning(
            f"{len(unmatched)} rows of the protected list match no dirigeant in the RNE."
        )
