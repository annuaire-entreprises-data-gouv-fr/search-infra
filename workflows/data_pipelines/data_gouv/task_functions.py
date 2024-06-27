from datetime import datetime
import gzip
import re
import shutil
import logging
import os
import pandas as pd
import json
from dag_datalake_sirene.config import (
    SIRENE_MINIO_DATA_PATH,
    AIRFLOW_DATAGOUV_DATA_DIR,
)
from dag_datalake_sirene.helpers.minio_helpers import minio_client
from dag_datalake_sirene.helpers.sqlite_client import SqliteClient
from dag_datalake_sirene.workflows.data_pipelines.data_gouv.queries import (
    etab_fields_to_select,
    ul_fields_to_select,
)
from dag_datalake_sirene.workflows.data_pipelines.elasticsearch.data_enrichment import (
    create_list_names_elus,
    format_adresse_complete,
    format_nom_complet,
    is_association,
    is_entrepreneur_individuel,
    is_ess,
    is_service_public,
    label_section_from_activite,
)
from dag_datalake_sirene.helpers.tchap import send_message
from dag_datalake_sirene.helpers.utils import (
    convert_date_format,
    sqlite_str_to_bool,
    str_to_list,
)

current_date = datetime.now().date()


def send_to_minio(list_files):
    minio_client.send_files(
        list_files=list_files,
    )


current_date = datetime.now().date()


def get_latest_database(**kwargs):
    database_files = minio_client.get_files_from_prefix(
        prefix=SIRENE_MINIO_DATA_PATH,
    )

    if not database_files:
        raise Exception(f"No database files were found in : {SIRENE_MINIO_DATA_PATH}")

    # Extract dates from the db file names and sort them
    dates = sorted(re.findall(r"sirene_(\d{4}-\d{2}-\d{2})", " ".join(database_files)))

    if dates:
        last_date = dates[-1]
        logging.info(f"***** Last database saved: {last_date}")
        minio_client.get_files(
            list_files=[
                {
                    "source_path": SIRENE_MINIO_DATA_PATH,
                    "source_name": f"sirene_{last_date}.db.gz",
                    "dest_path": AIRFLOW_DATAGOUV_DATA_DIR,
                    "dest_name": "sirene.db.gz",
                }
            ],
        )
        # Unzip database file
        db_path = f"{AIRFLOW_DATAGOUV_DATA_DIR}sirene.db"
        with gzip.open(f"{db_path}.gz", "rb") as f_in:
            with open(db_path, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)
        os.remove(f"{db_path}.gz")

    else:
        raise Exception(
            f"No dates in database files were found : {SIRENE_MINIO_DATA_PATH}"
        )


def process_chunk(chunk):
    # Apply transformations directly to the DataFrame
    chunk["colter_elus"] = chunk["colter_elus"].apply(json.loads)
    chunk["nom_complet"] = chunk.apply(
        lambda row: format_nom_complet(
            row["nom"], row["nom_usage"], row["nom_raison_sociale"], row["prenom"]
        ),
        axis=1,
    )
    chunk["nombre_etablissements_ouverts"].fillna(0, inplace=True)
    chunk["section_activite_principale"] = chunk["activite_principale"].apply(
        label_section_from_activite
    )
    chunk["est_entrepreneur_individuel"] = chunk["nature_juridique"].apply(
        is_entrepreneur_individuel
    )
    chunk["liste_elus"] = chunk["colter_elus"].apply(create_list_names_elus)
    chunk["est_association"] = chunk.apply(
        lambda row: is_association(
            row["nature_juridique"],
            row["identifiant_association"],
        ),
        axis=1,
    )
    chunk["est_entrepreneur_spectacle"] = chunk["est_entrepreneur_spectacle"].apply(
        sqlite_str_to_bool
    )
    chunk["est_ess"] = chunk.apply(
        lambda row: is_ess(
            sqlite_str_to_bool(row["est_ess_france"]),
            row["economie_sociale_solidaire"],
        ),
        axis=1,
    )
    chunk["egapro_renseignee"] = chunk["egapro_renseignee"].apply(sqlite_str_to_bool)
    chunk["est_siae"] = chunk["est_siae"].apply(sqlite_str_to_bool)
    chunk["liste_id_organisme_formation"] = chunk["liste_id_organisme_formation"].apply(
        str_to_list
    )
    chunk["est_organisme_formation"] = chunk["liste_id_organisme_formation"].apply(
        lambda x: bool(x)
    )
    chunk["est_qualiopi"] = chunk["est_qualiopi"].apply(sqlite_str_to_bool)
    chunk["liste_idcc"] = chunk["liste_idcc"].apply(str_to_list)
    chunk["date_mise_a_jour_rne"] = chunk["date_mise_a_jour_rne"].apply(
        convert_date_format
    )
    chunk["est_service_public"] = chunk.apply(
        lambda row: is_service_public(row["nature_juridique"], row["siren"]),
        axis=1,
    )

    return chunk


def fill_ul_file():
    chunk_size = 100000
    sqlite_client = SqliteClient(AIRFLOW_DATAGOUV_DATA_DIR + "sirene.db")

    ul_csv_path = f"{AIRFLOW_DATAGOUV_DATA_DIR}unites_legales.csv"

    # Define columns
    columns = [
        "siren",
        "siret_siege",
        "etat_administratif",
        "nom_raison_sociale",
        "nom",
        "nom_usage",
        "prenom",
        "sigle",
        "date_mise_a_jour_insee",
        "date_mise_a_jour_rne",
        "date_creation",
        "date_fermeture",
        "activite_principale",
        "nature_juridique",
        "economie_sociale_solidaire",
        "est_societe_mission",
        "liste_idcc",
        "nombre_etablissements",
        "nombre_etablissements_ouverts",
        "est_entrepreneur_spectacle",
        "statut_entrepreneur_spectacle",
        "egapro_renseignee",
        "colter_code_insee",
        "colter_code",
        "colter_niveau",
        "est_ess_france",
        "colter_elus",
        "est_qualiopi",
        "liste_id_organisme_formation",
        "est_siae",
        "type_siae",
        "nom_complet",
        "section_activite_principale",
        "est_entrepreneur_individuel",
        "liste_elus",
        "est_association",
        "est_entrepreneur_spectacle",
        "est_ess",
        "egapro_renseignee",
        "est_siae",
        "est_organisme_formation",
        "est_qualiopi",
        "date_mise_a_jour_rne",
        "est_service_public",
    ]

    first_chunk = not os.path.exists(ul_csv_path)

    for chunk in pd.read_sql_query(
        ul_fields_to_select, sqlite_client.db_conn, chunksize=chunk_size
    ):
        processed_chunk = process_chunk(chunk)

        # Append processed chunk to CSV file
        if first_chunk:
            processed_chunk.to_csv(
                ul_csv_path, mode="w", header=True, index=False, columns=columns
            )
            first_chunk = False
        else:
            processed_chunk.to_csv(
                ul_csv_path, mode="a", header=False, index=False, columns=columns
            )

    sqlite_client.commit_and_close_conn()


def upload_to_minio(**kwargs):
    ul_csv_path = f"{AIRFLOW_DATAGOUV_DATA_DIR}unites_legales.csv"

    with open(ul_csv_path, "rb") as f_in:
        with gzip.open(f"{ul_csv_path}.gz", "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)

    send_to_minio(
        [
            {
                "source_path": AIRFLOW_DATAGOUV_DATA_DIR,
                "source_name": "unites_legales.csv.gz",
                "dest_path": "sirene/test/",
                "dest_name": "unites_legales.csv.gz",
            }
        ]
    )


def fill_etab_file():
    sqlite_client = SqliteClient(AIRFLOW_DATAGOUV_DATA_DIR + "sirene.db")
    df_etab = pd.read_sql_query(etab_fields_to_select, sqlite_client.db_conn)
    df_etab

    df_etab["adresse"] = format_adresse_complete(
        df_etab["complement_adresse"],
        df_etab["numero_voie"],
        df_etab["indice_repetition"],
        df_etab["type_voie"],
        df_etab["libelle_voie"],
        df_etab["libelle_commune"],
        df_etab["libelle_cedex"],
        df_etab["distribution_speciale"],
        df_etab["code_postal"],
        df_etab["cedex"],
        df_etab["commune"],
        df_etab["libelle_commune_etranger"],
        df_etab["libelle_pays_etranger"],
    )

    etab_parquet_path = f"{AIRFLOW_DATAGOUV_DATA_DIR}etablissements.parquet"
    df_etab.to_parquet(etab_parquet_path, index=False)

    # del df_etab


def send_notification_failure_tchap(context):
    send_message("\U0001F534 Données :" "\nFail DAG de publication!!!!")
