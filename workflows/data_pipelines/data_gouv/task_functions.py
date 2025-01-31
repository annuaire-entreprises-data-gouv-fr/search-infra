from datetime import datetime
import gzip
import re
import shutil
import logging
import os
import pandas as pd
import json
from dag_datalake_sirene.helpers.minio_helpers import minio_client
from dag_datalake_sirene.helpers.sqlite_client import SqliteClient
from dag_datalake_sirene.helpers.datagouv import post_resource
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
    format_departement,
)
from dag_datalake_sirene.helpers.utils import (
    str_to_bool,
    str_to_list,
    convert_date_format,
    sqlite_str_to_bool,
)
from dag_datalake_sirene.helpers.geolocalisation import (
    transform_coordinates,
)
from dag_datalake_sirene.helpers.tchap import send_message
from dag_datalake_sirene.config import (
    SIRENE_MINIO_DATA_PATH,
    AIRFLOW_DATAGOUV_DATA_DIR,
)

current_date = datetime.now().date()
today_date = datetime.today().strftime("%Y-%m-%d")


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


def process_ul_chunk(chunk):
    # Transformations on columns
    chunk["colter_elus"] = chunk["colter_elus"].apply(json.loads)

    # Generate 'nom_complet'
    chunk["nom_complet"] = chunk.apply(
        lambda row: format_nom_complet(
            row["nom"], row["nom_usage"], row["nom_raison_sociale"], row["prenom"]
        ),
        axis=1,
    )

    # Fill NA values in 'nombre_etablissements_ouverts'
    chunk["nombre_etablissements_ouverts"].fillna(0, inplace=True)

    # Apply transformation functions
    chunk["est_entrepreneur_individuel"] = chunk["nature_juridique"].apply(
        is_entrepreneur_individuel
    )
    chunk["liste_elus"] = chunk["colter_elus"].apply(create_list_names_elus)

    chunk["est_association"] = chunk.apply(
        lambda row: is_association(
            row["nature_juridique"], row["identifiant_association"]
        ),
        axis=1,
    )

    chunk["est_entrepreneur_spectacle"] = chunk["est_entrepreneur_spectacle"].apply(
        sqlite_str_to_bool
    )

    chunk["est_ess"] = chunk.apply(
        lambda row: is_ess(
            sqlite_str_to_bool(row["est_ess_france"]), row["economie_sociale_solidaire"]
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
        lambda row: is_service_public(row["nature_juridique"]), axis=1
    )

    return chunk


def fill_ul_file():
    chunk_size = 100000
    sqlite_client = SqliteClient(AIRFLOW_DATAGOUV_DATA_DIR + "sirene.db")

    ul_csv_path = f"{AIRFLOW_DATAGOUV_DATA_DIR}unites_legales_{today_date}.csv"

    columns = [
        "siren",
        "siret_siege",
        "etat_administratif",
        "statut_diffusion",
        "nombre_etablissements",
        "nombre_etablissements_ouverts",
        "nom_complet",
        "colter_code",
        "colter_code_insee",
        "colter_elus",
        "colter_niveau",
        "date_mise_a_jour_insee",
        "date_mise_a_jour_rne",
        "egapro_renseignee",
        "est_association",
        "est_entrepreneur_individuel",
        "est_entrepreneur_spectacle",
        "statut_entrepreneur_spectacle",
        "est_ess",
        "est_organisme_formation",
        "est_qualiopi",
        "est_service_public",
        "est_societe_mission",
        "liste_elus",
        "liste_id_organisme_formation",
        "liste_idcc",
        "est_siae",
        "type_siae",
    ]

    # first_chunk = not os.path.exists(ul_csv_path)
    first_chunk = True

    for chunk in pd.read_sql_query(
        ul_fields_to_select, sqlite_client.db_conn, chunksize=chunk_size
    ):
        processed_chunk = process_ul_chunk(chunk)

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


def send_to_minio(list_files):
    minio_client.send_files(
        list_files=list_files,
    )


def upload_ul_to_minio(**kwargs):
    ul_csv_path = f"{AIRFLOW_DATAGOUV_DATA_DIR}unites_legales_{today_date}.csv"

    with open(ul_csv_path, "rb") as f_in:
        with gzip.open(f"{ul_csv_path}.gz", "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)

    send_to_minio(
        [
            {
                "source_path": AIRFLOW_DATAGOUV_DATA_DIR,
                "source_name": f"unites_legales_{today_date}.csv.gz",
                "dest_path": "data_gouv/",
                "dest_name": f"unites_legales_{today_date}.csv.gz",
            }
        ]
    )


def process_etablissement_chunk(chunk):
    chunk["is_non_diffusible"] = chunk["statut_diffusion"] != "O"

    chunk["adresse"] = chunk.apply(
        lambda row: format_adresse_complete(
            row["complement_adresse"],
            row["numero_voie"],
            row["indice_repetition"],
            row["type_voie"],
            row["libelle_voie"],
            row["libelle_commune"],
            row["libelle_cedex"],
            row["distribution_speciale"],
            row["code_postal"],
            row["cedex"],
            row["commune"],
            row["libelle_commune_etranger"],
            row["libelle_pays_etranger"],
            row["is_non_diffusible"],
        ),
        axis=1,
    )
    chunk["departement"] = chunk["commune"].apply(format_departement)
    coordinates = chunk.apply(
        lambda row: transform_coordinates(row["departement"], row["x"], row["y"]),
        axis=1,
    )
    chunk["latitude"], chunk["longitude"] = zip(*coordinates)
    chunk["est_siege"] = chunk["est_siege"].apply(str_to_bool)
    chunk["ancien_siege"] = chunk["ancien_siege"].apply(sqlite_str_to_bool)
    chunk["liste_idcc"] = chunk["liste_idcc"].apply(str_to_list)
    chunk["liste_rge"] = chunk["liste_rge"].apply(str_to_list)
    chunk["liste_uai"] = chunk["liste_uai"].apply(str_to_list)
    chunk["liste_finess"] = chunk["liste_finess"].apply(str_to_list)
    chunk["liste_id_bio"] = chunk["liste_id_bio"].apply(str_to_list)

    return chunk


def fill_etab_file():
    chunk_size = 100000
    sqlite_client = SqliteClient(AIRFLOW_DATAGOUV_DATA_DIR + "sirene.db")

    etab_csv_path = f"{AIRFLOW_DATAGOUV_DATA_DIR}etablissements_{today_date}.csv"

    columns = [
        "siren",
        "siret",
        "est_siege",
        "ancien_siege",
        "adresse",
        "etat_administratif",
        "statut_diffusion",
        "liste_finess",
        "liste_id_bio",
        "liste_idcc",
        "liste_rge",
        "liste_uai",
        "latitude",
        "longitude",
    ]

    # first_chunk = not os.path.exists(etab_csv_path)
    first_chunk = True

    for chunk in pd.read_sql_query(
        etab_fields_to_select, sqlite_client.db_conn, chunksize=chunk_size
    ):
        processed_chunk = process_etablissement_chunk(chunk)

        # Append processed chunk to CSV file
        if first_chunk:
            processed_chunk.to_csv(
                etab_csv_path, mode="w", header=True, index=False, columns=columns
            )
            first_chunk = False
        else:
            processed_chunk.to_csv(
                etab_csv_path, mode="a", header=False, index=False, columns=columns
            )

    sqlite_client.commit_and_close_conn()


def upload_etab_to_minio(**kwargs):
    ul_csv_path = f"{AIRFLOW_DATAGOUV_DATA_DIR}etablissements_{today_date}.csv"

    with open(ul_csv_path, "rb") as f_in:
        with gzip.open(f"{ul_csv_path}.gz", "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)

    send_to_minio(
        [
            {
                "source_path": AIRFLOW_DATAGOUV_DATA_DIR,
                "source_name": f"etablissements_{today_date}.csv.gz",
                "dest_path": "data_gouv/",
                "dest_name": f"etablissements_{today_date}.csv.gz",
            }
        ]
    )


def publish_data(**kwargs):
    response_ul = post_resource(
        file_to_upload={
            "dest_path": AIRFLOW_DATAGOUV_DATA_DIR,
            "dest_name": f"unites_legales_{today_date}.csv.gz",
        },
        dataset_id="667ebdd4547ab9bd6e4682d3",
        resource_id="b8e5376c-c158-4d88-91f3-f6bb0d165332",
    )

    logging.info(f"Publishing unité légale : {response_ul}")

    response_etab = post_resource(
        file_to_upload={
            "dest_path": AIRFLOW_DATAGOUV_DATA_DIR,
            "dest_name": f"etablissements_{today_date}.csv.gz",
        },
        dataset_id="667ebdd4547ab9bd6e4682d3",
        resource_id="12812d7d-d11c-45f4-965c-35a3b149c585",
    )

    logging.info(f"Publishing unité légale : {response_etab}")


def notification_tchap(ti):
    send_message("\U0001f7e2 Fichiers mis à jour sur DataGouv.")


def send_notification_failure_tchap(context):
    send_message("\U0001f534 Données :" "\nFail DAG de publication sur Data.gouv!!!!")
