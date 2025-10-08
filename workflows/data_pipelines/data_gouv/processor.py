import gzip
import json
import logging
import os
import re
import shutil
from datetime import datetime

import pandas as pd

from dag_datalake_sirene.config import AIRFLOW_DATAGOUV_DATA_DIR, SIRENE_MINIO_DATA_PATH
from dag_datalake_sirene.helpers.datagouv import post_resource
from dag_datalake_sirene.helpers.geolocalisation import transform_coordinates
from dag_datalake_sirene.helpers.minio_helpers import MinIOClient
from dag_datalake_sirene.helpers.sqlite_client import SqliteClient
from dag_datalake_sirene.helpers.utils import (
    convert_date_format,
    sqlite_str_to_bool,
    str_to_bool,
    str_to_list,
)
from dag_datalake_sirene.workflows.data_pipelines.data_gouv.queries import (
    etab_fields_to_select,
    ul_fields_to_select,
)
from dag_datalake_sirene.workflows.data_pipelines.elasticsearch.data_enrichment import (
    create_list_names_elus,
    format_adresse_complete,
    format_departement,
    format_nom_complet,
    is_administration_l100_3,
    is_association,
    is_entrepreneur_individuel,
    is_ess,
    is_personne_morale_insee,
    is_service_public,
)


class DataGouvProcessor:
    def __init__(self):
        self.today_date = datetime.today().strftime("%Y-%m-%d")
        self.chunk_size = 100000
        self.ul_columns = [
            "siren",
            "siret_siege",
            "etat_administratif",
            "statut_diffusion",
            "nombre_etablissements",
            "nombre_etablissements_ouverts",
            "nom_complet",
            "nature_juridique",
            "colter_code",
            "colter_code_insee",
            "colter_elus",
            "colter_niveau",
            "date_mise_a_jour_insee",
            "date_mise_a_jour_rne",
            "egapro_renseignee",
            "est_achats_responsables",
            "est_alim_confiance",
            "est_association",
            "est_entrepreneur_individuel",
            "est_entrepreneur_spectacle",
            "est_patrimoine_vivant",
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
        self.etab_columns = [
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

    def get_latest_database(self):
        """Get the latest database from MinIO storage"""
        minio_client = MinIOClient()
        database_files = minio_client.get_files_from_prefix(
            prefix=SIRENE_MINIO_DATA_PATH,
        )

        if not database_files:
            raise Exception(
                f"No database files were found in : {SIRENE_MINIO_DATA_PATH}"
            )

        # Extract dates from the db file names and sort them
        dates = sorted(
            re.findall(r"sirene_(\d{4}-\d{2}-\d{2})", " ".join(database_files))
        )

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

    def process_ul_chunk(self, chunk):
        chunk["colter_elus"] = chunk["colter_elus"].apply(json.loads)
        chunk["nom_complet"] = chunk.apply(
            lambda row: format_nom_complet(
                row["nom"],
                row["nom_usage"],
                row["prenom"],
                row["nom_raison_sociale"],
                est_personne_morale_insee=is_personne_morale_insee(
                    row["nature_juridique"]
                ),
                is_non_diffusible=row["statut_diffusion"] != "O",
            ),
            axis=1,
        )
        # Fill NA values in 'nombre_etablissements_ouverts'
        chunk["nombre_etablissements_ouverts"] = chunk[
            "nombre_etablissements_ouverts"
        ].fillna(0)
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
                sqlite_str_to_bool(row["est_ess_france"]),
                row["economie_sociale_solidaire"],
            ),
            axis=1,
        )
        chunk["egapro_renseignee"] = chunk["egapro_renseignee"].apply(
            sqlite_str_to_bool
        )
        chunk["est_achats_responsables"] = chunk["est_achats_responsables"].apply(
            sqlite_str_to_bool
        )
        chunk["est_alim_confiance"] = chunk["est_alim_confiance"].apply(
            sqlite_str_to_bool
        )
        chunk["est_patrimoine_vivant"] = chunk["est_patrimoine_vivant"].apply(
            sqlite_str_to_bool
        )
        chunk["est_siae"] = chunk["est_siae"].apply(sqlite_str_to_bool)
        chunk["liste_id_organisme_formation"] = chunk[
            "liste_id_organisme_formation"
        ].apply(str_to_list)
        chunk["est_organisme_formation"] = chunk["liste_id_organisme_formation"].apply(
            lambda x: bool(x)
        )
        chunk["est_qualiopi"] = chunk["est_qualiopi"].apply(sqlite_str_to_bool)
        chunk["liste_idcc"] = chunk["liste_idcc"].apply(str_to_list)
        chunk["date_mise_a_jour_rne"] = chunk["date_mise_a_jour_rne"].apply(
            convert_date_format
        )
        chunk["est_service_public"] = chunk.apply(
            lambda row: is_service_public(
                row["nature_juridique"], row["siren"], row["etat_administratif"]
            ),
            axis=1,
        )
        return chunk

    def process_etablissement_chunk(self, chunk):
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

    def fill_ul_file(self):
        """Process and fill unites legales file"""
        sqlite_client = SqliteClient(f"{AIRFLOW_DATAGOUV_DATA_DIR}sirene.db")
        ul_csv_path = f"{AIRFLOW_DATAGOUV_DATA_DIR}unites_legales_{self.today_date}.csv"

        first_chunk = True
        total_siren = 0
        for chunk in pd.read_sql_query(
            ul_fields_to_select, sqlite_client.db_conn, chunksize=self.chunk_size
        ):
            processed_chunk = self.process_ul_chunk(chunk)
            total_siren += len(processed_chunk["siren"].unique())
            self._write_chunk_to_csv(
                processed_chunk, ul_csv_path, first_chunk, self.ul_columns
            )
            first_chunk = False

        sqlite_client.commit_and_close_conn()
        logging.info(f"******* Nombre des unites_legales : {total_siren}")
        return ul_csv_path

    def fill_etab_file(self):
        sqlite_client = SqliteClient(f"{AIRFLOW_DATAGOUV_DATA_DIR}sirene.db")
        etab_csv_path = (
            f"{AIRFLOW_DATAGOUV_DATA_DIR}etablissements_{self.today_date}.csv"
        )

        first_chunk = True
        total_siret = 0
        for chunk in pd.read_sql_query(
            etab_fields_to_select, sqlite_client.db_conn, chunksize=self.chunk_size
        ):
            processed_chunk = self.process_etablissement_chunk(chunk)
            total_siret += len(processed_chunk["siret"].unique())
            self._write_chunk_to_csv(
                processed_chunk, etab_csv_path, first_chunk, self.etab_columns
            )
            first_chunk = False

        sqlite_client.commit_and_close_conn()
        logging.info(f"******* Nombre des etablissements : {total_siret}")
        return etab_csv_path

    def process_administration_list(self):
        ul_csv_path = f"{AIRFLOW_DATAGOUV_DATA_DIR}unites_legales_{self.today_date}.csv"
        admin_csv_path = (
            f"{AIRFLOW_DATAGOUV_DATA_DIR}liste_administrations_{self.today_date}.csv"
        )

        df = pd.read_csv(ul_csv_path, dtype=str)
        admin_df = df[df["est_service_public"] == "True"][
            ["siren", "nom_complet", "nature_juridique"]
        ]

        admin_df["administration_au_sens_article_L100-3"] = admin_df.apply(
            lambda row: is_administration_l100_3(
                row["siren"], row["nature_juridique"], True
            ),
            axis=1,
        )

        logging.info(f"******* Nombre d'administrations à publier : {len(admin_df)}")
        admin_df = admin_df.astype(str)
        admin_df.to_csv(admin_csv_path, index=False)
        return admin_csv_path

    def _write_chunk_to_csv(self, chunk, filepath, is_first_chunk, columns):
        mode = "w" if is_first_chunk else "a"
        header = is_first_chunk
        chunk.to_csv(filepath, mode=mode, header=header, index=False, columns=columns)

    def compress_and_upload_file(self, filepath):
        with open(filepath, "rb") as f_in:
            with gzip.open(f"{filepath}.gz", "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)

    def send_to_minio(self, list_files):
        """Send files to MinIO storage"""
        MinIOClient().send_files(list_files=list_files)

    def upload_ul_and_administration_to_minio(self):
        """Compress and upload unites legales and administration files to MinIO"""
        self.compress_and_upload_file(
            f"{AIRFLOW_DATAGOUV_DATA_DIR}unites_legales_{self.today_date}.csv"
        )

        self.send_to_minio(
            [
                {
                    "source_path": AIRFLOW_DATAGOUV_DATA_DIR,
                    "source_name": f"unites_legales_{self.today_date}.csv.gz",
                    "dest_path": "data_gouv/",
                    "dest_name": f"unites_legales_{self.today_date}.csv.gz",
                },
                {
                    "source_path": AIRFLOW_DATAGOUV_DATA_DIR,
                    "source_name": f"liste_administrations_{self.today_date}.csv",
                    "dest_path": "data_gouv/",
                    "dest_name": f"liste_administrations_{self.today_date}.csv",
                },
            ]
        )

    def upload_etab_to_minio(self):
        """Compress and upload establishments file to MinIO"""
        self.compress_and_upload_file(
            f"{AIRFLOW_DATAGOUV_DATA_DIR}etablissements_{self.today_date}.csv"
        )

        self.send_to_minio(
            [
                {
                    "source_path": AIRFLOW_DATAGOUV_DATA_DIR,
                    "source_name": f"etablissements_{self.today_date}.csv.gz",
                    "dest_path": "data_gouv/",
                    "dest_name": f"etablissements_{self.today_date}.csv.gz",
                }
            ]
        )

    def publish_to_datagouv(self):
        """Publish files to data.gouv.fr"""
        files_to_publish = [
            {
                "file": f"unites_legales_{self.today_date}.csv.gz",
                "dataset_id": "667ebdd4547ab9bd6e4682d3",
                "resource_id": "b8e5376c-c158-4d88-91f3-f6bb0d165332",
            },
            {
                "file": f"etablissements_{self.today_date}.csv.gz",
                "dataset_id": "667ebdd4547ab9bd6e4682d3",
                "resource_id": "12812d7d-d11c-45f4-965c-35a3b149c585",
            },
            {
                "file": f"liste_administrations_{self.today_date}.csv",
                "dataset_id": "67a5cd40941cbe4c206efcd1",
                "resource_id": "c0f355f1-66bd-4f57-8a3c-2c6f3527b364",
            },
        ]

        for file_info in files_to_publish:
            response = post_resource(
                file_to_upload={
                    "dest_path": AIRFLOW_DATAGOUV_DATA_DIR,
                    "dest_name": file_info["file"],
                },
                dataset_id=file_info["dataset_id"],
                resource_id=file_info["resource_id"],
            )
            logging.info(f"Publishing {file_info['file']}: {response}")
