import logging
import os
from datetime import datetime
from dataclasses import dataclass
import requests
from requests.adapters import HTTPAdapter
from dag_datalake_sirene.config import (
    DS_AUTH,
    DS_API_URL,
    DS_TMP_FOLDER,
    DS_MINIO_DATA_PATH,
    DEMARCHE_NUMBER,
)
import sqlite3
from dag_datalake_sirene.helpers.minio_helpers import minio_client
import gzip
import shutil
from minio.error import S3Error


@dataclass
class Demandeur:
    siret: str | None = None
    siege_social: bool | None = None


@dataclass
class Dossier:
    id: str
    number: int
    archived: bool
    state: str
    date_derniere_modification: datetime
    date_depot: datetime
    date_passage_en_construction: datetime
    date_passage_en_instruction: datetime
    date_traitement: datetime | None
    date_expiration: datetime | None
    date_suppression_par_usager: datetime | None
    demandeur: Demandeur


@dataclass
class PageInfo:
    has_next_page: bool
    has_previous_page: bool
    start_cursor: str
    end_cursor: str


@dataclass
class DemarcheResponse:
    id: str
    number: int
    title: str
    state: str
    date_creation: datetime
    dossiers: list[Dossier]
    page_info: PageInfo


class DemarcheSimplifieeClient:

    QUERY = """
        query getDemarche($demarcheNumber: Int!, $after: String) {
        demarche(number: $demarcheNumber) {
            id
            number
            title
            state
            dateCreation
            dossiers(after: $after, first: 100) {
            pageInfo {
                hasNextPage
                hasPreviousPage
                startCursor
                endCursor
            }
            nodes {
                id
                number
                archived
                state
                dateDerniereModification
                dateDepot
                datePassageEnConstruction
                datePassageEnInstruction
                dateTraitement
                dateExpiration
                dateSuppressionParUsager
                demandeur {
                ... on PersonneMorale {
                    siret
                    siegeSocial
                }
                }
            }
            }
        }
        }
        """

    def __init__(self):
        self.auth = DS_AUTH
        self.headers = {
            "Authorization": f"Bearer {self.auth}",
            "Content-Type": "application/json",
        }
        self.session = self.create_persistent_session()

    def create_persistent_session(self):
        """Create a session with a custom HTTP adapter for max retries."""
        session = requests.Session()
        adapter = HTTPAdapter(max_retries=20)
        session.mount("http://", adapter)
        return session

    def execute_query(self, query: str, variables) -> dict:
        response = self.session.post(
            DS_API_URL,
            json={"query": query, "variables": variables},
            headers=self.headers,
        )
        response.raise_for_status()
        return response.json()

    def get_demarche(self, demarche_number) -> DemarcheResponse:
        variables = {
            "demarcheNumber": demarche_number,
            "after": None,
        }
        all_dossiers = []

        while True:
            data = self.execute_query(self.QUERY, variables)
            demarche_data = data["data"]["demarche"]
            dossiers_data = demarche_data["dossiers"]

            logging.warning(f"??????????{dossiers_data['pageInfo']}")

            dossiers = [
                Dossier(
                    id=d["id"],
                    number=d["number"],
                    archived=d["archived"],
                    state=d["state"],
                    date_derniere_modification=datetime.fromisoformat(
                        d["dateDerniereModification"]
                    ),
                    date_depot=datetime.fromisoformat(d["dateDepot"]),
                    date_passage_en_construction=datetime.fromisoformat(
                        d["datePassageEnConstruction"]
                    ),
                    date_passage_en_instruction=datetime.fromisoformat(
                        d["datePassageEnInstruction"]
                    ),
                    date_traitement=(
                        datetime.fromisoformat(d["dateTraitement"])
                        if d["dateTraitement"]
                        else None
                    ),
                    date_expiration=(
                        datetime.fromisoformat(d["dateExpiration"])
                        if d["dateExpiration"]
                        else None
                    ),
                    date_suppression_par_usager=(
                        datetime.fromisoformat(d["dateSuppressionParUsager"])
                        if d["dateSuppressionParUsager"]
                        else None
                    ),
                    demandeur=Demandeur(
                        siret=d["demandeur"].get("siret"),
                        siege_social=d["demandeur"].get("siegeSocial"),
                    ),
                )
                for d in dossiers_data["nodes"]
            ]
            all_dossiers.extend(dossiers)

            page_info = PageInfo(
                has_next_page=dossiers_data["pageInfo"]["hasNextPage"],
                has_previous_page=dossiers_data["pageInfo"]["hasPreviousPage"],
                start_cursor=dossiers_data["pageInfo"]["startCursor"],
                end_cursor=dossiers_data["pageInfo"]["endCursor"],
            )

            logging.warning(f"?????????? {page_info}")

            if not page_info.has_next_page:
                break

            variables["after"] = page_info.end_cursor

        return DemarcheResponse(
            id=demarche_data["id"],
            number=int(demarche_data["number"]),
            title=demarche_data["title"],
            state=demarche_data["state"],
            date_creation=datetime.fromisoformat(demarche_data["dateCreation"]),
            dossiers=all_dossiers,
            page_info=page_info,
        )


def get_latest_db(**kwargs):
    """
    This function retrieves the RNE database file associated with the
    provided start date from a Minio server and saves it to a
    temporary folder for further processing.
    """
    try:
        minio_client.get_files(
            list_files=[
                {
                    "source_path": DS_MINIO_DATA_PATH,
                    "source_name": "demarches_simplifiees.db.gz",
                    "dest_path": DS_TMP_FOLDER,
                    "dest_name": "demarches_simplifiees.db.gz",
                }
            ],
        )
        # Unzip db file
        db_path = f"{DS_TMP_FOLDER}demarches_simplifiees.db"
        with gzip.open(f"{db_path}.gz", "rb") as f_in:
            with open(db_path, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)
        os.remove(f"{db_path}.gz")
    except S3Error as err:
        if err.code == "NoSuchKey":
            logging.warning(f"****** No DS databse found in MinIO : {err}")
            create_db_base()
        else:
            logging.warning("HEEERE")
            raise ()


def create_db_base():
    ds_db_path = DS_TMP_FOLDER + "demarches_simplifiees.db"
    if os.path.exists(ds_db_path):
        os.remove(ds_db_path)

    connection, cursor = connect_to_db(ds_db_path)
    create_tables(cursor)

    connection.commit()
    connection.close()


def create_tables(cursor):
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS dossiers
                 (id TEXT PRIMARY KEY, demarche_id TEXT, number INTEGER, archived BOOLEAN, state TEXT,
                 date_derniere_modification TEXT, date_depot TEXT, date_passage_en_construction TEXT,
                 date_passage_en_instruction TEXT, date_traitement TEXT, date_expiration TEXT,
                 date_suppression_par_usager TEXT,
                 FOREIGN KEY (demarche_id) REFERENCES demarches(id))"""
    )


def connect_to_db(database_location):
    connection = sqlite3.connect(database_location)
    cursor = connection.cursor()
    return connection, cursor


def fetch_and_save_df_data():
    ds_db_path = DS_TMP_FOLDER + "demarches_simplifiees.db"
    ds_client = DemarcheSimplifieeClient()
    demarche_response = ds_client.get_demarche(DEMARCHE_NUMBER)

    connection, cursor = connect_to_db(ds_db_path)

    try:
        # Insert dossiers
        for dossier in demarche_response.dossiers:
            # dossier.demandeur.siret
            cursor.execute(
                """INSERT OR REPLACE INTO dossiers
                              (id, demarche_id, number, archived, state, date_derniere_modification,
                               date_depot, date_passage_en_construction, date_passage_en_instruction,
                               date_traitement, date_expiration, date_suppression_par_usager)
                              VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    dossier.id,
                    demarche_response.id,
                    dossier.number,
                    dossier.archived,
                    dossier.state,
                    dossier.date_derniere_modification.isoformat(),
                    dossier.date_depot.isoformat(),
                    dossier.date_passage_en_construction.isoformat(),
                    dossier.date_passage_en_instruction.isoformat(),
                    (
                        dossier.date_traitement.isoformat()
                        if dossier.date_traitement
                        else None
                    ),
                    (
                        dossier.date_expiration.isoformat()
                        if dossier.date_expiration
                        else None
                    ),
                    (
                        dossier.date_suppression_par_usager.isoformat()
                        if dossier.date_suppression_par_usager
                        else None
                    ),
                ),
            )
        connection.commit()
        logging.info(f"Démarches saved to SQLite database: {ds_db_path}")
        connection.close()
    except Exception as e:
        raise e


def send_to_minio(**kwargs):
    ds_db_path = DS_TMP_FOLDER + "demarches_simplifiees.db"
    ds_db_path_zip = f"{ds_db_path}.gz"

    # Zip database
    with open(ds_db_path, "rb") as f_in:
        with gzip.open(ds_db_path_zip, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)

    os.remove(ds_db_path)

    logging.info("Sending database: emarches_simplifiees.db.gz")
    minio_client.send_files(
        [
            {
                "source_path": DS_TMP_FOLDER,
                "source_name": "demarches_simplifiees.db.gz",
                "dest_path": DS_MINIO_DATA_PATH,
                "dest_name": "demarches_simplifiees.db.gz",
            }
        ]
    )
    # Delete the local file after uploading to Minio
    if os.path.exists(ds_db_path_zip):
        os.remove(ds_db_path_zip)
    else:
        logging.warning(f"Warning: Database file '{ds_db_path_zip}' not found.")
