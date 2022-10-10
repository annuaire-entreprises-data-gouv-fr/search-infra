import json
import logging
import os
import re
import shutil
import sqlite3
import zipfile
from urllib.request import urlopen

import pandas as pd
import requests
from airflow.models import Variable
from dag_datalake_sirene.elasticsearch.create_sirene_index import ElasticCreateSiren
from dag_datalake_sirene.elasticsearch.indexing_unite_legale import (
    index_unites_legales_by_chunk,
)
from dag_datalake_sirene.helpers.utils import process_elus_files
from elasticsearch_dsl import connections
from minio import Minio

TMP_FOLDER = "/tmp/"
DAG_FOLDER = "dag_datalake_sirene/"
DAG_NAME = "insert-elk-sirene"
DATA_DIR = TMP_FOLDER + DAG_FOLDER + DAG_NAME + "/data/"
SIRENE_DATABASE_LOCATION = DATA_DIR + "sirene.db"
DIRIG_DATABASE_LOCATION = DATA_DIR + "inpi.db"
AIRFLOW_DAG_HOME = "/opt/airflow/dags/"
ELASTIC_BULK_SIZE = 1500
PATH_MINIO_INPI_DATA = "inpi/"

AIRFLOW_URL = Variable.get("AIRFLOW_URL")
COLOR_URL = Variable.get("COLOR_URL")
ELASTIC_PASSWORD = Variable.get("ELASTIC_PASSWORD")
ELASTIC_URL = Variable.get("ELASTIC_URL")
ELASTIC_USER = Variable.get("ELASTIC_USER")
MINIO_BUCKET = Variable.get("MINIO_BUCKET")
MINIO_PASSWORD = Variable.get("MINIO_PASSWORD")
MINIO_URL = Variable.get("MINIO_URL")
MINIO_USER = Variable.get("MINIO_USER")
ENV = Variable.get("ENV")


def get_colors(**kwargs):
    try:
        with urlopen(COLOR_URL, timeout=5) as url:
            data = json.loads(url.read().decode())
            next_color = data["NEXT_COLOR"]
            current_color = data["CURRENT_COLOR"]
            logging.info(f"******************** Color file URL: {COLOR_URL}")
            logging.info(f"******************** Next color from file: {next_color}")
            kwargs["ti"].xcom_push(key="next_color", value=next_color)
            kwargs["ti"].xcom_push(key="current_color", value=current_color)
    except BaseException as error:
        raise Exception(f"******************** Ouuups Error: {error}")


# Connect to database
def connect_to_db(db_location):
    db_conn = sqlite3.connect(db_location)
    logging.info(f"*********** Connecting to database {db_location}! ***********")
    db_cursor = db_conn.cursor()
    return db_conn, db_cursor


def commit_and_close_conn(db_conn):
    db_conn.commit()
    db_conn.close()


def preprocess_dirigeants_pp(query):
    cols = [column[0] for column in query.description]
    rep_chunk = pd.DataFrame.from_records(data=query.fetchall(), columns=cols)
    rep_chunk.sort_values(
        by=[
            "siren",
            "nom_patronymique",
            "nom_usage",
            "prenoms",
            "datenaissance",
            "villenaissance",
            "paysnaissance",
            "qualite",
        ],
        inplace=True,
        ascending=[True, False, False, False, False, False, False, False],
    )
    rep_chunk.drop_duplicates(
        subset=[
            "siren",
            "nom_patronymique",
            "nom_usage",
            "prenoms",
            "datenaissance",
            "villenaissance",
            "paysnaissance",
            "qualite",
        ],
        keep="first",
        inplace=True,
    )
    rep_clean = (
        rep_chunk.groupby(
            by=[
                "siren",
                "nom_patronymique",
                "nom_usage",
                "prenoms",
                "datenaissance",
                "villenaissance",
                "paysnaissance",
            ]
        )["qualite"]
        .apply(lambda x: ", ".join(x))
        .reset_index()
    )
    return rep_clean


def preprocess_dirigeant_pm(query):
    cols = [column[0] for column in query.description]
    rep_chunk = pd.DataFrame.from_records(data=query.fetchall(), columns=cols)
    rep_chunk.sort_values(
        by=["siren", "siren_pm", "denomination", "sigle", "qualite"],
        inplace=True,
        ascending=[True, False, False, False, False],
    )
    rep_chunk.drop_duplicates(
        subset=["siren", "siren_pm", "denomination", "sigle", "qualite"],
        keep="first",
        inplace=True,
    )
    rep_clean = (
        rep_chunk.groupby(by=["siren", "siren_pm", "denomination", "sigle"])["qualite"]
        .apply(lambda x: ", ".join(x))
        .reset_index()
    )
    return rep_clean


def create_sqlite_database():
    if os.path.exists(DATA_DIR) and os.path.isdir(DATA_DIR):
        shutil.rmtree(DATA_DIR)
    os.makedirs(os.path.dirname(DATA_DIR), exist_ok=True)
    if os.path.exists(SIRENE_DATABASE_LOCATION):
        os.remove(SIRENE_DATABASE_LOCATION)
        logging.info(
            f"******************** Existing database removed from "
            f"{SIRENE_DATABASE_LOCATION}"
        )
    siren_db_conn = sqlite3.connect(SIRENE_DATABASE_LOCATION)
    logging.info(
        "******************* Creating and connecting to database! *******************"
    )
    commit_and_close_conn(siren_db_conn)


def create_unite_legale_table(**kwargs):
    siren_db_conn, siren_db_cursor = connect_to_db(SIRENE_DATABASE_LOCATION)
    siren_db_cursor.execute("""DROP TABLE IF EXISTS unite_legale""")
    siren_db_cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS unite_legale
        (
            siren,
            date_creation_unite_legale,
            sigle,
            prenom,
            identifiant_association_unite_legale,
            tranche_effectif_salarie_unite_legale,
            date_mise_a_jour_unite_legale,
            categorie_entreprise,
            etat_administratif_unite_legale,
            nom,
            nom_usage,
            nom_raison_sociale,
            nature_juridique_unite_legale,
            activite_principale_unite_legale,
            economie_sociale_solidaire_unite_legale
        )
    """
    )
    siren_db_cursor.execute(
        """
        CREATE UNIQUE INDEX index_siren
        ON unite_legale (siren);
        """
    )
    url = "https://files.data.gouv.fr/insee-sirene/StockUniteLegale_utf8.zip"
    r = requests.get(url, allow_redirects=True)
    open(DATA_DIR + "StockUniteLegale_utf8.zip", "wb").write(r.content)
    shutil.unpack_archive(DATA_DIR + "StockUniteLegale_utf8.zip", DATA_DIR)
    df_iterator = pd.read_csv(
        DATA_DIR + "StockUniteLegale_utf8.csv", chunksize=100000, dtype=str
    )
    # Insert rows in database by chunk
    for i, df_unite_legale in enumerate(df_iterator):
        df_unite_legale = df_unite_legale[
            [
                "siren",
                "dateCreationUniteLegale",
                "sigleUniteLegale",
                "prenom1UniteLegale",
                "identifiantAssociationUniteLegale",
                "trancheEffectifsUniteLegale",
                "dateDernierTraitementUniteLegale",
                "categorieEntreprise",
                "etatAdministratifUniteLegale",
                "nomUniteLegale",
                "nomUsageUniteLegale",
                "denominationUniteLegale",
                "categorieJuridiqueUniteLegale",
                "activitePrincipaleUniteLegale",
                "economieSocialeSolidaireUniteLegale",
            ]
        ]
        # Rename columns
        df_unite_legale = df_unite_legale.rename(
            columns={
                "dateCreationUniteLegale": "date_creation_unite_legale",
                "sigleUniteLegale": "sigle",
                "prenom1UniteLegale": "prenom",
                "trancheEffectifsUniteLegale": "tranche_effectif_salarie_unite_legale",
                "dateDernierTraitementUniteLegale": "date_mise_a_jour_unite_legale",
                "categorieEntreprise": "categorie_entreprise",
                "etatAdministratifUniteLegale": "etat_administratif_unite_legale",
                "nomUniteLegale": "nom",
                "nomUsageUniteLegale": "nom_usage",
                "denominationUniteLegale": "nom_raison_sociale",
                "categorieJuridiqueUniteLegale": "nature_juridique_unite_legale",
                "activitePrincipaleUniteLegale": "activite_principale_unite_legale",
                "economieSocialeSolidaireUniteLegale": "economie_sociale_solidaire_"
                "unite_legale",
                "identifiantAssociationUniteLegale": "identifiant_association_"
                "unite_legale",
            }
        )
        df_unite_legale.to_sql(
            "unite_legale", siren_db_conn, if_exists="append", index=False
        )

        for row in siren_db_cursor.execute("""SELECT COUNT() FROM unite_legale"""):
            logging.info(
                f"************ {row} records have been added to the unite_legale table!"
            )

    del df_unite_legale

    for count_unites_legales in siren_db_cursor.execute(
        """
        SELECT COUNT()
        FROM unite_legale
        """
    ):
        logging.info(
            f"************ {count_unites_legales} records have been added to the "
            f"unite_legale table!"
        )
    kwargs["ti"].xcom_push(key="count_unites_legales", value=count_unites_legales[0])
    commit_and_close_conn(siren_db_conn)


def create_etablissement_table():
    siren_db_conn, siren_db_cursor = connect_to_db(SIRENE_DATABASE_LOCATION)
    # Create list of departement zip codes
    all_deps = [
        *"-0".join(list(str(x) for x in range(0, 10))).split("-")[1:],
        *list(str(x) for x in range(10, 20)),
        *["2A", "2B"],
        *list(str(x) for x in range(21, 96)),
        *"-7510".join(list(str(x) for x in range(0, 10))).split("-")[1:],
        *"-751".join(list(str(x) for x in range(9, 21))).split("-")[1:],
        *["971", "972", "973", "974", "976", "98"],
        *[""],
    ]
    # Remove Paris zip code
    all_deps.remove("75")

    # Create database
    siren_db_cursor.execute("""DROP TABLE IF EXISTS siret""")
    siren_db_cursor.execute(
        """CREATE TABLE IF NOT EXISTS siret
            (
            id INTEGER NOT NULL PRIMARY KEY,
            siren,
            siret,
            date_creation,
            tranche_effectif_salarie,
            activite_principale_registre_metier,
            is_siege,
            numero_voie,
            type_voie,
            libelle_voie,
            code_postal,
            libelle_cedex,
            libelle_commune,
            commune,
            complement_adresse,
            complement_adresse_2,
            numero_voie_2,
            indice_repetition_2,
            type_voie_2,
            libelle_voie_2,
            commune_2,
            libelle_commune_2,
            cedex_2,
            libelle_cedex_2,
            cedex,
            date_debut_activite,
            distribution_speciale,
            distribution_speciale_2,
            etat_administratif_etablissement,
            enseigne_1,
            enseigne_2,
            enseigne_3,
            activite_principale,
            indice_repetition,
            nom_commercial,
            libelle_commune_etranger,
            code_pays_etranger,
            libelle_pays_etranger,
            libelle_commune_etranger_2,
            code_pays_etranger_2,
            libelle_pays_etranger_2,
            longitude,
            latitude,
            geo_adresse,
            geo_id)
            """
    )
    siren_db_cursor.execute(
        """
        CREATE INDEX index_siret
        ON siret (siren);
        """
    )

    # Upload geo data by departement
    for dep in all_deps:
        url = f"https://files.data.gouv.fr/geo-sirene/last/dep/geo_siret_{dep}.csv.gz"
        print(url)
        df_dep = pd.read_csv(
            url,
            compression="gzip",
            dtype=str,
            usecols=[
                "siren",
                "siret",
                "dateCreationEtablissement",
                "trancheEffectifsEtablissement",
                "activitePrincipaleRegistreMetiersEtablissement",
                "etablissementSiege",
                "numeroVoieEtablissement",
                "libelleVoieEtablissement",
                "codePostalEtablissement",
                "libelleCommuneEtablissement",
                "libelleCedexEtablissement",
                "typeVoieEtablissement",
                "codeCommuneEtablissement",
                "codeCedexEtablissement",
                "complementAdresseEtablissement",
                "distributionSpecialeEtablissement",
                "complementAdresse2Etablissement",
                "indiceRepetition2Etablissement",
                "libelleCedex2Etablissement",
                "codeCedex2Etablissement",
                "numeroVoie2Etablissement",
                "typeVoie2Etablissement",
                "libelleVoie2Etablissement",
                "codeCommune2Etablissement",
                "libelleCommune2Etablissement",
                "distributionSpeciale2Etablissement",
                "dateDebut",
                "etatAdministratifEtablissement",
                "enseigne1Etablissement",
                "enseigne1Etablissement",
                "enseigne2Etablissement",
                "enseigne3Etablissement",
                "denominationUsuelleEtablissement",
                "activitePrincipaleEtablissement",
                "geo_adresse",
                "geo_id",
                "longitude",
                "latitude",
                "indiceRepetitionEtablissement",
                "libelleCommuneEtrangerEtablissement",
                "codePaysEtrangerEtablissement",
                "libellePaysEtrangerEtablissement",
                "libelleCommuneEtranger2Etablissement",
                "codePaysEtranger2Etablissement",
                "libellePaysEtranger2Etablissement",
            ],
        )
        df_dep = df_dep.rename(
            columns={
                "dateCreationEtablissement": "date_creation",
                "trancheEffectifsEtablissement": "tranche_effectif_salarie",
                "activitePrincipaleRegistreMetiersEtablissement": "activite_principale"
                "_registre_metier",
                "etablissementSiege": "is_siege",
                "numeroVoieEtablissement": "numero_voie",
                "typeVoieEtablissement": "type_voie",
                "libelleVoieEtablissement": "libelle_voie",
                "codePostalEtablissement": "code_postal",
                "libelleCedexEtablissement": "libelle_cedex",
                "libelleCommuneEtablissement": "libelle_commune",
                "codeCommuneEtablissement": "commune",
                "complementAdresseEtablissement": "complement_adresse",
                "complementAdresse2Etablissement": "complement_adresse_2",
                "numeroVoie2Etablissement": "numero_voie_2",
                "indiceRepetition2Etablissement": "indice_repetition_2",
                "typeVoie2Etablissement": "type_voie_2",
                "libelleVoie2Etablissement": "libelle_voie_2",
                "codeCommune2Etablissement": "commune_2",
                "libelleCommune2Etablissement": "libelle_commune_2",
                "codeCedex2Etablissement": "cedex_2",
                "libelleCedex2Etablissement": "libelle_cedex_2",
                "codeCedexEtablissement": "cedex",
                "dateDebut": "date_debut_activite",
                "distributionSpecialeEtablissement": "distribution_speciale",
                "distributionSpeciale2Etablissement": "distribution_speciale_2",
                "etatAdministratifEtablissement": "etat_administratif_etablissement",
                "enseigne1Etablissement": "enseigne_1",
                "enseigne2Etablissement": "enseigne_2",
                "enseigne3Etablissement": "enseigne_3",
                "activitePrincipaleEtablissement": "activite_principale",
                "indiceRepetitionEtablissement": "indice_repetition",
                "denominationUsuelleEtablissement": "nom_commercial",
                "libelleCommuneEtrangerEtablissement": "libelle_commune_etranger",
                "codePaysEtrangerEtablissement": "code_pays_etranger",
                "libellePaysEtrangerEtablissement": "libelle_pays_etranger",
                "libelleCommuneEtranger2Etablissement": "libelle_commune_etranger_2",
                "codePaysEtranger2Etablissement": "code_pays_etranger_2",
                "libellePaysEtranger2Etablissement": "libelle_pays_etranger_2",
            }
        )
        df_dep.to_sql("siret", siren_db_conn, if_exists="append", index=False)
        siren_db_conn.commit()
        for row in siren_db_cursor.execute("""SELECT COUNT() FROM siret"""):
            logging.info(
                f"************ {row} records have been added to the unite_legale table!"
            )
    del df_dep
    commit_and_close_conn(siren_db_conn)


def count_nombre_etablissements():
    # Connect to database
    siren_db_conn, siren_db_cursor = connect_to_db(SIRENE_DATABASE_LOCATION)
    # Create a count table
    siren_db_cursor.execute("""DROP TABLE IF EXISTS count_etab""")
    siren_db_cursor.execute(
        """CREATE TABLE count_etab (siren VARCHAR(10), count INTEGER)"""
    )
    # Create index
    siren_db_cursor.execute(
        """
        CREATE UNIQUE INDEX index_count_siren
        ON count_etab (siren);
        """
    )
    siren_db_cursor.execute(
        """
        INSERT INTO count_etab (siren, count)
        SELECT siren, count(*) as count
        FROM siret GROUP BY siren;
        """
    )
    commit_and_close_conn(siren_db_conn)


def count_nombre_etablissements_ouverts():
    siren_db_conn, siren_db_cursor = connect_to_db(SIRENE_DATABASE_LOCATION)
    siren_db_cursor.execute("""DROP TABLE IF EXISTS count_etab_ouvert""")
    siren_db_cursor.execute(
        """CREATE TABLE count_etab_ouvert (siren VARCHAR(10), count INTEGER)"""
    )
    siren_db_cursor.execute(
        """
        CREATE UNIQUE INDEX index_count_ouvert_siren
        ON count_etab_ouvert (siren);
        """
    )
    siren_db_cursor.execute(
        """
        INSERT INTO count_etab_ouvert (siren, count)
        SELECT siren, count(*) as count
        FROM siret
        WHERE etat_administratif_etablissement = 'A' GROUP BY siren;
        """
    )
    commit_and_close_conn(siren_db_conn)


def create_siege_only_table(**kwargs):
    siren_db_conn, siren_db_cursor = connect_to_db(SIRENE_DATABASE_LOCATION)
    siren_db_cursor.execute("""DROP TABLE IF EXISTS siretsiege""")
    siren_db_cursor.execute(
        """CREATE TABLE IF NOT EXISTS siretsiege
            (
            id INTEGER NOT NULL PRIMARY KEY,
            siren,
            siret,
            date_creation,
            tranche_effectif_salarie,
            activite_principale_registre_metier,
            is_siege,
            numero_voie,
            type_voie,
            libelle_voie,
            code_postal,
            libelle_cedex,
            libelle_commune,
            commune,
            complement_adresse,
            complement_adresse_2,
            numero_voie_2,
            indice_repetition_2,
            type_voie_2,
            libelle_voie_2,
            commune_2,
            libelle_commune_2,
            cedex_2,
            libelle_cedex_2,
            cedex,
            date_debut_activite,
            distribution_speciale,
            distribution_speciale_2,
            etat_administratif_etablissement,
            enseigne_1,
            enseigne_2,
            enseigne_3,
            activite_principale,
            indice_repetition,
            nom_commercial,
            libelle_commune_etranger,
            code_pays_etranger,
            libelle_pays_etranger,
            libelle_commune_etranger_2,
            code_pays_etranger_2,
            libelle_pays_etranger_2,
            longitude,
            latitude,
            geo_adresse,
            geo_id)
    """
    )
    siren_db_cursor.execute(
        """INSERT INTO siretsiege (
            siren,
            siret,
            date_creation,
            tranche_effectif_salarie,
            activite_principale_registre_metier,
            is_siege,
            numero_voie,
            type_voie,
            libelle_voie,
            code_postal,
            libelle_cedex,
            libelle_commune,
            commune,
            complement_adresse,
            complement_adresse_2,
            numero_voie_2,
            indice_repetition_2,
            type_voie_2,
            libelle_voie_2,
            commune_2,
            libelle_commune_2,
            cedex_2,
            libelle_cedex_2,
            cedex,
            date_debut_activite,
            distribution_speciale,
            distribution_speciale_2,
            etat_administratif_etablissement,
            enseigne_1,
            enseigne_2,
            enseigne_3,
            activite_principale,
            indice_repetition,
            nom_commercial,
            libelle_commune_etranger,
            code_pays_etranger,
            libelle_pays_etranger,
            libelle_commune_etranger_2,
            code_pays_etranger_2,
            libelle_pays_etranger_2,
            longitude,
            latitude,
            geo_adresse,
            geo_id)
        SELECT
            siren,
            siret,
            date_creation,
            tranche_effectif_salarie,
            activite_principale_registre_metier,
            is_siege,
            numero_voie,
            type_voie,
            libelle_voie,
            code_postal,
            libelle_cedex,
            libelle_commune,
            commune,
            complement_adresse,
            complement_adresse_2,
            numero_voie_2,
            indice_repetition_2,
            type_voie_2,
            libelle_voie_2,
            commune_2,
            libelle_commune_2,
            cedex_2,
            libelle_cedex_2,
            cedex,
            date_debut_activite,
            distribution_speciale,
            distribution_speciale_2,
            etat_administratif_etablissement,
            enseigne_1,
            enseigne_2,
            enseigne_3,
            activite_principale,
            indice_repetition,
            nom_commercial,
            libelle_commune_etranger,
            code_pays_etranger,
            libelle_pays_etranger,
            libelle_commune_etranger_2,
            code_pays_etranger_2,
            libelle_pays_etranger_2,
            longitude,
            latitude,
            geo_adresse,
            geo_id
        FROM siret
        WHERE is_siege = 'true';
    """
    )
    siren_db_cursor.execute(
        """CREATE INDEX index_siret_siren
        ON siretsiege (siren);"""
    )
    for count_sieges in siren_db_cursor.execute("""SELECT COUNT() FROM siretsiege"""):
        logging.info(
            f"************ {count_sieges} records have been added to the "
            f"unite_legale table!"
        )
    kwargs["ti"].xcom_push(key="count_sieges", value=count_sieges[0])
    commit_and_close_conn(siren_db_conn)


def get_dirig_database():
    minio_url = MINIO_URL
    minio_bucket = MINIO_BUCKET
    minio_user = MINIO_USER
    minio_password = MINIO_PASSWORD

    client = Minio(
        minio_url,
        access_key=minio_user,
        secret_key=minio_password,
        secure=True,
    )
    client.fget_object(
        minio_bucket,
        f"{PATH_MINIO_INPI_DATA}inpi.db",
        DIRIG_DATABASE_LOCATION,
    )


def create_dirig_pp_table():
    dirig_db_conn, dirig_db_cursor = connect_to_db(DIRIG_DATABASE_LOCATION)
    siren_db_conn, siren_db_cursor = connect_to_db(SIRENE_DATABASE_LOCATION)

    chunk_size = int(100000)
    for row in dirig_db_cursor.execute("""SELECT count(DISTINCT siren) FROM rep_pp;"""):
        nb_iter = int(int(row[0]) / chunk_size) + 1

    # Create table dirigeants_pp in siren database
    siren_db_cursor.execute("""DROP TABLE IF EXISTS dirigeant_pp""")
    siren_db_cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS dirigeant_pp
        (
            siren,
            nom_patronymique,
            nom_usage,
            prenoms,
            datenaissance,
            villenaissance,
            paysnaissance,
            qualite
        )
    """
    )
    siren_db_cursor.execute(
        """
                    CREATE INDEX siren_pp
                    ON dirigeant_pp (siren);
                    """
    )

    for i in range(nb_iter):
        query = dirig_db_cursor.execute(
            f"""
        SELECT DISTINCT siren, nom_patronymique, nom_usage, prenoms,
        datenaissance, villenaissance, paysnaissance, qualite
        FROM rep_pp
        WHERE siren IN
            (
            SELECT DISTINCT siren
            FROM rep_pp
            WHERE siren != ''
            LIMIT {chunk_size}
            OFFSET {int(i * chunk_size)})
        """
        )
        dir_pp_clean = preprocess_dirigeants_pp(query)
        dir_pp_clean.to_sql(
            "dirigeant_pp",
            siren_db_conn,
            if_exists="append",
            index=False,
        )
        logging.info(f"Iter: {i}")
    del dir_pp_clean
    commit_and_close_conn(siren_db_conn)
    commit_and_close_conn(dirig_db_conn)


def create_dirig_pm_table():
    dirig_db_conn, dirig_db_cursor = connect_to_db(DIRIG_DATABASE_LOCATION)
    siren_db_conn, siren_db_cursor = connect_to_db(SIRENE_DATABASE_LOCATION)

    chunk_size = int(100000)
    for row in dirig_db_cursor.execute("""SELECT count(DISTINCT siren) FROM rep_pm;"""):
        nb_iter = int(int(row[0]) / chunk_size) + 1

    # Create table dirigeants_pm in siren database
    siren_db_cursor.execute("""DROP TABLE IF EXISTS dirigeant_pm""")
    siren_db_cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS dirigeant_pm
        (
            siren,
            siren_pm,
            denomination,
            sigle,
            qualite
        )
    """
    )
    siren_db_cursor.execute(
        """
                    CREATE INDEX siren_pm
                    ON dirigeant_pm (siren);
                    """
    )

    for i in range(nb_iter):
        query = dirig_db_cursor.execute(
            f"""
        SELECT DISTINCT siren, siren_pm, denomination, sigle, qualite
        FROM rep_pm
        WHERE siren IN
        (
            SELECT DISTINCT siren
            FROM rep_pm
            WHERE siren != ''
            LIMIT {chunk_size}
            OFFSET {int(i * chunk_size)})
        """
        )
        dir_pm_clean = preprocess_dirigeant_pm(query)
        dir_pm_clean.to_sql(
            "dirigeant_pm", siren_db_conn, if_exists="append", index=False
        )
        logging.info(f"Iter: {i}")
    del dir_pm_clean
    commit_and_close_conn(siren_db_conn)
    commit_and_close_conn(dirig_db_conn)


def create_elastic_index(**kwargs):
    next_color = kwargs["ti"].xcom_pull(key="next_color", task_ids="get_colors")
    elastic_index = f"siren-{next_color}"
    logging.info(f"******************** Index to create: {elastic_index}")
    create_index = ElasticCreateSiren(
        elastic_url=ELASTIC_URL,
        elastic_index=elastic_index,
        elastic_user=ELASTIC_USER,
        elastic_password=ELASTIC_PASSWORD,
        elastic_bulk_size=ELASTIC_BULK_SIZE,
    )
    create_index.execute()


def fill_elastic_index(**kwargs):
    next_color = kwargs["ti"].xcom_pull(key="next_color", task_ids="get_colors")
    elastic_index = f"siren-{next_color}"
    siren_db_conn, siren_db_cursor = connect_to_db(SIRENE_DATABASE_LOCATION)
    siren_db_cursor.execute(
        """SELECT
        ul.siren,
        st.siret as siret_siege,
        st.date_creation as date_creation_siege,
        st.tranche_effectif_salarie as tranche_effectif_salarie_siege,
        st.date_debut_activite as date_debut_activite_siege,
        st.etat_administratif_etablissement as etat_administratif_siege,
        st.activite_principale as activite_principale_siege,
        st.complement_adresse as complement_adresse,
        st.numero_voie as numero_voie,
        st.indice_repetition as indice_repetition,
        st.type_voie as type_voie,
        st.libelle_voie as libelle_voie,
        st.distribution_speciale as distribution_speciale,
        st.cedex as cedex,
        st.libelle_cedex as libelle_cedex,
        st.commune as commune,
        st.libelle_commune as libelle_commune,
        st.code_pays_etranger as code_pays_etranger,
        st.libelle_commune_etranger as libelle_commune_etranger,
        st.libelle_pays_etranger as libelle_pays_etranger,
        st.code_postal as code_postal,
        st.geo_id as geo_id,
        st.longitude as longitude,
        st.latitude as latitude,
        st.activite_principale_registre_metier as activite_principale_registre_metier,
        ul.date_creation_unite_legale as date_creation_unite_legale,
        ul.tranche_effectif_salarie_unite_legale
        as tranche_effectif_salarie_unite_legale,
        ul.date_mise_a_jour_unite_legale as date_mise_a_jour,
        ul.categorie_entreprise as categorie_entreprise,
        ul.etat_administratif_unite_legale as etat_administratif_unite_legale,
        ul.nom_raison_sociale as nom_raison_sociale,
        ul.nature_juridique_unite_legale as nature_juridique_unite_legale,
        ul.activite_principale_unite_legale as activite_principale_unite_legale,
        ul.economie_sociale_solidaire_unite_legale as
        economie_sociale_solidaire_unite_legale,
        (SELECT count FROM count_etab ce WHERE ce.siren = st.siren) as
        nombre_etablissements,
        (SELECT count FROM count_etab_ouvert ceo WHERE ceo.siren = st.siren) as
        nombre_etablissements_ouverts,
        (SELECT json_group_array(
            json_object(
                'enseigne_1', enseigne_1,
                'enseigne_2', enseigne_2,
                'enseigne_3', enseigne_3)
            ) FROM
            (SELECT enseigne_1, enseigne_2, enseigne_3 from siret
            WHERE siren = st.siren)
        ) as enseignes,
        (SELECT json_group_array(
            json_object(
            'complement_adresse', complement_adresse,
            'numero_voie', numero_voie,
            'indice_repetition', indice_repetition,
            'type_voie', type_voie,
            'libelle_voie', libelle_voie,
            'libelle_commune', libelle_commune,
            'libelle_cedex', libelle_cedex,
            'distribution_speciale', distribution_speciale,
            'commune', commune,
            'cedex', cedex,
            'libelle_commune_etranger', libelle_commune_etranger,
            'libelle_pays_etranger', libelle_pays_etranger)
            ) FROM
            (SELECT complement_adresse, numero_voie, indice_repetition,
            type_voie, libelle_voie, libelle_commune, distribution_speciale,
            commune, cedex, libelle_commune_etranger, libelle_pays_etranger
            FROM siret
            WHERE siren = st.siren)
            ) as adresses,
            ul.sigle as sigle,
            ul.prenom as prenom,
            ul.nom as nom,
            ul.nom_usage as nom_usage,
            st.is_siege as is_siege,
            (SELECT json_group_array(
                json_object(
                    'siren', siren,
                    'nom_patronymique', nom_patronymique,
                    'nom_usage', nom_usage,
                    'prenoms', prenoms,
                    'date_naissance', datenaissance,
                    'ville_naissance', villenaissance,
                    'pays_naissance', paysnaissance,
                    'qualite', qualite
                    )
                ) FROM
                (
                    SELECT siren, nom_patronymique, nom_usage, prenoms,
                    datenaissance, villenaissance, paysnaissance, qualite
                    FROM dirigeant_pp
                    WHERE siren = st.siren
                )
            ) as dirigeants_pp,
        (SELECT json_group_array(
                json_object(
                    'siren', siren,
                    'siren_pm', siren_pm,
                    'denomination', denomination,
                    'sigle', sigle,
                    'qualite', qualite
                    )
                ) FROM
                (
                    SELECT siren, siren_pm, denomination, sigle, qualite
                    FROM dirigeant_pm
                    WHERE siren = st.siren
                )
            ) as dirigeants_pm,
        (SELECT code_colter
            FROM colter
            WHERE siren = st.siren
        ) as code_colter,
        (SELECT niveau_colter
            FROM colter
            WHERE siren = st.siren
        ) as niveau_colter,
        (SELECT code_insee
            FROM colter
            WHERE siren = st.siren
        ) as code_insee,
        (SELECT json_group_array(
            json_object(
                'nom', nom_elu,
                'prenom', prenom_elu,
                'date_naissance', date_naissance_elu,
                'sexe', sexe_elu,
                'fonction', fonction_elu
                )
            ) FROM
            (
                SELECT nom_elu, prenom_elu, date_naissance_elu, sexe_elu, fonction_elu
                FROM colter_elus
                WHERE siren = st.siren
            )
        ) as colter_elus
        FROM
            siretsiege st
        LEFT JOIN
            unite_legale ul
        ON
            ul.siren = st.siren;"""
    )
    connections.create_connection(
        hosts=[ELASTIC_URL],
        http_auth=(ELASTIC_USER, ELASTIC_PASSWORD),
        retry_on_timeout=True,
    )
    elastic_connection = connections.get_connection()

    doc_count = index_unites_legales_by_chunk(
        cursor=siren_db_cursor,
        elastic_connection=elastic_connection,
        elastic_bulk_size=ELASTIC_BULK_SIZE,
        elastic_index=elastic_index,
    )
    kwargs["ti"].xcom_push(key="doc_count", value=doc_count)
    commit_and_close_conn(siren_db_conn)


def check_elastic_index(**kwargs):
    doc_count = kwargs["ti"].xcom_pull(key="doc_count", task_ids="fill_elastic_index")
    count_sieges = kwargs["ti"].xcom_pull(
        key="count_sieges", task_ids="create_siege_only_table"
    )

    logging.info(f"******************** Documents indexed: {doc_count}")

    if float(count_sieges) - float(doc_count) > 7000:
        raise ValueError(
            f"*******The data has not been correctly indexed: "
            f"{doc_count} documents indexed instead of {count_sieges}."
        )


def update_color_file(**kwargs):
    next_color = kwargs["ti"].xcom_pull(key="next_color", task_ids="get_colors")
    current_color = kwargs["ti"].xcom_pull(key="current_color", task_ids="get_colors")
    colors = {"CURRENT_COLOR": next_color, "NEXT_COLOR": current_color}
    logging.info(f"******************** Next color configuration: {colors}")

    with open("colors.json", "w") as write_file:
        json.dump(colors, write_file)
    minio_filepath = f"ae/colors-{ENV}.json"
    minio_url = MINIO_URL
    minio_bucket = MINIO_BUCKET
    minio_user = MINIO_USER
    minio_password = MINIO_PASSWORD

    # Start client
    client = Minio(
        minio_url,
        access_key=minio_user,
        secret_key=minio_password,
        secure=True,
    )

    # Check if bucket exists
    found = client.bucket_exists(minio_bucket)
    if found:
        client.fput_object(
            bucket_name=minio_bucket,
            object_name=minio_filepath,
            file_path="colors.json",
            content_type="application/json",
        )


def create_sitemap():
    siren_db_conn, siren_db_cursor = connect_to_db(SIRENE_DATABASE_LOCATION)
    siren_db_cursor.execute(
        """SELECT
        ul.siren,
        ul.nom_raison_sociale as nom_raison_sociale,
        ul.sigle as sigle,
        ul.etat_administratif_unite_legale as etat_administratif_unite_legale,
        ul.nature_juridique_unite_legale as nature_juridique_unite_legale,
        st.code_postal as code_postal,
        ul.activite_principale_unite_legale as activite_principale_unite_legale
        FROM
            unite_legale ul
        JOIN
            siretsiege st
        ON st.siren = ul.siren;"""  # noqa
    )

    if os.path.exists(DATA_DIR + "sitemap-" + ENV + ".csv"):
        os.remove(DATA_DIR + "sitemap-" + ENV + ".csv")

    chunk_unites_legales_sqlite = 1
    while chunk_unites_legales_sqlite:
        chunk_unites_legales_sqlite = siren_db_cursor.fetchmany(1500)
        unite_legale_columns = tuple([x[0] for x in siren_db_cursor.description])
        liste_unites_legales_sqlite = []
        # Group all fetched unites_legales from sqlite in one list
        for unite_legale in chunk_unites_legales_sqlite:
            liste_unites_legales_sqlite.append(
                {
                    unite_legale_columns: value
                    for unite_legale_columns, value in zip(
                        unite_legale_columns, unite_legale
                    )
                }
            )
        noms_url = ""
        for ul in liste_unites_legales_sqlite:
            if (
                ul["etat_administratif_unite_legale"] == "A"
                and ul["nature_juridique_unite_legale"] != "1000"
            ):
                if not ul["code_postal"]:
                    ul["code_postal"] = ""
                if not ul["activite_principale_unite_legale"]:
                    ul["activite_principale_unite_legale"] = ""
                array_url = [ul["nom_raison_sociale"], ul["sigle"], ul["siren"]]
                nom_url = str(
                    re.sub(
                        "[^0-9a-zA-Z]+", "-", "-".join(filter(None, array_url))
                    ).lower()
                )
                noms_url = (
                    noms_url
                    + ul["code_postal"]
                    + ","
                    + ul["activite_principale_unite_legale"]
                    + ","
                    + nom_url
                    + "\n"
                )

        with open(DATA_DIR + "sitemap-" + ENV + ".csv", "a+") as f:
            f.write(noms_url)


def update_sitemap():

    minio_filepath = "ae/sitemap-" + ENV + ".csv"
    minio_url = MINIO_URL
    minio_bucket = MINIO_BUCKET
    minio_user = MINIO_USER
    minio_password = MINIO_PASSWORD

    # Start client
    client = Minio(
        minio_url,
        access_key=minio_user,
        secret_key=minio_password,
        secure=True,
    )

    # Check if bucket exists
    found = client.bucket_exists(minio_bucket)
    if found:
        client.fput_object(
            bucket_name=minio_bucket,
            object_name=minio_filepath,
            file_path=DATA_DIR + "sitemap-" + ENV + ".csv",
            content_type="text/csv",
        )


def preprocess_colter(**kwargs):
    # Process Régions
    df = pd.read_csv(
        "https://www.data.gouv.fr/fr/datasets/r/619ee62e-8f9e-4c62-b166-abc6f2b86201",
        dtype=str,
        sep=";",
    )
    df = df[df["exer"] == df.exer.max()][["reg_code", "siren"]]
    df = df.drop_duplicates(keep="first")
    df = df.rename(columns={"reg_code": "code_insee"})
    df["code_colter"] = df["code_insee"]
    df["niveau_colter"] = "region"

    # Cas particulier Corse
    df.loc[df["code_insee"] == "94", "niveau_colter"] = "particulier"
    dfcolter = df

    # Process Départements
    df = pd.read_csv(
        "https://www.data.gouv.fr/fr/datasets/r/2f4f901d-e3ce-4760-b122-56a311340fc4",
        dtype=str,
        sep=";",
    )
    df = df[df["exer"] == df["exer"].max()]
    df = df[["dep_code", "siren"]]
    df = df.drop_duplicates(keep="first")
    df = df.rename(columns={"dep_code": "code_insee"})
    df["code_colter"] = df["code_insee"] + "D"
    df["niveau_colter"] = "departement"

    # Cas Métropole de Lyon
    df.loc[df["code_insee"] == "691", "code_colter"] = "69M"
    df.loc[df["code_insee"] == "691", "niveau_colter"] = "particulier"
    df.loc[df["code_insee"] == "691", "code_insee"] = None

    # Cas Conseil départemental du Rhone
    df.loc[df["code_insee"] == "69", "niveau_colter"] = "particulier"
    df.loc[df["code_insee"] == "69", "code_insee"] = None

    # Cas Collectivité Européenne d"Alsace
    df.loc[df["code_insee"] == "67A", "code_colter"] = "6AE"
    df.loc[df["code_insee"] == "67A", "niveau_colter"] = "particulier"
    df.loc[df["code_insee"] == "67A", "code_insee"] = None

    # Remove Paris
    df = df[df["code_insee"] != "75"]

    dfcolter = pd.concat([dfcolter, df])

    # Process EPCI
    df = pd.read_excel(
        "https://www.collectivites-locales.gouv.fr/files/2022/epcisanscom2022.xlsx",
        dtype=str,
        engine="openpyxl",
    )
    df["code_insee"] = None
    df["siren"] = df["siren_epci"]
    df["code_colter"] = df["siren"]
    df["niveau_colter"] = "epci"
    df = df[["code_insee", "siren", "code_colter", "niveau_colter"]]
    dfcolter = pd.concat([dfcolter, df])

    # Process Communes
    URL = "https://www.data.gouv.fr/fr/datasets/r/42b16d68-958e-4518-8551-93e095fe8fda"
    response = requests.get(URL)
    open("/tmp/siren-communes.zip", "wb").write(response.content)

    with zipfile.ZipFile("/tmp/siren-communes.zip", "r") as zip_ref:
        zip_ref.extractall("/tmp/siren-communes")

    df = pd.read_excel(
        "/tmp/siren-communes/Banatic_SirenInsee2022.xlsx", dtype=str, engine="openpyxl"
    )
    df["code_insee"] = df["insee"]
    df["code_colter"] = df["insee"]
    df["niveau_colter"] = "commune"
    df = df[["code_insee", "siren", "code_colter", "niveau_colter"]]
    df.loc[df["code_insee"] == "75056", "code_colter"] = "75C"
    df.loc[df["code_insee"] == "75056", "niveau_colter"] = "particulier"

    dfcolter = pd.concat([dfcolter, df])

    if os.path.exists(DATA_DIR + "colter.csv"):
        os.remove(DATA_DIR + "colter.csv")

    dfcolter.to_csv(DATA_DIR + "colter.csv", index=False)

    siren_db_conn, siren_db_cursor = connect_to_db(SIRENE_DATABASE_LOCATION)

    # Create table colter in siren database
    siren_db_cursor.execute("""DROP TABLE IF EXISTS colter""")
    siren_db_cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS colter
        (
            siren,
            code_insee,
            code_colter,
            niveau_colter
        )
        """
    )

    siren_db_cursor.execute(
        """
        CREATE INDEX siren_colter
        ON colter (siren);
        """
    )

    dfcolter.to_sql("colter", siren_db_conn, if_exists="append", index=False)


def preprocess_elus_colter(**kwargs):
    colter = pd.read_csv(DATA_DIR + "colter.csv", dtype=str)
    # Conseillers régionaux
    elus = process_elus_files(
        "https://www.data.gouv.fr/fr/datasets/r/430e13f9-834b-4411-a1a8-da0b4b6e715c",
        "Code de la région",
    )
    # Conseillers départementaux
    df = process_elus_files(
        "https://www.data.gouv.fr/fr/datasets/r/601ef073-d986-4582-8e1a-ed14dc857fba",
        "Code du département",
    )
    df["code_colter"] = df["code_colter"] + "D"
    df.loc[df["code_colter"] == "6AED", "code_colter"] = "6AE"
    elus = pd.concat([elus, df])
    # membres des assemblées des collectivités à statut particulier
    df = process_elus_files(
        "https://www.data.gouv.fr/fr/datasets/r/a595be27-cfab-4810-b9d4-22e193bffe35",
        "Code de la collectivité à statut particulier",
    )
    df.loc[df["code_colter"] == "972", "code_colter"] = "02"
    df.loc[df["code_colter"] == "973", "code_colter"] = "03"
    elus = pd.concat([elus, df])
    # Conseillers communautaires
    df = process_elus_files(
        "https://www.data.gouv.fr/fr/datasets/r/41d95d7d-b172-4636-ac44-32656367cdc7",
        "N° SIREN",
    )
    elus = pd.concat([elus, df])
    # Conseillers municipaux
    df = process_elus_files(
        "https://www.data.gouv.fr/fr/datasets/r/d5f400de-ae3f-4966-8cb6-a85c70c6c24a",
        "Code de la commune",
    )
    df.loc[df["code_colter"] == "75056", "code_colter"] = "75C"
    elus = pd.concat([elus, df])
    colter_elus = elus.merge(colter, on="code_colter", how="left")
    colter_elus = colter_elus[colter_elus["siren"].notna()]
    colter_elus["date_naissance_elu"] = colter_elus["date_naissance_elu"].apply(
        lambda x: x.split("/")[2] + "-" + x.split("/")[1] + "-" + x.split("/")[0]
    )
    colter_elus = colter_elus[
        [
            "siren",
            "nom_elu",
            "prenom_elu",
            "date_naissance_elu",
            "sexe_elu",
            "fonction_elu"
        ]
    ]
    siren_db_conn, siren_db_cursor = connect_to_db(SIRENE_DATABASE_LOCATION)

    # Create table colter in siren database
    siren_db_cursor.execute("""DROP TABLE IF EXISTS colter_elus""")
    siren_db_cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS colter_elus
        (
            siren,
            nom_elu,
            prenom_elu,
            date_naissance_elu,
            sexe_elu,
            fonction_elu
        )
        """
    )
    siren_db_cursor.execute(
        """
        CREATE INDEX siren_colter_elus ON colter_elus (siren);
        """
    )

    colter_elus.to_sql("colter_elus", siren_db_conn, if_exists="append", index=False)
