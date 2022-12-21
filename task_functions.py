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
from dag_datalake_sirene.data_aggregation.collectivite_territoriale import (
    process_elus_files,
)
from dag_datalake_sirene.elasticsearch.create_sirene_index import ElasticCreateSiren
from dag_datalake_sirene.elasticsearch.indexing_unite_legale import (
    index_unites_legales_by_chunk,
)
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
                "economieSocialeSolidaireUniteLegale": "economie_sociale_solidaire"
                "_unite_legale",
                "identifiantAssociationUniteLegale": "identifiant_association"
                "_unite_legale",
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
            est_siege,
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
                "etablissementSiege": "est_siege",
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
            est_siege,
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
            est_siege,
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
            est_siege,
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
        WHERE est_siege = 'true';
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


def get_object_minio(
    filename: str,
    minio_path: str,
    local_path: str,
) -> None:
    print(filename, minio_path, local_path)
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
        f"{minio_path}{filename}",
        local_path,
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


def create_convention_collective_table():
    siren_db_conn, siren_db_cursor = connect_to_db(SIRENE_DATABASE_LOCATION)
    siren_db_cursor.execute("""DROP TABLE IF EXISTS convention_collective""")
    siren_db_cursor.execute(
        """
     CREATE TABLE IF NOT EXISTS convention_collective
     (
         siren,
         liste_idcc
     )
    """
    )
    siren_db_cursor.execute(
        """
     CREATE UNIQUE INDEX index_convention_collective
     ON convention_collective (siren);
     """
    )

    cc_url = (
        "https://www.data.gouv.fr/fr/datasets/r/bfc3a658-c054-4ecc-ba4b" "-22f3f5789dc7"
    )
    r = requests.get(cc_url, allow_redirects=True)
    with open(DATA_DIR + "convcollective-download.csv", "wb") as f:
        for chunk in r.iter_content(1024):
            f.write(chunk)
    df_conv_coll = pd.read_csv(
        DATA_DIR + "convcollective-download.csv",
        dtype=str,
        names=["mois", "siret", "idcc", "date_maj"],
        header=0,
    )
    df_conv_coll["siren"] = df_conv_coll["siret"].str[0:9]
    df_conv_coll = df_conv_coll[df_conv_coll["siren"].notna()]
    df_conv_coll["idcc"] = df_conv_coll["idcc"].apply(lambda x: str(x).replace(" ", ""))
    df_liste_cc = (
        df_conv_coll.groupby(by=["siren"])["idcc"]
        .apply(list)
        .reset_index(name="liste_idcc")
    )
    # df_liste_cc["siren"] = df_liste_cc["siret"].str[0:9]
    df_liste_cc["liste_idcc"] = df_liste_cc["liste_idcc"].astype(str)
    df_liste_cc.to_sql(
        "convention_collective", siren_db_conn, if_exists="append", index=False
    )

    for row in siren_db_cursor.execute("""SELECT COUNT() FROM convention_collective"""):
        logging.info(
            f"************ {row}"
            f"records have been added to the CONVENTION COLLECTIVE table!"
        )

    del df_liste_cc
    del df_conv_coll

    commit_and_close_conn(siren_db_conn)


def create_rge_table():
    siren_db_conn, siren_db_cursor = connect_to_db(SIRENE_DATABASE_LOCATION)
    siren_db_cursor.execute("""DROP TABLE IF EXISTS rge""")
    siren_db_cursor.execute(
        """
     CREATE TABLE IF NOT EXISTS rge
     (
         siren,
         liste_rge
     )
    """
    )
    siren_db_cursor.execute(
        """
     CREATE UNIQUE INDEX index_rge
     ON rge (siren);
     """
    )

    rge_url = (
        "https://data.ademe.fr/data-fair/api/v1/datasets/liste-des-entreprises-rge-2/"
        "lines?size=10000&select=siret%2Ccode_qualification"
    )
    r = requests.get(rge_url, allow_redirects=True)
    data = r.json()
    from typing import List

    list_rge: List[str] = []
    list_rge = list_rge + data["results"]
    cpt = 0
    while "next" in data:
        cpt = cpt + 1
        r = requests.get(data["next"])
        data = r.json()
        list_rge = list_rge + data["results"]
    df_rge = pd.DataFrame(list_rge)
    df_rge["siren"] = df_rge["siret"].str[:9]
    df_rge = df_rge[df_rge["siren"].notna()]
    df_list_rge = (
        df_rge.groupby(["siren"])["code_qualification"]
        .apply(list)
        .reset_index(name="liste_rge")
    )
    df_list_rge = df_list_rge[["siren", "liste_rge"]]
    df_list_rge["liste_rge"] = df_list_rge["liste_rge"].astype(str)
    df_list_rge.to_sql("rge", siren_db_conn, if_exists="append", index=False)
    for row in siren_db_cursor.execute("""SELECT COUNT() FROM rge"""):
        logging.info(f"************ {row} records have been added to the RGE table!")

    del df_list_rge
    del df_rge

    commit_and_close_conn(siren_db_conn)


def create_uai_table():
    siren_db_conn, siren_db_cursor = connect_to_db(SIRENE_DATABASE_LOCATION)
    siren_db_cursor.execute("""DROP TABLE IF EXISTS uai""")
    siren_db_cursor.execute(
        """
     CREATE TABLE IF NOT EXISTS uai
     (
         siren,
         liste_uai
     )
    """
    )
    siren_db_cursor.execute(
        """
     CREATE INDEX index_uai
     ON uai (siren);
     """
    )

    r = requests.get(
        "https://www.data.gouv.fr/fr/datasets/r/b22f04bf-64a8-495d-b8bb-d84dbc4c7983"
    )
    with open(DATA_DIR + "uai-download.csv", "wb") as f:
        for chunk in r.iter_content(1024):
            f.write(chunk)
    df_uai = pd.read_csv(DATA_DIR + "uai-download.csv", dtype=str, sep=";")
    df_uai = df_uai[["identifiant_de_l_etablissement", "siren_siret", "code_nature"]]
    df_uai = df_uai.rename(
        columns={"identifiant_de_l_etablissement": "uai", "siren_siret": "siren"}
    )
    df_uai["siren"] = df_uai["siren"].str[:9]
    df_list_uai = (
        df_uai.groupby(["siren"])["uai"].apply(list).reset_index(name="liste_uai")
    )
    df_list_uai = df_list_uai[["siren", "liste_uai"]]
    df_list_uai["liste_uai"] = df_list_uai["liste_uai"].astype(str)
    df_list_uai.to_sql("uai", siren_db_conn, if_exists="append", index=False)
    for row in siren_db_cursor.execute("""SELECT COUNT() FROM uai"""):
        logging.info(f"************ {row} records have been added to the UAI table!")
    del df_list_uai
    del df_uai

    commit_and_close_conn(siren_db_conn)


def create_finess_table():
    siren_db_conn, siren_db_cursor = connect_to_db(SIRENE_DATABASE_LOCATION)
    siren_db_cursor.execute("""DROP TABLE IF EXISTS finess""")
    siren_db_cursor.execute(
        """
     CREATE TABLE IF NOT EXISTS finess
     (
         siren,
         liste_finess
     )
    """
    )
    siren_db_cursor.execute(
        """
     CREATE INDEX index_finess
     ON finess (siren);
     """
    )

    r = requests.get(
        "https://www.data.gouv.fr/fr/datasets/r/2ce43ade-8d2c-4d1d-81da-ca06c82abc68"
    )
    with open(DATA_DIR + "finess-download.csv", "wb") as f:
        for chunk in r.iter_content(1024):
            f.write(chunk)

    df_finess = pd.read_csv(
        DATA_DIR + "finess-download.csv",
        dtype=str,
        sep=";",
        encoding="Latin-1",
        skiprows=1,
        header=None,
    )
    df_finess = df_finess[[1, 18, 22]]
    df_finess = df_finess.rename(
        columns={1: "finess", 18: "cat_etablissement", 22: "siren"}
    )
    df_finess["siren"] = df_finess["siren"].str[:9]
    df_finess = df_finess[df_finess["siren"].notna()]
    df_list_finess = (
        df_finess.groupby(["siren"])["finess"]
        .apply(list)
        .reset_index(name="liste_finess")
    )
    df_list_finess = df_list_finess[["siren", "liste_finess"]]
    df_list_finess["liste_finess"] = df_list_finess["liste_finess"].astype(str)
    df_list_finess.to_sql("finess", siren_db_conn, if_exists="append", index=False)
    for row in siren_db_cursor.execute("""SELECT COUNT() FROM finess"""):
        logging.info(f"************ {row} records have been added to the FINESS table!")

    del df_list_finess
    del df_finess

    commit_and_close_conn(siren_db_conn)


def create_spectacle_table():
    siren_db_conn, siren_db_cursor = connect_to_db(SIRENE_DATABASE_LOCATION)
    siren_db_cursor.execute("""DROP TABLE IF EXISTS spectacle""")
    siren_db_cursor.execute(
        """
     CREATE TABLE IF NOT EXISTS spectacle
     (
         siren,
         est_entrepreneur_spectacle
     )
    """
    )
    siren_db_cursor.execute(
        """
     CREATE INDEX index_spectacle
     ON spectacle (siren);
     """
    )

    r = requests.get(
        "https://www.data.gouv.fr/fr/datasets/r/fb6c3b2e-da8c-4e69-a719-6a96329e4cb2"
    )
    with open(DATA_DIR + "spectacle-download.csv", "wb") as f:
        for chunk in r.iter_content(1024):
            f.write(chunk)

    df_spectacle = pd.read_csv(DATA_DIR + "spectacle-download.csv", dtype=str, sep=";")
    df_spectacle = df_spectacle[df_spectacle["statut_du_recepisse"] == "Valide"]
    df_spectacle["est_entrepreneur_spectacle"] = True
    df_spectacle["siren"] = df_spectacle[
        "siren_personne_physique_siret_personne_morale"
    ].str[:9]
    df_spectacle = df_spectacle[["siren", "est_entrepreneur_spectacle"]]
    df_spectacle = df_spectacle[df_spectacle["siren"].notna()]
    df_spectacle.to_sql("spectacle", siren_db_conn, if_exists="append", index=False)
    for row in siren_db_cursor.execute("""SELECT COUNT() FROM spectacle"""):
        logging.info(
            f"************ {row} records have been added to the SPECTACLE table!"
        )
    del df_spectacle

    commit_and_close_conn(siren_db_conn)


def create_colter_table():
    siren_db_conn, siren_db_cursor = connect_to_db(SIRENE_DATABASE_LOCATION)
    siren_db_cursor.execute("""DROP TABLE IF EXISTS colter""")
    siren_db_cursor.execute(
        """
     CREATE TABLE IF NOT EXISTS colter
     (
         siren,
         colter_code,
         colter_code_insee,
         colter_niveau
     )
    """
    )
    siren_db_cursor.execute(
        """
     CREATE INDEX index_colter
     ON colter (siren);
     """
    )

    # Process Régions
    df_regions = pd.read_csv(
        "https://www.data.gouv.fr/fr/datasets/r/619ee62e-8f9e-4c62-b166-abc6f2b86201",
        dtype=str,
        sep=";",
    )
    df_regions = df_regions[df_regions["exer"] == df_regions.exer.max()][
        ["reg_code", "siren"]
    ]
    df_regions = df_regions.drop_duplicates(keep="first")
    df_regions = df_regions.rename(columns={"reg_code": "colter_code_insee"})
    df_regions["colter_code"] = df_regions["colter_code_insee"]
    df_regions["colter_niveau"] = "region"

    # Cas particulier Corse
    df_regions.loc[
        df_regions["colter_code_insee"] == "94", "colter_niveau"
    ] = "particulier"
    df_colter = df_regions

    # Process Départements
    df_deps = pd.read_csv(
        "https://www.data.gouv.fr/fr/datasets/r/2f4f901d-e3ce-4760-b122-56a311340fc4",
        dtype=str,
        sep=";",
    )
    df_deps = df_deps[df_deps["exer"] == df_deps["exer"].max()]
    df_deps = df_deps[["dep_code", "siren"]]
    df_deps = df_deps.drop_duplicates(keep="first")
    df_deps = df_deps.rename(columns={"dep_code": "colter_code_insee"})
    df_deps["colter_code"] = df_deps["colter_code_insee"] + "D"
    df_deps["colter_niveau"] = "departement"

    # Cas Métropole de Lyon
    df_deps.loc[df_deps["colter_code_insee"] == "691", "colter_code"] = "69M"
    df_deps.loc[df_deps["colter_code_insee"] == "691", "colter_niveau"] = "particulier"
    df_deps.loc[df_deps["colter_code_insee"] == "691", "colter_code_insee"] = None

    # Cas Conseil départemental du Rhone
    df_deps.loc[df_deps["colter_code_insee"] == "69", "colter_niveau"] = "particulier"
    df_deps.loc[df_deps["colter_code_insee"] == "69", "colter_code_insee"] = None

    # Cas Collectivité Européenne d"Alsace
    df_deps.loc[df_deps["colter_code_insee"] == "67A", "colter_code"] = "6AE"
    df_deps.loc[df_deps["colter_code_insee"] == "67A", "colter_niveau"] = "particulier"
    df_deps.loc[df_deps["colter_code_insee"] == "67A", "colter_code_insee"] = None

    # Remove Paris
    df_deps = df_deps[df_deps["colter_code_insee"] != "75"]

    df_colter = pd.concat([df_colter, df_deps])

    # Process EPCI
    df_epci = pd.read_excel(
        "https://www.collectivites-locales.gouv.fr/files/2022/epcisanscom2022.xlsx",
        dtype=str,
        engine="openpyxl",
    )
    df_epci["colter_code_insee"] = None
    df_epci["siren"] = df_epci["siren_epci"]
    df_epci["colter_code"] = df_epci["siren"]
    df_epci["colter_niveau"] = "epci"
    df_epci = df_epci[["colter_code_insee", "siren", "colter_code", "colter_niveau"]]
    df_colter = pd.concat([df_colter, df_epci])

    # Process Communes
    URL = "https://www.data.gouv.fr/fr/datasets/r/42b16d68-958e-4518-8551-93e095fe8fda"
    response = requests.get(URL)
    open(DATA_DIR + "siren-communes.zip", "wb").write(response.content)

    with zipfile.ZipFile(DATA_DIR + "siren-communes.zip", "r") as zip_ref:
        zip_ref.extractall(DATA_DIR + "siren-communes")

    df_communes = pd.read_excel(
        DATA_DIR + "siren-communes/Banatic_SirenInsee2022.xlsx",
        dtype=str,
        engine="openpyxl",
    )
    df_communes["colter_code_insee"] = df_communes["insee"]
    df_communes["colter_code"] = df_communes["insee"]
    df_communes["colter_niveau"] = "commune"
    df_communes = df_communes[
        ["colter_code_insee", "siren", "colter_code", "colter_niveau"]
    ]
    df_communes.loc[df_communes["colter_code_insee"] == "75056", "colter_code"] = "75C"
    df_communes.loc[
        df_communes["colter_code_insee"] == "75056", "colter_niveau"
    ] = "particulier"

    df_colter = pd.concat([df_colter, df_communes])

    df_colter.to_csv(DATA_DIR + "colter-new.csv", index=False)

    df_colter.to_sql("colter", siren_db_conn, if_exists="append", index=False)
    for row in siren_db_cursor.execute("""SELECT COUNT() FROM colter"""):
        logging.info(f"************ {row} records have been added to the COLTER table!")

    del df_colter
    del df_communes

    commit_and_close_conn(siren_db_conn)


def create_elu_table():
    siren_db_conn, siren_db_cursor = connect_to_db(SIRENE_DATABASE_LOCATION)
    siren_db_cursor.execute("""DROP TABLE IF EXISTS elus""")
    siren_db_cursor.execute(
        """
     CREATE TABLE IF NOT EXISTS elus
     (
         siren,
         nom,
         prenom,
         date_naissance,
         sexe,
         fonction
     )
    """
    )
    siren_db_cursor.execute(
        """
     CREATE INDEX index_elus
     ON elus (siren);
     """
    )

    df_colter = pd.read_csv(DATA_DIR + "colter-new.csv", dtype=str)
    # Conseillers régionaux
    elus = process_elus_files(
        "https://www.data.gouv.fr/fr/datasets/r/430e13f9-834b-4411-a1a8-da0b4b6e715c",
        "Code de la région",
    )

    # Conseillers départementaux
    df_elus_deps = process_elus_files(
        "https://www.data.gouv.fr/fr/datasets/r/601ef073-d986-4582-8e1a-ed14dc857fba",
        "Code du département",
    )
    df_elus_deps["colter_code"] = df_elus_deps["colter_code"] + "D"
    df_elus_deps.loc[df_elus_deps["colter_code"] == "6AED", "colter_code"] = "6AE"
    elus = pd.concat([elus, df_elus_deps])

    # membres des assemblées des collectivités à statut particulier
    df_elus_part = process_elus_files(
        "https://www.data.gouv.fr/fr/datasets/r/a595be27-cfab-4810-b9d4-22e193bffe35",
        "Code de la collectivité à statut particulier",
    )
    df_elus_part.loc[df_elus_part["colter_code"] == "972", "colter_code"] = "02"
    df_elus_part.loc[df_elus_part["colter_code"] == "973", "colter_code"] = "03"
    elus = pd.concat([elus, df_elus_part])
    # Conseillers communautaires
    df_elus_epci = process_elus_files(
        "https://www.data.gouv.fr/fr/datasets/r/41d95d7d-b172-4636-ac44-32656367cdc7",
        "N° SIREN",
    )
    elus = pd.concat([elus, df_elus_epci])
    # Conseillers municipaux
    df_elus_epci = process_elus_files(
        "https://www.data.gouv.fr/fr/datasets/r/d5f400de-ae3f-4966-8cb6-a85c70c6c24a",
        "Code de la commune",
    )
    df_elus_epci.loc[df_elus_epci["colter_code"] == "75056", "colter_code"] = "75C"
    elus = pd.concat([elus, df_elus_epci])
    df_colter_elus = elus.merge(df_colter, on="colter_code", how="left")
    df_colter_elus = df_colter_elus[df_colter_elus["siren"].notna()]
    df_colter_elus["date_naissance_elu"] = df_colter_elus["date_naissance_elu"].apply(
        lambda x: x.split("/")[2] + "-" + x.split("/")[1] + "-" + x.split("/")[0]
    )
    df_colter_elus = df_colter_elus[
        [
            "siren",
            "nom_elu",
            "prenom_elu",
            "date_naissance_elu",
            "sexe_elu",
            "fonction_elu",
        ]
    ]
    for col in df_colter_elus.columns:
        df_colter_elus = df_colter_elus.rename(columns={col: col.replace("_elu", "")})

    df_colter_elus.to_sql("elus", siren_db_conn, if_exists="append", index=False)

    del df_colter_elus
    del elus
    del df_elus_part
    del df_colter
    del df_elus_epci

    commit_and_close_conn(siren_db_conn)


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
        ul.identifiant_association_unite_legale as identifiant_association_unite_legale,
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
            st.est_siege as est_siege,
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
        (SELECT liste_idcc FROM convention_collective cc WHERE siren = st.siren)
        as liste_idcc,
        (SELECT liste_rge FROM rge WHERE siren = st.siren) as liste_rge,
        (SELECT liste_uai FROM uai WHERE siren = st.siren) as liste_uai,
        (SELECT liste_finess FROM finess WHERE siren = st.siren) as liste_finess,
        (SELECT est_entrepreneur_spectacle FROM spectacle sp WHERE
        siren = st.siren) as est_entrepreneur_spectacle,
        (SELECT colter_code_insee FROM colter WHERE siren = st.siren)
        as colter_code_insee,
        (SELECT colter_code FROM colter WHERE siren = st.siren) as colter_code,
        (SELECT colter_niveau FROM colter WHERE siren = st.siren) as colter_niveau,
        (SELECT json_group_array(
                json_object(
                    'siren', siren,
                    'nom', nom,
                    'prenom', prenom,
                    'date_naissance', date_naissance,
                    'sexe', sexe,
                    'fonction', fonction
                    )
                ) FROM
                (
                    SELECT siren, nom, prenom, date_naissance,
                    sexe, fonction
                    FROM elus
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

    if float(count_sieges) - float(doc_count) > 100000:
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


def put_object_minio(
    filename: str,
    minio_path: str,
    local_path: str,
):
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
            object_name=minio_path,
            file_path=local_path + filename,
        )
