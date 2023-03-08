import json
import logging
import os
import re
import redis
import requests
import shutil
import sqlite3
from urllib.request import urlopen

from airflow.models import Variable
from dag_datalake_sirene.data_preprocessing.collectivite_territoriale import (
    preprocess_colter_data,
    preprocess_elus_data,
)
from dag_datalake_sirene.data_preprocessing.convention_collective import (
    preprocess_convcollective_data,
)
from dag_datalake_sirene.data_preprocessing.dirigeants_pm import preprocess_dirigeant_pm
from dag_datalake_sirene.data_preprocessing.dirigeants_pp import (
    preprocess_dirigeants_pp,
)
from dag_datalake_sirene.data_preprocessing.entrepreneur_spectacle import (
    preprocess_spectacle_data,
)
from dag_datalake_sirene.data_preprocessing.etablissements import (
    preprocess_etablissements_data,
)
from dag_datalake_sirene.data_preprocessing.finess import preprocess_finess_data
from dag_datalake_sirene.data_preprocessing.rge import preprocess_rge_data
from dag_datalake_sirene.data_preprocessing.uai import preprocess_uai_data
from dag_datalake_sirene.data_preprocessing.unite_legale import (
    preprocess_unite_legale_data,
)
from dag_datalake_sirene.elasticsearch.create_sirene_index import ElasticCreateSiren
from dag_datalake_sirene.elasticsearch.indexing_unite_legale import (
    index_unites_legales_by_chunk,
)
from dag_datalake_sirene.labels.departements import all_deps
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
AIO_URL = Variable.get("AIO_URL")
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
            denomination_usuelle_1,
            denomination_usuelle_2,
            denomination_usuelle_3,
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

    for df_unite_legale in preprocess_unite_legale_data(DATA_DIR):
        df_unite_legale.to_sql(
            "unite_legale", siren_db_conn, if_exists="append", index=False
        )

        for row in siren_db_cursor.execute("""SELECT COUNT() FROM unite_legale"""):
            logging.info(
                f"************ {row} total records have been added "
                f"to the unité légale table!"
            )

    del df_unite_legale

    for count_unites_legales in siren_db_cursor.execute(
        """
        SELECT COUNT()
        FROM unite_legale
        """
    ):
        logging.info(
            f"************ {count_unites_legales} total records have been added to the "
            f"unité légale table!"
        )
    kwargs["ti"].xcom_push(key="count_unites_legales", value=count_unites_legales[0])
    commit_and_close_conn(siren_db_conn)


def create_etablissements_table():
    siren_db_conn, siren_db_cursor = connect_to_db(SIRENE_DATABASE_LOCATION)
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
        df_dep = preprocess_etablissements_data(dep)
        df_dep.to_sql("siret", siren_db_conn, if_exists="append", index=False)
        siren_db_conn.commit()
        for row in siren_db_cursor.execute("""SELECT COUNT() FROM siret"""):
            logging.info(
                f"************ {row} total records have been added to the "
                f"`établissements` table!"
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
            f"************ {count_sieges} total records have been added to the "
            f"unité légale table!"
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
         siret,
         liste_idcc
     )
    """
    )
    siren_db_cursor.execute(
        """
     CREATE UNIQUE INDEX index_convention_collective
     ON convention_collective (siret);
     """
    )

    df_liste_cc = preprocess_convcollective_data(DATA_DIR)
    df_liste_cc.to_sql(
        "convention_collective", siren_db_conn, if_exists="append", index=False
    )
    del df_liste_cc

    for row in siren_db_cursor.execute("""SELECT COUNT() FROM convention_collective"""):
        logging.info(
            f"************ {row} "
            f"total records have been added to the CONVENTION COLLECTIVE table!"
        )
    commit_and_close_conn(siren_db_conn)


def create_rge_table():
    siren_db_conn, siren_db_cursor = connect_to_db(SIRENE_DATABASE_LOCATION)
    siren_db_cursor.execute("""DROP TABLE IF EXISTS rge""")
    siren_db_cursor.execute(
        """
     CREATE TABLE IF NOT EXISTS rge
     (
         siret,
         liste_rge
     )
    """
    )
    siren_db_cursor.execute(
        """
     CREATE UNIQUE INDEX index_rge
     ON rge (siret);
     """
    )
    df_list_rge = preprocess_rge_data()
    df_list_rge.to_sql("rge", siren_db_conn, if_exists="append", index=False)
    del df_list_rge

    for row in siren_db_cursor.execute("""SELECT COUNT() FROM rge"""):
        logging.info(
            f"************ {row} total records have been added to the RGE table!"
        )
    commit_and_close_conn(siren_db_conn)


def create_uai_table():
    siren_db_conn, siren_db_cursor = connect_to_db(SIRENE_DATABASE_LOCATION)
    siren_db_cursor.execute("""DROP TABLE IF EXISTS uai""")
    siren_db_cursor.execute(
        """
     CREATE TABLE IF NOT EXISTS uai
     (
         siret,
         liste_uai
     )
    """
    )
    siren_db_cursor.execute(
        """
     CREATE INDEX index_uai
     ON uai (siret);
     """
    )

    df_list_uai = preprocess_uai_data(DATA_DIR)
    df_list_uai.to_sql("uai", siren_db_conn, if_exists="append", index=False)
    del df_list_uai

    for row in siren_db_cursor.execute("""SELECT COUNT() FROM uai"""):
        logging.info(
            f"************ {row} total records have been added to the UAI table!"
        )
    commit_and_close_conn(siren_db_conn)


def create_finess_table():
    siren_db_conn, siren_db_cursor = connect_to_db(SIRENE_DATABASE_LOCATION)
    siren_db_cursor.execute("""DROP TABLE IF EXISTS finess""")
    siren_db_cursor.execute(
        """
     CREATE TABLE IF NOT EXISTS finess
     (
         siret,
         liste_finess
     )
    """
    )
    siren_db_cursor.execute(
        """
     CREATE INDEX index_finess
     ON finess (siret);
     """
    )

    df_list_finess = preprocess_finess_data(DATA_DIR)
    df_list_finess.to_sql("finess", siren_db_conn, if_exists="append", index=False)
    del df_list_finess

    for row in siren_db_cursor.execute("""SELECT COUNT() FROM finess"""):
        logging.info(
            f"************ {row} total records have been added to the FINESS table!"
        )
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

    df_spectacle = preprocess_spectacle_data(DATA_DIR)
    df_spectacle.to_sql("spectacle", siren_db_conn, if_exists="append", index=False)
    del df_spectacle

    for row in siren_db_cursor.execute("""SELECT COUNT() FROM spectacle"""):
        logging.info(
            f"************ {row} total records have been added to the SPECTACLE table!"
        )
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

    df_colter = preprocess_colter_data(DATA_DIR)
    df_colter.to_sql("colter", siren_db_conn, if_exists="append", index=False)
    del df_colter

    for row in siren_db_cursor.execute("""SELECT COUNT() FROM colter"""):
        logging.info(
            f"************ {row} total records have been added to the COLTER table!"
        )
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

    df_colter_elus = preprocess_elus_data(DATA_DIR)
    df_colter_elus.to_sql("elus", siren_db_conn, if_exists="append", index=False)
    del df_colter_elus

    for row in siren_db_cursor.execute("""SELECT COUNT() FROM elus"""):
        logging.info(
            f"************ {row} total records have been added to the ÉLUS table!"
        )
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


def fill_elastic_siren_index(**kwargs):
    next_color = kwargs["ti"].xcom_pull(key="next_color", task_ids="get_colors")
    elastic_index = f"siren-{next_color}"
    siren_db_conn, siren_db_cursor = connect_to_db(SIRENE_DATABASE_LOCATION)
    siren_db_cursor.execute(
        """SELECT
            ul.activite_principale_unite_legale as activite_principale_unite_legale,
            ul.categorie_entreprise as categorie_entreprise,
            ul.date_creation_unite_legale as date_creation_unite_legale,
            ul.date_mise_a_jour_unite_legale as date_mise_a_jour_unite_legale,
            ul.denomination_usuelle_1 as denomination_usuelle_1_unite_legale,
            ul.denomination_usuelle_2 as denomination_usuelle_2_unite_legale,
            ul.denomination_usuelle_3 as denomination_usuelle_3_unite_legale,
            ul.economie_sociale_solidaire_unite_legale as
            economie_sociale_solidaire_unite_legale,
            ul.etat_administratif_unite_legale as etat_administratif_unite_legale,
            ul.identifiant_association_unite_legale as
            identifiant_association_unite_legale,
            ul.nature_juridique_unite_legale as nature_juridique_unite_legale,
            ul.nom as nom,
            ul.nom_raison_sociale as nom_raison_sociale,
            ul.nom_usage as nom_usage,
            ul.prenom as prenom,
            ul.sigle as sigle,
            ul.siren,
            st.siret as siret_siege,
            ul.tranche_effectif_salarie_unite_legale as
            tranche_effectif_salarie_unite_legale,
            (SELECT count FROM count_etab ce WHERE ce.siren = st.siren) as
            nombre_etablissements,
            (SELECT count FROM count_etab_ouvert ceo WHERE ceo.siren = st.siren) as
            nombre_etablissements_ouverts,
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
            (SELECT json_group_array(
                    json_object(
                        'activite_principale',activite_principale,
                        'activite_principale_registre_metier',
                        activite_principale_registre_metier,
                        'cedex',cedex,
                        'cedex_2',cedex_2,
                        'code_pays_etranger',code_pays_etranger,
                        'code_pays_etranger_2',code_pays_etranger_2,
                        'code_postal',code_postal,
                        'commune',commune,
                        'commune_2',commune_2,
                        'complement_adresse',complement_adresse,
                        'complement_adresse_2',complement_adresse_2,
                        'date_creation',date_creation,
                        'date_debut_activite',date_debut_activite,
                        'distribution_speciale',distribution_speciale,
                        'distribution_speciale_2',distribution_speciale_2,
                        'enseigne_1',enseigne_1,
                        'enseigne_2',enseigne_2,
                        'enseigne_3',enseigne_3,
                        'est_siege',est_siege,
                        'etat_administratif',etat_administratif_etablissement,
                        'geo_adresse',geo_adresse,
                        'geo_id',geo_id,
                        'indice_repetition',indice_repetition,
                        'indice_repetition_2',indice_repetition_2,
                        'latitude',latitude,
                        'libelle_cedex',libelle_cedex,
                        'libelle_cedex_2',libelle_cedex_2,
                        'libelle_commune',libelle_commune,
                        'libelle_commune_2',libelle_commune_2,
                        'libelle_commune_etranger',libelle_commune_etranger,
                        'libelle_commune_etranger_2',libelle_commune_etranger_2,
                        'libelle_pays_etranger',libelle_pays_etranger,
                        'libelle_pays_etranger_2',libelle_pays_etranger_2,
                        'libelle_voie',libelle_voie,
                        'libelle_voie_2',libelle_voie_2,
                        'liste_finess', liste_finess,
                        'liste_idcc', liste_idcc,
                        'liste_rge', liste_rge,
                        'liste_uai', liste_uai,
                        'longitude',longitude,
                        'nom_commercial',nom_commercial,
                        'numero_voie',numero_voie,
                        'numero_voie_2',numero_voie_2,
                        'siren', siren,
                        'siret', siret,
                        'tranche_effectif_salarie',tranche_effectif_salarie,
                        'type_voie',type_voie,
                        'type_voie_2',type_voie_2
                        )
                    ) FROM
                    (
                        SELECT
                        s.activite_principale as activite_principale,
                        s.activite_principale_registre_metier as
                        activite_principale_registre_metier,
                        s.cedex as cedex,
                        s.cedex_2 as cedex_2,
                        s.code_pays_etranger as code_pays_etranger,
                        s.code_pays_etranger_2 as code_pays_etranger_2,
                        s.code_postal as code_postal,
                        s.commune as commune,
                        s.commune_2 as commune_2,
                        s.complement_adresse as complement_adresse,
                        s.complement_adresse_2 as complement_adresse_2,
                        s.date_creation as date_creation,
                        s.date_debut_activite as date_debut_activite,
                        s.distribution_speciale as distribution_speciale,
                        s.distribution_speciale_2 as distribution_speciale_2,
                        s.enseigne_1 as enseigne_1,
                        s.enseigne_2 as enseigne_2,
                        s.enseigne_3 as enseigne_3,
                        s.est_siege as est_siege,
                        s.etat_administratif_etablissement as
                        etat_administratif_etablissement,
                        s.geo_adresse as geo_adresse,
                        s.geo_id as geo_id,
                        s.indice_repetition as indice_repetition,
                        s.indice_repetition_2 as indice_repetition_2,
                        s.latitude as latitude,
                        s.libelle_cedex as libelle_cedex,
                        s.libelle_cedex_2 as libelle_cedex_2,
                        s.libelle_commune as libelle_commune,
                        s.libelle_commune_2 as libelle_commune_2,
                        s.libelle_commune_etranger as libelle_commune_etranger,
                        s.libelle_commune_etranger_2 as libelle_commune_etranger_2,
                        s.libelle_pays_etranger as libelle_pays_etranger,
                        s.libelle_pays_etranger_2 as libelle_pays_etranger_2,
                        s.libelle_voie as libelle_voie,
                        s.libelle_voie_2 as libelle_voie_2,
                        (SELECT liste_finess FROM finess WHERE siret = s.siret) as
                        liste_finess,
                        (SELECT liste_idcc FROM convention_collective WHERE siret =
                        s.siret) as liste_idcc,
                        (SELECT liste_rge FROM rge WHERE siret = s.siret) as liste_rge,
                        (SELECT liste_uai FROM uai WHERE siret = s.siret) as liste_uai,
                        s.longitude as longitude,
                        s.nom_commercial as nom_commercial,
                        s.numero_voie as numero_voie,
                        s.numero_voie_2 as numero_voie_2,
                        s.siren as siren,
                        s.siret as siret,
                        s.tranche_effectif_salarie as tranche_effectif_salarie,
                        s.type_voie as type_voie,
                        s.type_voie_2 as type_voie_2
                        FROM siret s
                        WHERE s.siren = ul.siren
                    )
                ) as etablissements,
            (SELECT json_object(
                        'activite_principale',activite_principale,
                        'activite_principale_registre_metier',
                        activite_principale_registre_metier,
                        'cedex',cedex,
                        'cedex_2',cedex_2,
                        'code_pays_etranger',code_pays_etranger,
                        'code_pays_etranger_2',code_pays_etranger_2,
                        'code_postal',code_postal,
                        'commune',commune,
                        'commune_2',commune_2,
                        'complement_adresse',complement_adresse,
                        'complement_adresse_2',complement_adresse_2,
                        'date_creation',date_creation,
                        'date_debut_activite',date_debut_activite,
                        'distribution_speciale',distribution_speciale,
                        'distribution_speciale_2',distribution_speciale_2,
                        'enseigne_1',enseigne_1,
                        'enseigne_2',enseigne_2,
                        'enseigne_3',enseigne_3,
                        'est_siege',est_siege,
                        'etat_administratif',etat_administratif_etablissement,
                        'geo_adresse',geo_adresse,
                        'geo_id',geo_id,
                        'indice_repetition',indice_repetition,
                        'indice_repetition_2',indice_repetition_2,
                        'latitude',latitude,
                        'libelle_cedex',libelle_cedex,
                        'libelle_cedex_2',libelle_cedex_2,
                        'libelle_commune',libelle_commune,
                        'libelle_commune_2',libelle_commune_2,
                        'libelle_commune_etranger',libelle_commune_etranger,
                        'libelle_commune_etranger_2',libelle_commune_etranger_2,
                        'libelle_pays_etranger',libelle_pays_etranger,
                        'libelle_pays_etranger_2',libelle_pays_etranger_2,
                        'libelle_voie',libelle_voie,
                        'libelle_voie_2',libelle_voie_2,
                        'liste_finess', liste_finess,
                        'liste_idcc', liste_idcc,
                        'liste_rge', liste_rge,
                        'liste_uai', liste_uai,
                        'longitude',longitude,
                        'nom_commercial',nom_commercial,
                        'numero_voie',numero_voie,
                        'numero_voie_2',numero_voie_2,
                        'siren', siren,
                        'siret', siret,
                        'tranche_effectif_salarie',tranche_effectif_salarie,
                        'type_voie',type_voie,
                        'type_voie_2',type_voie_2
                        )
                    FROM
                    (
                        SELECT
                        s.activite_principale as activite_principale,
                        s.activite_principale_registre_metier as
                        activite_principale_registre_metier,
                        s.cedex as cedex,
                        s.cedex_2 as cedex_2,
                        s.code_pays_etranger as code_pays_etranger,
                        s.code_pays_etranger_2 as code_pays_etranger_2,
                        s.code_postal as code_postal,
                        s.commune as commune,
                        s.commune_2 as commune_2,
                        s.complement_adresse as complement_adresse,
                        s.complement_adresse_2 as complement_adresse_2,
                        s.date_creation as date_creation,
                        s.date_debut_activite as date_debut_activite,
                        s.distribution_speciale as distribution_speciale,
                        s.distribution_speciale_2 as distribution_speciale_2,
                        s.enseigne_1 as enseigne_1,
                        s.enseigne_2 as enseigne_2,
                        s.enseigne_3 as enseigne_3,
                        s.est_siege as est_siege,
                        s.etat_administratif_etablissement as
                        etat_administratif_etablissement,
                        s.geo_adresse as geo_adresse,
                        s.geo_id as geo_id,
                        s.indice_repetition as indice_repetition,
                        s.indice_repetition_2 as indice_repetition_2,
                        s.latitude as latitude,
                        s.libelle_cedex as libelle_cedex,
                        s.libelle_cedex_2 as libelle_cedex_2,
                        s.libelle_commune as libelle_commune,
                        s.libelle_commune_2 as libelle_commune_2,
                        s.libelle_commune_etranger as libelle_commune_etranger,
                        s.libelle_commune_etranger_2 as libelle_commune_etranger_2,
                        s.libelle_pays_etranger as libelle_pays_etranger,
                        s.libelle_pays_etranger_2 as libelle_pays_etranger_2,
                        s.libelle_voie as libelle_voie,
                        s.libelle_voie_2 as libelle_voie_2,
                        (SELECT liste_finess FROM finess WHERE siret = s.siret) as
                        liste_finess,
                        (SELECT liste_idcc FROM convention_collective WHERE siret =
                        s.siret) as liste_idcc,
                        (SELECT liste_rge FROM rge WHERE siret = s.siret) as liste_rge,
                        (SELECT liste_uai FROM uai WHERE siret = s.siret) as liste_uai,
                        s.longitude as longitude,
                        s.nom_commercial as nom_commercial,
                        s.numero_voie as numero_voie,
                        s.numero_voie_2 as numero_voie_2,
                        s.siren as siren,
                        s.siret as siret,
                        s.tranche_effectif_salarie as tranche_effectif_salarie,
                        s.type_voie as type_voie,
                        s.type_voie_2 as type_voie_2
                        FROM siretsiege as s
                        WHERE s.siren = st.siren
                    )
                ) as siege,
            (SELECT est_entrepreneur_spectacle FROM spectacle WHERE siren = ul.siren) as
             est_entrepreneur_spectacle,
            (SELECT colter_code_insee FROM colter WHERE siren = ul.siren) as
            colter_code_insee,
            (SELECT colter_code FROM colter WHERE siren = ul.siren) as colter_code,
            (SELECT colter_niveau FROM colter WHERE siren = ul.siren) as colter_niveau,
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
                    WHERE siren = ul.siren
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
    doc_count = kwargs["ti"].xcom_pull(
        key="doc_count", task_ids="fill_elastic_siren_index"
    )
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


def flush_cache(host, port, db, password):
    redis_client = redis.Redis(
        host=host,
        port=port,
        db=db,
        password=password,
    )
    # Delete keys in the background in a different thread without blocking the server
    flush_command = redis_client.execute_command("FLUSHALL ASYNC")
    logging.info(f"Flush cache command status: {flush_command}")
    if redis_client.keys():
        raise Exception(f"****** Could not flush cache: {redis_client.keys()}")


def execute_slow_requests():
    session = requests.Session()
    base_url = AIO_URL
    slow_queries = ["q=rue", "q=rue%20de%20la", "q=france"]
    for query in slow_queries:
        try:
            path = f"/search?{query}"
            logging.info(f"******* Searching query : {query}")
            response = session.get(url=base_url + path)
            logging.info(f"******* Request status : {response.status_code}")
            response.raise_for_status()
        except requests.exceptions.RequestException as error:
            raise SystemExit(error)
