import json
import logging
import os
import shutil
import sqlite3
from urllib.request import urlopen

import pandas as pd
import requests
from airflow.models import Variable
from dag_datalake_sirene.elasticsearch.create_siren import ElasticCreateSiren
from dag_datalake_sirene.elasticsearch.index_doc import index_by_chunk
from elasticsearch_dsl import connections
from minio import Minio

TMP_FOLDER = "/tmp/"
DAG_FOLDER = "dag_datalake_sirene/"
DAG_NAME = "insert-elk-sirene"
DATA_DIR = TMP_FOLDER + DAG_FOLDER + DAG_NAME + "/data/"
DATABASE_LOCATION = DATA_DIR + "sirene.db"
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
def connect_to_db():
    siren_db_conn = sqlite3.connect(DATABASE_LOCATION)
    logging.info("******************* Connecting to database! *******************")
    siren_db_cursor = siren_db_conn.cursor()
    return siren_db_conn, siren_db_cursor


def commit_and_close_conn(siren_db_conn):
    siren_db_conn.commit()
    siren_db_conn.close()


def create_sqlite_database():
    if os.path.exists(DATA_DIR) and os.path.isdir(DATA_DIR):
        shutil.rmtree(DATA_DIR)
    os.makedirs(os.path.dirname(DATA_DIR), exist_ok=True)
    if os.path.exists(DATABASE_LOCATION):
        os.remove(DATABASE_LOCATION)
        logging.info(
            "******************** Existing database removed from {DATABASE_LOCATION}"
        )
    siren_db_conn = sqlite3.connect(DATABASE_LOCATION)
    logging.info(
        "******************* Creating and connecting to database! *******************"
    )
    commit_and_close_conn(siren_db_conn)


def create_unite_legale_table(**kwargs):
    siren_db_conn, siren_db_cursor = connect_to_db()
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
                "economieSocialeSolidaireUniteLegale":
                "economie_sociale_solidaire_unite_legale",
                "identifiantAssociationUniteLegale":
                "identifiant_association_unite_legale",
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
        SELECT COUNT() FROM 
        unite_legale
        """
    ):
        logging.info(
            f"************ {count_unites_legales} records have been added to the "
            f"unite_legale table!"
        )
    kwargs["ti"].xcom_push(key="count_unites_legales", value=count_unites_legales)
    commit_and_close_conn(siren_db_conn)


def create_etablissement_table():
    siren_db_conn, siren_db_cursor = connect_to_db()
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
                "activitePrincipaleRegistreMetiersEtablissement":
                "activite_principale_registre_metier",
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
    siren_db_conn, siren_db_cursor = connect_to_db()
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
        """ INSERT INTO count_etab (siren, count) 
        SELECT siren, count(*) as count 
        FROM siret GROUP BY siren; """
    )
    commit_and_close_conn(siren_db_conn)


def count_nombre_etablissements_ouverts():
    siren_db_conn, siren_db_cursor = connect_to_db()
    siren_db_cursor.execute("""DROP TABLE IF EXISTS count_etab_ouvert""")
    siren_db_cursor.execute(
        """CREATE TABLE count_etab_ouvert (siren VARCHAR(10), count INTEGER)"""
    )
    siren_db_cursor.execute(
        """CREATE UNIQUE INDEX index_count_ouvert_siren
        ON count_etab_ouvert (siren);"""
    )
    siren_db_cursor.execute(
        """INSERT INTO count_etab_ouvert (siren, count) 
        SELECT siren, count(*) as count 
        FROM siret 
        WHERE etat_administratif_etablissement = 'A' GROUP BY siren;"""
    )
    commit_and_close_conn(siren_db_conn)


def create_siege_only_table(**kwargs):
    siren_db_conn, siren_db_cursor = connect_to_db()
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
    kwargs["ti"].xcom_push(key="count_sieges", value=count_sieges)
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
    siren_db_conn, siren_db_cursor = connect_to_db()
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
        ul.tranche_effectif_salarie_unite_legale as tranche_effectif_salarie_unite_legale,
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
            st.is_siege as is_siege
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

    doc_count = index_by_chunk(
        cursor=siren_db_cursor,
        elastic_connection=elastic_connection,
        elastic_bulk_size=ELASTIC_BULK_SIZE,
        elastic_index=elastic_index,
    )
    kwargs["ti"].xcom_push(key="doc_count", value=doc_count)
    commit_and_close_conn(siren_db_conn)


def check_elastic_index(**kwargs):
    doc_count = kwargs["ti"].xcom_pull(key="doc_count", task_ids="fill_elastic_index")
    """
    count_sieges = kwargs["ti"].xcom_pull(
        key="count_sieges", task_ids="create_siege_only_table"
    )[0]
    """
    logging.info(f"******************** Documents indexed: {doc_count}")
    """
    if float(doc_count) != float(count_sieges):
        raise ValueError(
            f"*******The data has not been correctly indexed: "
            f"{doc_count} documents indexed instead of {count_sieges}."
        )
    """


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
