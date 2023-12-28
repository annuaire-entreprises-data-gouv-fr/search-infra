import os
from minio import Minio


from dag_datalake_sirene.data_pipelines.elasticsearch.data_enrichment import (
    format_nom_complet,
    format_slug,
)

from dag_datalake_sirene.helpers.sqlite_client import SqliteClient
from dag_datalake_sirene.data_pipelines.elasticsearch.sqlite.sitemap import (
    select_sitemap_fields_query,
)
from dag_datalake_sirene.config import (
    AIRFLOW_ELK_DATA_DIR,
    AIRFLOW_ENV,
    SIRENE_DATABASE_LOCATION,
    MINIO_URL,
    MINIO_BUCKET,
    MINIO_USER,
    MINIO_PASSWORD,
)


def create_sitemap():
    sqlite_client = SqliteClient(SIRENE_DATABASE_LOCATION)
    sqlite_client.execute(select_sitemap_fields_query)

    if os.path.exists(AIRFLOW_ELK_DATA_DIR + "sitemap-" + AIRFLOW_ENV + ".csv"):
        os.remove(AIRFLOW_ELK_DATA_DIR + "sitemap-" + AIRFLOW_ENV + ".csv")

    chunk_unites_legales_sqlite = 1
    while chunk_unites_legales_sqlite:
        chunk_unites_legales_sqlite = sqlite_client.db_cursor.fetchmany(1500)

        unite_legale_columns = tuple(
            [x[0] for x in sqlite_client.db_cursor.description]
        )
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
        slugs = ""
        for ul in liste_unites_legales_sqlite:
            if (
                ul["etat_administratif_unite_legale"] == "A"
                and ul["nature_juridique_unite_legale"] != "1000"
                and ul["statut_diffusion_unite_legale"] == "O"
            ):
                if not ul["code_postal"]:
                    ul["code_postal"] = ""
                if not ul["activite_principale_unite_legale"]:
                    ul["activite_principale_unite_legale"] = ""
                nom_complet = format_nom_complet(
                    nom_raison_sociale=ul["nom_raison_sociale"],
                )
                slug = format_slug(
                    nom_complet,
                    ul["sigle"],
                    ul["denomination_usuelle_1_unite_legale"],
                    ul["denomination_usuelle_2_unite_legale"],
                    ul["denomination_usuelle_3_unite_legale"],
                    ul["siren"],
                    ul["statut_diffusion_unite_legale"],
                )
                slugs = (
                    f"{slugs}{ul['code_postal']},"
                    f"{ul['activite_principale_unite_legale']},{slug}\n"
                )

        with open(AIRFLOW_ELK_DATA_DIR + "sitemap-" + AIRFLOW_ENV + ".csv", "a+") as f:
            f.write(slugs)


def update_sitemap():
    minio_filepath = "ae/sitemap-" + AIRFLOW_ENV + ".csv"
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
            file_path=AIRFLOW_ELK_DATA_DIR + "sitemap-" + AIRFLOW_ENV + ".csv",
            content_type="text/csv",
        )
