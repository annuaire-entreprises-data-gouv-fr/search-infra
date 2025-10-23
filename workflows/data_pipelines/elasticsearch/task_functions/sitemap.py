import os

from dag_datalake_sirene.config import (
    AIRFLOW_ELK_DATA_DIR,
    AIRFLOW_ENV,
)
from dag_datalake_sirene.helpers.minio_helpers import MinIOClient
from dag_datalake_sirene.helpers.sqlite_client import SqliteClient
from dag_datalake_sirene.workflows.data_pipelines.elasticsearch.data_enrichment import (
    format_nom_complet,
    format_slug,
    is_personne_morale_insee,
)
from dag_datalake_sirene.workflows.data_pipelines.elasticsearch.sqlite.sitemap import (
    select_sitemap_fields_query,
)

# dev-01 -> sitemap-dev.csv / prod -> sitemap-prod.csv
filename = "sitemap-" + AIRFLOW_ENV.split("-")[0] + ".csv"


def create_sitemap():
    sqlite_client = SqliteClient(AIRFLOW_ELK_DATA_DIR + "sirene.db")
    sqlite_client.execute(select_sitemap_fields_query)
    filepath = AIRFLOW_ELK_DATA_DIR + filename

    if os.path.exists(filepath):
        os.remove(filepath)

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
                and (
                    ul["statut_diffusion_unite_legale"] == "O"
                    or is_personne_morale_insee(ul["nature_juridique_unite_legale"])
                )
            ):
                if not ul["activite_principale_unite_legale"]:
                    ul["activite_principale_unite_legale"] = ""
                nom_complet = format_nom_complet(
                    nom_raison_sociale=ul["nom_raison_sociale"],
                )
                slug = format_slug(
                    nom_complet,
                    ul["siren"],
                    ul["sigle"],
                    ul["nom_commercial"],
                    ul["statut_diffusion_unite_legale"],
                    ul["nature_juridique_unite_legale"],
                )

                code_localisation = ul["code_postal"]
                if not code_localisation or code_localisation == "[ND]":
                    code_localisation = ul["code_commune"]
                if not code_localisation and ul["code_pays_etranger"]:
                    code_localisation = ul["code_pays_etranger"]
                if not code_localisation:
                    code_localisation = ""
                # Clean the field to avoid corrupted sitemap csv entries
                code_localisation = code_localisation.replace(",", "").replace("'", "")

                slugs = (
                    f"{slugs}{code_localisation},"
                    f"{ul['activite_principale_unite_legale']},{slug}\n"
                )

        with open(filepath, "a+") as f:
            f.write(slugs)


def update_sitemap():
    MinIOClient().send_files(
        list_files=[
            {
                "source_path": AIRFLOW_ELK_DATA_DIR,
                "source_name": filename,
                "dest_path": "",
                "dest_name": "sitemap.csv",
                "content_type": "text/csv",
            }
        ],
    )
