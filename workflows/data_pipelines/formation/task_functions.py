import pandas as pd
import logging
import requests

from helpers.minio_helpers import minio_client
from helpers.settings import Settings
from helpers.tchap import send_message


def preprocess_organisme_formation_data(ti):
    # get dataset directly from dge website
    r = requests.get(Settings.URL_ORGANISME_FORMATION)
    with open(Settings.FORMATION_TMP_FOLDER + "qualiopi-download.csv", "wb") as f:
        for chunk in r.iter_content(1024):
            f.write(chunk)
    df_organisme_formation = pd.read_csv(
        Settings.FORMATION_TMP_FOLDER + "qualiopi-download.csv", dtype=str, sep=";"
    )

    # Renommage des colonnes
    df_organisme_formation = df_organisme_formation.rename(
        columns={
            "id_nda": "id_nda",
            "id_nda_precedent": "id_nda_precedent",
            "denomination": "denomination",
            "siren": "siren",
            "siret": "siret",
            "adresse": "adresse",
            "code_postal": "code_postal",
            "ville": "ville",
            "code_region": "code_region",
            "geocodageban": "geocodageban",
            "cert_adf": "cert_adf",
            "cert_bdc": "cert_bdc",
            "cert_vae": "cert_vae",
            "cert_app": "cert_app",
            "Dénomination Organisme Etranger Représenté": "denomination_organisme_etranger",
            "Adresse Organisme Etranger Représenté": "adresse_organisme_etranger",
            "Code Postal Organisme Etranger Représenté": "code_postal_organisme_etranger",
            "Ville Organisme Etranger Représenté": "ville_organisme_etranger",
            "Pays Organisme Etranger Représenté": "pays_organisme_etranger",
            "Date dernière déclaration": "date_derniere_declaration",
            "Début d'exercice": "debut_exercice",
            "Fin d'exercice": "fin_exercice",
            "Code Spécialité 1": "code_specialite_1",
            "Libellé Spécialité 1": "libelle_specialite_1",
            "Code Spécialité 2": "code_specialite_2",
            "Libellé Spécialité 2": "libelle_specialite_2",
            "Code Spécialité 3": "code_specialite_3",
            "Libellé Spécialité 3": "libelle_specialite_3",
            "Nombre de stagiaires": "nombre_stagiaires",
            "Nombre de stagiaires confiés par un autre Organisme de formation": "nombre_stagiaires_confies",
            "Effectifs de formateurs": "effectifs_formateurs",
            "Code Commune": "code_commune",
            "Nom Officiel Commune / Arrondissement Municipal": "nom_commune",
            "Code Officiel EPCI": "code_epci",
            "Nom Officiel EPCI": "nom_epci",
            "Code Officiel Département": "code_departement",
            "Nom Officiel Département": "nom_departement",
            "Nom Officiel Région": "nom_region",
            "Code Officiel Région": "code_region_officiel",
            "Toutes spécialités de l'organisme de formation": "toutes_specialites",
            "Organisme Formation Géocodé": "organisme_formation_geocode",
            "certifications": "certifications",
            "random_id": "random_id"
        }
    )

    # Sauvegarde de la version 2 après renommage
    df_organisme_formation.to_csv(
        Settings.FORMATION_TMP_FOLDER + "qualiopi-download-v2.csv", index=False, sep=";"
    )

    df_organisme_formation = df_organisme_formation[
        [
            "id_nda",
            "siren",
            "siret",
            "cert_adf",
            "cert_bdc",
            "cert_vae",
            "cert_app",
            "certifications",
        ]
    ]
    df_organisme_formation = df_organisme_formation.where(
        pd.notnull(df_organisme_formation), None
    )
    df_organisme_formation["est_qualiopi"] = df_organisme_formation.apply(
        lambda x: True if x["certifications"] else False, axis=1
    )
    df_organisme_formation = df_organisme_formation[["siren", "est_qualiopi", "id_nda"]]
    df_liste_organisme_formation = (
        df_organisme_formation.groupby(["siren"])[["id_nda"]].agg(list).reset_index()
    )
    df_liste_organisme_formation["id_nda"] = df_liste_organisme_formation[
        "id_nda"
    ].astype(str)
    df_liste_organisme_formation = pd.merge(
        df_liste_organisme_formation,
        df_organisme_formation[["siren", "est_qualiopi"]],
        on="siren",
        how="left",
    )
    df_liste_organisme_formation = df_liste_organisme_formation.rename(
        columns={
            "id_nda": "liste_id_organisme_formation",
        }
    )
    # Drop est_qualiopi=False when True value exists
    df_liste_organisme_formation.sort_values(
        "est_qualiopi", ascending=False, inplace=True
    )
    df_liste_organisme_formation.drop_duplicates(
        subset=["siren", "liste_id_organisme_formation"], keep="first", inplace=True
    )

    df_liste_organisme_formation.to_csv(
        f"{Settings.FORMATION_TMP_FOLDER}formation.csv", index=False
    )
    ti.xcom_push(
        key="nb_siren_formation",
        value=str(df_liste_organisme_formation["siren"].nunique()),
    )

    del df_organisme_formation
    del df_liste_organisme_formation


def send_file_to_minio():
    minio_client.send_files(
        list_files=[
            {
                "source_path": Settings.FORMATION_TMP_FOLDER,
                "source_name": "formation.csv",
                "dest_path": "formation/new/",
                "dest_name": "formation.csv",
            },
        ],
    )


def compare_files_minio():
    is_same = minio_client.compare_files(
        file_path_1="formation/new/",
        file_name_2="formation.csv",
        file_path_2="formation/latest/",
        file_name_1="formation.csv",
    )
    if is_same:
        return False

    if is_same is None:
        logging.info("First time in this Minio env. Creating")

    minio_client.send_files(
        list_files=[
            {
                "source_path": Settings.FORMATION_TMP_FOLDER,
                "source_name": "formation.csv",
                "dest_path": "formation/latest/",
                "dest_name": "formation.csv",
            },
        ],
    )

    return True


def send_notification(ti):
    nb_siren = ti.xcom_pull(
        key="nb_siren_formation", task_ids="preprocess_organisme_formation_data"
    )
    send_message(
        f"\U0001F7E2 Données Organisme formation mises à jour.\n"
        f"- {nb_siren} unités légales représentées."
    )
