import pandas as pd
import logging

from helpers.minio_helpers import minio_client
from helpers.settings import Settings
from helpers.tchap import send_message


def preprocess_egapro_data(ti):
    df_egapro = pd.read_excel(
        Settings.URL_EGAPRO,
        dtype=str,
        engine="openpyxl",
    )
    df_egapro = df_egapro.drop_duplicates(subset=["SIREN"], keep="first")
    df_egapro = df_egapro.rename(columns={
        "SIREN": "siren",
        "Année": "annee",
        "Structure": "structure",
        "Tranche d'effectifs": "tranche_effectifs",
        "Raison Sociale": "raison_sociale",
        "Nom UES": "nom_ues",
        "Entreprises UES (SIREN)": "entreprises_ues_siren",
        "Région": "region",
        "Département": "departement",
        "Pays": "pays",
        "Code NAF": "code_naf",
        "Note Ecart rémunération": "note_ecart_remuneration",
        "Note Ecart taux d'augmentation (hors promotion)": "note_ecart_taux_augmentation_hors_promotion",
        "Note Ecart taux de promotion": "note_ecart_taux_promotion",
        "Note Ecart taux d'augmentation": "note_ecart_taux_augmentation",
        "Note Retour congé maternité": "note_retour_conge_maternite",
        "Note Hautes rémunérations": "note_hautes_remunerations",
        "Note Index": "note_index"
    })
    df_egapro.to_csv(f"{Settings.EGAPRO_TMP_FOLDER}egapro.csv", index=False)
    ti.xcom_push(key="nb_siren_egapro", value=str(df_egapro["siren"].nunique()))
    del df_egapro


def send_file_to_minio():
    minio_client.send_files(
        list_files=[
            {
                "source_path": Settings.EGAPRO_TMP_FOLDER,
                "source_name": "egapro.csv",
                "dest_path": "egapro/new/",
                "dest_name": "egapro.csv",
            },
        ],
    )


def compare_files_minio():
    is_same = minio_client.compare_files(
        file_path_1="egapro/new/",
        file_name_2="egapro.csv",
        file_path_2="egapro/latest/",
        file_name_1="egapro.csv",
    )
    if is_same:
        return False

    if is_same is None:
        logging.info("First time in this Minio env. Creating")

    minio_client.send_files(
        list_files=[
            {
                "source_path": Settings.EGAPRO_TMP_FOLDER,
                "source_name": "egapro.csv",
                "dest_path": "egapro/latest/",
                "dest_name": "egapro.csv",
            },
        ],
    )

    return True


def send_notification(ti):
    nb_siren = ti.xcom_pull(key="nb_siren_egapro", task_ids="process_egapro")
    send_message(
        f"\U0001F7E2 Données Egapro mises à jour.\n"
        f"- {nb_siren} unités légales représentées."
    )
