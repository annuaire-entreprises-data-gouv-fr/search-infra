import pandas as pd

from dag_datalake_sirene.helpers import DataProcessor, Notification
from dag_datalake_sirene.workflows.data_pipelines.uai.config import (
    UAI_CONFIG,
)


class UaiProcessor(DataProcessor):
    def __init__(self):
        super().__init__(UAI_CONFIG)

    def preprocess_data(self):
        target_columns = [
            "uai",
            "denomination",
            "sigle",
            "adresse",
            "code_postal",
            "code_commune",
            "commune",
            "siren",
            "siret",
            "public_prive",
            "statut_prive",
            "type",
        ]

        df_menj = (
            pd.read_csv(
                self.config.files_to_download["menj"]["destination"],
                dtype="string",
                sep=";",
            )
            .rename(
                columns={
                    "Identifiant_de_l_etablissement": "uai",
                    "Nom_etablissement": "denomination",
                    "Adresse_1": "adresse",
                    "Code_postal": "code_postal",
                    "Code_commune": "code_commune",
                    "Nom_commune": "commune",
                    "SIREN_SIRET": "siret",
                    "Statut_public_prive": "public_prive",
                    "Type_contrat_prive": "statut_prive",
                    "Type_etablissement": "type",
                }
            )
            .assign(
                sigle=None,
                siren=lambda x: x["siret"].str[:9],
            )
            .filter(target_columns)
        )

        df_mesr = (
            pd.read_csv(
                self.config.files_to_download["mesr"]["destination"], dtype=str, sep=";"
            )
            .rename(
                columns={
                    "uai - identifiant": "uai",
                    "uo_lib_officiel": "denomination",
                    "sigle": "sigle",
                    "Adresse": "adresse",
                    "Code postal": "code_postal",
                    "Code commune": "code_commune",
                    "Commune": "commune",
                    "siren": "siren",
                    "siret": "siret",
                    "secteur d'établissement": "public_prive",
                    "type d'établissement": "type",
                }
            )
            .assign(
                statut_prive=None,
            )
            .filter(target_columns)
        )

        df_onisep = (
            pd.read_csv(
                self.config.files_to_download["onisep"]["destination"],
                dtype=str,
                sep=";",
            )
            .rename(
                columns={
                    "code UAI": "uai",
                    "nom": "denomination",
                    "sigle": "sigle",
                    "adresse": "adresse",
                    "CP": "code_postal",
                    "commune (COG)": "code_commune",
                    "commune": "commune",
                    "n° SIRET": "siret",
                    "statut": "public_prive",
                    "type d'établissement": "type",
                }
            )
            .assign(
                siren=lambda x: x["siret"].str[:9],
                statut_prive=None,
            )
            .filter(target_columns)
        )

        annuaire_uai = (
            pd.concat([df_menj, df_mesr, df_onisep], ignore_index=True)
            .drop_duplicates(subset=["uai"], keep="first")
            .groupby(["siret"])["uai"]
            .agg(lambda x: str(list(x)))
            .reset_index(name="liste_uai")
            .filter(["siret", "liste_uai"])
        )
        annuaire_uai.to_csv(self.config.file_output, index=False)

        self.push_message(
            Notification.notification_xcom_key,
            column=annuaire_uai["siret"],
        )

        del df_onisep
        del df_menj
        del df_mesr
