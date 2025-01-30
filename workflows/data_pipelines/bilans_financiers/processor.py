from datetime import datetime

import pandas as pd

from dag_datalake_sirene.helpers import DataProcessor, Notification
from dag_datalake_sirene.helpers.utils import get_fiscal_year
from dag_datalake_sirene.workflows.data_pipelines.bilans_financiers.config import (
    BILANS_FINANCIERS_CONFIG,
)


class BilansFinanciersProcessor(DataProcessor):
    def __init__(self) -> None:
        super().__init__(BILANS_FINANCIERS_CONFIG)

    def preprocess_data(self) -> None:
        df_bilan = pd.read_csv(
            self.config.files_to_download["bilans_financiers"]["destination"],
            dtype=str,
            sep=";",
            usecols=[
                "siren",
                "Chiffre_d_affaires",
                "Resultat_net",
                "date_cloture_exercice",
                "type_bilan",
            ],
            encoding="utf-8-sig",
        )

        # Comptes consolidés (consolidated accounts) are published before the 15th of June
        # So we are waiting July before processing the bilans of the current year
        current_fiscal_year = get_fiscal_year(datetime.now())
        df_bilan = (
            df_bilan.rename(
                columns={
                    "Chiffre_d_affaires": "ca",
                    "Resultat_net": "resultat_net",
                }
            )
            .assign(
                date_cloture_exercice=lambda df: pd.to_datetime(
                    df["date_cloture_exercice"], errors="coerce"
                ),
                annee_cloture_exercice=lambda df: df["date_cloture_exercice"].apply(
                    get_fiscal_year
                ),
            )
            .loc[lambda x: x["annee_cloture_exercice"] <= current_fiscal_year, :]
            .filter(
                items=[
                    "siren",
                    "ca",
                    "date_cloture_exercice",
                    "resultat_net",
                    "type_bilan",
                    "annee_cloture_exercice",
                ]
            )
            .drop_duplicates(
                subset=["siren", "annee_cloture_exercice", "type_bilan"], keep="last"
            )
        )

        # type_bilan values :
        # - K: bilan consolidé
        # - C: bilan complet
        # - S: bilan simplifié (small businesses)
        # - No code: direct information
        # We are interested in bilan consolidés first.
        # But, if none is available, the last one.
        siren_with_K = df_bilan.loc[df_bilan["type_bilan"] == "K", "siren"].unique()
        df_bilan = (
            df_bilan.loc[
                ~df_bilan["siren"].isin(siren_with_K) | (df_bilan["type_bilan"] == "K")
            ]
            .sort_values(
                ["siren", "annee_cloture_exercice", "type_bilan"],
                ascending=[True, False, True],
            )
            .drop_duplicates(subset=["siren"], keep="first")
            .filter(
                items=[
                    "siren",
                    "ca",
                    "date_cloture_exercice",
                    "resultat_net",
                    "annee_cloture_exercice",
                ]
            )
        )

        df_bilan["ca"] = df_bilan["ca"].astype(float)
        df_bilan["resultat_net"] = df_bilan["resultat_net"].astype(float)

        df_bilan.to_csv(
            self.config.file_output,
            index=False,
        )

        DataProcessor.push_message(
            Notification.notification_xcom_key,
            column=df_bilan.siren,
        )
