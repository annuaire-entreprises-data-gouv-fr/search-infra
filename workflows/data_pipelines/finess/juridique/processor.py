import pandas as pd
from dag_datalake_sirene.helpers import DataProcessor, Notification
from dag_datalake_sirene.workflows.data_pipelines.finess.juridique.config import (
    FINESS_JURIDIQUE_CONFIG,
)


class FinessJuridiqueProcessor(DataProcessor):
    def __init__(self):
        super().__init__(FINESS_JURIDIQUE_CONFIG)

    def preprocess_data(self):
        df_finess_geographique = (
            pd.read_csv(
                self.config.files_to_download["finess_geographique"]["destination"],
                dtype=str,
                sep=";",
                encoding="Latin-1",
                skiprows=1,
                header=None,
            )
            .rename(columns={2: "finess_juridique", 22: "siret"})
            .filter(["finess_juridique", "siret"])
            .assign(siren=lambda df: df["siret"].str[:9])
            .filter(["finess_juridique", "siren"])
            .loc[lambda x: x["siren"].notna()]
        )

        valid_finess_ids = df_finess_geographique["finess_juridique"].unique()

        df_finess_juridique = (
            pd.read_csv(
                self.config.files_to_download["finess_juridique"]["destination"],
                dtype=str,
                sep=";",
                encoding="Latin-1",
                skiprows=1,
                header=None,
            )
            .rename(columns={1: "finess_juridique", 20: "siren"})
            .filter(["finess_juridique", "siren"])
            .loc[lambda x: x["siren"].notna()]
            .loc[lambda x: x["finess_juridique"].isin(valid_finess_ids)]
        )

        df_list_finess_juridique = (
            pd.concat(
                [df_finess_geographique, df_finess_juridique],
                ignore_index=True,
            )
            .groupby(["siren"], as_index=False)
            .agg(
                liste_finess_juridique=(
                    "finess_juridique",
                    lambda x: str(list(x.unique())),
                )
            )
        )
        df_list_finess_juridique.to_csv(
            f"{self.config.tmp_folder}/finess_juridique.csv", index=False
        )

        DataProcessor.push_message(
            Notification.notification_xcom_key,
            column=df_list_finess_juridique["siren"],
        )
