import pandas as pd
from dag_datalake_sirene.helpers import DataProcessor, Notification
from dag_datalake_sirene.workflows.data_pipelines.finess.geographique.config import (
    FINESS_GEOGRAPHIQUE_CONFIG,
)


class FinessGeographiqueProcessor(DataProcessor):
    def __init__(self):
        super().__init__(FINESS_GEOGRAPHIQUE_CONFIG)

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
            .rename(
                columns={
                    1: "finess_geographique",
                    22: "siret",
                }
            )
            .filter(["finess_geographique", "siret"])
            .loc[lambda x: x["siret"].notna()]
            .groupby(["siret"], as_index=False)
            .agg(
                liste_finess_geographique=(
                    "finess_geographique",
                    lambda x: str(list(x.unique())),
                )
            )
        )
        df_finess_geographique.to_csv(
            f"{self.config.tmp_folder}/finess_geographique.csv", index=False
        )

        DataProcessor.push_message(
            Notification.notification_xcom_key,
            column=df_finess_geographique["siret"],
        )
