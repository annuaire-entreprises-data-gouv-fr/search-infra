import pandas as pd
from data_pipelines_annuaire.helpers import (
    DataProcessor,
    Notification,
    clean_sirent_column,
)
from data_pipelines_annuaire.workflows.data_pipelines.finess.geographique.config import (
    FINESS_GEOGRAPHIQUE_CONFIG,
)


class FinessGeographiqueProcessor(DataProcessor):
    def __init__(self):
        super().__init__(FINESS_GEOGRAPHIQUE_CONFIG)

    def preprocess_data(self):
        """
        Create a list of all the Finess Géographique IDs related to a Siret.

        CSV Output columns:
            - siret
            - liste_juridique_geographique: list of Finess Géographiques of the siret
        """
        df_finess_geographique = (
            # Load the data..
            pd.read_csv(
                self.config.files_to_download["finess_geographique"]["destination"],
                dtype=str,
                sep=";",
                encoding="Latin-1",
                skiprows=1,
                header=None,
            )
            # Clean it..
            .rename(
                columns={
                    1: "finess_geographique",
                    22: "siret",
                }
            )
            .filter(["finess_geographique", "siret"])
            .loc[lambda x: x["siret"].notna()]
            # And group by Siret to get the list of unique Finess Géographique
            .groupby(["siret"], as_index=False)
            .agg(
                liste_finess_geographique=(
                    "finess_geographique",
                    lambda x: str(list(x.unique())),
                )
            )
        )

        # Clean siret column and remove invalid rows
        df_finess_geographique = clean_sirent_column(
            df_finess_geographique,
            column_type="siret",
        )

        df_finess_geographique.to_csv(
            f"{self.config.tmp_folder}/finess_geographique.csv", index=False
        )

        DataProcessor.push_message(
            Notification.notification_xcom_key,
            column=df_finess_geographique["siret"],
        )
