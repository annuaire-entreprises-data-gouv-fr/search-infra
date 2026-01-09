import pandas as pd
from data_pipelines_annuaire.helpers import (
    DataProcessor,
    Notification,
    clean_sirent_column,
)
from data_pipelines_annuaire.workflows.data_pipelines.finess.juridique.config import (
    FINESS_JURIDIQUE_CONFIG,
)


class FinessJuridiqueProcessor(DataProcessor):
    def __init__(self):
        super().__init__(FINESS_JURIDIQUE_CONFIG)

    def preprocess_data(self):
        """
        Create a list of all the Finess Juridique IDs of a Siren
        including the ones from any of the établissement of the Siren
        but excluding any Finess Juridique without a Finess Géographique.

        CSV Output columns:
            - siren
            - liste_finess_juridique: list of Finess Juridique of the siren
            - has_finess_from_geographique_only: True if at least one Finess
                comes from a Siret and not from the Siren
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
            # And clean it
            .rename(columns={2: "finess_juridique", 22: "siret"})
            .filter(["finess_juridique", "siret"])
            .assign(
                siren=lambda df: df["siret"].str[:9],
                source=lambda df: "geographique",
            )
            .filter(["finess_juridique", "siren", "source"])
            .loc[lambda x: x["siren"].notna()]
        )

        # Only keep Finess Juridique IDs that have at least one Finess Géographique
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
            .assign(source=lambda df: "juridique")
            .loc[lambda x: x["siren"].notna()]
            # Exclude any Finess Juridique with no Finess Géographique
            .loc[lambda x: x["finess_juridique"].isin(valid_finess_ids)]
        )

        # Concat the Finess Juridique from both files
        df_combined = pd.concat(
            [df_finess_geographique, df_finess_juridique],
            ignore_index=True,
        )

        # Get all entries where at least one Finess Juridique of a Siren comes from
        # one of its Siret without being registered at the Siren level
        # Used to calculate the has_finess_from_geographique_only flag
        finess_geographique_only = set(
            df_combined[df_combined["source"] == "geographique"]["finess_juridique"]
        ) - set(df_combined[df_combined["source"] == "juridique"]["finess_juridique"])

        # Deduplicate the Finess Juridiques and calculate the flag
        df_list_finess_juridique = df_combined.groupby(["siren"], as_index=False).agg(
            liste_finess_juridique=(
                "finess_juridique",
                lambda x: str(list(x.unique())),
            ),
            has_finess_from_geographique_only=(
                "finess_juridique",
                lambda x: any(fid in finess_geographique_only for fid in x.unique()),
            ),
        )

        # Clean siren column and remove invalid rows
        df_list_finess_juridique = clean_sirent_column(
            df_list_finess_juridique,
            column_type="siren",
        )

        df_list_finess_juridique.to_csv(
            f"{self.config.tmp_folder}/finess_juridique.csv", index=False
        )

        DataProcessor.push_message(
            Notification.notification_xcom_key,
            column=df_list_finess_juridique["siren"],
        )
