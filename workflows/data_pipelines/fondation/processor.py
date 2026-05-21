import zipfile

import pandas as pd
from data_pipelines_annuaire.helpers import DataProcessor, Notification
from data_pipelines_annuaire.workflows.data_pipelines.fondation.config import (
    FONDATION_CONFIG,
)


class FondationProcessor(DataProcessor):
    def __init__(self):
        super().__init__(FONDATION_CONFIG)

    def preprocess_data(self):
        zip_path = self.config.files_to_download["fondation"]["destination"]
        with zipfile.ZipFile(zip_path, mode="r") as z:
            csv_name = z.namelist()[0]
            z.extract(csv_name, path=self.config.tmp_folder)

        csv_path = f"{self.config.tmp_folder}/{csv_name}"

        df = pd.read_csv(
            csv_path,
            dtype="string",
            sep=",",
            usecols=[
                "titre",
                "numero_rnf",
                "siret",
                "date_creation",
                "adresse",
                "code_postal",
                "ville",
            ],
        )

        df["siren"] = df["siret"].str[:9]
        df["date_creation"] = df["date_creation"].str[:10]

        df_with_siret = df[df["siret"].notna()]
        df_without_siret = df[df["siret"].isna()]

        # If a SIRET is duplicated we need to know to informe the producer so it can fix it
        duplicated_mask = df_with_siret.duplicated(subset=["siret"], keep=False)
        duplicated_sirets = df_with_siret.loc[duplicated_mask, "siret"].unique()
        n_duplicates = len(duplicated_sirets)
        if n_duplicates > 0:
            sample_sirets = duplicated_sirets[:10].tolist()
            warning_message = f"⚠️ {n_duplicates} SIRET dupliqués, échantillon : {sample_sirets}. À remonter au producteur."
            DataProcessor.push_message(
                Notification.notification_xcom_key,
                description=warning_message,
            )

        # When we have duplicated on the SIRET, and until we know better
        # let's prioritise the most recent date_creation
        df_with_siret = df_with_siret.sort_values("date_creation", ascending=False)
        df = pd.concat(
            [
                df_without_siret,
                df_with_siret.drop_duplicates(subset=["siret"], keep="first"),
            ]
        )

        df.to_csv(self.config.file_output, index=False)

        DataProcessor.push_message(
            Notification.notification_xcom_key,
            column=df.siret,
        )
        DataProcessor.push_message(
            Notification.notification_xcom_key,
            column=df.numero_rnf,
        )

        del df
