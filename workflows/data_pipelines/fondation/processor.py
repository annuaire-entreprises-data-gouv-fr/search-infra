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
                "adresse",
                "code_postal",
                "ville",
            ],
        )

        df["siren"] = df["siret"].str[:9]

        # Five SIRET are duplicates
        # Until we know better, let's prioritise the last numero_rnf
        df = df.sort_values("numero_rnf", ascending=True)
        df = pd.concat(
            [
                df[df["siret"].isna()],
                df[df["siret"].notna()].drop_duplicates(subset=["siret"], keep="first"),
            ]
        )

        df.to_csv(self.config.file_output, index=False)

        DataProcessor.push_message(
            Notification.notification_xcom_key,
            column=df.siret,
        )

        del df
