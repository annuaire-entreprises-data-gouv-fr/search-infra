import pandas as pd

from data_pipelines_annuaire.helpers import (
    DataProcessor,
    Notification,
    clean_sirent_column,
)
from data_pipelines_annuaire.workflows.data_pipelines.avocat.config import AVOCAT_CONFIG


class AvocatProcessor(DataProcessor):
    def __init__(self):
        super().__init__(AVOCAT_CONFIG)

    def preprocess_data(self):
        df_avocat = (
            pd.read_csv(
                self.config.files_to_download["avocat"]["destination"],
                dtype="string",
                sep=";",
                usecols=["cbSiretSiren"],
            )
            .assign(
                cbSiretSiren=lambda df: (
                    df["cbSiretSiren"].str.replace(" ", "", regex=False).str.strip()
                ),
                siren=lambda df: df["cbSiretSiren"].str.extract(
                    r"^(\d{9})", expand=False
                ),
            )
            .assign(est_avocat=1)[["siren", "est_avocat"]]
            .drop_duplicates(subset=["siren"])
            .pipe(clean_sirent_column, column_type="siren", max_removal_percentage=10)
        )

        df_avocat.to_csv(self.config.file_output, index=False)

        DataProcessor.push_message(
            Notification.notification_xcom_key,
            column=df_avocat.siren,
        )
