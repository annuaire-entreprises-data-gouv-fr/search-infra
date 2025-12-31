import pandas as pd

from data_pipelines_annuaire.helpers import (
    DataProcessor,
    Notification,
    clean_sirent_column,
)
from data_pipelines_annuaire.workflows.data_pipelines.achats_responsables.config import (
    ACHATS_RESPONSABLES_CONFIG,
)


class AchatsResponsablesProcessor(DataProcessor):
    def __init__(self):
        super().__init__(ACHATS_RESPONSABLES_CONFIG)

    def preprocess_data(self):
        df_achats = (
            pd.read_csv(
                self.config.files_to_download["achats_responsables"]["destination"],
                dtype="string",
                sep=";",
                usecols=["SIREN", "PERIMETRE"],
            )
            .rename(
                columns={
                    "SIREN": "siren",
                    "PERIMETRE": "perimetre_label",
                }
            )
            .assign(
                est_achats_responsables=1,
            )
        )

        # Clean siren column and remove invalid rows
        df_achats = clean_sirent_column(df_achats, column_type="siren")
        df_achats = df_achats.drop_duplicates(subset=["siren"])

        df_achats.to_csv(self.config.file_output, index=False)

        DataProcessor.push_message(
            Notification.notification_xcom_key,
            column=df_achats.siren,
        )

        del df_achats
