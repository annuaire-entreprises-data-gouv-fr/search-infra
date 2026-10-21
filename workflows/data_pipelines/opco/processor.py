import pandas as pd

from data_pipelines_annuaire.helpers import (
    DataProcessor,
    Notification,
    clean_sirent_column,
)
from data_pipelines_annuaire.workflows.data_pipelines.opco.config import OPCO_CONFIG


class OpcoProcessor(DataProcessor):
    def __init__(self) -> None:
        super().__init__(OPCO_CONFIG)

    def preprocess_data(self) -> None:
        df_opco = (
            pd.read_csv(
                self.config.files_to_download["opco"]["destination"],
                dtype="string",
                sep="|",
                usecols=["SIRET", "OPCO_PROPRIETAIRE"],
            )
            # SIRET en "anomalie déclarative" are listed without any OPCO
            .dropna(subset=["OPCO_PROPRIETAIRE"])
            .rename(columns={"SIRET": "siret"})
            .drop_duplicates(subset=["siret"])
            .pipe(clean_sirent_column, column_type="siret", max_removal_percentage=1)
            .assign(a_opco=1)
        )[["siret", "a_opco"]]

        df_opco.to_csv(self.config.file_output, index=False)

        DataProcessor.push_message(
            Notification.notification_xcom_key,
            column=df_opco.siret,
        )
