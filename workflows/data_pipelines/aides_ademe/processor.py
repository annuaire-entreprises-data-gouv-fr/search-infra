import pandas as pd

from data_pipelines_annuaire.helpers import (
    DataProcessor,
    Notification,
    clean_sirent_column,
)
from data_pipelines_annuaire.workflows.data_pipelines.aides_ademe.config import (
    AIDES_ADEME_CONFIG,
)


class AidesAdemeProcessor(DataProcessor):
    def __init__(self):
        super().__init__(AIDES_ADEME_CONFIG)

    def preprocess_data(self):
        df_aides_ademe = (
            pd.read_csv(
                self.config.files_to_download["aides_ademe"]["destination"],
                dtype="string",
                usecols=["idBeneficiaire"],
            )
            .assign(
                idBeneficiaire=lambda df: (
                    df["idBeneficiaire"].str.replace(" ", "", regex=False).str.strip()
                ),
                siren=lambda df: df["idBeneficiaire"].str.extract(
                    r"^(\d{9})", expand=False
                ),
            )
            .dropna(subset=["siren"])
            .assign(aide_ademe_renseignee=1)[["siren", "aide_ademe_renseignee"]]
            .drop_duplicates(subset=["siren"])
            .pipe(clean_sirent_column, column_type="siren")
        )

        df_aides_ademe.to_csv(self.config.file_output, index=False)

        DataProcessor.push_message(
            Notification.notification_xcom_key,
            column=df_aides_ademe.siren,
        )
