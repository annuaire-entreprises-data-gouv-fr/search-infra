import logging

import pandas as pd

from data_pipelines_annuaire.helpers import (
    DataProcessor,
    Notification,
)
from data_pipelines_annuaire.helpers.data_quality import clean_sirent_column
from data_pipelines_annuaire.workflows.data_pipelines.bodacc.config import (
    BODACC_CONFIG,
)
from data_pipelines_annuaire.workflows.data_pipelines.bodacc.utils import (
    extract_siren_from_registre,
    filter_cancelled_announcements,
    is_cloture,
    parse_jugement_json,
    parse_radiation_json,
)


def process_radiation_chunk(chunk: pd.DataFrame) -> pd.DataFrame:
    """Traiter un chunk de données radiations."""
    chunk = extract_siren_from_registre(chunk)

    # Nettoyer et valider les SIREN
    chunk = clean_sirent_column(
        chunk,
        column_type="siren",
        column_name="siren",
        add_leading_zeros=True,
        max_removal_percentage=0.1,
    )

    if chunk.empty:
        return chunk

    # Parser le JSON radiationaurcs pour extraire la date
    chunk["date_radiation"] = chunk["radiationaurcs"].apply(parse_radiation_json)

    # Marquer comme radié (présent dans le fichier = radié)
    chunk["est_radie_rcs"] = True

    # Garder uniquement les colonnes nécessaires
    chunk["date_publication_radiation"] = chunk["dateparution"]
    chunk["id_radiation"] = chunk["id"]
    return chunk[
        [
            "siren",
            "id_radiation",
            "est_radie_rcs",
            "date_radiation",
            "date_publication_radiation",
        ]
    ]


def process_procedure_chunk(chunk: pd.DataFrame) -> pd.DataFrame:
    """Traiter un chunk de données procédures collectives."""
    chunk = extract_siren_from_registre(chunk)

    # Nettoyer et valider les SIREN
    chunk = clean_sirent_column(
        chunk,
        column_type="siren",
        column_name="siren",
        add_leading_zeros=True,
        max_removal_percentage=0.1,
    )

    if chunk.empty:
        return chunk

    # Parser le JSON jugement (famille, nature, date)
    chunk["jugement_parsed"] = chunk["jugement"].apply(parse_jugement_json)
    chunk["famille_jugement"] = chunk["jugement_parsed"].apply(
        lambda x: x.get("famille", "") if x else ""
    )

    # Exclure les familles non pertinentes
    familles_exclues = ["Avis de dépôt", "Extrait de jugement"]
    chunk = chunk[~chunk["famille_jugement"].isin(familles_exclues)]

    if chunk.empty:
        return chunk

    chunk["nature_jugement"] = chunk["jugement_parsed"].apply(
        lambda x: x.get("nature", "") if x else ""
    )
    chunk["date_jugement"] = chunk["jugement_parsed"].apply(
        lambda x: x.get("date", "") if x else ""
    )

    # Garder uniquement les colonnes nécessaires
    chunk["date_publication_procedure"] = chunk["dateparution"]
    chunk["id_procedure"] = chunk["id"]
    return chunk[
        [
            "siren",
            "id_procedure",
            "famille_jugement",
            "nature_jugement",
            "date_jugement",
            "date_publication_procedure",
        ]
    ]


class BodaccProcessor(DataProcessor):
    CHUNK_SIZE = 100_000

    COLUMNS_RADIATIONS = [
        "id",
        "registre",
        "dateparution",
        "typeavis",
        "radiationaurcs",
        "parutionavisprecedent",
    ]
    COLUMNS_PROCEDURES = [
        "id",
        "registre",
        "dateparution",
        "typeavis",
        "jugement",
        "parutionavisprecedent",
    ]

    def __init__(self):
        super().__init__(BODACC_CONFIG)

    def preprocess_data(self):
        logging.info("Processing BODACC data...")

        df_radiations = self._process_radiations()
        logging.info(f"Radiations: {len(df_radiations)} unique SIRENs")

        df_procedures = self._process_procedures_collectives()
        logging.info(f"Procédures collectives: {len(df_procedures)} unique SIRENs")

        df = pd.merge(
            df_radiations,
            df_procedures,
            on="siren",
            how="outer",
        )
        logging.info(f"After merge: {len(df)} unique SIRENs")
        df.to_csv(f"{self.config.tmp_folder}/{self.config.file_name}.csv", index=False)

        DataProcessor.push_message(
            Notification.notification_xcom_key,
            description=f"{len(df)} SIREN traités au BODACC<ul><li>radiations : {len(df_radiations)}</li><li>procédures collectives : {len(df_procedures)}</li></ul>",
        )

    def _process_radiations(self) -> pd.DataFrame:
        logging.info("Processing radiations...")
        chunks_processed = []
        total_rows = 0

        # Charger le fichier complet pour gérer les annulations
        df_full = pd.read_csv(
            self.config.files_to_download["radiations"]["destination"],
            dtype=str,
            sep=";",
            usecols=self.COLUMNS_RADIATIONS,
        )
        total_rows = len(df_full)
        logging.info(f"Loaded {total_rows} radiation rows")

        # Filtrer les annulations
        df_full = filter_cancelled_announcements(df_full)
        logging.info(f"After filtering cancellations: {len(df_full)} rows")

        # Traiter par chunks pour la mémoire
        for i in range(0, len(df_full), self.CHUNK_SIZE):
            chunk = df_full.iloc[i : i + self.CHUNK_SIZE]
            processed = process_radiation_chunk(chunk)
            if not processed.empty:
                chunks_processed.append(processed)

        if not chunks_processed:
            logging.warning("No radiations found")
            return pd.DataFrame(
                columns=[
                    "siren",
                    "id_radiation",
                    "est_radie_rcs",
                    "date_radiation",
                    "date_radiation_str",
                    "date_publication_radiation",
                    "date_publication_radiation_str",
                ]
            )

        # Concaténer tous les chunks
        df = pd.concat(chunks_processed, ignore_index=True)

        # Renommer les colonnes string et créer les colonnes datetime
        df["date_radiation_str"] = df["date_radiation"]
        df["date_radiation"] = pd.to_datetime(
            df["date_radiation_str"], errors="coerce", format="%Y-%m-%d"
        )
        df["date_publication_radiation_str"] = df["date_publication_radiation"]
        df["date_publication_radiation"] = pd.to_datetime(
            df["date_publication_radiation_str"], errors="coerce", format="%Y-%m-%d"
        )

        # Dédupliquer par Siren en gardant la radiation la plus récente
        df = df.sort_values(
            ["date_radiation", "date_publication_radiation"], ascending=[False, False]
        )
        duplicated_mask = df.duplicated(subset=["siren"], keep="first")
        n_duplicates = duplicated_mask.sum()
        if n_duplicates > 0:
            sample_sirens = df.loc[duplicated_mask, "siren"].head(5).tolist()
            logging.info(
                f"Radiations: {n_duplicates} duplicate Siren, sample:\n{sample_sirens}"
            )
        df = df.drop_duplicates(subset=["siren"], keep="first")

        return df[
            [
                "siren",
                "id_radiation",
                "est_radie_rcs",
                "date_radiation",
                "date_radiation_str",
                "date_publication_radiation",
                "date_publication_radiation_str",
            ]
        ]

    def _process_procedures_collectives(self) -> pd.DataFrame:
        logging.info("Processing procédures collectives...")
        chunks_processed = []
        total_rows = 0

        # Charger le fichier complet pour gérer les annulations
        df_full = pd.read_csv(
            self.config.files_to_download["procedures_collectives"]["destination"],
            dtype=str,
            sep=";",
            usecols=self.COLUMNS_PROCEDURES,
        )
        total_rows = len(df_full)
        logging.info(f"Loaded {total_rows} procedure rows")

        # Filtrer les annulations et rétractations
        df_full = filter_cancelled_announcements(df_full)
        logging.info(f"After filtering cancellations: {len(df_full)} rows")

        # Traiter par chunks pour la mémoire
        for i in range(0, len(df_full), self.CHUNK_SIZE):
            chunk = df_full.iloc[i : i + self.CHUNK_SIZE]
            processed = process_procedure_chunk(chunk)
            if not processed.empty:
                chunks_processed.append(processed)

        if not chunks_processed:
            logging.warning("No procedures collectives found")
            return pd.DataFrame(
                columns=[
                    "siren",
                    "id_procedure",
                    "nature_jugement",
                    "date_jugement",
                    "date_jugement_str",
                    "date_publication_procedure",
                    "date_publication_procedure_str",
                    "procedure_collective_cloturee_nature",
                ]
            )

        # Concaténer tous les chunks
        df = pd.concat(chunks_processed, ignore_index=True)

        # Renommer les colonnes string et créer les colonnes datetime
        df["date_jugement_str"] = df["date_jugement"]
        df["date_jugement"] = pd.to_datetime(
            df["date_jugement_str"], errors="coerce", format="%Y-%m-%d"
        )
        df["date_publication_procedure_str"] = df["date_publication_procedure"]
        df["date_publication_procedure"] = pd.to_datetime(
            df["date_publication_procedure_str"], errors="coerce", format="%Y-%m-%d"
        )

        # Dédupliquer par SIREN en gardant la procédure la plus récente
        df = df.sort_values(
            ["date_jugement", "date_publication_procedure"], ascending=[False, False]
        )
        duplicated_mask = df.duplicated(subset=["siren"], keep="first")
        n_duplicates = duplicated_mask.sum()
        if n_duplicates > 0:
            sample_sirens = df.loc[duplicated_mask, "siren"].head(5).tolist()
            logging.info(
                f"Procedures: {n_duplicates} duplicate Siren, sample:\n{sample_sirens}"
            )
        df = df.drop_duplicates(subset=["siren"], keep="first")

        # Déterminer si la procédure la plus récente est une clôture
        df["is_cloture"] = df["famille_jugement"].apply(is_cloture)

        # Créer les colonnes finales
        df["procedure_collective_cloturee_nature"] = df.apply(
            lambda row: row["nature_jugement"] if row["is_cloture"] else "",
            axis=1,
        )
        df["nature_jugement_final"] = df.apply(
            lambda row: "" if row["is_cloture"] else row["nature_jugement"],
            axis=1,
        )

        # Renommer pour l'export
        df["nature_jugement"] = df["nature_jugement_final"]

        logging.info(
            f"Procedures: {len(df)} unique SIRENs, "
            f"{df['is_cloture'].sum()} clôturées, "
            f"{(~df['is_cloture']).sum()} en cours"
        )

        return df[
            [
                "siren",
                "id_procedure",
                "nature_jugement",
                "date_jugement",
                "date_jugement_str",
                "date_publication_procedure",
                "date_publication_procedure_str",
                "procedure_collective_cloturee_nature",
            ]
        ]
