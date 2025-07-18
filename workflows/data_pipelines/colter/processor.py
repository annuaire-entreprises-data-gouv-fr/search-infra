import logging

import numpy as np
import pandas as pd

from dag_datalake_sirene.helpers import DataProcessor
from dag_datalake_sirene.workflows.data_pipelines.colter.config import (
    COLTER_CONFIG,
    ELUS_CONFIG,
)


class ColterProcessor(DataProcessor):
    def __init__(self):
        super().__init__(COLTER_CONFIG)

    def preprocess_data(self):
        ### RÉGIONS ###

        def regions_colter_niveau(colter_code_insee) -> str:
            if (
                colter_code_insee
                in [
                    "02",  # Martinique https://www.legifrance.gouv.fr/codes/section_lc/LEGITEXT000006070633/LEGISCTA000024405632/
                    "03",  # Guyane https://www.legifrance.gouv.fr/codes/section_lc/LEGITEXT000006070633/LEGISCTA000024405343/
                ]
            ):
                return "Collectivité territoriale unique"
            elif colter_code_insee == "94":
                # Corse https://www.legifrance.gouv.fr/codes/article_lc/LEGIARTI000033463146
                return "Collectivité de Corse"
            elif (
                colter_code_insee
                in [
                    "01",  # Guadeloupe https://www.legifrance.gouv.fr/codes/article_lc/LEGIARTI000024413373
                    "04",  # La Réunion https://www.legifrance.gouv.fr/codes/article_lc/LEGIARTI000024413373
                ]
            ):
                return "Région d'outre-mer"
            return "Région"

        df_colter = (
            pd.read_csv(
                self.config.files_to_download["colter_regions"]["destination"],
                dtype="string",
                sep=";",
                usecols=[
                    "Code Insee 2024 Région",
                    "Code Siren Collectivité",
                    "Exercice",
                ],
            )
            .loc[lambda df: df["Exercice"] == df["Exercice"].max()]
            .rename(
                columns={
                    "Code Insee 2024 Région": "colter_code_insee",
                    "Code Siren Collectivité": "siren",
                }
            )
            .filter(["colter_code_insee", "siren"])
            .drop_duplicates(keep="first")
            .assign(
                colter_code=lambda df: df["colter_code_insee"],
                colter_niveau=lambda df: df["colter_code_insee"].apply(
                    regions_colter_niveau
                ),
            )
            .sort_values(by=["colter_code"], ascending=False)
        )

        ### DÉPARTEMENTS ###
        def departements_colter_code(colter_code_insee):
            if colter_code_insee == "691":
                # Métropole de Lyon https://www.legifrance.gouv.fr/codes/section_lc/LEGITEXT000006070633/LEGISCTA000028528959/
                return "69M"
            elif colter_code_insee == "67A":
                # Collectivité Européenne d'Alsace https://www.legifrance.gouv.fr/jorf/id/JORFTEXT000038872957
                return "6AE"
            elif colter_code_insee == "75":
                # Paris https://www.legifrance.gouv.fr/codes/section_lc/LEGITEXT000006070633/LEGISCTA000006164590/
                return "75C"
            return colter_code_insee + "D"

        def departements_colter_niveau(colter_code_insee):
            if colter_code_insee == "976":
                # Mayotte https://www.legifrance.gouv.fr/codes/section_lc/LEGITEXT000006070633/LEGISCTA000006135501/
                return "Département de Mayotte"
            elif colter_code_insee in ["974", "971"]:
                # Guadeloupe et La Réunion https://www.legifrance.gouv.fr/codes/section_lc/LEGITEXT000006070633/LEGISCTA000006149273/
                return "Département d'outre-mer"
            elif colter_code_insee == "75":
                return "Ville de Paris"
            elif colter_code_insee == "691":
                return "Métropole de Lyon"
            elif colter_code_insee == "67A":
                return "Collectivité Européenne d'Alsace"
            return "Département"

        def departements_colter_code_insee(colter_code_insee):
            if colter_code_insee in ["67A", "69", "691"]:
                return None
            return colter_code_insee

        df_deps = (
            pd.read_csv(
                self.config.files_to_download["colter_deps"]["destination"],
                dtype="string",
                sep=";",
                usecols=[
                    "Code Insee 2024 Département",
                    "Code Siren Collectivité",
                    "Exercice",
                ],
            )
            .loc[lambda df: df["Exercice"] == df["Exercice"].max()]
            .rename(
                columns={
                    "Code Insee 2024 Département": "colter_code_insee",
                    "Code Siren Collectivité": "siren",
                }
            )
            .filter(["colter_code_insee", "siren"])
            .drop_duplicates(keep="first")
            .assign(
                colter_code=lambda df: df["colter_code_insee"].apply(
                    departements_colter_code
                ),
                colter_niveau=lambda df: df["colter_code_insee"].apply(
                    departements_colter_niveau
                ),
                colter_code_insee=lambda df: df["colter_code_insee"].apply(
                    departements_colter_code_insee
                ),
            )
            .sort_values(by=["colter_code_insee"])
        )

        df_colter = pd.concat([df_colter, df_deps])
        del df_deps

        ### EPCI ###
        df_epci = (
            pd.read_excel(
                self.config.files_to_download["colter_epci"]["destination"],
                dtype="string",
                engine="openpyxl",
                usecols=["siren_epci"],
            )
            .rename(columns={"siren_epci": "siren"})
            .assign(
                colter_code_insee=np.nan,
                colter_niveau="EPCI",
                colter_code=lambda df: df["siren"],
            )
            .query(
                "colter_code != '200046977'"
            )  # Remove Métropole de Lyon, already in Départements
        )
        df_colter = pd.concat([df_colter, df_epci])
        del df_epci

        ### COMMUNES ###
        df_communes = (
            pd.read_csv(
                self.config.files_to_download["colter_communes"]["destination"],
                dtype="string",
                sep=";",
                usecols=["siren", "insee"],
                encoding="latin1",
            )
            .rename(
                columns={
                    "siren_membre": "siren",
                    "insee": "colter_code_insee",
                }
            )
            .query(
                # Paris https://www.legifrance.gouv.fr/codes/section_lc/LEGITEXT000006070633/LEGISCTA000006164590/
                # Removed, already included in Départements
                "colter_code_insee != '75056'"
            )
            .assign(
                colter_code=lambda df: df["colter_code_insee"],
                colter_niveau="Commune",
            )
        )

        df_colter = pd.concat([df_colter, df_communes])
        del df_communes

        df_colter.to_csv(self.config.file_output, index=False)

    def data_validation(self) -> None:
        if not self.config.file_output:
            raise ValueError("Output file not specified in config.")

        df_colter = pd.read_csv(self.config.file_output, dtype="string")
        validations = {}

        # Validate volume thresholds
        niveau_thresholds = {
            "Région": 12,
            "Département": 91,
            "Commune": 30_000,
            "EPCI": 1_000,
        }
        for niveau, threshold in niveau_thresholds.items():
            count = df_colter[df_colter["colter_niveau"] == niveau].shape[0]
            validations[niveau] = count >= threshold
            logging.info(
                f"Valid? {validations[niveau]}: {niveau}: {count} entries, Threshold: {threshold}"
            )

        # Validate special cases
        special_code_insee = {
            "Collectivité territoriale unique": ["02", "03"],
            "Collectivité de Corse": ["94"],
            "Région d'outre-mer": ["01", "04"],
            "Département de Mayotte": ["976D"],
            "Département d'outre-mer": ["974D", "971D"],
            "Ville de Paris": ["75C"],
            "Métropole de Lyon": ["69M"],
            "Collectivité Européenne d'Alsace": ["6AE"],
        }
        for niveau, expected_codes in special_code_insee.items():
            actual_codes = list(
                df_colter[df_colter["colter_niveau"] == niveau]["colter_code"].unique()
            )
            validations[niveau] = set(expected_codes).issubset(set(actual_codes))
            logging.info(
                f"Valid? {validations[niveau]}: {niveau}: Expected {expected_codes}, Found {actual_codes}"
            )

        # Validate for uniqueness (ignoring nulls)
        columns_to_validate = ["siren", "colter_code"]
        for column in columns_to_validate:
            duplicates = list(
                df_colter[column][df_colter[column].dropna().duplicated(keep=False)]
            )
            validations[column] = duplicates == []
            logging.info(
                f"Valid? {duplicates == []}: {column} has {duplicates} duplicated elements."
            )

        # Overall validation result
        overall_validation = all(validations.values())
        logging.info(f"Overall validation? {overall_validation}")
        if not overall_validation:
            raise ValueError("Data validation failed.")


class ElusProcessor(DataProcessor):
    def __init__(self):
        super().__init__(ELUS_CONFIG)

    def preprocess_data(self):
        if not COLTER_CONFIG.url_minio:
            raise ValueError("MinIO file not specified in COLTER_CONFIG.")

        df_colter = pd.read_csv(COLTER_CONFIG.url_minio, dtype=str)
        # Conseillers régionaux
        elus = self.process_elus_files(
            self.config.files_to_download["conseillers_regionaux"]["url"],
            "Code de la région",
        )

        # Conseillers départementaux
        df_elus_deps = self.process_elus_files(
            self.config.files_to_download["conseillers_departementaux"]["url"],
            "Code du département",
        )
        df_elus_deps["colter_code"] = df_elus_deps["colter_code"] + "D"
        df_elus_deps.loc[df_elus_deps["colter_code"] == "6AED", "colter_code"] = "6AE"
        elus = pd.concat([elus, df_elus_deps])

        # membres des assemblées des collectivités à statut particulier
        df_elus_part = self.process_elus_files(
            self.config.files_to_download["assemblee_col_statut_particulier"]["url"],
            "Code de la collectivité à statut particulier",
        )
        df_elus_part.loc[df_elus_part["colter_code"] == "972", "colter_code"] = "02"
        df_elus_part.loc[df_elus_part["colter_code"] == "973", "colter_code"] = "03"
        elus = pd.concat([elus, df_elus_part])

        # Conseillers communautaires
        df_elus_epci = self.process_elus_files(
            self.config.files_to_download["elus_epci"]["url"],
            "N° SIREN",
        )
        elus = pd.concat([elus, df_elus_epci])

        # Conseillers municipaux
        df_elus_epci = self.process_elus_files(
            self.config.files_to_download["conseillers_municipaux"]["url"],
            "Code de la commune",
        )
        df_elus_epci.loc[df_elus_epci["colter_code"] == "75056", "colter_code"] = "75C"
        elus = pd.concat([elus, df_elus_epci])
        df_colter_elus = elus.merge(df_colter, on="colter_code", how="left")
        df_colter_elus = df_colter_elus[df_colter_elus["siren"].notna()]
        df_colter_elus = df_colter_elus[
            [
                "siren",
                "nom_elu",
                "prenom_elu",
                "date_naissance_elu",
                "sexe_elu",
                "fonction_elu",
            ]
        ]
        for col in df_colter_elus.columns:
            df_colter_elus = df_colter_elus.rename(
                columns={col: col.replace("_elu", "")}
            )

        df_colter_elus.drop_duplicates(inplace=True, ignore_index=True)
        df_colter_elus.to_csv(self.config.file_output, index=False)

    def process_elus_files(self, url, colname):
        df_elus = pd.read_csv(url, dtype=str, sep=";")
        df_elus.rename(columns=lambda x: x.replace("’", "'"), inplace=True)

        column_mapping = {
            "Nom de l'élu": "nom_elu",
            "Prénom de l'élu": "prenom_elu",
            "Code sexe": "sexe_elu",
            "Date de naissance": "date_naissance_elu",
            "Libellé de la fonction": "libelle_fonction",
        }

        common_columns = [
            colname,
            "nom_elu",
            "prenom_elu",
            "sexe_elu",
            "date_naissance_elu",
            "libelle_fonction",
        ]

        df_elus = df_elus.rename(columns=column_mapping)[common_columns]
        df_elus = df_elus.rename(
            columns={colname: "colter_code", "libelle_fonction": "fonction_elu"}
        )

        return df_elus
