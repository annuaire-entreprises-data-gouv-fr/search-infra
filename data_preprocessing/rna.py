import pandas as pd
import requests
import zipfile
import os
import logging


def preprocess_rna(data_dir):
    zip_url = (
        "https://www.data.gouv.fr/fr/datasets/r/e0c09ef1-c704-4171-a860-841945f26cb9"
    )
    destination_folder = f"{data_dir}rna-unzipped"

    # Step 1: Download the zip file
    response = requests.get(zip_url)
    if response.status_code == 200:
        with open(f"{data_dir}data-rna.zip", "wb") as f:
            f.write(response.content)
        logging.info("RNA zip file downloaded successfully.")
    else:
        logging.error("Failed to download RNA zip file.")

    # Step 2: Extract the zip file
    with zipfile.ZipFile(f"{data_dir}data-rna.zip", "r") as zip_ref:
        zip_ref.extractall(destination_folder)
        logging.info("RNA zip file extracted successfully.")

    # Step 3: Read CSV files from the extracted folder
    rna_csv_files = [
        file for file in os.listdir(destination_folder) if file.endswith(".csv")
    ]

    columns = [
        "id",
        "siret",
        "date_creat",
        "titre",
        "adrs_complement",
        "adrs_numvoie",
        "adrs_repetition",
        "adrs_typevoie",
        "adrs_libvoie",
        "adrs_distrib",
        "adrs_codeinsee",
        "adrs_codepostal",
        "adrs_libcommune",
    ]

    rna_dataframes = []
    for rna_csv_file in rna_csv_files:
        csv_path = os.path.join(destination_folder, rna_csv_file)
        df_rna = pd.read_csv(
            csv_path,
            sep=";",
            error_bad_lines=False,
            encoding="ISO-8859-1",
            header=0,
            low_memory=False,
            usecols=columns,
            dtype="str",
        )
        rna_dataframes.append(df_rna)

    # Merge the list of DataFrames into a single DataFrame
    merged_rna_df = pd.concat(rna_dataframes, ignore_index=True)

    merged_rna_df = merged_rna_df.rename(
        columns={
            "id": "identifiant_association",
            "date_creat": "date_creation",
            "adrs_numvoie": "numero_voie",
            "adrs_typevoie": "type_voie",
            "adrs_libvoie": "libelle_voie",
            "adrs_codepostal": "code_postal",
            "adrs_libcommune": "libelle_commune",
            "adrs_codeinsee": "commune",
            "adrs_complement": "complement_adresse",
            "adrs_repetition": "indice_repetition",
            "adrs_distrib": "distribution_speciale",
        }
    )
    # merged_rna_df["siren"] = merged_rna_df["siret"].str[:9]
    # merged_rna_df["siren"] = merged_rna_df["identifiant_association"]
    # merged_rna_df["est_siege"] = "true"

    del rna_dataframes

    return merged_rna_df
