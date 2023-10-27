import logging
import sqlite3
from dag_datalake_sirene.config import (
    AIRFLOW_DAG_TMP,
)

TMP_FOLDER = f"{AIRFLOW_DAG_TMP}rne/database/"


def insert_record(list_dirigeants_pp, list_dirigeants_pm, file_path, db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM rep_pp")
    count_pp = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM rep_pm")
    count_pm = cursor.fetchone()[0]

    cursor.execute("SELECT * FROM rep_pp LIMIT 1")
    cursor.fetchone()

    cursor.execute("SELECT * FROM rep_pm LIMIT 1")
    cursor.fetchone()

    for dirigeant_pp in list_dirigeants_pp:
        # Check if the record with the same siren exists and has a different file name
        cursor.execute(
            "SELECT * FROM rep_pp WHERE siren = ? AND file_name != ?",
            (dirigeant_pp["siren"], file_path),
        )
        existing_record = cursor.fetchone()

        if existing_record:
            # Record with the same siren and different file exists, so skip insertion
            logging.info(
                f"Skipping record with siren {dirigeant_pp['siren']} "
                f"as it already exists with a different file "
                f"{file_path}*****{existing_record}."
            )
            continue

        logging.info(f"++++++++++++++++++ Counts records dirigeants PP: {count_pp}")
        logging.info(f"{dirigeant_pp['siren']}")

        # If the record doesn't exist, insert it
        cursor.execute(
            """
            INSERT INTO rep_pp (siren,
            date_mise_a_jour, date_de_naissance,
            role, nom,
                                        nom_usage,
                                        prenoms, genre, nationalite,
                                        situation_matrimoniale,
                                        pays, code_pays, code_postal,
                                        commune, code_insee_commune, voie, file_name)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            (
                dirigeant_pp["siren"],
                dirigeant_pp["date_mise_a_jour"],
                dirigeant_pp["date_de_naissance"],
                dirigeant_pp["role"],
                dirigeant_pp["nom"],
                dirigeant_pp["nom_usage"],
                dirigeant_pp["prenoms"],
                dirigeant_pp["genre"],
                dirigeant_pp["nationalite"],
                dirigeant_pp["situation_matrimoniale"],
                dirigeant_pp["pays"],
                dirigeant_pp["code_pays"],
                dirigeant_pp["code_postal"],
                dirigeant_pp["commune"],
                dirigeant_pp["code_insee_commune"],
                dirigeant_pp["voie"],
                file_path,
            ),
        )
        conn.commit()

    for dirigeant_pm in list_dirigeants_pm:
        # Check if the record with the same siren exists and has a different file name
        cursor.execute(
            "SELECT * FROM rep_pm WHERE siren = ? AND file_name != ?",
            (dirigeant_pm["siren"], file_path),
        )
        existing_record = cursor.fetchone()

        if existing_record:
            # Record with the same siren and different file exists, so skip insertion
            logging.info(
                f"Skipping record with siren {dirigeant_pm['siren']} "
                f"as it already exists with a different file "
                f"{file_path}*****{existing_record}."
            )
            continue

        logging.info(f"++++++++++++++++++ Counts records dirigeants PM: {count_pm}")
        logging.info(f"{dirigeant_pm['siren']}")
        # If the record doesn't exist, insert it
        cursor.execute(
            """
            INSERT INTO rep_pm
            (siren, date_mise_a_jour, denomination, siren_dirigeant, role,
                                        forme_juridique, pays, code_pays, code_postal,
                                        commune, code_insee_commune, voie, file_name)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            (
                dirigeant_pm["siren"],
                dirigeant_pm["date_mise_a_jour"],
                dirigeant_pm["denomination"],
                dirigeant_pm["siren_dirigeant"],
                dirigeant_pm["role"],
                dirigeant_pm["forme_juridique"],
                dirigeant_pm["pays"],
                dirigeant_pm["code_pays"],
                dirigeant_pm["code_postal"],
                dirigeant_pm["commune"],
                dirigeant_pm["code_insee_commune"],
                dirigeant_pm["voie"],
                file_path,
            ),
        )
        conn.commit()
    conn.close()


def get_company_data_from_stock(entity):
    siren = entity.get("siren")
    date_maj = entity.get("updatedAt")
    dirigeants = (
        entity.get("formality", {})
        .get("content", {})
        .get("personneMorale", {})
        .get("composition", {})
        .get("pouvoirs", [])
    )
    return siren, date_maj, dirigeants


def get_company_data_from_flux(entity):
    siren = entity.get("company", {}).get("siren")
    date_maj = entity.get("company", {}).get("updatedAt")
    dirigeants = (
        entity.get("company", {})
        .get("formality", {})
        .get("content", {})
        .get("exploitation", {})
        .get("composition", {})
        .get("pouvoirs", [])
    )
    return siren, date_maj, dirigeants


def extract_dirigeants_data(entity, file_type="flux"):
    """Extract and categorize "dirigeants" by type."""
    list_dirigeants_pp = []
    list_dirigeants_pm = []
    if file_type == "flux":
        siren, date_maj, dirigeants = get_company_data_from_flux(entity)

    elif file_type == "stock":
        siren, date_maj, dirigeants = get_company_data_from_stock(entity)

    for dirigeant in dirigeants:
        type_de_personne = dirigeant.get("typeDePersonne", None)

        if type_de_personne == "INDIVIDU":
            individu = dirigeant.get("individu", {}).get("descriptionPersonne", {})
            adresse_domicile = dirigeant.get("individu", {}).get("adresseDomicile", {})

            dirigeant_pp = {
                "siren": siren,
                "date_mise_a_jour": date_maj,
                "date_de_naissance": individu.get("dateDeNaissance", None),
                "role": individu.get("role", None),
                "nom": individu.get("nom", None),
                "nom_usage": individu.get("nomUsage", None),
                "prenoms": individu.get("prenoms", None),
                "genre": individu.get("genre", None),
                "nationalite": individu.get("nationalite", None),
                "situation_matrimoniale": individu.get("situationMatrimoniale", None),
                "pays": adresse_domicile.get("pays", None),
                "code_pays": adresse_domicile.get("codePays", None),
                "code_postal": adresse_domicile.get("codePostal", None),
                "commune": adresse_domicile.get("commune", None),
                "code_insee_commune": adresse_domicile.get("codeInseeCommune", None),
                "voie": adresse_domicile.get("voie", None),
            }
            dirigeant_pp["prenoms"] = " ".join(dirigeant_pp["prenoms"])

            list_dirigeants_pp.append(dirigeant_pp)

        elif type_de_personne == "ENTREPRISE":
            entreprise = dirigeant.get("entreprise", {})
            adresse_entreprise = dirigeant.get("adresseEntreprise", {})

            dirigeant_pm = {
                "siren": siren,
                "date_mise_a_jour": date_maj,
                "denomination": entreprise.get("denomination", None),
                "siren_dirigeant": entreprise.get("siren", None),
                "role": entreprise.get("roleEntreprise", None),
                "forme_juridique": entreprise.get("formeJuridique", None),
                "pays": adresse_entreprise.get("pays", None),
                "code_pays": adresse_entreprise.get("codePays", None),
                "code_postal": adresse_entreprise.get("codePostal", None),
                "commune": adresse_entreprise.get("commune", None),
                "code_insee_commune": adresse_entreprise.get("codeInseeCommune", None),
                "voie": adresse_entreprise.get("voie", None),
            }

            list_dirigeants_pm.append(dirigeant_pm)

    return list_dirigeants_pp, list_dirigeants_pm


def create_tables(cursor):
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS rep_pp
        (
            siren TEXT,
            date_mise_a_jour TEXT,
            date_de_naissance TEXT,
            role TEXT,
            nom TEXT,
            nom_usage TEXT,
            prenoms TEXT,
            genre TEXT,
            nationalite TEXT,
            situation_matrimoniale TEXT,
            pays TEXT,
            code_pays TEXT,
            code_postal TEXT,
            commune TEXT,
            code_insee_commune TEXT,
            voie TEXT,
            file_name TEXT
        )
    """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS rep_pm
        (
            siren TEXT,
            date_mise_a_jour TEXT,
            denomination TEXT,
            siren_dirigeant TEXT,
            role TEXT,
            forme_juridique TEXT,
            pays TEXT,
            code_pays TEXT,
            code_postal TEXT,
            commune TEXT,
            code_insee_commune TEXT,
            voie TEXT,
            file_name TEXT
        )
    """
    )
    create_index_db(cursor)


def create_index_db(cursor):
    cursor.execute("""CREATE INDEX idx_siren_pp ON rep_pp (siren);""")
    cursor.execute("""CREATE INDEX idx_siren_pm ON rep_pm (siren);""")
