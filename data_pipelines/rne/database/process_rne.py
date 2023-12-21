import logging
import json
from dag_datalake_sirene.data_pipelines.rne.database.db_connexion import (
    connect_to_db,
)
from dag_datalake_sirene.data_pipelines.rne.database.rne_model import (
    RNECompany,
)


def create_tables(cursor):
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS dirigeants_pp
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
        CREATE TABLE IF NOT EXISTS dirigeants_pm
        (
            siren TEXT,
            date_mise_a_jour TEXT,
            denomination TEXT,
            siren_dirigeant TEXT,
            role TEXT,
            forme_juridique TEXT,
            pays TEXT,
            file_name TEXT
        )
    """
    )
    create_index_db(cursor)


def create_index_db(cursor):
    index_statements = [
        "CREATE INDEX IF NOT EXISTS idx_siren_pp ON dirigeants_pp (siren);",
        "CREATE INDEX IF NOT EXISTS idx_siren_pm ON dirigeants_pm (siren);",
        "CREATE INDEX IF NOT EXISTS file_pp ON dirigeants_pp (file_name);",
        "CREATE INDEX IF NOT EXISTS file_pm ON dirigeants_pm (file_name);",
        """CREATE INDEX IF NOT EXISTS idx_pp_siren_file_name
        ON dirigeants_pp (siren, file_name);""",
        """CREATE INDEX IF NOT EXISTS idx_pm_siren_file_name
        ON dirigeants_pm (siren, file_name);""",
    ]

    for statement in index_statements:
        cursor.execute(statement)


def inject_records_into_db(file_path, db_path, file_type):
    dirigeants_pp, dirigeants_pm = [], []

    with open(file_path, "r") as file:
        logging.info(f"Injecting records from file: {file_path}")
        try:
            if file_type == "stock":
                json_data = file.read()
                data = json.loads(json_data)
                dirigeants_pp, dirigeants_pm = process_records_to_extract_dirig(
                    data, file_type
                )
            elif file_type == "flux":
                for line in file:
                    data = json.loads(line)
                    (
                        dirigeants_pp_temp,
                        dirigeants_pm_temp,
                    ) = process_records_to_extract_dirig(data, file_type)
                    dirigeants_pp += dirigeants_pp_temp
                    dirigeants_pm += dirigeants_pm_temp
        except json.JSONDecodeError as e:
            raise Exception(f"JSONDecodeError: {e} in file {file_path}")
        insert_dirigeants_into_db(dirigeants_pp, dirigeants_pm, file_path, db_path)


def process_records_to_extract_dirig(data, file_type):
    dirigeants_pp, dirigeants_pm = [], []
    for record in data:
        list_dirigeants_pp, list_dirigeants_pm = extract_dirigeants_data(
            record, file_type
        )
        dirigeants_pp = dirigeants_pp + list_dirigeants_pp
        dirigeants_pm = dirigeants_pm + list_dirigeants_pm
    return dirigeants_pp, dirigeants_pm


def find_and_delete_same_siren(cursor, siren, file_path):
    """
    Find and delete older rows with the same SIREN as they are outdated.

    Args:
        cursor: The database cursor.
        siren (str): The SIREN to search for and delete.
        file_path (str): The file path to filter the rows.

    """
    cursor.execute(
        "SELECT COUNT(*) FROM dirigeants_pp WHERE siren = ? AND file_name != ?",
        (siren, file_path),
    )
    count_already_existing_siren = cursor.fetchone()[0]
    # If existing rows are found, delete them as they are outdated
    if count_already_existing_siren is not None:
        cursor.execute(
            "DELETE FROM dirigeants_pp WHERE siren = ? AND file_name != ?",
            (siren, file_path),
        )


def insert_dirigeants_into_db(
    list_dirigeants_pp, list_dirigeants_pm, file_path, db_path
):
    connection, cursor = connect_to_db(db_path)

    for dirigeant_pp in list_dirigeants_pp:
        find_and_delete_same_siren(cursor, dirigeant_pp["siren"], file_path)
        cursor.execute(
            """
            INSERT INTO dirigeants_pp (siren,
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

    for dirigeant_pm in list_dirigeants_pm:
        find_and_delete_same_siren(cursor, dirigeant_pm["siren"], file_path)
        cursor.execute(
            """
            INSERT INTO dirigeants_pm
            (siren, date_mise_a_jour, denomination, siren_dirigeant, role,
                                        forme_juridique, pays, file_name)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
            (
                dirigeant_pm["siren"],
                dirigeant_pm["date_mise_a_jour"],
                dirigeant_pm["denomination"],
                dirigeant_pm["siren_dirigeant"],
                dirigeant_pm["role"],
                dirigeant_pm["forme_juridique"],
                dirigeant_pm["pays"],
                file_path,
            ),
        )
    cursor.execute("SELECT COUNT(*) FROM dirigeants_pp")
    count_pp = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM dirigeants_pm")
    count_pm = cursor.fetchone()[0]

    logging.info(f"************Count pp: {count_pp}, Count pm: {count_pm}")

    connection.commit()
    connection.close()


def get_tables_count(db_path):
    connection, cursor = connect_to_db(db_path)
    cursor.execute("SELECT COUNT(*) FROM dirigeants_pp")
    count_pp = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM dirigeants_pm")
    count_pm = cursor.fetchone()[0]

    connection.close()
    return count_pp, count_pm


def extract_dirigeants_data(entity, file_type="flux"):
    """Extract and format "dirigeants" by type."""
    list_dirigeants_pp = []
    list_dirigeants_pm = []

    company = entity.get("company", {}) if file_type == "flux" else entity

    rne_company = RNECompany.model_validate(company)
    siren = rne_company.siren
    date_maj = rne_company.updatedAt
    dirigeants = rne_company.dirigeants

    for dirigeant in dirigeants:
        # Cas personne morale et exploitation
        if hasattr(dirigeant, "typeDePersonne"):
            if dirigeant.typeDePersonne == "INDIVIDU":
                list_dirigeants_pp.append(
                    format_dirigeant_pp_fields(dirigeant.individu, siren, date_maj)
                )

            elif dirigeant.typeDePersonne == "ENTREPRISE":
                list_dirigeants_pm.append(
                    format_dirigeant_pm_fields(dirigeant.entreprise, siren, date_maj)
                )
        else:
            # Cas personne physique
            list_dirigeants_pp.append(
                format_dirigeant_pp_fields(dirigeant, siren, date_maj)
            )

    return list_dirigeants_pp, list_dirigeants_pm


def format_dirigeant_pp_fields(dirigeant, siren, date_maj):
    individu = dirigeant.descriptionPersonne
    adresse_domicile = dirigeant.adresseDomicile
    dirigeant_formated = {
        "siren": siren,
        "date_mise_a_jour": date_maj,
        "date_de_naissance": individu.dateDeNaissance,
        "role": individu.role,
        "nom": individu.nom,
        "nom_usage": individu.nomUsage,
        "prenoms": " ".join(individu.prenoms)
        if isinstance(individu.prenoms, list)
        else individu.prenoms,
        "genre": individu.genre,
        "nationalite": individu.nationalite,
        "situation_matrimoniale": individu.situationMatrimoniale,
        "pays": adresse_domicile.pays,
        "code_pays": adresse_domicile.codePays,
        "code_postal": adresse_domicile.codePostal,
        "commune": adresse_domicile.commune,
        "code_insee_commune": adresse_domicile.codeInseeCommune,
        "voie": adresse_domicile.voie,
    }
    return dirigeant_formated


def format_dirigeant_pm_fields(dirigeant, siren, date_maj):
    dirigeant_formated = {
        "siren": siren,
        "date_mise_a_jour": date_maj,
        "denomination": dirigeant.denomination,
        "siren_dirigeant": dirigeant.siren,
        "role": dirigeant.role,
        "forme_juridique": dirigeant.formeJuridique,
        "pays": dirigeant.pays,
    }
    return dirigeant_formated
