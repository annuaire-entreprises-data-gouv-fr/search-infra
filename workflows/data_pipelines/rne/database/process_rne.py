import logging
import json
from dag_datalake_sirene.workflows.data_pipelines.rne.database.db_connexion import (
    connect_to_db,
)
from dag_datalake_sirene.workflows.data_pipelines.rne.database.rne_model import (
    RNECompany,
)


def create_tables(cursor):
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS unites_legales
        (
            siren TEXT,
            nom_complet TEXT,
            date_mise_a_jour_unite_legale TEXT,
            adresse_complete TEXT,
            forme_juridique TEXT,
            statut_diffusion_unite_legale TEXT,
            nature_juridique_unite_legale TEXT,
            file_name TEXT
        )
    """
    )
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
        "CREATE INDEX IF NOT EXISTS idx_siren_ul ON unites_legales (siren);",
        "CREATE INDEX IF NOT EXISTS idx_siren_pp ON dirigeants_pp (siren);",
        "CREATE INDEX IF NOT EXISTS idx_siren_pm ON dirigeants_pm (siren);",
        "CREATE INDEX IF NOT EXISTS file_ul ON unites_legales (file_name);",
        "CREATE INDEX IF NOT EXISTS file_pp ON dirigeants_pp (file_name);",
        "CREATE INDEX IF NOT EXISTS file_pm ON dirigeants_pm (file_name);",
        """CREATE INDEX IF NOT EXISTS idx_ul_siren_file_name
        ON unites_legales (siren, file_name);""",
        """CREATE INDEX IF NOT EXISTS idx_pp_siren_file_name
        ON dirigeants_pp (siren, file_name);""",
        """CREATE INDEX IF NOT EXISTS idx_pm_siren_file_name
        ON dirigeants_pm (siren, file_name);""",
    ]

    for statement in index_statements:
        cursor.execute(statement)


def inject_records_into_db(file_path, db_path, file_type):
    dirigeants_pp, dirigeants_pm = [], []
    unites_legales = []

    with open(file_path, "r") as file:
        logging.info(f"Injecting records from file: {file_path}")
        try:
            if file_type == "stock":
                json_data = file.read()
                data = json.loads(json_data)
                (
                    unites_legales,
                    dirigeants_pp,
                    dirigeants_pm,
                ) = process_records_to_extract_rne_data(data, file_type)
            elif file_type == "flux":
                for line in file:
                    data = json.loads(line)
                    (
                        unites_legales_temp,
                        dirigeants_pp_temp,
                        dirigeants_pm_temp,
                    ) = process_records_to_extract_rne_data(data, file_type)
                    dirigeants_pp += dirigeants_pp_temp
                    dirigeants_pm += dirigeants_pm_temp
                    unites_legales += unites_legales_temp
        except json.JSONDecodeError as e:
            raise Exception(f"JSONDecodeError: {e} in file {file_path}")
        insert_dirigeants_into_db(
            unites_legales, dirigeants_pp, dirigeants_pm, file_path, db_path
        )


def process_records_to_extract_rne_data(data, file_type):
    dirigeants_pp, dirigeants_pm = [], []
    unites_legales = []
    for record in data:
        unite_legale, list_dirigeants_pp, list_dirigeants_pm = extract_rne_data(
            record, file_type
        )
        unites_legales.append(unite_legale)
        dirigeants_pp = dirigeants_pp + list_dirigeants_pp
        dirigeants_pm = dirigeants_pm + list_dirigeants_pm
    return unites_legales, dirigeants_pp, dirigeants_pm


def find_and_delete_same_siren_dirig(cursor, siren, file_path):
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


def find_and_delete_same_siren(cursor, siren, file_path):
    """
    Find and delete older rows with the same SIREN as they are outdated.

    Args:
        cursor: The database cursor.
        siren (str): The SIREN to search for and delete.
        file_path (str): The file path to filter the rows.

    """
    cursor.execute(
        "SELECT COUNT(*) FROM unites_legales WHERE siren = ? AND file_name != ?",
        (siren, file_path),
    )
    count_already_existing_siren = cursor.fetchone()[0]
    # If existing rows are found, delete them as they are outdated
    if count_already_existing_siren is not None:
        cursor.execute(
            "DELETE FROM unites_legales WHERE siren = ? AND file_name != ?",
            (siren, file_path),
        )


def insert_dirigeants_into_db(
    list_unites_legales, list_dirigeants_pp, list_dirigeants_pm, file_path, db_path
):
    connection, cursor = connect_to_db(db_path)

    for unite_legale in list_unites_legales:
        find_and_delete_same_siren(cursor, unite_legale["siren"], file_path)

        cursor.execute(
            """
            INSERT INTO unites_legales (siren,
            nom_complet, date_mise_a_jour_unite_legale,
            adresse_complete, forme_juridique,
            statut_diffusion_unite_legale,
            nature_juridique_unite_legale, file_name)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
            (
                unite_legale["siren"],
                unite_legale["nom_complet"],
                unite_legale["date_mise_a_jour_unite_legale"],
                unite_legale["adresse_complete"],
                unite_legale["forme_juridique"],
                unite_legale["statut_diffusion_unite_legale"],
                unite_legale["nature_juridique_unite_legale"],
                file_path,
            ),
        )

    for dirigeant_pp in list_dirigeants_pp:
        find_and_delete_same_siren_dirig(cursor, dirigeant_pp["siren"], file_path)
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
        find_and_delete_same_siren_dirig(cursor, dirigeant_pm["siren"], file_path)
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

    cursor.execute("SELECT COUNT(*) FROM unites_legales")
    count_ul = cursor.fetchone()[0]

    logging.info(
        f"************Count UL: {count_ul}, "
        f"Count pp: {count_pp}, Count pm: {count_pm}"
    )

    cursor.execute("SELECT * FROM unites_legales ORDER BY rowid DESC LIMIT 1")
    first_record = cursor.fetchone()

    # Print the first record
    if first_record:
        logging.info(f"///////First Record: {first_record}")
    connection.commit()
    connection.close()


def get_tables_count(db_path):
    connection, cursor = connect_to_db(db_path)

    cursor.execute("SELECT COUNT(*) FROM unites_legales")
    count_ul = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM dirigeants_pp")
    count_pp = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM dirigeants_pm")
    count_pm = cursor.fetchone()[0]

    connection.close()
    return count_ul, count_pp, count_pm


def extract_rne_data(entity, file_type="flux"):
    """Extract and format "dirigeants" and "unites legales"
    data from RNE json object."""
    list_dirigeants_pp = []
    list_dirigeants_pm = []

    company = entity.get("company", {}) if file_type == "flux" else entity

    rne_company = RNECompany.model_validate(company)

    rne_company_dict = format_rne_company_dict(rne_company)

    for dirigeant in rne_company_dict["dirigeants"]:
        # Cas personne morale et exploitation
        if hasattr(dirigeant, "typeDePersonne"):
            if dirigeant.typeDePersonne == "INDIVIDU":
                list_dirigeants_pp.append(
                    format_dirigeant_pp_fields(
                        dirigeant.individu,
                        rne_company_dict["siren"],
                        rne_company_dict["date_mise_a_jour_unite_legale"],
                    )
                )

            elif dirigeant.typeDePersonne == "ENTREPRISE":
                list_dirigeants_pm.append(
                    format_dirigeant_pm_fields(
                        dirigeant.entreprise,
                        rne_company_dict["siren"],
                        rne_company_dict["date_mise_a_jour_unite_legale"],
                    )
                )
        else:
            # Cas personne physique
            list_dirigeants_pp.append(
                format_dirigeant_pp_fields(
                    dirigeant,
                    rne_company_dict["siren"],
                    rne_company_dict["date_mise_a_jour_unite_legale"],
                )
            )
    rne_company_dict.pop("dirigeants", None)

    if rne_company_dict["siren"] == "817883150":
        logging.info(f"%%%%%%%%%%%%%%%%%%%%%%******* Ganymede : {rne_company_dict}")
    return rne_company_dict, list_dirigeants_pp, list_dirigeants_pm


def format_rne_company_dict(rne_company):
    """Format RNECompany data into a dictionary."""
    return {
        "siren": rne_company.siren,
        "dirigeants": rne_company.dirigeants,
        "nom_complet": rne_company.formality.companyName,
        "date_mise_a_jour_unite_legale": rne_company.updatedAt,
        "adresse_complete": rne_company.adresse,
        "forme_juridique": rne_company.formality.formeJuridique,
        "statut_diffusion_unite_legale": rne_company.formality.diffusionINSEE,
        "nature_juridique_unite_legale": rne_company.formality.codeAPE,
    }


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
