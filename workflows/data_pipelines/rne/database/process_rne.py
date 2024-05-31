import logging
import json
from dag_datalake_sirene.workflows.data_pipelines.rne.database.db_connexion import (
    connect_to_db,
)
from dag_datalake_sirene.workflows.data_pipelines.rne.database.rne_model import (
    RNECompany,
)
from dag_datalake_sirene.workflows.data_pipelines.rne.database.ul_model import (
    UniteLegale,
)
from dag_datalake_sirene.workflows.data_pipelines.rne.database.map_rne import (
    map_rne_company_to_ul,
)


def create_tables(cursor):
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS unites_legales
        (
            siren TEXT,
            denomination TEXT,
            nom TEXT,
            nom_usage TEXT,
            prenom TEXT,
            nom_commercial TEXT,
            date_creation TEXT,
            date_mise_a_jour DATE,
            date_immatriculation DATE,
            date_radiation DATE,
            activite_principale TEXT,
            tranche_effectif_salarie TEXT,
            nature_juridique TEXT,
            etat_administratif TEXT,
            forme_exercice_activite_principale TEXT,
            statut_diffusion TEXT,
            adresse TEXT,
            file_name TEXT
        )
    """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS sieges
        (
            siren TEXT,
            siret TEXT,
            enseigne TEXT,
            nom_commercial TEXT,
            pays TEXT,
            code_pays TEXT,
            commune TEXT,
            code_postal TEXT,
            code_commune TEXT,
            voie TEXT,
            num_voie TEXT,
            type_voie TEXT,
            indice_repetition TEXT,
            complement_localisation TEXT,
            distribution_speciale TEXT,
            date_mise_a_jour DATE,
            file_name TEXT
        )
    """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS dirigeants_pp
        (
            siren TEXT,
            date_mise_a_jour DATE,
            nom TEXT,
            nom_usage TEXT,
            prenoms TEXT,
            genre TEXT,
            date_de_naissance TEXT,
            role TEXT,
            nationalite TEXT,
            situation_matrimoniale TEXT,
            file_name TEXT
        )
    """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS dirigeants_pm
        (
            siren TEXT,
            siren_dirigeant TEXT,
            date_mise_a_jour DATE,
            denomination TEXT,
            role TEXT,
            pays TEXT,
            forme_juridique TEXT,
            file_name TEXT
        )
    """
    )
    create_index_db(cursor)


def create_index_db(cursor):
    index_statements = [
        "CREATE INDEX IF NOT EXISTS idx_siren_ul ON unites_legales (siren);",
        "CREATE INDEX IF NOT EXISTS idx_siren_etab ON sieges (siren);",
        "CREATE INDEX IF NOT EXISTS idx_siret_etab ON sieges (siret);",
        "CREATE INDEX IF NOT EXISTS idx_siren_pp ON dirigeants_pp (siren);",
        "CREATE INDEX IF NOT EXISTS idx_siren_pm ON dirigeants_pm (siren);",
        "CREATE INDEX IF NOT EXISTS file_ul ON unites_legales (file_name);",
        "CREATE INDEX IF NOT EXISTS file_etab ON sieges (file_name);",
        "CREATE INDEX IF NOT EXISTS file_pp ON dirigeants_pp (file_name);",
        "CREATE INDEX IF NOT EXISTS file_pm ON dirigeants_pm (file_name);",
        """CREATE INDEX IF NOT EXISTS idx_ul_siren_file_name
        ON unites_legales (siren, file_name);""",
        """CREATE INDEX IF NOT EXISTS idx_etab_siren_file_name
        ON sieges (siren, file_name);""",
        """CREATE INDEX IF NOT EXISTS idx_etab_siret_file_name
        ON sieges (siret, file_name);""",
        """CREATE INDEX IF NOT EXISTS idx_pp_siren_file_name
        ON dirigeants_pp (siren, file_name);""",
        """CREATE INDEX IF NOT EXISTS idx_pm_siren_file_name
        ON dirigeants_pm (siren, file_name);""",
    ]

    for statement in index_statements:
        cursor.execute(statement)


def inject_records_into_db(file_path, db_path, file_type):
    unites_legales = []

    with open(file_path, "r") as file:
        logging.info(f"Injecting records from file: {file_path}")
        try:
            if file_type == "stock":
                json_data = file.read()
                data = json.loads(json_data)
                unites_legales = process_records_to_extract_rne_data(data, file_type)
            elif file_type == "flux":
                error_count = 0
                for line in file:
                    try:
                        data = json.loads(line)
                        unites_legales_temp = process_records_to_extract_rne_data(
                            data, file_type
                        )
                        unites_legales += unites_legales_temp
                    except json.JSONDecodeError as e:
                        if error_count < 3:
                            logging.error(
                                f"JSONDecodeError: {e} in file "
                                f"{file_path} at line {line}"
                            )
                            error_count += 1
                        else:
                            logging.error(
                                "More JSONDecodeErrors occurred but logging is limited."
                            )
                        # Skip the problematic line and continue with the next line
                        continue
                    # If the pending queue exceeds 100,000, we insert it directly;
                    # otherwise, it is inserted at the end of the loop.
                    if len(unites_legales) > 100000:
                        insert_unites_legales_into_db(
                            unites_legales, file_path, db_path
                        )
                        unites_legales = []

        except Exception as e:
            raise Exception(f"Exception: {e} in file {file_path}")
        insert_unites_legales_into_db(unites_legales, file_path, db_path)


def process_records_to_extract_rne_data(data, file_type):
    unites_legales = []

    def process_record(record):
        return extract_rne_data(record, file_type)

    if file_type == "stock":
        unites_legales = [process_record(record) for record in data]
    elif file_type == "flux":
        unites_legale = process_record(data)
        unites_legales.append(unites_legale)

    return unites_legales


def find_and_delete_same_siren(cursor, siren, file_path):
    """
    Find and delete older rows with the same SIREN as they are outdated.

    Args:
        cursor: The database cursor.
        siren (str): The SIREN to search for and delete.
        file_path (str): The file path to filter the rows.
    """
    tables = ["dirigeants_pm", "dirigeants_pp", "unites_legales", "sieges"]

    for table in tables:
        cursor.execute(
            f"SELECT COUNT(*) FROM {table} WHERE siren = ? AND file_name != ?",
            (siren, file_path),
        )
        count_already_existing = cursor.fetchone()[0]
        # If existing rows are found, delete them as they are outdated
        if count_already_existing is not None and count_already_existing > 0:
            cursor.execute(
                f"DELETE FROM {table} WHERE siren = ? AND file_name != ?",
                (siren, file_path),
            )


def insert_unites_legales_into_db(list_unites_legales, file_path, db_path):
    connection, cursor = connect_to_db(db_path)

    for unite_legale in list_unites_legales:
        find_and_delete_same_siren(cursor, unite_legale.siren, file_path)

        unite_legale_columns = [
            "siren",
            "denomination",
            "nom",
            "nom_usage",
            "prenom",
            "nom_commercial",
            "date_creation",
            "date_mise_a_jour",
            "date_immatriculation",
            "date_radiation",
            "activite_principale",
            "tranche_effectif_salarie",
            "nature_juridique",
            "etat_administratif",
            "forme_exercice_activite_principale",
            "statut_diffusion",
            "adresse",
            "file_name",
        ]

        cursor.execute(
            f"""
                INSERT INTO unites_legales ({', '.join(unite_legale_columns)})
                VALUES ({', '.join(['?'] * len(unite_legale_columns))})
            """,
            (
                unite_legale.siren,
                unite_legale.denomination,
                unite_legale.nom,
                unite_legale.nom_usage,
                unite_legale.prenom,
                unite_legale.nom_commercial,
                unite_legale.date_creation,
                unite_legale.date_mise_a_jour,
                unite_legale.date_immatriculation,
                unite_legale.date_radiation,
                unite_legale.activite_principale,
                unite_legale.tranche_effectif_salarie,
                unite_legale.nature_juridique,
                unite_legale.etat_administratif,
                unite_legale.forme_exercice_activite_principale,
                unite_legale.statut_diffusion,
                unite_legale.format_address(),
                file_path,
            ),
        )
        siege = unite_legale.siege

        # Define the columns for the sieges table
        sieges_columns = [
            "siren",
            "siret",
            "enseigne",
            "nom_commercial",
            "pays",
            "code_pays",
            "commune",
            "code_postal",
            "code_commune",
            "voie",
            "num_voie",
            "type_voie",
            "indice_repetition",
            "complement_localisation",
            "distribution_speciale",
            "date_mise_a_jour",
            "file_name",
        ]

        cursor.execute(
            f"""
                INSERT INTO sieges ({', '.join(sieges_columns)})
                VALUES ({', '.join(['?'] * len(sieges_columns))})
            """,
            (
                unite_legale.siren,
                siege.siret,
                siege.enseigne,
                siege.nom_commercial,
                siege.adresse.pays,
                siege.adresse.code_pays,
                siege.adresse.commune,
                siege.adresse.code_postal,
                siege.adresse.code_commune,
                siege.adresse.voie,
                siege.adresse.num_voie,
                siege.adresse.type_voie,
                siege.adresse.indice_repetition,
                siege.adresse.complement_localisation,
                siege.adresse.distribution_speciale,
                unite_legale.date_mise_a_jour,
                file_path,
            ),
        )

        list_dirigeants_pp, list_dirigeants_pm = unite_legale.get_dirigeants_list()

        for dirigeant_pp in list_dirigeants_pp:
            # Define the columns for the dirigeants_pp table
            dirigeants_pp_columns = [
                "siren",
                "date_mise_a_jour",
                "nom",
                "nom_usage",
                "prenoms",
                "genre",
                "date_de_naissance",
                "role",
                "nationalite",
                "situation_matrimoniale",
                "file_name",
            ]
            cursor.execute(
                f"""
                INSERT INTO dirigeants_pp ({', '.join(dirigeants_pp_columns)})
                VALUES ({', '.join(['?'] * len(dirigeants_pp_columns))})
            """,
                (
                    unite_legale.siren,
                    unite_legale.date_mise_a_jour,
                    dirigeant_pp.nom,
                    dirigeant_pp.nom_usage,
                    dirigeant_pp.prenoms,
                    dirigeant_pp.genre,
                    dirigeant_pp.date_de_naissance,
                    dirigeant_pp.role,
                    dirigeant_pp.nationalite,
                    dirigeant_pp.situation_matrimoniale,
                    file_path,
                ),
            )

        for dirigeant_pm in list_dirigeants_pm:
            # Define the columns for the dirigeants_pm table
            dirigeants_pm_columns = [
                "siren",
                "siren_dirigeant",
                "date_mise_a_jour",
                "denomination",
                "role",
                "pays",
                "forme_juridique",
                "file_name",
            ]
            cursor.execute(
                f"""
                INSERT INTO dirigeants_pm ({', '.join(dirigeants_pm_columns)})
                VALUES ({', '.join(['?'] * len(dirigeants_pm_columns))})
            """,
                (
                    unite_legale.siren,
                    dirigeant_pm.siren,
                    unite_legale.date_mise_a_jour,
                    dirigeant_pm.denomination,
                    dirigeant_pm.role,
                    dirigeant_pm.pays,
                    dirigeant_pm.forme_juridique,
                    file_path,
                ),
            )

    cursor.execute("SELECT COUNT(*) FROM dirigeants_pp")
    count_pp = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM dirigeants_pm")
    count_pm = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM unites_legales")
    count_ul = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM sieges")
    count_sieges = cursor.fetchone()[0]

    logging.info(
        f"************Count UL: {count_ul}, Count sieges: {count_sieges}, "
        f"Count pp: {count_pp}, Count pm: {count_pm}"
    )

    cursor.execute("SELECT * FROM unites_legales ORDER BY rowid DESC LIMIT 1")
    cursor.fetchone()

    connection.commit()
    connection.close()


def get_tables_count(db_path):
    connection, cursor = connect_to_db(db_path)

    cursor.execute("SELECT COUNT(*) FROM unites_legales")
    count_ul = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM sieges")
    count_sieges = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM dirigeants_pp")
    count_pp = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM dirigeants_pm")
    count_pm = cursor.fetchone()[0]

    connection.close()
    return count_ul, count_sieges, count_pp, count_pm


def extract_rne_data(entity, file_type="flux"):
    """Extract "unites legales" data from RNE json object."""

    company = entity.get("company", {}) if file_type == "flux" else entity

    rne_company = RNECompany.model_validate(company)
    unite_legale = UniteLegale()
    unite_legale_formatted = map_rne_company_to_ul(rne_company, unite_legale)

    return unite_legale_formatted
