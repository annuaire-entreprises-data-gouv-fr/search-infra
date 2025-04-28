import json
import logging

from dag_datalake_sirene.workflows.data_pipelines.rne.database.db_connexion import (
    connect_to_db,
)
from dag_datalake_sirene.workflows.data_pipelines.rne.database.map_rne import (
    map_rne_company_to_ul,
)
from dag_datalake_sirene.workflows.data_pipelines.rne.database.rne_model import (
    RNECompany,
)
from dag_datalake_sirene.workflows.data_pipelines.rne.database.ul_model import (
    UniteLegale,
)


def create_tables(cursor):
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS unite_legale
        (
            siren TEXT,
            denomination TEXT,
            nom TEXT,
            nom_usage TEXT,
            prenom TEXT,
            nom_commercial TEXT,
            date_creation TEXT,
            date_mise_a_jour DATE,
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
        CREATE TABLE IF NOT EXISTS siege
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
        CREATE TABLE IF NOT EXISTS dirigeant_pp
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
        CREATE TABLE IF NOT EXISTS dirigeant_pm
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
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS immatriculation
        (
            siren TEXT,
            date_mise_a_jour DATE,
            date_immatriculation DATE,
            date_radiation DATE,
            indicateur_associe_unique TEXT,
            capital_social REAL,
            date_cloture_exercice TEXT,
            duree_personne_morale INT,
            nature_entreprise TEXT,
            date_debut_activite TEXT,
            capital_variable TEXT,
            devise_capital TEXT,
            file_name TEXT
        )
    """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS etablissement
        (
            siren TEXT,
            siret TEXT,
            date_mise_a_jour DATE,
            file_name TEXT
        )
    """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS activite
        (
            siren TEXT,
            siret TEXT,
            code_category TEXT,
            indicateur_principal BOOLEAN,
            indicateur_prolongement BOOLEAN,
            date_debut DATE,
            form_exercice TEXT,
            categorisation_activite1 TEXT,
            categorisation_activite2 TEXT,
            categorisation_activite3 TEXT,
            indicateur_activitee_ape BOOLEAN,
            code_ape TEXT,
            activite_rattachee_eirl BOOLEAN,
            date_mise_a_jour DATE,
            file_name TEXT
        )
    """
    )

    create_index_db(cursor)


def create_index_db(cursor):
    index_statements = [
        "CREATE INDEX IF NOT EXISTS idx_siren_ul ON unite_legale (siren);",
        "CREATE INDEX IF NOT EXISTS idx_siren_etab ON siege (siren);",
        "CREATE INDEX IF NOT EXISTS idx_siret_etab ON siege (siret);",
        "CREATE INDEX IF NOT EXISTS idx_siren_pp ON dirigeant_pp (siren);",
        "CREATE INDEX IF NOT EXISTS idx_siren_pm ON dirigeant_pm (siren);",
        "CREATE INDEX IF NOT EXISTS file_ul ON unite_legale (file_name);",
        "CREATE INDEX IF NOT EXISTS file_etab ON siege (file_name);",
        "CREATE INDEX IF NOT EXISTS file_pp ON dirigeant_pp (file_name);",
        "CREATE INDEX IF NOT EXISTS file_pm ON dirigeant_pm (file_name);",
        "CREATE INDEX IF NOT EXISTS idx_siren_immat ON immatriculation (siren);",
        """CREATE INDEX IF NOT EXISTS idx_ul_siren_file_name
        ON unite_legale (siren, file_name);""",
        """CREATE INDEX IF NOT EXISTS idx_etab_siren_file_name
        ON siege (siren, file_name);""",
        """CREATE INDEX IF NOT EXISTS idx_etab_siret_file_name
        ON siege (siret, file_name);""",
        """CREATE INDEX IF NOT EXISTS idx_pp_siren_file_name
        ON dirigeant_pp (siren, file_name);""",
        """CREATE INDEX IF NOT EXISTS idx_pm_siren_file_name
        ON dirigeant_pm (siren, file_name);""",
        """CREATE INDEX IF NOT EXISTS idx_siren_immat_file_name
        ON immatriculation (siren, file_name);""",
        "CREATE INDEX IF NOT EXISTS idx_siren_etab_other ON etablissement (siren);",
        "CREATE INDEX IF NOT EXISTS idx_siret_etab_other ON etablissement (siret);",
        "CREATE INDEX IF NOT EXISTS idx_siren_activite ON activite (siren);",
        "CREATE INDEX IF NOT EXISTS idx_siret_activite ON activite (siret);",
        """CREATE INDEX IF NOT EXISTS idx_etab_other_siren_file_name
        ON etablissement (siren, file_name);""",
        """CREATE INDEX IF NOT EXISTS idx_etab_other_siret_file_name
        ON etablissement (siret, file_name);""",
        """CREATE INDEX IF NOT EXISTS idx_activite_siren_file_name
        ON activite (siren, file_name);""",
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
    tables = [
        "dirigeant_pm",
        "dirigeant_pp",
        "unite_legale",
        "siege",
        "immatriculation",
        "etablissement",
        "activite",
    ]

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
                INSERT INTO unite_legale ({", ".join(unite_legale_columns)})
                VALUES ({", ".join(["?"] * len(unite_legale_columns))})
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

        # Define the columns for the siege table
        siege_columns = [
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
                INSERT INTO siege ({", ".join(siege_columns)})
                VALUES ({", ".join(["?"] * len(siege_columns))})
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
        # Insert siege activities if they exist
        if siege and siege.activites:
            for activite in siege.activites:
                # Define the columns for the activite table
                activite_columns = [
                    "siren",
                    "siret",
                    "code_category",
                    "indicateur_principal",
                    "indicateur_prolongement",
                    "date_debut",
                    "form_exercice",
                    "categorisation_activite1",
                    "categorisation_activite2",
                    "categorisation_activite3",
                    "indicateur_activitee_ape",
                    "code_ape",
                    "activite_rattachee_eirl",
                    "date_mise_a_jour",
                    "file_name",
                ]
                cursor.execute(
                    f"""
                    INSERT INTO activite ({", ".join(activite_columns)})
                    VALUES ({", ".join(["?"] * len(activite_columns))})
                    """,
                    (
                        unite_legale.siren,
                        siege.siret,
                        activite.code_category,
                        activite.indicateur_principal,
                        activite.indicateur_prolongement,
                        activite.date_debut,
                        activite.form_exercice,
                        activite.categorisation_activite1,
                        activite.categorisation_activite2,
                        activite.categorisation_activite3,
                        activite.indicateur_activitee_ape,
                        activite.code_ape,
                        activite.activite_rattachee_eirl,
                        unite_legale.date_mise_a_jour,
                        file_path,
                    ),
                )

        # Insert etablissements and their activities
        if unite_legale.etablissements:
            for etablissement in unite_legale.etablissements:
                # Define the columns for the etablissement table
                etablissement_columns = [
                    "siren",
                    "siret",
                    "date_mise_a_jour",
                    "file_name",
                ]
                cursor.execute(
                    f"""
                    INSERT INTO etablissement ({", ".join(etablissement_columns)})
                    VALUES ({", ".join(["?"] * len(etablissement_columns))})
                    """,
                    (
                        unite_legale.siren,
                        etablissement.siret,
                        unite_legale.date_mise_a_jour,
                        file_path,
                    ),
                )

                # Insert activites for this etablissement
                if etablissement.activites:
                    for activite in etablissement.activites:
                        # Define the columns for the activite table
                        activite_columns = [
                            "siren",
                            "siret",
                            "code_category",
                            "indicateur_principal",
                            "indicateur_prolongement",
                            "date_debut",
                            "form_exercice",
                            "categorisation_activite1",
                            "categorisation_activite2",
                            "categorisation_activite3",
                            "indicateur_activitee_ape",
                            "code_ape",
                            "activite_rattachee_eirl",
                            "date_mise_a_jour",
                            "file_name",
                        ]
                        cursor.execute(
                            f"""
                            INSERT INTO activite ({", ".join(activite_columns)})
                            VALUES ({", ".join(["?"] * len(activite_columns))})
                            """,
                            (
                                unite_legale.siren,
                                etablissement.siret,
                                activite.code_category,
                                activite.indicateur_principal,
                                activite.indicateur_prolongement,
                                activite.date_debut,
                                activite.form_exercice,
                                activite.categorisation_activite1,
                                activite.categorisation_activite2,
                                activite.categorisation_activite3,
                                activite.indicateur_activitee_ape,
                                activite.code_ape,
                                activite.activite_rattachee_eirl,
                                unite_legale.date_mise_a_jour,
                                file_path,
                            ),
                        )

        list_dirigeants_pp, list_dirigeants_pm = unite_legale.get_dirigeants_list()

        for dirigeant_pp in list_dirigeants_pp:
            # Define the columns for the dirigeant_pp table
            dirigeant_pp_columns = [
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
                INSERT INTO dirigeant_pp ({", ".join(dirigeant_pp_columns)})
                VALUES ({", ".join(["?"] * len(dirigeant_pp_columns))})
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
            # Define the columns for the dirigeant_pm table
            dirigeant_pm_columns = [
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
                INSERT INTO dirigeant_pm ({", ".join(dirigeant_pm_columns)})
                VALUES ({", ".join(["?"] * len(dirigeant_pm_columns))})
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

        immatriculation = unite_legale.immatriculation

        # Define the columns for the immatriculation table
        immat_columns = [
            "siren",
            "date_mise_a_jour",
            "date_immatriculation",
            "date_radiation",
            "indicateur_associe_unique",
            "capital_social",
            "date_cloture_exercice",
            "nature_entreprise",
            "date_debut_activite",
            "capital_variable",
            "devise_capital",
            "duree_personne_morale",
            "file_name",
        ]

        cursor.execute(
            f"""
                INSERT INTO immatriculation ({", ".join(immat_columns)})
                VALUES ({", ".join(["?"] * len(immat_columns))})
            """,
            (
                unite_legale.siren,
                unite_legale.date_mise_a_jour,
                immatriculation.date_immatriculation,
                immatriculation.date_radiation,
                immatriculation.indicateur_associe_unique,
                immatriculation.capital_social,
                immatriculation.date_cloture_exercice,
                json.dumps(immatriculation.nature_entreprise),
                immatriculation.date_debut_activite,
                immatriculation.capital_variable,
                immatriculation.devise_capital,
                immatriculation.duree_personne_morale,
                file_path,
            ),
        )

    cursor.execute("SELECT COUNT(*) FROM dirigeant_pp")
    count_pp = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM dirigeant_pm")
    count_pm = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM unite_legale")
    count_ul = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM siege")
    count_siege = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM immatriculation")
    count_immat = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM etablissement")
    count_etab = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM activite")
    count_activite = cursor.fetchone()[0]

    logging.info(
        f"************Count UL: {count_ul}, Count siege: {count_siege}, "
        f"Count pp: {count_pp}, Count pm: {count_pm}, "
        f"Count immat: {count_immat}, "
        f"Count etablissement: {count_etab}, Count activite: {count_activite}."
    )

    cursor.execute("SELECT * FROM unite_legale ORDER BY rowid DESC LIMIT 1")
    cursor.fetchone()

    connection.commit()
    connection.close()


def remove_duplicates_from_tables(cursor, table_name):
    # Get the schema of the original table
    cursor.execute(
        f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{table_name}'"
    )
    create_table_sql = cursor.fetchone()[0]

    # Modify the SQL to create the temporary table without altering column names
    temp_table = f"{table_name}_temp"
    create_temp_table_sql = create_table_sql.replace(
        f"CREATE TABLE {table_name}", f"CREATE TABLE {temp_table}"
    )

    # Create the temporary table with the modified schema
    cursor.execute(create_temp_table_sql)

    # Insert distinct rows into the temporary table
    cursor.execute(f"INSERT INTO {temp_table} SELECT DISTINCT * FROM {table_name}")

    # Drop the original table
    cursor.execute(f"DROP TABLE {table_name}")

    # Rename the temporary table to the original table name
    cursor.execute(f"ALTER TABLE {temp_table} RENAME TO {table_name}")

    # Recreate the indexes
    create_index_db(cursor)


def get_tables_count(db_path):
    connection, cursor = connect_to_db(db_path)

    cursor.execute("SELECT COUNT(*) FROM unite_legale")
    count_ul = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM siege")
    count_siege = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM dirigeant_pp")
    count_pp = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM dirigeant_pm")
    count_pm = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM immatriculation")
    count_immat = cursor.fetchone()[0]

    connection.close()
    return count_ul, count_siege, count_pp, count_pm, count_immat


def extract_rne_data(entity, file_type="flux"):
    """Extract "unites legales" data from RNE json object."""

    company = entity.get("company", {}) if file_type == "flux" else entity

    rne_company = RNECompany.model_validate(company)
    unite_legale = UniteLegale()
    unite_legale_formatted = map_rne_company_to_ul(rne_company, unite_legale)

    return unite_legale_formatted
