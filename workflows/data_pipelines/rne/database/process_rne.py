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
            prenoms TEXT,
            nom_commercial TEXT,
            nom_exploitation TEXT,
            date_creation TEXT,
            date_mise_a_jour_formalite DATE,
            date_mise_a_jour_rne DATE,
            activite_principale TEXT,
            tranche_effectif_salarie TEXT,
            nature_juridique TEXT,
            etat_administratif_insee TEXT,
            forme_exercice_activite_principale TEXT,
            statut_diffusion TEXT,
            adresse_pays TEXT,
            adresse_code_pays TEXT,
            adresse_commune TEXT,
            adresse_code_postal TEXT,
            adresse_code_commune TEXT,
            adresse_voie TEXT,
            adresse_num_voie TEXT,
            adresse_type_voie TEXT,
            adresse_indice_repetition TEXT,
            adresse_complement_localisation TEXT,
            adresse_distribution_speciale TEXT,
            siren_doublons TEXT,
            nombre_representants_actifs INTEGER,
            nombre_etablissements_ouverts INTEGER,
            societe_etrangere BOOLEAN,
            forme_juridique TEXT,
            forme_juridique_insee TEXT,
            indicateur_associe_unique BOOLEAN,
            indicateur_ess BOOLEAN,
            eirl BOOLEAN,
            entreprise_agricole BOOLEAN,
            reliee_entreprise_agricole BOOLEAN,
            date_sommeil TEXT,
            date_radiation TEXT,
            montant_capital REAL,
            capital_variable BOOLEAN,
            devise_capital TEXT,
            societe_mission BOOLEAN,
            entreprise_domiciliataire_siren TEXT,
            entreprise_domiciliataire_denomination TEXT,
            entreprise_domiciliataire_nom_commercial TEXT,
            date_cloture_exercice_social TEXT,
            duree_personne_morale INTEGER,
            date_fin_existence TEXT,
            code_ape TEXT,
            genre TEXT,
            date_naissance TEXT,
            lieu_naissance TEXT,
            pays_naissance TEXT,
            nationalite TEXT,
            qualite_artisan TEXT,
            mail TEXT,
            telephone TEXT,
            role_conjoint TEXT,
            nom_conjoint TEXT,
            nom_usage_conjoint TEXT,
            prenoms_conjoint TEXT,
            genre_conjoint TEXT,
            date_naissance_conjoint TEXT,
            lieu_naissance_conjoint TEXT,
            pays_naissance_conjoint TEXT,
            nationalite_conjoint TEXT,
            mail_conjoint TEXT,
            telephone_conjoint TEXT,
            adresse_conjoint_pays TEXT,
            adresse_conjoint_code_pays TEXT,
            adresse_conjoint_commune TEXT,
            adresse_conjoint_code_postal TEXT,
            adresse_conjoint_code_commune TEXT,
            adresse_conjoint_voie TEXT,
            adresse_conjoint_num_voie TEXT,
            adresse_conjoint_type_voie TEXT,
            adresse_conjoint_indice_repetition TEXT,
            adresse_conjoint_complement_localisation TEXT,
            adresse_conjoint_distribution_speciale TEXT,
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
            statut TEXT,
            type TEXT,
            date_ouverture TEXT,
            date_fermeture TEXT,
            enseigne TEXT,
            nom_commercial TEXT,
            code_ape TEXT,
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
            date_mise_a_jour_formalite DATE,
            date_mise_a_jour_rne DATE,
            file_name TEXT
        )
    """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS dirigeant_pp
        (
            siren TEXT,
            date_mise_a_jour_formalite DATE,
            date_mise_a_jour_rne DATE,
            nom TEXT,
            nom_usage TEXT,
            prenoms TEXT,
            genre TEXT,
            date_naissance TEXT,
            lieu_naissance TEXT,
            pays_naissance TEXT,
            role TEXT,
            libelle_role TEXT,
            autre_role TEXT,
            second_role TEXT,
            libelle_second_role TEXT,
            qualite_artisan TEXT,
            nationalite TEXT,
            mail TEXT,
            telephone TEXT,
            adresse_pays TEXT,
            adresse_code_pays TEXT,
            adresse_commune TEXT,
            adresse_code_postal TEXT,
            adresse_code_commune TEXT,
            adresse_voie TEXT,
            adresse_num_voie TEXT,
            adresse_type_voie TEXT,
            adresse_indice_repetition TEXT,
            adresse_complement_localisation TEXT,
            adresse_distribution_speciale TEXT,
            date_debut TEXT,
            date_fin TEXT,
            actif BOOLEAN,
            role_conjoint TEXT,
            nom_conjoint TEXT,
            nom_usage_conjoint TEXT,
            prenoms_conjoint TEXT,
            genre_conjoint TEXT,
            date_naissance_conjoint TEXT,
            lieu_naissance_conjoint TEXT,
            pays_naissance_conjoint TEXT,
            nationalite_conjoint TEXT,
            mail_conjoint TEXT,
            telephone_conjoint TEXT,
            adresse_conjoint_pays TEXT,
            adresse_conjoint_code_pays TEXT,
            adresse_conjoint_commune TEXT,
            adresse_conjoint_code_postal TEXT,
            adresse_conjoint_code_commune TEXT,
            adresse_conjoint_voie TEXT,
            adresse_conjoint_num_voie TEXT,
            adresse_conjoint_type_voie TEXT,
            adresse_conjoint_indice_repetition TEXT,
            adresse_conjoint_complement_localisation TEXT,
            adresse_conjoint_distribution_speciale TEXT,
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
            date_mise_a_jour_formalite DATE,
            date_mise_a_jour_rne DATE,
            denomination TEXT,
            role TEXT,
            libelle_role TEXT,
            autre_role TEXT,
            second_role TEXT,
            libelle_second_role TEXT,
            qualite_artisan TEXT,
            pays TEXT,
            forme_juridique TEXT,
            lieu_registre TEXT,
            date_debut TEXT,
            date_fin TEXT,
            actif BOOLEAN,
            nom_individu_representant TEXT,
            nom_usage_individu_representant TEXT,
            prenoms_individu_representant TEXT,
            genre_individu_representant TEXT,
            date_naissance_individu_representant TEXT,
            lieu_naissance_individu_representant TEXT,
            pays_naissance_individu_representant TEXT,
            nationalite_individu_representant TEXT,
            adresse_individu_representant_pays TEXT,
            adresse_individu_representant_code_pays TEXT,
            adresse_individu_representant_commune TEXT,
            adresse_individu_representant_code_postal TEXT,
            adresse_individu_representant_code_commune TEXT,
            adresse_individu_representant_voie TEXT,
            adresse_individu_representant_num_voie TEXT,
            adresse_individu_representant_type_voie TEXT,
            adresse_individu_representant_indice_repetition TEXT,
            adresse_individu_representant_complement_localisation TEXT,
            adresse_individu_representant_distribution_speciale TEXT,
            file_name TEXT
        )
    """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS immatriculation
        (
            siren TEXT,
            date_mise_a_jour_formalite DATE,
            date_mise_a_jour_rne DATE,
            date_immatriculation DATE,
            date_radiation DATE,
            capital_social REAL,
            date_cloture_exercice TEXT,
            duree_personne_morale INT,
            nature_entreprise TEXT,
            date_debut_activite TEXT,
            capital_variable BOOLEAN,
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
            statut TEXT,
            type TEXT,
            date_ouverture TEXT,
            date_fermeture TEXT,
            enseigne TEXT,
            nom_commercial TEXT,
            code_ape TEXT,
            adresse_pays TEXT,
            adresse_code_pays TEXT,
            adresse_commune TEXT,
            adresse_code_postal TEXT,
            adresse_code_commune TEXT,
            adresse_voie TEXT,
            adresse_num_voie TEXT,
            adresse_type_voie TEXT,
            adresse_indice_repetition TEXT,
            adresse_complement_localisation TEXT,
            adresse_distribution_speciale TEXT,
            date_mise_a_jour_formalite DATE,
            date_mise_a_jour_rne DATE,
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
            activite_reguliere TEXT,
            qualite_non_sedentaire TEXT,
            indicateur_non_sedentaire BOOLEAN,
            indicateur_artiste_auteur BOOLEAN,
            indicateur_marin_professionnel BOOLEAN,
            indicateur_prolongement BOOLEAN,
            description_detaillee TEXT,
            forme_exercice TEXT,
            code_aprm TEXT,
            metier_art BOOLEAN,
            date_debut DATE,
            date_fin TEXT,
            categorisation_activite_1 TEXT,
            categorisation_activite_2 TEXT,
            categorisation_activite_3 TEXT,
            categorisation_activite_4 TEXT,
            type_origine_fond TEXT,
            code_ape TEXT,
            activite_rattachee_eirl BOOLEAN,
            date_mise_a_jour_formalite DATE,
            date_mise_a_jour_rne DATE,
            file_name TEXT
        )
    """
    )

    # New tables for additional entities
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS beneficiaire_effectif
        (
            siren TEXT,
            nom TEXT,
            nom_usage TEXT,
            prenoms TEXT,
            genre TEXT,
            date_naissance TEXT,
            lieu_naissance TEXT,
            pays_naissance TEXT,
            adresse_pays TEXT,
            adresse_code_pays TEXT,
            adresse_commune TEXT,
            adresse_code_postal TEXT,
            adresse_code_commune TEXT,
            adresse_voie TEXT,
            adresse_num_voie TEXT,
            adresse_type_voie TEXT,
            adresse_indice_repetition TEXT,
            adresse_complement_localisation TEXT,
            adresse_distribution_speciale TEXT,
            date_effet TEXT,
            detention_part_directe BOOLEAN,
            modalites_de_controle TEXT,
            detention_25p_capital BOOLEAN,
            detention_25p_droit_vote BOOLEAN,
            date_mise_a_jour_formalite DATE,
            date_mise_a_jour_rne DATE,
            file_name TEXT
        )
    """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS historique
        (
            siren TEXT,
            date_integration TEXT,
            code_evenement TEXT,
            libelle_evenement TEXT,
            numero_liasse TEXT,
            patch_id TEXT,
            date_effet TEXT,
            chemin_date_effet TEXT,
            chemin_date_effet_id TEXT,
            date_mise_a_jour_formalite DATE,
            date_mise_a_jour_rne DATE,
            file_name TEXT
        )
    """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS inscription_office
        (
            siren TEXT,
            event TEXT,
            date_effet TEXT,
            observation TEXT,
            partner_center TEXT,
            partner_code TEXT,
            observation_complementaire TEXT,
            date_mise_a_jour_formalite DATE,
            date_mise_a_jour_rne DATE,
            file_name TEXT
        )
    """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS mention
        (
            siren TEXT,
            inscription_office_id INTEGER,
            num_observation TEXT,
            date_effet TEXT,
            texte TEXT,
            date_mise_a_jour_formalite DATE,
            date_mise_a_jour_rne DATE,
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
        "CREATE INDEX IF NOT EXISTS idx_beneficiaire_siren ON beneficiaire_effectif (siren);",
        "CREATE INDEX IF NOT EXISTS idx_beneficiaire_siren_file_name ON beneficiaire_effectif (siren, file_name);",
        "CREATE INDEX IF NOT EXISTS idx_historique_siren ON historique (siren);",
        "CREATE INDEX IF NOT EXISTS idx_historique_siren_file_name ON historique (siren, file_name);",
        "CREATE INDEX IF NOT EXISTS idx_inscription_office_siren ON inscription_office (siren);",
        "CREATE INDEX IF NOT EXISTS idx_inscription_office_siren_file_name ON inscription_office (siren, file_name);",
        "CREATE INDEX IF NOT EXISTS idx_mention_siren ON mention (siren);",
        "CREATE INDEX IF NOT EXISTS idx_mention_siren_file_name ON mention (siren, file_name);",
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
            "prenoms",
            "nom_commercial",
            "nom_exploitation",
            "date_creation",
            "date_mise_a_jour_formalite",
            "date_mise_a_jour_rne",
            "activite_principale",
            "tranche_effectif_salarie",
            "nature_juridique",
            "etat_administratif_insee",
            "forme_exercice_activite_principale",
            "statut_diffusion",
            "adresse_pays",
            "adresse_code_pays",
            "adresse_commune",
            "adresse_code_postal",
            "adresse_code_commune",
            "adresse_voie",
            "adresse_num_voie",
            "adresse_type_voie",
            "adresse_indice_repetition",
            "adresse_complement_localisation",
            "adresse_distribution_speciale",
            "siren_doublons",
            "nombre_representants_actifs",
            "nombre_etablissements_ouverts",
            "societe_etrangere",
            "forme_juridique",
            "forme_juridique_insee",
            "indicateur_associe_unique",
            "indicateur_ess",
            "eirl",
            "entreprise_agricole",
            "reliee_entreprise_agricole",
            "date_sommeil",
            "date_radiation",
            "montant_capital",
            "capital_variable",
            "devise_capital",
            "societe_mission",
            "entreprise_domiciliataire_siren",
            "entreprise_domiciliataire_denomination",
            "entreprise_domiciliataire_nom_commercial",
            "date_cloture_exercice_social",
            "duree_personne_morale",
            "date_fin_existence",
            "code_ape",
            "genre",
            "date_naissance",
            "lieu_naissance",
            "pays_naissance",
            "nationalite",
            "qualite_artisan",
            "mail",
            "telephone",
            "role_conjoint",
            "nom_conjoint",
            "nom_usage_conjoint",
            "prenoms_conjoint",
            "genre_conjoint",
            "date_naissance_conjoint",
            "lieu_naissance_conjoint",
            "pays_naissance_conjoint",
            "nationalite_conjoint",
            "mail_conjoint",
            "telephone_conjoint",
            "adresse_conjoint_pays",
            "adresse_conjoint_code_pays",
            "adresse_conjoint_commune",
            "adresse_conjoint_code_postal",
            "adresse_conjoint_code_commune",
            "adresse_conjoint_voie",
            "adresse_conjoint_num_voie",
            "adresse_conjoint_type_voie",
            "adresse_conjoint_indice_repetition",
            "adresse_conjoint_complement_localisation",
            "adresse_conjoint_distribution_speciale",
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
                unite_legale.prenoms,
                unite_legale.nom_commercial,
                unite_legale.nom_exploitation,
                unite_legale.date_creation,
                unite_legale.date_mise_a_jour_formalite,
                unite_legale.date_mise_a_jour_rne,
                unite_legale.activite_principale,
                unite_legale.tranche_effectif_salarie,
                unite_legale.nature_juridique,
                unite_legale.etat_administratif_insee,
                unite_legale.forme_exercice_activite_principale,
                unite_legale.statut_diffusion,
                unite_legale.adresse.pays if unite_legale.adresse else None,
                unite_legale.adresse.code_pays if unite_legale.adresse else None,
                unite_legale.adresse.commune if unite_legale.adresse else None,
                unite_legale.adresse.code_postal if unite_legale.adresse else None,
                unite_legale.adresse.code_commune if unite_legale.adresse else None,
                unite_legale.adresse.voie if unite_legale.adresse else None,
                unite_legale.adresse.num_voie if unite_legale.adresse else None,
                unite_legale.adresse.type_voie if unite_legale.adresse else None,
                unite_legale.adresse.indice_repetition
                if unite_legale.adresse
                else None,
                unite_legale.adresse.complement_localisation
                if unite_legale.adresse
                else None,
                unite_legale.adresse.distribution_speciale
                if unite_legale.adresse
                else None,
                unite_legale.siren_doublons,
                unite_legale.nombre_representants_actifs,
                unite_legale.nombre_etablissements_ouverts,
                unite_legale.societe_etrangere,
                unite_legale.forme_juridique,
                unite_legale.forme_juridique_insee,
                unite_legale.indicateur_associe_unique,
                unite_legale.indicateur_ess,
                unite_legale.eirl,
                unite_legale.entreprise_agricole,
                unite_legale.reliee_entreprise_agricole,
                unite_legale.date_sommeil,
                unite_legale.date_radiation,
                unite_legale.montant_capital,
                unite_legale.capital_variable,
                unite_legale.devise_capital,
                unite_legale.societe_mission,
                unite_legale.entreprise_domiciliataire_siren,
                unite_legale.entreprise_domiciliataire_denomination,
                unite_legale.entreprise_domiciliataire_nom_commercial,
                unite_legale.date_cloture_exercice_social,
                unite_legale.duree_personne_morale,
                unite_legale.date_fin_existence,
                unite_legale.code_ape,
                unite_legale.genre,
                unite_legale.date_naissance,
                unite_legale.lieu_naissance,
                unite_legale.pays_naissance,
                unite_legale.nationalite,
                unite_legale.qualite_artisan,
                unite_legale.mail,
                unite_legale.telephone,
                unite_legale.role_conjoint,
                unite_legale.nom_conjoint,
                unite_legale.nom_usage_conjoint,
                unite_legale.prenoms_conjoint,
                unite_legale.genre_conjoint,
                unite_legale.date_naissance_conjoint,
                unite_legale.lieu_naissance_conjoint,
                unite_legale.pays_naissance_conjoint,
                unite_legale.nationalite_conjoint,
                unite_legale.mail_conjoint,
                unite_legale.telephone_conjoint,
                unite_legale.adresse_conjoint.pays
                if unite_legale.adresse_conjoint
                else None,
                unite_legale.adresse_conjoint.code_pays
                if unite_legale.adresse_conjoint
                else None,
                unite_legale.adresse_conjoint.commune
                if unite_legale.adresse_conjoint
                else None,
                unite_legale.adresse_conjoint.code_postal
                if unite_legale.adresse_conjoint
                else None,
                unite_legale.adresse_conjoint.code_commune
                if unite_legale.adresse_conjoint
                else None,
                unite_legale.adresse_conjoint.voie
                if unite_legale.adresse_conjoint
                else None,
                unite_legale.adresse_conjoint.num_voie
                if unite_legale.adresse_conjoint
                else None,
                unite_legale.adresse_conjoint.type_voie
                if unite_legale.adresse_conjoint
                else None,
                unite_legale.adresse_conjoint.indice_repetition
                if unite_legale.adresse_conjoint
                else None,
                unite_legale.adresse_conjoint.complement_localisation
                if unite_legale.adresse_conjoint
                else None,
                unite_legale.adresse_conjoint.distribution_speciale
                if unite_legale.adresse_conjoint
                else None,
                file_path,
            ),
        )
        siege = unite_legale.siege

        # Insert siege only if present
        if siege:
            # Define the columns for the siege table
            siege_columns = [
                "siren",
                "siret",
                "statut",
                "type",
                "date_ouverture",
                "date_fermeture",
                "enseigne",
                "nom_commercial",
                "code_ape",
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
                "date_mise_a_jour_formalite",
                "date_mise_a_jour_rne",
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
                    siege.statut,
                    siege.type,
                    siege.date_ouverture,
                    siege.date_fermeture,
                    siege.enseigne,
                    siege.nom_commercial,
                    siege.code_ape,
                    siege.adresse.pays if siege.adresse else None,
                    siege.adresse.code_pays if siege.adresse else None,
                    siege.adresse.commune if siege.adresse else None,
                    siege.adresse.code_postal if siege.adresse else None,
                    siege.adresse.code_commune if siege.adresse else None,
                    siege.adresse.voie if siege.adresse else None,
                    siege.adresse.num_voie if siege.adresse else None,
                    siege.adresse.type_voie if siege.adresse else None,
                    siege.adresse.indice_repetition if siege.adresse else None,
                    siege.adresse.complement_localisation if siege.adresse else None,
                    siege.adresse.distribution_speciale if siege.adresse else None,
                    unite_legale.date_mise_a_jour_formalite,
                    unite_legale.date_mise_a_jour_rne,
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
                    "activite_reguliere",
                    "qualite_non_sedentaire",
                    "indicateur_non_sedentaire",
                    "indicateur_artiste_auteur",
                    "indicateur_marin_professionnel",
                    "indicateur_prolongement",
                    "description_detaillee",
                    "forme_exercice",
                    "code_aprm",
                    "metier_art",
                    "date_debut",
                    "date_fin",
                    "categorisation_activite_1",
                    "categorisation_activite_2",
                    "categorisation_activite_3",
                    "categorisation_activite_4",
                    "type_origine_fond",
                    "code_ape",
                    "activite_rattachee_eirl",
                    "date_mise_a_jour_formalite",
                    "date_mise_a_jour_rne",
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
                        activite.activite_reguliere,
                        activite.qualite_non_sedentaire,
                        activite.indicateur_non_sedentaire,
                        activite.indicateur_artiste_auteur,
                        activite.indicateur_marin_professionnel,
                        activite.indicateur_prolongement,
                        activite.description_detaillee,
                        activite.forme_exercice,
                        activite.code_aprm,
                        activite.metier_art,
                        activite.date_debut,
                        activite.date_fin,
                        activite.categorisation_activite_1,
                        activite.categorisation_activite_2,
                        activite.categorisation_activite_3,
                        activite.categorisation_activite_4,
                        activite.type_origine_fond,
                        activite.code_ape,
                        activite.activite_rattachee_eirl,
                        unite_legale.date_mise_a_jour_formalite,
                        unite_legale.date_mise_a_jour_rne,
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
                    "statut",
                    "type",
                    "date_ouverture",
                    "date_fermeture",
                    "enseigne",
                    "nom_commercial",
                    "code_ape",
                    "adresse_pays",
                    "adresse_code_pays",
                    "adresse_commune",
                    "adresse_code_postal",
                    "adresse_code_commune",
                    "adresse_voie",
                    "adresse_num_voie",
                    "adresse_type_voie",
                    "adresse_indice_repetition",
                    "adresse_complement_localisation",
                    "adresse_distribution_speciale",
                    "date_mise_a_jour_formalite",
                    "date_mise_a_jour_rne",
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
                        etablissement.statut,
                        etablissement.type,
                        etablissement.date_ouverture,
                        etablissement.date_fermeture,
                        etablissement.enseigne,
                        etablissement.nom_commercial,
                        etablissement.code_ape,
                        etablissement.adresse.pays if etablissement.adresse else None,
                        etablissement.adresse.code_pays
                        if etablissement.adresse
                        else None,
                        etablissement.adresse.commune
                        if etablissement.adresse
                        else None,
                        etablissement.adresse.code_postal
                        if etablissement.adresse
                        else None,
                        etablissement.adresse.code_commune
                        if etablissement.adresse
                        else None,
                        etablissement.adresse.voie if etablissement.adresse else None,
                        etablissement.adresse.num_voie
                        if etablissement.adresse
                        else None,
                        etablissement.adresse.type_voie
                        if etablissement.adresse
                        else None,
                        etablissement.adresse.indice_repetition
                        if etablissement.adresse
                        else None,
                        etablissement.adresse.complement_localisation
                        if etablissement.adresse
                        else None,
                        etablissement.adresse.distribution_speciale
                        if etablissement.adresse
                        else None,
                        unite_legale.date_mise_a_jour_formalite,
                        unite_legale.date_mise_a_jour_rne,
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
                            "activite_reguliere",
                            "qualite_non_sedentaire",
                            "indicateur_non_sedentaire",
                            "indicateur_artiste_auteur",
                            "indicateur_marin_professionnel",
                            "indicateur_prolongement",
                            "description_detaillee",
                            "forme_exercice",
                            "code_aprm",
                            "metier_art",
                            "date_debut",
                            "date_fin",
                            "categorisation_activite_1",
                            "categorisation_activite_2",
                            "categorisation_activite_3",
                            "categorisation_activite_4",
                            "type_origine_fond",
                            "code_ape",
                            "activite_rattachee_eirl",
                            "date_mise_a_jour_formalite",
                            "date_mise_a_jour_rne",
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
                                activite.activite_reguliere,
                                activite.qualite_non_sedentaire,
                                activite.indicateur_non_sedentaire,
                                activite.indicateur_artiste_auteur,
                                activite.indicateur_marin_professionnel,
                                activite.indicateur_prolongement,
                                activite.description_detaillee,
                                activite.forme_exercice,
                                activite.code_aprm,
                                activite.metier_art,
                                activite.date_debut,
                                activite.date_fin,
                                activite.categorisation_activite_1,
                                activite.categorisation_activite_2,
                                activite.categorisation_activite_3,
                                activite.categorisation_activite_4,
                                activite.type_origine_fond,
                                activite.code_ape,
                                activite.activite_rattachee_eirl,
                                unite_legale.date_mise_a_jour_formalite,
                                unite_legale.date_mise_a_jour_rne,
                                file_path,
                            ),
                        )

        list_dirigeants_pp, list_dirigeants_pm = unite_legale.get_dirigeants_list()

        for dirigeant_pp in list_dirigeants_pp:
            # Define the columns for the dirigeant_pp table
            dirigeant_pp_columns = [
                "siren",
                "date_mise_a_jour_formalite",
                "date_mise_a_jour_rne",
                "nom",
                "nom_usage",
                "prenoms",
                "genre",
                "date_naissance",
                "lieu_naissance",
                "pays_naissance",
                "role",
                "libelle_role",
                "autre_role",
                "second_role",
                "libelle_second_role",
                "qualite_artisan",
                "nationalite",
                "mail",
                "telephone",
                "adresse_pays",
                "adresse_code_pays",
                "adresse_commune",
                "adresse_code_postal",
                "adresse_code_commune",
                "adresse_voie",
                "adresse_num_voie",
                "adresse_type_voie",
                "adresse_indice_repetition",
                "adresse_complement_localisation",
                "adresse_distribution_speciale",
                "date_debut",
                "date_fin",
                "actif",
                "role_conjoint",
                "nom_conjoint",
                "nom_usage_conjoint",
                "prenoms_conjoint",
                "genre_conjoint",
                "date_naissance_conjoint",
                "lieu_naissance_conjoint",
                "pays_naissance_conjoint",
                "nationalite_conjoint",
                "mail_conjoint",
                "telephone_conjoint",
                "adresse_conjoint_pays",
                "adresse_conjoint_code_pays",
                "adresse_conjoint_commune",
                "adresse_conjoint_code_postal",
                "adresse_conjoint_code_commune",
                "adresse_conjoint_voie",
                "adresse_conjoint_num_voie",
                "adresse_conjoint_type_voie",
                "adresse_conjoint_indice_repetition",
                "adresse_conjoint_complement_localisation",
                "adresse_conjoint_distribution_speciale",
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
                    unite_legale.date_mise_a_jour_formalite,
                    unite_legale.date_mise_a_jour_rne,
                    dirigeant_pp.nom,
                    dirigeant_pp.nom_usage,
                    dirigeant_pp.prenoms,
                    dirigeant_pp.genre,
                    dirigeant_pp.date_naissance,
                    dirigeant_pp.lieu_naissance,
                    dirigeant_pp.pays_naissance,
                    dirigeant_pp.role,
                    dirigeant_pp.libelle_role,
                    dirigeant_pp.autre_role,
                    dirigeant_pp.second_role,
                    dirigeant_pp.libelle_second_role,
                    dirigeant_pp.qualite_artisan,
                    dirigeant_pp.nationalite,
                    dirigeant_pp.mail,
                    dirigeant_pp.telephone,
                    dirigeant_pp.adresse.pays if dirigeant_pp.adresse else None,
                    dirigeant_pp.adresse.code_pays if dirigeant_pp.adresse else None,
                    dirigeant_pp.adresse.commune if dirigeant_pp.adresse else None,
                    dirigeant_pp.adresse.code_postal if dirigeant_pp.adresse else None,
                    dirigeant_pp.adresse.code_commune if dirigeant_pp.adresse else None,
                    dirigeant_pp.adresse.voie if dirigeant_pp.adresse else None,
                    dirigeant_pp.adresse.num_voie if dirigeant_pp.adresse else None,
                    dirigeant_pp.adresse.type_voie if dirigeant_pp.adresse else None,
                    dirigeant_pp.adresse.indice_repetition
                    if dirigeant_pp.adresse
                    else None,
                    dirigeant_pp.adresse.complement_localisation
                    if dirigeant_pp.adresse
                    else None,
                    dirigeant_pp.adresse.distribution_speciale
                    if dirigeant_pp.adresse
                    else None,
                    dirigeant_pp.date_debut,
                    dirigeant_pp.date_fin,
                    dirigeant_pp.actif,
                    dirigeant_pp.role_conjoint,
                    dirigeant_pp.nom_conjoint,
                    dirigeant_pp.nom_usage_conjoint,
                    dirigeant_pp.prenoms_conjoint,
                    dirigeant_pp.genre_conjoint,
                    dirigeant_pp.date_naissance_conjoint,
                    dirigeant_pp.lieu_naissance_conjoint,
                    dirigeant_pp.pays_naissance_conjoint,
                    dirigeant_pp.nationalite_conjoint,
                    dirigeant_pp.mail_conjoint,
                    dirigeant_pp.telephone_conjoint,
                    dirigeant_pp.adresse_conjoint.pays
                    if dirigeant_pp.adresse_conjoint
                    else None,
                    dirigeant_pp.adresse_conjoint.code_pays
                    if dirigeant_pp.adresse_conjoint
                    else None,
                    dirigeant_pp.adresse_conjoint.commune
                    if dirigeant_pp.adresse_conjoint
                    else None,
                    dirigeant_pp.adresse_conjoint.code_postal
                    if dirigeant_pp.adresse_conjoint
                    else None,
                    dirigeant_pp.adresse_conjoint.code_commune
                    if dirigeant_pp.adresse_conjoint
                    else None,
                    dirigeant_pp.adresse_conjoint.voie
                    if dirigeant_pp.adresse_conjoint
                    else None,
                    dirigeant_pp.adresse_conjoint.num_voie
                    if dirigeant_pp.adresse_conjoint
                    else None,
                    dirigeant_pp.adresse_conjoint.type_voie
                    if dirigeant_pp.adresse_conjoint
                    else None,
                    dirigeant_pp.adresse_conjoint.indice_repetition
                    if dirigeant_pp.adresse_conjoint
                    else None,
                    dirigeant_pp.adresse_conjoint.complement_localisation
                    if dirigeant_pp.adresse_conjoint
                    else None,
                    dirigeant_pp.adresse_conjoint.distribution_speciale
                    if dirigeant_pp.adresse_conjoint
                    else None,
                    dirigeant_pp.situation_matrimoniale,
                    file_path,
                ),
            )

        for dirigeant_pm in list_dirigeants_pm:
            # Define the columns for the dirigeant_pm table
            dirigeant_pm_columns = [
                "siren",
                "siren_dirigeant",
                "date_mise_a_jour_formalite",
                "date_mise_a_jour_rne",
                "denomination",
                "role",
                "libelle_role",
                "autre_role",
                "second_role",
                "libelle_second_role",
                "qualite_artisan",
                "pays",
                "forme_juridique",
                "lieu_registre",
                "date_debut",
                "date_fin",
                "actif",
                "nom_individu_representant",
                "nom_usage_individu_representant",
                "prenoms_individu_representant",
                "genre_individu_representant",
                "date_naissance_individu_representant",
                "lieu_naissance_individu_representant",
                "pays_naissance_individu_representant",
                "nationalite_individu_representant",
                "adresse_individu_representant_pays",
                "adresse_individu_representant_code_pays",
                "adresse_individu_representant_commune",
                "adresse_individu_representant_code_postal",
                "adresse_individu_representant_code_commune",
                "adresse_individu_representant_voie",
                "adresse_individu_representant_num_voie",
                "adresse_individu_representant_type_voie",
                "adresse_individu_representant_indice_repetition",
                "adresse_individu_representant_complement_localisation",
                "adresse_individu_representant_distribution_speciale",
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
                    unite_legale.date_mise_a_jour_formalite,
                    unite_legale.date_mise_a_jour_rne,
                    dirigeant_pm.denomination,
                    dirigeant_pm.role,
                    dirigeant_pm.libelle_role,
                    dirigeant_pm.autre_role,
                    dirigeant_pm.second_role,
                    dirigeant_pm.libelle_second_role,
                    dirigeant_pm.qualite_artisan,
                    dirigeant_pm.pays,
                    dirigeant_pm.forme_juridique,
                    dirigeant_pm.lieu_registre,
                    dirigeant_pm.date_debut,
                    dirigeant_pm.date_fin,
                    dirigeant_pm.actif,
                    dirigeant_pm.nom_individu_representant,
                    dirigeant_pm.nom_usage_individu_representant,
                    dirigeant_pm.prenoms_individu_representant,
                    dirigeant_pm.genre_individu_representant,
                    dirigeant_pm.date_naissance_individu_representant,
                    dirigeant_pm.lieu_naissance_individu_representant,
                    dirigeant_pm.pays_naissance_individu_representant,
                    dirigeant_pm.nationalite_individu_representant,
                    dirigeant_pm.adresse_individu_representant.pays
                    if dirigeant_pm.adresse_individu_representant
                    else None,
                    dirigeant_pm.adresse_individu_representant.code_pays
                    if dirigeant_pm.adresse_individu_representant
                    else None,
                    dirigeant_pm.adresse_individu_representant.commune
                    if dirigeant_pm.adresse_individu_representant
                    else None,
                    dirigeant_pm.adresse_individu_representant.code_postal
                    if dirigeant_pm.adresse_individu_representant
                    else None,
                    dirigeant_pm.adresse_individu_representant.code_commune
                    if dirigeant_pm.adresse_individu_representant
                    else None,
                    dirigeant_pm.adresse_individu_representant.voie
                    if dirigeant_pm.adresse_individu_representant
                    else None,
                    dirigeant_pm.adresse_individu_representant.num_voie
                    if dirigeant_pm.adresse_individu_representant
                    else None,
                    dirigeant_pm.adresse_individu_representant.type_voie
                    if dirigeant_pm.adresse_individu_representant
                    else None,
                    dirigeant_pm.adresse_individu_representant.indice_repetition
                    if dirigeant_pm.adresse_individu_representant
                    else None,
                    dirigeant_pm.adresse_individu_representant.complement_localisation
                    if dirigeant_pm.adresse_individu_representant
                    else None,
                    dirigeant_pm.adresse_individu_representant.distribution_speciale
                    if dirigeant_pm.adresse_individu_representant
                    else None,
                    file_path,
                ),
            )

        immatriculation = unite_legale.immatriculation

        # Define the columns for the immatriculation table
        immat_columns = [
            "siren",
            "date_mise_a_jour_formalite",
            "date_mise_a_jour_rne",
            "date_immatriculation",
            "date_radiation",
            "capital_social",
            "date_cloture_exercice",
            "duree_personne_morale",
            "nature_entreprise",
            "date_debut_activite",
            "capital_variable",
            "devise_capital",
            "file_name",
        ]

        cursor.execute(
            f"""
                INSERT INTO immatriculation ({", ".join(immat_columns)})
                VALUES ({", ".join(["?"] * len(immat_columns))})
            """,
            (
                unite_legale.siren,
                unite_legale.date_mise_a_jour_formalite,
                unite_legale.date_mise_a_jour_rne,
                immatriculation.date_immatriculation,
                immatriculation.date_radiation,
                immatriculation.capital_social,
                immatriculation.date_cloture_exercice,
                immatriculation.duree_personne_morale,
                json.dumps(immatriculation.nature_entreprise),
                immatriculation.date_debut_activite,
                immatriculation.capital_variable,
                immatriculation.devise_capital,
                file_path,
            ),
        )

        # Insert beneficiaires effectifs
        if unite_legale.beneficiaires_effectifs:
            for beneficiaire in unite_legale.beneficiaires_effectifs:
                beneficiaire_columns = [
                    "siren",
                    "nom",
                    "nom_usage",
                    "prenoms",
                    "genre",
                    "date_naissance",
                    "lieu_naissance",
                    "pays_naissance",
                    "adresse_pays",
                    "adresse_code_pays",
                    "adresse_commune",
                    "adresse_code_postal",
                    "adresse_code_commune",
                    "adresse_voie",
                    "adresse_num_voie",
                    "adresse_type_voie",
                    "adresse_indice_repetition",
                    "adresse_complement_localisation",
                    "adresse_distribution_speciale",
                    "date_effet",
                    "detention_part_directe",
                    "modalites_de_controle",
                    "detention_25p_capital",
                    "detention_25p_droit_vote",
                    "date_mise_a_jour_formalite",
                    "date_mise_a_jour_rne",
                    "file_name",
                ]
                cursor.execute(
                    f"""
                    INSERT INTO beneficiaire_effectif ({", ".join(beneficiaire_columns)})
                    VALUES ({", ".join(["?"] * len(beneficiaire_columns))})
                    """,
                    (
                        unite_legale.siren,
                        beneficiaire.nom,
                        beneficiaire.nom_usage,
                        beneficiaire.prenoms,
                        beneficiaire.genre,
                        beneficiaire.date_naissance,
                        beneficiaire.lieu_naissance,
                        beneficiaire.pays_naissance,
                        beneficiaire.adresse.pays if beneficiaire.adresse else None,
                        beneficiaire.adresse.code_pays
                        if beneficiaire.adresse
                        else None,
                        beneficiaire.adresse.commune if beneficiaire.adresse else None,
                        beneficiaire.adresse.code_postal
                        if beneficiaire.adresse
                        else None,
                        beneficiaire.adresse.code_commune
                        if beneficiaire.adresse
                        else None,
                        beneficiaire.adresse.voie if beneficiaire.adresse else None,
                        beneficiaire.adresse.num_voie if beneficiaire.adresse else None,
                        beneficiaire.adresse.type_voie
                        if beneficiaire.adresse
                        else None,
                        beneficiaire.adresse.indice_repetition
                        if beneficiaire.adresse
                        else None,
                        beneficiaire.adresse.complement_localisation
                        if beneficiaire.adresse
                        else None,
                        beneficiaire.adresse.distribution_speciale
                        if beneficiaire.adresse
                        else None,
                        beneficiaire.modalite.date_effet
                        if beneficiaire.modalite
                        else None,
                        beneficiaire.modalite.detention_part_directe
                        if beneficiaire.modalite
                        else None,
                        json.dumps(beneficiaire.modalite.modalites_de_controle)
                        if beneficiaire.modalite
                        and beneficiaire.modalite.modalites_de_controle
                        else None,
                        beneficiaire.modalite.detention_25p_capital
                        if beneficiaire.modalite
                        else None,
                        beneficiaire.modalite.detention_25p_droit_vote
                        if beneficiaire.modalite
                        else None,
                        unite_legale.date_mise_a_jour_formalite,
                        unite_legale.date_mise_a_jour_rne,
                        file_path,
                    ),
                )

        # Insert historique data
        if unite_legale.historique:
            historique_columns = [
                "siren",
                "date_integration",
                "code_evenement",
                "libelle_evenement",
                "numero_liasse",
                "patch_id",
                "date_effet",
                "chemin_date_effet",
                "chemin_date_effet_id",
                "date_mise_a_jour_formalite",
                "date_mise_a_jour_rne",
                "file_name",
            ]
            for hist in unite_legale.historique:
                cursor.execute(
                    f"""
                    INSERT INTO historique ({", ".join(historique_columns)})
                    VALUES ({", ".join(["?"] * len(historique_columns))})
                    """,
                    (
                        unite_legale.siren,
                        hist.date_integration,
                        hist.code_evenement,
                        hist.libelle_evenement,
                        hist.numero_liasse,
                        hist.patch_id,
                        hist.date_effet,
                        hist.chemin_date_effet,
                        hist.chemin_date_effet_id,
                        unite_legale.date_mise_a_jour_formalite,
                        unite_legale.date_mise_a_jour_rne,
                        file_path,
                    ),
                )

        # Insert inscriptions office data
        if unite_legale.inscriptions_offices:
            for inscription in unite_legale.inscriptions_offices:
                inscription_columns = [
                    "siren",
                    "event",
                    "date_effet",
                    "observation",
                    "partner_center",
                    "partner_code",
                    "observation_complementaire",
                    "date_mise_a_jour_formalite",
                    "date_mise_a_jour_rne",
                    "file_name",
                ]
                cursor.execute(
                    f"""
                    INSERT INTO inscription_office ({", ".join(inscription_columns)})
                    VALUES ({", ".join(["?"] * len(inscription_columns))})
                    """,
                    (
                        unite_legale.siren,
                        inscription.event,
                        inscription.date_effet,
                        inscription.observation,
                        inscription.partner_center,
                        inscription.partner_code,
                        inscription.observation_complementaire,
                        unite_legale.date_mise_a_jour_formalite,
                        unite_legale.date_mise_a_jour_rne,
                        file_path,
                    ),
                )

                # Get the ID of the inserted inscription for mentions
                inscription_id = cursor.lastrowid

                # Insert mentions for this inscription
                if inscription.mentions:
                    for mention in inscription.mentions:
                        mention_columns = [
                            "siren",
                            "inscription_office_id",
                            "num_observation",
                            "date_effet",
                            "texte",
                            "date_mise_a_jour_formalite",
                            "date_mise_a_jour_rne",
                            "file_name",
                        ]
                        cursor.execute(
                            f"""
                            INSERT INTO mention ({", ".join(mention_columns)})
                            VALUES ({", ".join(["?"] * len(mention_columns))})
                            """,
                            (
                                unite_legale.siren,
                                inscription_id,
                                mention.num_observation,
                                mention.date_effet,
                                mention.texte,
                                unite_legale.date_mise_a_jour_formalite,
                                unite_legale.date_mise_a_jour_rne,
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

    cursor.execute("SELECT COUNT(*) FROM beneficiaire_effectif")
    count_beneficiaire = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM historique")
    count_historique = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM inscription_office")
    count_inscription = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM mention")
    count_mention = cursor.fetchone()[0]

    logging.info("************Database Counts after insertion:************")
    logging.info(f"Unite Legale: {count_ul}")
    logging.info(f"Siege: {count_siege}")
    logging.info(f"Dirigeant PP: {count_pp}")
    logging.info(f"Dirigeant PM: {count_pm}")
    logging.info(f"Immatriculation: {count_immat}")
    logging.info(f"Etablissement: {count_etab}")
    logging.info(f"Activite: {count_activite}")
    logging.info(f"Beneficiaire Effectif: {count_beneficiaire}")
    logging.info(f"Historique: {count_historique}")
    logging.info(f"Inscription Office: {count_inscription}")
    logging.info(f"Mention: {count_mention}")
    logging.info("************************************************")

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

    cursor.execute("SELECT COUNT(*) FROM etablissement")
    count_etab = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM activite")
    count_activite = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM beneficiaire_effectif")
    count_beneficiaire = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM historique")
    count_historique = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM inscription_office")
    count_inscription = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM mention")
    count_mention = cursor.fetchone()[0]

    connection.close()
    return (
        count_ul,
        count_siege,
        count_pp,
        count_pm,
        count_immat,
        count_etab,
        count_activite,
        count_beneficiaire,
        count_historique,
        count_inscription,
        count_mention,
    )


def extract_rne_data(entity, file_type="flux"):
    """Extract "unites legales" data from RNE json object."""

    company = entity.get("company", {}) if file_type == "flux" else entity

    rne_company = RNECompany.model_validate(company)

    unite_legale = UniteLegale()
    unite_legale_formatted = map_rne_company_to_ul(rne_company, unite_legale)

    return unite_legale_formatted
