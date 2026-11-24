create_table_dirigeant_pp_query = """
        CREATE TABLE IF NOT EXISTS dirigeant_pp
        (
            siren,
            date_mise_a_jour,
            date_de_naissance,
            role,
            nom,
            nom_usage,
            prenoms,
            nationalite,
            role_description,
            date_demission
        )
    """

create_table_dirigeant_pm_query = """
        CREATE TABLE IF NOT EXISTS dirigeant_pm
        (
            siren TEXT,
            date_mise_a_jour DATE,
            denomination TEXT,
            siren_dirigeant TEXT,
            role TEXT,
            forme_juridique TEXT,
            role_description TEXT,
            date_demission DATE
        )
    """


# mention_demission = 0 likely means the resignation has been canceled
DATE_DEMISSION = """
    CASE WHEN mention_demission = 1 THEN date_mention_demission END AS date_demission
"""


def get_chunk_dirig_pm_from_db_query(chunk_size, iterator):
    query = f"""
        SELECT siren, date_mise_a_jour, denomination,
        siren_dirigeant, role, forme_juridique, {DATE_DEMISSION}
        FROM dirigeant_pm
        WHERE siren IN
        (
            SELECT DISTINCT siren
            FROM dirigeant_pm
            WHERE siren != ''
            LIMIT {chunk_size}
            OFFSET {int(iterator * chunk_size)})
        """
    return query


def get_chunk_dirig_pp_from_db_query(chunk_size, iterator):
    query = f"""
        SELECT siren, date_mise_a_jour, date_de_naissance, role,
        nom, nom_usage, prenoms, nationalite, {DATE_DEMISSION}
        FROM dirigeant_pp
        WHERE siren IN
            (
            SELECT DISTINCT siren
            FROM dirigeant_pp
            WHERE siren != ''
            LIMIT {chunk_size}
            OFFSET {int(iterator * chunk_size)})
        """
    return query


def add_boolean_column_query(table, column):
    return f"""
        ALTER TABLE {table}
        ADD COLUMN {column} INTEGER DEFAULT 0
    """


get_dirigeant_pp_from_sirens_query = """
        SELECT rowid, siren, nom, prenoms, date_de_naissance
        FROM dirigeant_pp
        WHERE siren IN (SELECT value FROM json_each(?))
    """

update_est_protege_in_dirigeant_pp_query = """
        UPDATE dirigeant_pp
        SET est_protege = 1
        WHERE rowid IN (SELECT value FROM json_each(?))
    """

update_est_protege_unite_legale_query = """
        UPDATE unite_legale
        SET est_protege = 1
        WHERE siren IN (SELECT value FROM json_each(?))
    """

update_est_protege_legacy_unite_legale_query = """
        UPDATE unite_legale
        SET est_protege_legacy = 1
        WHERE siren IN (SELECT value FROM json_each(?))
    """
