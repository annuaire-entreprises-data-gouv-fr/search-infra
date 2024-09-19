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
            role_description
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
            role_description TEXT
        )
    """


def get_chunk_dirig_pm_from_db_query(chunk_size, iterator):
    query = f"""
        SELECT DISTINCT siren, date_mise_a_jour, denomination,
        siren_dirigeant, role, forme_juridique
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
        SELECT DISTINCT siren, date_mise_a_jour, date_de_naissance, role,
        nom, nom_usage, prenoms, nationalite
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
