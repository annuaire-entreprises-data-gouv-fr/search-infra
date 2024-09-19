create_table_benef_query = """
        CREATE TABLE IF NOT EXISTS beneficiaire
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


def get_chunk_benef_from_db_query(chunk_size, iterator):
    query = f"""
        SELECT DISTINCT siren, date_mise_a_jour, date_de_naissance, role,
        nom, nom_usage, prenoms, nationalite
        FROM beneficiaire
        WHERE siren IN
            (
            SELECT DISTINCT siren
            FROM beneficiaire
            WHERE siren != ''
            LIMIT {chunk_size}
            OFFSET {int(iterator * chunk_size)})
        """
    return query
