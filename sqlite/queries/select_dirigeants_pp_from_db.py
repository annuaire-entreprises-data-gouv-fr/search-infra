def get_chunk_dirig_pp_from_db_query(chunk_size, iterator):
    query = f"""
        SELECT DISTINCT siren, date_mise_a_jour, date_de_naissance, role,
        nom, nom_usage, prenoms, nationalite
        FROM dirigeants_pp
        WHERE siren IN
            (
            SELECT DISTINCT siren
            FROM dirigeants_pp
            WHERE siren != ''
            LIMIT {chunk_size}
            OFFSET {int(iterator * chunk_size)})
        """
    return query
