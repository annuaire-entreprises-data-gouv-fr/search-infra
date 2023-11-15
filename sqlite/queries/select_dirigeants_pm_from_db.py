def get_chunk_dirig_pm_from_db_query(chunk_size, iterator):
    query = f"""
        SELECT DISTINCT siren, date_mise_a_jour, denomination,
        siren_dirigeant, role, forme_juridique
        FROM dirigeants_pm
        WHERE siren IN
        (
            SELECT DISTINCT siren
            FROM dirigeants_pm
            WHERE siren != ''
            LIMIT {chunk_size}
            OFFSET {int(iterator * chunk_size)})
        """
    return query
