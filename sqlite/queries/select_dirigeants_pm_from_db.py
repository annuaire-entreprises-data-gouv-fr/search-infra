def get_chunk_dirig_pm_from_db_query(chunk_size, iterator):
    query = f"""
        SELECT DISTINCT siren, siren_pm, denomination, sigle, qualite
        FROM rep_pm
        WHERE siren IN
        (
            SELECT DISTINCT siren
            FROM rep_pm
            WHERE siren != ''
            LIMIT {chunk_size}
            OFFSET {int(iterator * chunk_size)})
        """
    return query
