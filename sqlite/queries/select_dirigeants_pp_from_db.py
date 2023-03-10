def get_chunk_dirig_pp_from_db_query(chunk_size, iterator):
    query = f"""
        SELECT DISTINCT siren, nom_patronymique, nom_usage, prenoms,
        datenaissance, villenaissance, paysnaissance, qualite
        FROM rep_pp
        WHERE siren IN
            (
            SELECT DISTINCT siren
            FROM rep_pp
            WHERE siren != ''
            LIMIT {chunk_size}
            OFFSET {int(iterator * chunk_size)})
        """
    return query
