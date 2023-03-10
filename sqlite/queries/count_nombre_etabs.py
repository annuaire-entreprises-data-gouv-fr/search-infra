count_nombre_etablissements_query = """
        INSERT INTO count_etab (siren, count)
        SELECT siren, count(*) as count
        FROM siret GROUP BY siren;
        """
