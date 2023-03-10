count_etablissements_ouverts_query = """
        INSERT INTO count_etab_ouvert (siren, count)
        SELECT siren, count(*) as count
        FROM siret
        WHERE etat_administratif_etablissement = 'A' GROUP BY siren;
        """
