create_table_convention_collective_query = """
     CREATE TABLE IF NOT EXISTS convention_collective
     (
         siret,
         siren,
         liste_idcc_etablissement,
         liste_idcc_unite_legale,
         sirets_par_idcc
     )
    """
