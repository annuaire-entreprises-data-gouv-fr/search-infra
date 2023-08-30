create_table_rna_query = """CREATE TABLE IF NOT EXISTS rna
            (
            identifiant_association TEXT PRIMARY KEY,
            siret,
            date_creation,
            titre,
            type_voie,
            numero_voie,
            libelle_voie,
            code_postal,
            libelle_commune,
            commune,
            complement_adresse,
            indice_repetition,
            distribution_speciale,
            )
            """
