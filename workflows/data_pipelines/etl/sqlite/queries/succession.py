create_table_liens_succession_query = """
    CREATE TABLE IF NOT EXISTS liens_succession
    (
        siret_predecesseur TEXT NOT NULL,
        siret_successeur TEXT NOT NULL,
        date_lien_succession DATE,
        transfert_siege INT,
        continuite_economique INT
    )
"""
