create_table_doublons_query = """
    CREATE TABLE IF NOT EXISTS doublons
    (
        siren_doublon TEXT NOT NULL,
        siren_conserve TEXT NOT NULL,
        date_dernier_traitement_doublon DATE
    )
"""
