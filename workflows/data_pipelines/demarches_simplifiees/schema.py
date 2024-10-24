class DatabaseSchema:
    @staticmethod
    def get_schema_statements() -> list[str]:
        return [
            """
            CREATE TABLE IF NOT EXISTS demarches (
                id TEXT PRIMARY KEY,
                number INTEGER,
                title TEXT,
                state TEXT,
                date_creation TEXT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS dossiers (
                id TEXT PRIMARY KEY,
                demarche_id TEXT,
                number INTEGER,
                archived BOOLEAN,
                state TEXT,
                date_derniere_modification TEXT,
                date_depot TEXT,
                date_passage_en_construction TEXT,
                date_passage_en_instruction TEXT,
                date_traitement TEXT,
                date_expiration TEXT,
                date_suppression_par_usager TEXT,
                usager_email TEXT,
                FOREIGN KEY (demarche_id) REFERENCES demarches(id)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS entreprises (
                dossier_id TEXT PRIMARY KEY,
                siren TEXT,
                forme_juridique TEXT,
                forme_juridique_code TEXT,
                nom_commercial TEXT,
                raison_sociale TEXT,
                nom TEXT,
                prenom TEXT,
                FOREIGN KEY (dossier_id) REFERENCES dossiers(id)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS demandeurs (
                dossier_id TEXT PRIMARY KEY,
                siret TEXT,
                siege_social BOOLEAN,
                FOREIGN KEY (dossier_id) REFERENCES dossiers(id)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS traitements (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                dossier_id TEXT,
                state TEXT,
                email_agent_traitant TEXT,
                date_traitement TEXT,
                motivation TEXT,
                FOREIGN KEY (dossier_id) REFERENCES dossiers(id)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS champs (
                id TEXT PRIMARY KEY,
                dossier_id TEXT,
                champ_descriptor_id TEXT,
                type_name TEXT,
                label TEXT,
                string_value TEXT,
                updated_at TEXT,
                prefilled BOOLEAN,
                FOREIGN KEY (dossier_id) REFERENCES dossiers(id)
            )
            """,
        ]
