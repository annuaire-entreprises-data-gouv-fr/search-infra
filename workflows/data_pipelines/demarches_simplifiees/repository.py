# database/repository.py
import sqlite3
import logging
from contextlib import contextmanager
from dag_datalake_sirene.workflows.data_pipelines.demarches_simplifiees.models import (
    DemarcheResponse,
    Dossier,
)
from dag_datalake_sirene.workflows.data_pipelines.demarches_simplifiees.schema import (
    DatabaseSchema,
)


class DatabaseRepository:
    def __init__(self, db_path: str):
        self.db_path = db_path

    @contextmanager
    def get_connection(self):
        connection = sqlite3.connect(self.db_path)
        try:
            yield connection
        finally:
            connection.close()

    def initialize_database(self):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            for statement in DatabaseSchema.get_schema_statements():
                cursor.execute(statement)
            conn.commit()

    def save_demarche(self, demarche: DemarcheResponse):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                # Save demarche
                cursor.execute(
                    """
                    INSERT OR REPLACE INTO demarches (
                        id, number, title, state, date_creation
                        )
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        demarche.id,
                        demarche.number,
                        demarche.title,
                        demarche.state,
                        demarche.date_creation.isoformat(),
                    ),
                )

                # Save dossiers and related data
                for dossier in demarche.dossiers:
                    self._save_dossier(cursor, demarche.id, dossier)

                conn.commit()
            except Exception as e:
                conn.rollback()
                logging.error(f"Error saving demarche: {e}")
                raise

    def _save_dossier(self, cursor: sqlite3.Cursor, demarche_id: str, dossier: Dossier):
        # Save dossier
        cursor.execute(
            """
            INSERT OR REPLACE INTO dossiers (
                id, demarche_id, number, archived, state,
                date_derniere_modification, date_depot,
                date_passage_en_construction, date_passage_en_instruction,
                date_traitement, date_expiration,
                date_suppression_par_usager, usager_email
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                dossier.id,
                demarche_id,
                dossier.number,
                dossier.archived,
                dossier.state,
                dossier.date_derniere_modification.isoformat(),
                dossier.date_depot.isoformat(),
                dossier.date_passage_en_construction.isoformat(),
                dossier.date_passage_en_instruction.isoformat(),
                (
                    dossier.date_traitement.isoformat()
                    if dossier.date_traitement
                    else None
                ),
                (
                    dossier.date_expiration.isoformat()
                    if dossier.date_expiration
                    else None
                ),
                (
                    dossier.date_suppression_par_usager.isoformat()
                    if dossier.date_suppression_par_usager
                    else None
                ),
                dossier.usager_email,
            ),
        )

        # Save demandeur
        if dossier.demandeur:
            cursor.execute(
                """
                INSERT OR REPLACE INTO demandeurs (dossier_id, siret, siege_social)
                VALUES (?, ?, ?)
                """,
                (dossier.id, dossier.demandeur.siret, dossier.demandeur.siege_social),
            )

            # Save entreprise
            if dossier.demandeur.entreprise:
                cursor.execute(
                    """
                    INSERT OR REPLACE INTO entreprises (
                        dossier_id, siren, forme_juridique, forme_juridique_code,
                        nom_commercial, raison_sociale, nom, prenom
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        dossier.id,
                        dossier.demandeur.entreprise.siren,
                        dossier.demandeur.entreprise.forme_juridique,
                        dossier.demandeur.entreprise.forme_juridique_code,
                        dossier.demandeur.entreprise.nom_commercial,
                        dossier.demandeur.entreprise.raison_sociale,
                        dossier.demandeur.entreprise.nom,
                        dossier.demandeur.entreprise.prenom,
                    ),
                )

        # Save traitements
        for traitement in dossier.traitements:
            cursor.execute(
                """
                INSERT INTO traitements (
                    dossier_id, state, email_agent_traitant,
                    date_traitement, motivation
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    dossier.id,
                    traitement.state,
                    traitement.email_agent_traitant,
                    (
                        traitement.date_traitement.isoformat()
                        if traitement.date_traitement
                        else None
                    ),
                    traitement.motivation,
                ),
            )

        # Save champs
        for champ in dossier.champs:
            cursor.execute(
                """
                INSERT OR REPLACE INTO champs (
                    id, dossier_id, champ_descriptor_id, type_name,
                    label, string_value, updated_at, prefilled
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    champ.id,
                    dossier.id,
                    champ.champ_descriptor_id,
                    champ.type_name,
                    champ.label,
                    champ.string_value,
                    champ.updated_at.isoformat(),
                    champ.prefilled,
                ),
            )
