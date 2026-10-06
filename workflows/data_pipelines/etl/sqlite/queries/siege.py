current_siege_sirets = """
        SELECT siege_siret
        FROM historique_unite_legale
        WHERE date_fin_periode IS NULL
"""

update_siege_fields_in_etablissement = f"""
        UPDATE etablissement
            SET est_siege = (
                CASE WHEN siret IN ({current_siege_sirets})
                then 'true' else 'false' end
            ),
            -- An ancien siège that became siège again is not an ancien siège
            ancien_siege = (
                CASE
                    WHEN siret IN ({current_siege_sirets}) then 'false'
                    WHEN siret IN (
                        SELECT siege_siret
                        FROM historique_unite_legale
                        WHERE date_fin_periode IS NOT NULL
                    ) then 'true'
                    else 'false'
                end
            )
        """

# The WHERE clauses filtering on sièges must contain `est_siege = 'true'` verbatim
# for SQLite to use this partial index
create_index_siege_etablissement = """
        CREATE INDEX index_etablissement_siege
            ON etablissement (siren, siret)
            WHERE est_siege = 'true'
        """

update_siege_etablissement_with_rne_data_query = """
            UPDATE etablissement
            SET date_mise_a_jour_rne = (
                    SELECT date_mise_a_jour
                    FROM db_rne.siege
                    WHERE etablissement.siren = db_rne.siege.siren
                    )
            WHERE est_siege = 'true'
            AND siren IN (SELECT siren FROM db_rne.siege)
        """
