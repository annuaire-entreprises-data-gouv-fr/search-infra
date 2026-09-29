update_est_siege_in_etablissement = """
        UPDATE etablissement
            SET est_siege = (
                CASE WHEN siret IN (
                    SELECT siege_siret
                    FROM historique_unite_legale
                    WHERE date_fin_periode IS NULL
                ) then 'true' else 'false' end
        )
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

create_table_ancien_siege_query = """
        CREATE TABLE IF NOT EXISTS ancien_siege
        (
            siren TEXT,
            nic_siege TEXT,
            siret TEXT
        )
    """

populate_ancien_siege_from_historique_query = """
        INSERT INTO ancien_siege (siren, nic_siege, siret)
        SELECT DISTINCT siren, nic_siege, siege_siret
        FROM historique_unite_legale
        WHERE date_fin_periode IS NOT NULL;
    """

delete_current_siege_from_ancien_siege_query = """
        DELETE FROM ancien_siege
        WHERE siret IN (
            SELECT siret FROM etablissement WHERE est_siege = 'true'
        );
    """
