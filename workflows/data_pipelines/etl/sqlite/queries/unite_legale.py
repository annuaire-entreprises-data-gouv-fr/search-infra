create_table_unite_legale_query = """
        CREATE TABLE IF NOT EXISTS unite_legale
        (
            siren TEXT,
            date_creation_unite_legale DATE,
            sigle TEXT,
            prenom TEXT,
            identifiant_association_unite_legale TEXT,
            tranche_effectif_salarie_unite_legale TEXT,
            categorie_entreprise TEXT,
            etat_administratif_unite_legale TEXT,
            nom TEXT,
            nom_usage TEXT,
            nom_raison_sociale TEXT,
            denomination_usuelle_1 TEXT,
            denomination_usuelle_2 TEXT,
            denomination_usuelle_3 TEXT,
            nature_juridique_unite_legale TEXT,
            activite_principale_unite_legale TEXT,
            economie_sociale_solidaire_unite_legale TEXT,
            statut_diffusion_unite_legale TEXT,
            est_societe_mission TEXT,
            annee_categorie_entreprise TEXT,
            annee_tranche_effectif_salarie TEXT,
            caractere_employeur TEXT,
            from_insee BOOLEAN,
            from_rne BOOLEAN DEFAULT FALSE,
            date_mise_a_jour_insee DATE,
            date_mise_a_jour_rne DATE,
            date_fermeture_unite_legale DATE
        )
    """


create_table_flux_unite_legale_query = """
        CREATE TABLE IF NOT EXISTS flux_unite_legale
        (
            siren TEXT,
            date_creation_unite_legale DATE,
            sigle TEXT,
            prenom TEXT,
            identifiant_association_unite_legale TEXT,
            tranche_effectif_salarie_unite_legale TEXT,
            categorie_entreprise TEXT,
            etat_administratif_unite_legale TEXT,
            nom TEXT,
            nom_usage TEXT,
            nom_raison_sociale TEXT,
            denomination_usuelle_1 TEXT,
            denomination_usuelle_2 TEXT,
            denomination_usuelle_3 TEXT,
            nature_juridique_unite_legale TEXT,
            activite_principale_unite_legale TEXT,
            economie_sociale_solidaire_unite_legale TEXT,
            statut_diffusion_unite_legale TEXT,
            est_societe_mission TEXT,
            annee_categorie_entreprise TEXT,
            annee_tranche_effectif_salarie TEXT,
            caractere_employeur TEXT,
            from_insee BOOLEAN,
            from_rne BOOLEAN DEFAULT FALSE,
            date_mise_a_jour_insee DATE,
            date_mise_a_jour_rne DATE,
            date_fermeture_unite_legale DATE
        )
    """

replace_table_unite_legale_query = """
        REPLACE INTO unite_legale
        (
            siren,
            date_creation_unite_legale,
            sigle,
            prenom,
            identifiant_association_unite_legale,
            tranche_effectif_salarie_unite_legale,
            categorie_entreprise,
            etat_administratif_unite_legale,
            nom,
            nom_usage,
            nom_raison_sociale,
            denomination_usuelle_1,
            denomination_usuelle_2,
            denomination_usuelle_3,
            nature_juridique_unite_legale,
            activite_principale_unite_legale,
            economie_sociale_solidaire_unite_legale,
            statut_diffusion_unite_legale,
            est_societe_mission,
            annee_categorie_entreprise,
            annee_tranche_effectif_salarie,
            caractere_employeur,
            from_insee,
            from_rne,
            date_mise_a_jour_insee,
            date_mise_a_jour_rne,
            date_fermeture_unite_legale
        ) SELECT
            a.siren,
            a.date_creation_unite_legale,
            a.sigle,
            a.prenom,
            a.identifiant_association_unite_legale,
            a.tranche_effectif_salarie_unite_legale,
            a.categorie_entreprise,
            a.etat_administratif_unite_legale,
            a.nom,
            a.nom_usage,
            a.nom_raison_sociale,
            a.denomination_usuelle_1,
            a.denomination_usuelle_2,
            a.denomination_usuelle_3,
            a.nature_juridique_unite_legale,
            a.activite_principale_unite_legale,
            a.economie_sociale_solidaire_unite_legale,
            a.statut_diffusion_unite_legale,
            a.est_societe_mission,
            a.annee_categorie_entreprise,
            a.annee_tranche_effectif_salarie,
            a.caractere_employeur,
            a.from_insee,
            a.from_rne,
            a.date_mise_a_jour_insee,
            a.date_mise_a_jour_rne,
            a.date_fermeture_unite_legale
        FROM flux_unite_legale a LEFT JOIN unite_legale b
        ON a.siren = b.siren
    """

# Update existing rows in unite_legale based on siren from rne.unite_legale
update_main_table_fields_with_rne_data_query = """
            UPDATE unite_legale
            SET from_rne = TRUE,
                date_mise_a_jour_rne = (
                    SELECT date_mise_a_jour
                    FROM db_rne.unite_legale
                    WHERE unite_legale.siren = db_rne.unite_legale.siren
                    )
            WHERE siren IN (SELECT siren FROM db_rne.unite_legale)
        """

insert_remaining_rne_data_into_main_table_query = """
            INSERT OR IGNORE INTO unite_legale
            SELECT DISTINCT
                siren,
                date_creation AS date_creation_unite_legale,
                NULL AS sigle,
                prenom AS prenom,
                NULL AS identifiant_association_unite_legale,
                tranche_effectif_salarie AS tranche_effectif_salarie_unite_legale,
                NULL AS categorie_entreprise,
                etat_administratif AS etat_administratif_unite_legale,
                nom AS nom,
                nom_usage AS nom_usage,
                denomination AS nom_raison_sociale,
                NULL AS denomination_usuelle_1,
                NULL AS denomination_usuelle_2,
                NULL AS denomination_usuelle_3,
                nature_juridique AS nature_juridique_unite_legale,
                activite_principale AS activite_principale_unite_legale,
                NULL AS economie_sociale_solidaire_unite_legale,
                NULL AS statut_diffusion_unite_legale,
                NULL AS est_societe_mission,
                NULL AS annee_categorie_entreprise,
                NULL AS annee_tranche_effectif_salarie,
                NULL AS caractere_employeur,
                FALSE AS from_insee,
                TRUE AS from_rne,
                NULL AS date_mise_a_jour_insee,
                date_mise_a_jour AS date_mise_a_jour_rne,
                NULL AS date_fermeture_unite_legale
            FROM db_rne.unite_legale
            WHERE siren NOT IN (SELECT siren FROM unite_legale)
        """


create_table_historique_unite_legale_query = """
        CREATE TABLE IF NOT EXISTS historique_unite_legale
        (
            siren TEXT,
            date_fin_periode DATE,
            date_debut_periode DATE,
            etat_administratif_unite_legale TEXT,
            changement_etat_administratif_unite_legale TEXT,
            nic_siege TEXT,
            changement_nic_siege_unite_legale TEXT
        )
    """

create_table_ancien_siege_query = """
        CREATE TABLE IF NOT EXISTS ancien_siege
        (
            siren TEXT,
            nic_siege TEXT,
            siret TEXT
        )
    """

delete_current_siege_from_ancien_siege_query = """
        DELETE FROM ancien_siege
        WHERE siret IN (SELECT siret FROM siege);
"""

create_table_date_fermeture_unite_legale_query = """
        CREATE TABLE IF NOT EXISTS date_fermeture_unite_legale AS
            SELECT siren, MAX(date_debut_periode) AS date_fermeture_unite_legale
            FROM historique_unite_legale
            WHERE etat_administratif_unite_legale = 'C'
            AND changement_etat_administratif_unite_legale = 'true'
            GROUP BY siren;
    """


insert_date_fermeture_unite_legale_query = """
    UPDATE unite_legale
    SET date_fermeture_unite_legale = (
        SELECT date_fermeture_unite_legale
        FROM date_fermeture_unite_legale
        WHERE unite_legale.siren = date_fermeture_unite_legale.siren
    )
    WHERE unite_legale.etat_administratif_unite_legale = 'C'
"""
