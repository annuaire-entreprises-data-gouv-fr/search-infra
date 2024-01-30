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
            date_mise_a_jour_rne DATE
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
            date_mise_a_jour_rne DATE
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
            date_mise_a_jour_rne
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
            a.date_mise_a_jour_rne
        FROM flux_unite_legale a LEFT JOIN unite_legale b
        ON a.siren = b.siren
    """

# Update existing rows in unite_legale based on siren from rne.unites_legales
update_main_table_fields_with_rne_data_query = """
            UPDATE unite_legale
            SET from_rne = TRUE,
                date_mise_a_jour_rne = (
                    SELECT date_mise_a_jour
                    FROM db_rne.unites_legales
                    WHERE unite_legale.siren = db_rne.unites_legales.siren
                    )
            WHERE siren IN (SELECT siren FROM db_rne.unites_legales)
        """

insert_remaining_rne_data_into_main_table_query = """
            INSERT OR IGNORE INTO unite_legale
            SELECT DISTINCT
                siren,
                date_creation AS date_creation_unite_legale,
                NULL AS sigle,
                NULL AS prenom,
                NULL AS identifiant_association_unite_legale,
                tranche_effectif_salarie AS tranche_effectif_salarie_unite_legale,
                NULL AS categorie_entreprise,
                etat_administratif AS etat_administratif_unite_legale,
                NULL AS nom,
                NULL AS nom_usage,
                nom_commercial AS nom_raison_sociale,
                NULL AS denomination_usuelle_1,
                NULL AS denomination_usuelle_2,
                NULL AS denomination_usuelle_3,
                nature_juridique AS nature_juridique_unite_legale,
                activite_principale AS activite_principale_unite_legale,
                NULL AS economie_sociale_solidaire_unite_legale,
                statut_diffusion AS statut_diffusion_unite_legale,
                NULL AS est_societe_mission,
                NULL AS annee_categorie_entreprise,
                NULL AS annee_tranche_effectif_salarie,
                NULL AS caractere_employeur,
                FALSE AS from_insee,
                TRUE AS from_rne,
                NULL AS date_mise_a_jour_insee,
                date_mise_a_jour AS date_mise_a_jour_rne
            FROM db_rne.unites_legales
            WHERE siren NOT IN (SELECT siren FROM unite_legale)
        """
