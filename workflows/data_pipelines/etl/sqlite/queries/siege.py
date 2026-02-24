create_table_siege_query = """CREATE TABLE IF NOT EXISTS siege
            (
            siren TEXT,
            siret TEXT PRIMARY KEY,
            date_creation DATE,
            tranche_effectif_salarie TEXT,
            caractere_employeur TEXT,
            annee_tranche_effectif_salarie TEXT,
            activite_principale_registre_metier TEXT,
            est_siege TEXT,
            numero_voie TEXT,
            type_voie TEXT,
            libelle_voie TEXT,
            code_postal TEXT,
            libelle_cedex TEXT,
            libelle_commune TEXT,
            commune TEXT,
            complement_adresse TEXT,
            cedex TEXT,
            date_debut_activite DATE,
            distribution_speciale TEXT,
            etat_administratif_etablissement TEXT,
            enseigne_1 TEXT,
            enseigne_2 TEXT,
            enseigne_3 TEXT,
            activite_principale TEXT,
            activite_principale_naf25 TEXT,
            indice_repetition TEXT,
            nom_commercial TEXT,
            libelle_commune_etranger TEXT,
            code_pays_etranger TEXT,
            libelle_pays_etranger TEXT,
            x TEXT,
            y TEXT,
            latitude TEXT,
            longitude TEXT,
            geo_adresse TEXT,
            geo_id TEXT,
            geo_score TEXT,
            statut_diffusion_etablissement TEXT,
            date_mise_a_jour_insee DATE,
            date_mise_a_jour_rne DATE,
            date_fermeture_etablissement DATE
            )
    """

populate_table_siege_query = """INSERT INTO siege (
            siren,
            siret,
            date_creation,
            tranche_effectif_salarie,
            caractere_employeur,
            annee_tranche_effectif_salarie,
            activite_principale_registre_metier,
            est_siege,
            numero_voie,
            type_voie,
            libelle_voie,
            code_postal,
            libelle_cedex,
            libelle_commune,
            commune,
            complement_adresse,
            cedex,
            date_debut_activite,
            distribution_speciale,
            etat_administratif_etablissement,
            enseigne_1,
            enseigne_2,
            enseigne_3,
            activite_principale,
            activite_principale_naf25,
            indice_repetition,
            nom_commercial,
            libelle_commune_etranger,
            code_pays_etranger,
            libelle_pays_etranger,
            x,
            y,
            latitude,
            longitude,
            geo_adresse,
            geo_id,
            geo_score,
            statut_diffusion_etablissement,
            date_mise_a_jour_insee,
            date_mise_a_jour_rne,
            date_fermeture_etablissement
            )
        SELECT
            e.siren,
            e.siret,
            e.date_creation,
            e.tranche_effectif_salarie,
            e.caractere_employeur,
            e.annee_tranche_effectif_salarie,
            e.activite_principale_registre_metier,
            'true' as est_siege,
            e.numero_voie,
            e.type_voie,
            e.libelle_voie,
            e.code_postal,
            e.libelle_cedex,
            e.libelle_commune,
            e.commune,
            e.complement_adresse,
            e.cedex,
            e.date_debut_activite,
            e.distribution_speciale,
            e.etat_administratif_etablissement,
            e.enseigne_1,
            e.enseigne_2,
            e.enseigne_3,
            e.activite_principale,
            e.activite_principale_naf25,
            e.indice_repetition,
            e.nom_commercial,
            e.libelle_commune_etranger,
            e.code_pays_etranger,
            e.libelle_pays_etranger,
            e.x,
            e.y,
            e.latitude,
            e.longitude,
            e.geo_adresse,
            e.geo_id,
            e.geo_score,
            e.statut_diffusion_etablissement,
            e.date_mise_a_jour_insee,
            e.date_mise_a_jour_rne,
            e.date_fermeture_etablissement
        FROM etablissement e
        INNER JOIN historique_unite_legale h ON e.siren = h.siren
        WHERE h.date_fin_periode IS NULL
        AND e.siret = h.siege_siret;
    """

update_est_siege_in_etablissement = """
        UPDATE etablissement
            SET est_siege = (
                CASE WHEN EXISTS (
                    SELECT 1
                    FROM siege
                    WHERE siege.siret = etablissement.siret
                ) then 'true' else 'false' end
        )
        """

update_siege_table_fields_with_rne_data_query = """
            UPDATE siege
            SET date_mise_a_jour_rne = (
                    SELECT date_mise_a_jour
                    FROM db_rne.siege
                    WHERE siege.siren = db_rne.siege.siren
                    )
            WHERE siren IN (SELECT siren FROM db_rne.siege)
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
            SELECT siret FROM siege
        );
    """
