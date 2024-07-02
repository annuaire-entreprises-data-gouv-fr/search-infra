create_table_etablissements_query = """CREATE TABLE IF NOT EXISTS siret
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
            complement_adresse_2 TEXT,
            numero_voie_2 TEXT,
            indice_repetition_2 TEXT,
            type_voie_2 TEXT,
            libelle_voie_2 TEXT,
            commune_2 TEXT,
            libelle_commune_2 TEXT,
            cedex_2 TEXT,
            libelle_cedex_2 TEXT,
            cedex TEXT,
            date_debut_activite DATE,
            distribution_speciale TEXT,
            distribution_speciale_2 TEXT,
            etat_administratif_etablissement TEXT,
            enseigne_1 TEXT,
            enseigne_2 TEXT,
            enseigne_3 TEXT,
            activite_principale TEXT,
            indice_repetition TEXT,
            nom_commercial TEXT,
            libelle_commune_etranger TEXT,
            code_pays_etranger TEXT,
            libelle_pays_etranger TEXT,
            libelle_commune_etranger_2 TEXT,
            code_pays_etranger_2 TEXT,
            libelle_pays_etranger_2 TEXT,
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

create_table_siret_siege_query = """CREATE TABLE IF NOT EXISTS siretsiege
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
            complement_adresse_2 TEXT,
            numero_voie_2 TEXT,
            indice_repetition_2 TEXT,
            type_voie_2 TEXT,
            libelle_voie_2 TEXT,
            commune_2 TEXT,
            libelle_commune_2 TEXT,
            cedex_2 TEXT,
            libelle_cedex_2 TEXT,
            cedex TEXT,
            date_debut_activite DATE,
            distribution_speciale TEXT,
            distribution_speciale_2 TEXT,
            etat_administratif_etablissement TEXT,
            enseigne_1 TEXT,
            enseigne_2 TEXT,
            enseigne_3 TEXT,
            activite_principale TEXT,
            indice_repetition TEXT,
            nom_commercial TEXT,
            libelle_commune_etranger TEXT,
            code_pays_etranger TEXT,
            libelle_pays_etranger TEXT,
            libelle_commune_etranger_2 TEXT,
            code_pays_etranger_2 TEXT,
            libelle_pays_etranger_2 TEXT,
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

create_table_flux_etablissements_query = """CREATE TABLE IF NOT EXISTS flux_siret
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
            complement_adresse_2 TEXT,
            numero_voie_2 TEXT,
            indice_repetition_2 TEXT,
            type_voie_2 TEXT,
            libelle_voie_2 TEXT,
            commune_2 TEXT,
            libelle_commune_2 TEXT,
            cedex_2 TEXT,
            libelle_cedex_2 TEXT,
            cedex TEXT,
            date_debut_activite DATE,
            distribution_speciale TEXT,
            distribution_speciale_2 TEXT,
            etat_administratif_etablissement TEXT,
            enseigne_1 TEXT,
            enseigne_2 TEXT,
            enseigne_3 TEXT,
            activite_principale TEXT,
            indice_repetition TEXT,
            nom_commercial TEXT,
            libelle_commune_etranger TEXT,
            code_pays_etranger TEXT,
            libelle_pays_etranger TEXT,
            libelle_commune_etranger_2 TEXT,
            code_pays_etranger_2 TEXT,
            libelle_pays_etranger_2 TEXT,
            statut_diffusion_etablissement TEXT,
            date_mise_a_jour_insee DATE,
            date_mise_a_jour_rne DATE,
            date_fermeture_etablissement DATE,
            x TEXT,
            y TEXT,
            latitude TEXT,
            longitude TEXT
            )
            """


create_table_count_etablissements_query = """CREATE TABLE count_etab (siren VARCHAR(
10), count INTEGER)"""

count_etablissements_ouverts_query = """
        INSERT INTO count_etab_ouvert (siren, count)
        SELECT siren, count(*) as count
        FROM siret
        WHERE etat_administratif_etablissement = 'A' GROUP BY siren;
        """

count_nombre_etablissements_ouverts_query = """
        INSERT INTO count_etab_ouvert (siren, count)
        SELECT siren, count(*) as count
        FROM siret
        WHERE etat_administratif_etablissement = 'A' GROUP BY siren;
        """

count_nombre_etablissements_query = """
        INSERT INTO count_etab (siren, count)
        SELECT siren, count(*) as count
        FROM siret GROUP BY siren;
        """

create_table_count_etablissements_ouverts_query = """CREATE TABLE count_etab_ouvert (
siren VARCHAR(10), count INTEGER)"""


populate_table_siret_siege_query = """INSERT INTO siretsiege (
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
            complement_adresse_2,
            numero_voie_2,
            indice_repetition_2,
            type_voie_2,
            libelle_voie_2,
            commune_2,
            libelle_commune_2,
            cedex_2,
            libelle_cedex_2,
            cedex,
            date_debut_activite,
            distribution_speciale,
            distribution_speciale_2,
            etat_administratif_etablissement,
            enseigne_1,
            enseigne_2,
            enseigne_3,
            activite_principale,
            indice_repetition,
            nom_commercial,
            libelle_commune_etranger,
            code_pays_etranger,
            libelle_pays_etranger,
            libelle_commune_etranger_2,
            code_pays_etranger_2,
            libelle_pays_etranger_2,
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
            complement_adresse_2,
            numero_voie_2,
            indice_repetition_2,
            type_voie_2,
            libelle_voie_2,
            commune_2,
            libelle_commune_2,
            cedex_2,
            libelle_cedex_2,
            cedex,
            date_debut_activite,
            distribution_speciale,
            distribution_speciale_2,
            etat_administratif_etablissement,
            enseigne_1,
            enseigne_2,
            enseigne_3,
            activite_principale,
            indice_repetition,
            nom_commercial,
            libelle_commune_etranger,
            code_pays_etranger,
            libelle_pays_etranger,
            libelle_commune_etranger_2,
            code_pays_etranger_2,
            libelle_pays_etranger_2,
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
        FROM siret
        WHERE est_siege = 'true';
    """

replace_table_siret_siege_query = """
        REPLACE INTO siretsiege
        (
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
            complement_adresse_2,
            numero_voie_2,
            indice_repetition_2,
            type_voie_2,
            libelle_voie_2,
            commune_2,
            libelle_commune_2,
            cedex_2,
            libelle_cedex_2,
            cedex,
            date_debut_activite,
            distribution_speciale,
            distribution_speciale_2,
            etat_administratif_etablissement,
            enseigne_1,
            enseigne_2,
            enseigne_3,
            activite_principale,
            indice_repetition,
            nom_commercial,
            libelle_commune_etranger,
            code_pays_etranger,
            libelle_pays_etranger,
            libelle_commune_etranger_2,
            code_pays_etranger_2,
            libelle_pays_etranger_2,
            caractere_employeur,
            statut_diffusion_etablissement,
            date_mise_a_jour_insee,
            date_mise_a_jour_rne,
            date_fermeture_etablissement,
            x,
            y
        ) SELECT
            a.siren,
            a.siret,
            a.date_creation,
            a.tranche_effectif_salarie,
            a.caractere_employeur,
            a.annee_tranche_effectif_salarie,
            a.activite_principale_registre_metier,
            a.est_siege,
            a.numero_voie,
            a.type_voie,
            a.libelle_voie,
            a.code_postal,
            a.libelle_cedex,
            a.libelle_commune,
            a.commune,
            a.complement_adresse,
            a.complement_adresse_2,
            a.numero_voie_2,
            a.indice_repetition_2,
            a.type_voie_2,
            a.libelle_voie_2,
            a.commune_2,
            a.libelle_commune_2,
            a.cedex_2,
            a.libelle_cedex_2,
            a.cedex,
            a.date_debut_activite,
            a.distribution_speciale,
            a.distribution_speciale_2,
            a.etat_administratif_etablissement,
            a.enseigne_1,
            a.enseigne_2,
            a.enseigne_3,
            a.activite_principale,
            a.indice_repetition,
            a.nom_commercial,
            a.libelle_commune_etranger,
            a.code_pays_etranger,
            a.libelle_pays_etranger,
            a.libelle_commune_etranger_2,
            a.code_pays_etranger_2,
            a.libelle_pays_etranger_2,
            a.caractere_employeur,
            a.statut_diffusion_etablissement,
            a.date_mise_a_jour_insee,
            a.date_mise_a_jour_rne,
            a.date_fermeture_etablissement,
            a.x,
            a.y
        FROM flux_siret a LEFT JOIN siretsiege b
        ON a.siret = b.siret
        WHERE a.est_siege = 'true'
    """

replace_table_etablissement_query = """
        REPLACE INTO siret
        (
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
            complement_adresse_2,
            numero_voie_2,
            indice_repetition_2,
            type_voie_2,
            libelle_voie_2,
            commune_2,
            libelle_commune_2,
            cedex_2,
            libelle_cedex_2,
            cedex,
            date_debut_activite,
            distribution_speciale,
            distribution_speciale_2,
            etat_administratif_etablissement,
            enseigne_1,
            enseigne_2,
            enseigne_3,
            activite_principale,
            indice_repetition,
            nom_commercial,
            libelle_commune_etranger,
            code_pays_etranger,
            libelle_pays_etranger,
            libelle_commune_etranger_2,
            code_pays_etranger_2,
            libelle_pays_etranger_2,
            statut_diffusion_etablissement,
            date_mise_a_jour_insee,
            date_mise_a_jour_rne,
            date_fermeture_etablissement,
            x,
            y
        ) SELECT
            a.siren,
            a.siret,
            a.date_creation,
            a.tranche_effectif_salarie,
            a.caractere_employeur,
            a.annee_tranche_effectif_salarie,
            a.activite_principale_registre_metier,
            a.est_siege,
            a.numero_voie,
            a.type_voie,
            a.libelle_voie,
            a.code_postal,
            a.libelle_cedex,
            a.libelle_commune,
            a.commune,
            a.complement_adresse,
            a.complement_adresse_2,
            a.numero_voie_2,
            a.indice_repetition_2,
            a.type_voie_2,
            a.libelle_voie_2,
            a.commune_2,
            a.libelle_commune_2,
            a.cedex_2,
            a.libelle_cedex_2,
            a.cedex,
            a.date_debut_activite,
            a.distribution_speciale,
            a.distribution_speciale_2,
            a.etat_administratif_etablissement,
            a.enseigne_1,
            a.enseigne_2,
            a.enseigne_3,
            a.activite_principale,
            a.indice_repetition,
            a.nom_commercial,
            a.libelle_commune_etranger,
            a.code_pays_etranger,
            a.libelle_pays_etranger,
            a.libelle_commune_etranger_2,
            a.code_pays_etranger_2,
            a.libelle_pays_etranger_2,
            a.statut_diffusion_etablissement,
            a.date_mise_a_jour_insee,
            a.date_mise_a_jour_rne,
            a.date_fermeture_etablissement,
            a.x,
            a.y
        FROM flux_siret a LEFT JOIN siret b
        ON a.siret = b.siret
    """

update_sieges_table_fields_with_rne_data_query = """
            UPDATE siretsiege
            SET date_mise_a_jour_rne = (
                    SELECT date_mise_a_jour
                    FROM db_rne.sieges
                    WHERE siretsiege.siren = db_rne.sieges.siren
                    )
            WHERE siren IN (SELECT siren FROM db_rne.sieges)
        """

insert_remaining_rne_sieges_data_into_main_table_query = """
            INSERT OR IGNORE INTO siretsiege
            SELECT DISTINCT
                siren,
                siret,
                NULL AS date_creation,
                NULL AS tranche_effectif_salarie,
                NULL AS caractere_employeur,
                NULL AS annee_tranche_effectif_salarie,
                NULL AS activite_principale_registre_metier,
                'true' AS est_siege,
                num_voie AS numero_voie,
                type_voie,
                voie AS libelle_voie,
                code_postal,
                NULL AS libelle_cedex,
                commune AS libelle_commune,
                code_commune AS commune,
                complement_localisation AS complement_adresse,
                NULL AS complement_adresse_2,
                NULL AS numero_voie_2,
                NULL AS indice_repetition_2,
                NULL AS type_voie_2,
                NULL AS libelle_voie_2,
                NULL AS commune_2,
                NULL AS libelle_commune_2,
                NULL AS cedex_2,
                NULL AS libelle_cedex_2,
                NULL AS cedex,
                NULL AS date_debut_activite,
                NULL AS distribution_speciale,
                NULL AS distribution_speciale_2,
                NULL AS etat_administratif_etablissement,
                enseigne AS enseigne_1,
                NULL AS enseigne_2,
                NULL AS enseigne_3,
                NULL AS activite_principale,
                indice_repetition,
                nom_commercial,
                NULL AS libelle_commune_etranger,
                NULL AS code_pays_etranger,
                NULL AS libelle_pays_etranger,
                NULL AS libelle_commune_etranger_2,
                NULL AS code_pays_etranger_2,
                NULL AS libelle_pays_etranger_2,
                NULL as x,
                NULL as y,
                NULL as latitude,
                NULL as longitude,
                NULL as geo_adresse,
                NULL as geo_id,
                NULL as geo_score,
                NULL AS statut_diffusion_etablissement,
                NULL as date_mise_a_jour_insee,
                date_mise_a_jour AS date_mise_a_jour_rne,
                NULL as date_fermeture_etablissement
                FROM db_rne.sieges
                WHERE siren NOT IN (SELECT siren FROM siretsiege)
        """

create_table_historique_etablissement_query = """
        CREATE TABLE IF NOT EXISTS historique_etablissement
        (
            siren TEXT,
            siret TEXT,
            date_fin_periode DATE,
            date_debut_periode DATE,
            etat_administratif_etablissement TEXT,
            changement_etat_administratif_etablissement TEXT
        )
    """

create_table_date_fermeture_etablissement_query = """
        CREATE TABLE IF NOT EXISTS date_fermeture_etablissement AS
            SELECT siret, MAX(date_debut_periode) AS date_fermeture_etablissement
            FROM historique_etablissement
            WHERE etat_administratif_etablissement = 'F'
            AND changement_etat_administratif_etablissement = 'true'
            GROUP BY siret;
    """


insert_date_fermeture_etablissement_query = """
    UPDATE siret
    SET date_fermeture_etablissement = (
        SELECT date_fermeture_etablissement
        FROM date_fermeture_etablissement
        WHERE siret.siret = date_fermeture_etablissement.siret
    )
    WHERE siret.etat_administratif_etablissement = 'F'
"""

insert_date_fermeture_siege_query = """
    UPDATE siretsiege
    SET date_fermeture_etablissement = (
        SELECT date_fermeture_etablissement
        FROM date_fermeture_etablissement
        WHERE siretsiege.siret = date_fermeture_etablissement.siret
    )
    WHERE siretsiege.etat_administratif_etablissement = 'F'
"""
