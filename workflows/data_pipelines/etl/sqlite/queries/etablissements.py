create_table_etablissement_query = """CREATE TABLE IF NOT EXISTS etablissement
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
            coord_source TEXT,
            statut_diffusion_etablissement TEXT,
            date_mise_a_jour_insee DATE,
            date_mise_a_jour_rne DATE,
            date_fermeture_etablissement DATE
            )
            """


create_table_flux_etablissement_query = """CREATE TABLE IF NOT EXISTS flux_etablissement
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
            statut_diffusion_etablissement TEXT,
            date_mise_a_jour_insee DATE,
            date_mise_a_jour_rne DATE,
            date_fermeture_etablissement DATE,
            x TEXT,
            y TEXT,
            latitude TEXT,
            longitude TEXT,
            coord_source TEXT
            )
            """


create_table_count_etablissement_query = """
        CREATE TABLE count_etablissement
        (siren VARCHAR(10), count INTEGER)"""


create_table_count_etablissement_ouvert_query = """
        CREATE TABLE count_etablissement_ouvert (
        siren VARCHAR(10), count INTEGER)
        """

count_nombre_etablissement_ouvert_query = """
        INSERT INTO count_etablissement_ouvert (siren, count)
        SELECT siren, count(*) as count
        FROM etablissement
        WHERE etat_administratif_etablissement = 'A' GROUP BY siren;
        """

count_nombre_etablissement_query = """
        INSERT INTO count_etablissement (siren, count)
        SELECT siren, count(*) as count
        FROM etablissement GROUP BY siren;
        """


replace_table_etablissement_query = """
        REPLACE INTO etablissement
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
            statut_diffusion_etablissement,
            date_mise_a_jour_insee,
            date_mise_a_jour_rne,
            date_fermeture_etablissement,
            x,
            y,
            latitude,
            longitude,
            coord_source
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
            a.cedex,
            a.date_debut_activite,
            a.distribution_speciale,
            a.etat_administratif_etablissement,
            a.enseigne_1,
            a.enseigne_2,
            a.enseigne_3,
            a.activite_principale,
            a.activite_principale_naf25,
            a.indice_repetition,
            a.nom_commercial,
            a.libelle_commune_etranger,
            a.code_pays_etranger,
            a.libelle_pays_etranger,
            a.statut_diffusion_etablissement,
            a.date_mise_a_jour_insee,
            a.date_mise_a_jour_rne,
            a.date_fermeture_etablissement,
            a.x,
            a.y,
            a.latitude,
            a.longitude,
            a.coord_source
        FROM flux_etablissement a
    """


create_table_geo_stats_query = """
        CREATE TABLE IF NOT EXISTS geo_stats
        (
            siret TEXT,
            latitude TEXT,
            longitude TEXT
        )
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
    UPDATE etablissement
    SET date_fermeture_etablissement = (
        SELECT date_fermeture_etablissement
        FROM date_fermeture_etablissement
        WHERE etablissement.siret = date_fermeture_etablissement.siret
    )
    WHERE etablissement.etat_administratif_etablissement = 'F'
"""


update_etablissement_coordinates_from_geo_stats_query = """
UPDATE etablissement
SET
    latitude = (
        SELECT geo_stats.latitude
        FROM geo_stats
        WHERE geo_stats.siret = etablissement.siret
    ),
    longitude = (
        SELECT geo_stats.longitude
        FROM geo_stats
        WHERE geo_stats.siret = etablissement.siret
    ),
    coord_source = 'geo_stats'
WHERE EXISTS (
    SELECT 1
    FROM geo_stats
    WHERE geo_stats.siret = etablissement.siret
      AND geo_stats.latitude IS NOT NULL
      AND geo_stats.longitude IS NOT NULL
    )
"""
