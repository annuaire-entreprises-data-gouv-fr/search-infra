# Toutes les requêtes doivent être triées par code commune

# Établissements actifs employeurs, hors personnes physiques (1000 et 2xxx).
ETABLISSEMENTS_QUERY = """
    SELECT
        e.commune,
        ul.nom_raison_sociale,
        e.siret,
        e.latitude,
        e.longitude
    FROM etablissement AS e
    INNER JOIN unite_legale AS ul ON ul.siren = e.siren
    WHERE e.commune IS NOT NULL
        AND e.etat_administratif_etablissement = 'A'
        AND e.caractere_employeur = 'O'
        AND ul.nature_juridique_unite_legale != '1000'
        AND ul.nature_juridique_unite_legale NOT LIKE '2%'
    ORDER BY e.commune, e.siret
"""

# Pour un établissement fermé, date_debut_activite est la date de début de la
# période "fermé" : les ouvertures se comptent donc sur date_creation.
FLUX_QUERY = """
    SELECT commune, mois, SUM(ouverture), SUM(fermeture)
    FROM (
        SELECT commune, substr(date_creation, 1, 7) AS mois, 1 AS ouverture, 0 AS fermeture
        FROM etablissement
        WHERE commune IS NOT NULL
            AND date_creation >= :date_debut
            AND date_creation <= :date_fin
        UNION ALL
        SELECT commune, substr(date_fermeture_etablissement, 1, 7), 0, 1
        FROM etablissement
        WHERE commune IS NOT NULL
            AND date_fermeture_etablissement >= :date_debut
            AND date_fermeture_etablissement <= :date_fin
    )
    GROUP BY commune, mois
    ORDER BY commune, mois
"""

EFFECTIFS_QUERY = """
    SELECT code_commune, annee, grand_secteur_activite, effectif
    FROM effectifs
    ORDER BY code_commune
"""
