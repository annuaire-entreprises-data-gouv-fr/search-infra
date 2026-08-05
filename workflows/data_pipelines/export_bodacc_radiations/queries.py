# Unités légales radiées au RCS d'après le BODACC mais toujours actives dans
# SIRENE et/ou dans le RNE.
#
# Chaque ligne décrit l'état de l'unité légale dans les trois sources.
# Les colonnes sont suffixées par leur source pour permettre de filtrer
# les différents types d'incohérences. Exemples :
#   - SIRENE actif, RNE actif, BODACC radié :
#     `etat_administratif_sirene = 'A' and etat_administratif_rne = 'A'
#     Potentiellement une liasse greffe perdue dans la nature avant 2023
#   - SIRENE cessé, RNE actif, BODACC radié :
#     `etat_administratif_sirene = 'C' and etat_administratif_rne = 'A'
#     Potentiellement une radiation d’office pas intégrée par l’INPI à partir de 2025
#
# Une valeur état administratif à null indique l'absence de l'entreprise du répertoire
# en question.

RADIATIONS_INCOHERENCES_QUERY = """
    WITH rne AS (
        -- 179 SIREN sont en doublons dans la table immatriculation
        -- Ce traitement peut être enlevé une fois ces SIREN dédoublonnés
        -- Voir : https://github.com/annuaire-entreprises-data-gouv-fr/search-infra/issues/739
        SELECT
            siren,
            MAX(date_immatriculation) AS date_immatriculation,
            MAX(date_radiation) AS date_radiation,
            MAX(date_fin_existence) AS date_fin_existence
        FROM immatriculation
        GROUP BY siren
    )
    SELECT
        r.siren,
        CASE
            WHEN ul.siren IS NULL THEN NULL
            WHEN ul.nature_juridique_unite_legale = '1000'
                OR ul.nature_juridique_unite_legale LIKE '2%'
            THEN 'true'
            ELSE 'false'
        END AS est_personne_physique,
        ul.etat_administratif_unite_legale AS etat_administratif_sirene,
        CASE
            WHEN rne.siren IS NULL THEN NULL
            WHEN rne.date_radiation IS NULL THEN 'A'
            ELSE 'C'
        END AS etat_administratif_rne,
        ul.nom_raison_sociale AS raison_sociale_sirene,
        ul.nom AS nom_sirene,
        ul.prenom AS prenom_sirene,
        ul.nature_juridique_unite_legale AS nature_juridique_sirene,
        ul.date_fermeture_unite_legale AS date_fermeture_sirene,
        ul.en_sommeil AS en_sommeil_sirene,
        rne.date_immatriculation AS date_immatriculation_rne,
        rne.date_radiation AS date_radiation_rne,
        rne.date_fin_existence AS date_fin_existence_rne,
        r.date AS date_radiation_bodacc,
        r.date_publication AS date_publication_bodacc,
        r.id_annonce AS id_annonce_bodacc,
        'https://www.bodacc.fr/pages/annonces-commerciales-detail/?q.id=id:'
            || r.id_annonce AS url_annonce_bodacc,
        r.visibility AS radiation_visible_annuaire
    FROM bodacc_radiations AS r
    LEFT JOIN unite_legale AS ul ON ul.siren = r.siren
    LEFT JOIN rne ON rne.siren = r.siren
    WHERE r.est_radie = 1
      AND (
        ul.etat_administratif_unite_legale = 'A'
        OR (rne.siren IS NOT NULL AND rne.date_radiation IS NULL)
      )
"""
