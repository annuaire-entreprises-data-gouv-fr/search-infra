# Entreprises individuelles radiées au RCS d'après le BODACC où
# l'entreprise a au moins un établissement actif dans le répertoire SIRENE sauf :
#   - si l'unité légale n'a qu'un seul établissement
#   - si aucun de ses établissements n'a été créé après la date de radiation
RADIATIONS_PP_QUERY = """
    SELECT
        b.siren,
        ul.nom_raison_sociale AS sirene_raison_sociale,
        ul.nom AS sirene_nom,
        ul.prenom AS sirene_prenom,
        b.radiation_date AS bodacc_radiation_date,
        b.radiation_date_publication AS bodacc_radiation_date_publication,
        b.radiation_id_annonce AS bodacc_radiation_id_annonce,
        'https://www.bodacc.fr/pages/annonces-commerciales-detail/?q.id=id:' || b.radiation_id_annonce AS bodacc_annonce_url
    FROM bodacc AS b
    INNER JOIN unite_legale AS ul ON ul.siren = b.siren
    WHERE b.radiation_est_radie = 1
      AND (
        ul.nature_juridique_unite_legale = '1000'
        OR ul.nature_juridique_unite_legale LIKE '2%'
        )
      and b.radiation_visibility_reason = 'ei_active_on_sirene'
    ORDER BY b.radiation_date_publication DESC
"""

# Personnes morales radiées au RCS d'après le BODACC où
# mais dont la radiation n'est pas reflétée dans SIRENE
RADIATIONS_PM_QUERY = """
    SELECT
        b.siren,
        ul.nom_raison_sociale AS sirene_raison_sociale,
        ul.nature_juridique_unite_legale AS sirene_nature_juridique,
        b.radiation_date AS bodacc_radiation_date,
        b.radiation_date_publication AS bodacc_radiation_date_publication,
        b.radiation_id_annonce AS bodacc_radiation_id_annonce,
        'https://www.bodacc.fr/pages/annonces-commerciales-detail/?q.id=id:' || b.radiation_id_annonce AS bodacc_annonce_url
    FROM bodacc AS b
    INNER JOIN unite_legale AS ul ON ul.siren = b.siren
    WHERE b.radiation_est_radie = 1
        AND ul.nature_juridique_unite_legale != '1000'
        AND ul.nature_juridique_unite_legale NOT LIKE '2%'
        AND ul.etat_administratif_unite_legale = 'A'
    ORDER BY b.siren
"""
