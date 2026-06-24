# Entreprises individuelles radiées au RCS d'après le BODACC où
# l'entreprise a au moins un établissement actif dans le répertoire SIRENE sauf :
#   - si l'unité légale n'a qu'un seul établissement
#   - si aucun de ses établissements n'a été créé après la date de radiation
RADIATIONS_PP_QUERY = """
    SELECT
        r.siren,
        ul.nom_raison_sociale AS sirene_raison_sociale,
        ul.nom AS sirene_nom,
        ul.prenom AS sirene_prenom,
        r.date AS bodacc_radiation_date,
        r.date_publication AS bodacc_radiation_date_publication,
        r.id_annonce AS bodacc_radiation_id_annonce,
        'https://www.bodacc.fr/pages/annonces-commerciales-detail/?q.id=id:' || r.id_annonce AS bodacc_annonce_url
    FROM bodacc_radiations AS r
    INNER JOIN unite_legale AS ul ON ul.siren = r.siren
    WHERE r.est_radie = 1
      AND (
        ul.nature_juridique_unite_legale = '1000'
        OR ul.nature_juridique_unite_legale LIKE '2%'
        )
      and r.visibility_reason = 'ei_active_on_sirene'
    ORDER BY r.date_publication DESC
"""

# Personnes morales radiées au RCS d'après le BODACC où
# mais dont la radiation n'est pas reflétée dans SIRENE
RADIATIONS_PM_QUERY = """
    SELECT
        r.siren,
        ul.nom_raison_sociale AS sirene_raison_sociale,
        ul.nature_juridique_unite_legale AS sirene_nature_juridique,
        r.date AS bodacc_radiation_date,
        r.date_publication AS bodacc_radiation_date_publication,
        r.id_annonce AS bodacc_radiation_id_annonce,
        'https://www.bodacc.fr/pages/annonces-commerciales-detail/?q.id=id:' || r.id_annonce AS bodacc_annonce_url
    FROM bodacc_radiations AS r
    INNER JOIN unite_legale AS ul ON ul.siren = r.siren
    WHERE r.est_radie = 1
        AND ul.nature_juridique_unite_legale != '1000'
        AND ul.nature_juridique_unite_legale NOT LIKE '2%'
        AND ul.etat_administratif_unite_legale = 'A'
    ORDER BY r.siren
"""
