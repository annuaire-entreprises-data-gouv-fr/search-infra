select_sitemap_fields_query = """SELECT
        ul.nom_raison_sociale,
        ul.denomination_usuelle_1 as denomination_usuelle_1_unite_legale,
        ul.denomination_usuelle_2 as denomination_usuelle_2_unite_legale,
        ul.denomination_usuelle_3 as denomination_usuelle_3_unite_legale,
        ul.sigle,
        ul.siren,
        ul.etat_administratif_unite_legale,
        ul.nature_juridique_unite_legale,
        st.code_postal,
        st.commune as code_commune,
        st.code_pays_etranger,
        st.nom_commercial,
        ul.activite_principale_unite_legale,
        ul.statut_diffusion_unite_legale,
        ul.nature_juridique_unite_legale
        FROM
            unite_legale ul
        JOIN
            etablissement st
        ON st.siren = ul.siren AND st.est_siege = 'true';"""
