SELECT_FIELDS_TO_INDEX_QUERY = """SELECT
            ul.activite_principale_unite_legale as activite_principale_unite_legale,
            ul.activite_principale_naf25_unite_legale as activite_principale_naf25_unite_legale,
            ul.caractere_employeur as caractere_employeur,
            ul.categorie_entreprise as categorie_entreprise,
            ul.date_creation_unite_legale as date_creation_unite_legale,
            ul.date_fermeture_unite_legale as date_fermeture,
            ul.date_mise_a_jour_insee as date_mise_a_jour_insee,
            ul.date_mise_a_jour_rne as date_mise_a_jour_rne,
            ul.denomination_usuelle_1 as denomination_usuelle_1_unite_legale,
            ul.denomination_usuelle_2 as denomination_usuelle_2_unite_legale,
            ul.denomination_usuelle_3 as denomination_usuelle_3_unite_legale,
            ul.economie_sociale_solidaire_unite_legale as
            economie_sociale_solidaire_unite_legale,
            ul.etat_administratif_unite_legale as etat_administratif_unite_legale,
            ul.from_insee as from_insee,
            ul.from_rne as from_rne,
            ul.identifiant_association_unite_legale as
            identifiant_association_unite_legale,
            ul.nature_juridique_unite_legale as nature_juridique_unite_legale,
            ul.nom as nom,
            ul.nom_raison_sociale as nom_raison_sociale,
            ul.nom_usage as nom_usage,
            ul.prenom as prenom,
            ul.sigle as sigle,
            ul.siren,
            st.siret as siret_siege,
            ul.tranche_effectif_salarie_unite_legale as
            tranche_effectif_salarie_unite_legale,
            ul.statut_diffusion_unite_legale as
            statut_diffusion_unite_legale,
            ul.est_societe_mission as est_societe_mission,
            ul.annee_categorie_entreprise as annee_categorie_entreprise,
            ul.annee_tranche_effectif_salarie as annee_tranche_effectif_salarie,
            (SELECT sirets_par_idcc FROM convention_collective WHERE
                        siren = ul.siren) as sirets_par_idcc,
            (SELECT liste_idcc_unite_legale FROM convention_collective WHERE
                        siren = ul.siren) as liste_idcc_unite_legale,
            ce."count" as nombre_etablissements,
            ceo."count" as nombre_etablissements_ouverts,
            CASE WHEN bf.siren IS NOT NULL THEN json_object(
                'ca', bf.ca,
                'resultat_net', bf.resultat_net,
                'date_cloture_exercice', bf.date_cloture_exercice,
                'annee_cloture_exercice', bf.annee_cloture_exercice
            ) END as bilan_financier,
            (SELECT json_group_array(
                json_object(
                    'siren', siren,
                    'date_mise_a_jour', date_mise_a_jour,
                    'date_de_naissance', date_de_naissance,
                    'nom', nom,
                    'nom_usage', nom_usage,
                    'prenoms', prenoms,
                    'nationalite', nationalite,
                    'role_description', role_description
                    )
                ) FROM
                (
                    SELECT siren, date_mise_a_jour, date_de_naissance, nom,
                    nom_usage, prenoms, nationalite, role_description
                    FROM dirigeant_pp
                    WHERE siren = ul.siren
                )
            ) as dirigeants_pp,
            (SELECT json_group_array(
                    json_object(
                        'siren', siren,
                        'date_mise_a_jour', date_mise_a_jour,
                        'denomination', denomination,
                        'siren_dirigeant', siren_dirigeant,
                        'role_description', role_description,
                        'forme_juridique', forme_juridique
                        )
                    ) FROM
                    (
                        SELECT siren, date_mise_a_jour, denomination, siren_dirigeant,
                        role_description, forme_juridique
                        FROM dirigeant_pm
                        WHERE siren = ul.siren
                    )
                ) as dirigeants_pm,
            (SELECT json_group_array(
                    json_object(
                        'activite_principale',activite_principale,
                        'activite_principale_naf25',activite_principale_naf25,
                        'activite_principale_registre_metier',
                        activite_principale_registre_metier,
                        'ancien_siege',ancien_siege,
                        'caractere_employeur',caractere_employeur,
                        'cedex',cedex,
                        'code_pays_etranger',code_pays_etranger,
                        'code_postal',code_postal,
                        'commune',commune,
                        'complement_adresse',complement_adresse,
                        'date_creation',date_creation,
                        'date_debut_activite',date_debut_activite,
                        'date_fermeture',date_fermeture,
                        'distribution_speciale',distribution_speciale,
                        'enseigne_1',enseigne_1,
                        'enseigne_2',enseigne_2,
                        'enseigne_3',enseigne_3,
                        'est_siege',est_siege,
                        'etat_administratif',etat_administratif_etablissement,
                        'geo_adresse',geo_adresse,
                        'geo_id',geo_id,
                        'geo_score',geo_score,
                        'indice_repetition',indice_repetition,
                        'latitude',latitude,
                        'libelle_cedex',libelle_cedex,
                        'libelle_commune',libelle_commune,
                        'libelle_commune_etranger',libelle_commune_etranger,
                        'libelle_pays_etranger',libelle_pays_etranger,
                        'libelle_voie',libelle_voie,
                        'liste_finess_geographique',liste_finess_geographique,
                        'liste_id_bio',liste_id_bio,
                        'liste_idcc',liste_idcc,
                        'liste_rge',liste_rge,
                        'liste_uai',liste_uai,
                        'longitude',longitude,
                        'nom_commercial',nom_commercial,
                        'numero_voie',numero_voie,
                        'dernier_numero_voie',dernier_numero_voie,
                        'siren',siren,
                        'siret',siret,
                        'statut_diffusion_etablissement',
                        statut_diffusion_etablissement,
                        'tranche_effectif_salarie',tranche_effectif_salarie,
                        'annee_tranche_effectif_salarie',annee_tranche_effectif_salarie,
                        'date_mise_a_jour_insee',date_mise_a_jour_insee,
                        'type_voie',type_voie,
                        'x',x,
                        'y',y,
                        'successions',json_object(
                            'predecesseurs',successions_predecesseurs,
                            'successeurs',successions_successeurs
                        )
                        )
                    ) FROM
                    (
                        SELECT
                        s.activite_principale as activite_principale,
                        s.activite_principale_naf25 as activite_principale_naf25,
                        s.activite_principale_registre_metier as
                        activite_principale_registre_metier,
                        CASE
                            WHEN EXISTS (
                                SELECT 1
                                FROM ancien_siege
                                WHERE siret = s.siret
                                )
                                THEN TRUE
                            ELSE FALSE
                        END AS ancien_siege,
                        s.caractere_employeur as caractere_employeur,
                        s.cedex as cedex,
                        s.code_pays_etranger as code_pays_etranger,
                        s.code_postal as code_postal,
                        s.commune as commune,
                        s.complement_adresse as complement_adresse,
                        s.date_creation as date_creation,
                        s.date_debut_activite as date_debut_activite,
                        s.date_fermeture_etablissement as date_fermeture,
                        s.distribution_speciale as distribution_speciale,
                        s.enseigne_1 as enseigne_1,
                        s.enseigne_2 as enseigne_2,
                        s.enseigne_3 as enseigne_3,
                        s.est_siege as est_siege,
                        s.etat_administratif_etablissement as
                        etat_administratif_etablissement,
                        NULL as geo_adresse,
                        NULL as geo_id,
                        NULL as geo_score,
                        s.indice_repetition as indice_repetition,
                        s.latitude as latitude,
                        s.libelle_cedex as libelle_cedex,
                        s.libelle_commune as libelle_commune,
                        s.libelle_commune_etranger as libelle_commune_etranger,
                        s.libelle_pays_etranger as libelle_pays_etranger,
                        s.libelle_voie as libelle_voie,
                        s.longitude as longitude,
                        (SELECT liste_finess_geographique FROM finess_geographique WHERE siret = s.siret) as
                        liste_finess_geographique,
                        (SELECT liste_id_bio FROM agence_bio WHERE siret = s.siret) as
                        liste_id_bio,
                        (SELECT liste_idcc_etablissement FROM convention_collective
                        WHERE siret = s.siret) as liste_idcc,
                        (SELECT liste_rge FROM rge WHERE siret = s.siret) as liste_rge,
                        (SELECT liste_uai FROM uai WHERE siret = s.siret) as liste_uai,
                        s.nom_commercial as nom_commercial,
                        s.numero_voie as numero_voie,
                        s.dernier_numero_voie as dernier_numero_voie,
                        s.siren as siren,
                        s.siret as siret,
                        s.statut_diffusion_etablissement as
                        statut_diffusion_etablissement,
                        s.tranche_effectif_salarie as
                        tranche_effectif_salarie,
                        s.annee_tranche_effectif_salarie as
                        annee_tranche_effectif_salarie,
                        s.date_mise_a_jour_insee as date_mise_a_jour_insee,
                        s.type_voie as type_voie,
                        s.x as x,
                        s.y as y,
                        (SELECT json_group_array(json_object(
                            'siret', siret_predecesseur,
                            'date_lien_succession', date_lien_succession,
                            'transfert_siege', transfert_siege,
                            'continuite_economique', continuite_economique
                            ))
                        FROM liens_succession
                        WHERE siret_successeur = s.siret
                        ) as successions_predecesseurs,
                        (SELECT json_group_array(json_object(
                            'siret', siret_successeur,
                            'date_lien_succession', date_lien_succession,
                            'transfert_siege', transfert_siege,
                            'continuite_economique', continuite_economique
                            ))
                        FROM liens_succession
                        WHERE siret_predecesseur = s.siret
                        ) as successions_successeurs
                        FROM etablissement s
                        WHERE s.siren = ul.siren
                    )
                ) as etablissements,
            (SELECT json_object(
                        'activite_principale',activite_principale,
                        'activite_principale_naf25', activite_principale_naf25,
                        'activite_principale_registre_metier',
                        activite_principale_registre_metier,
                        'caractere_employeur',caractere_employeur,
                        'cedex',cedex,
                        'code_pays_etranger',code_pays_etranger,
                        'code_postal',code_postal,
                        'commune',commune,
                        'complement_adresse',complement_adresse,
                        'date_creation',date_creation,
                        'date_debut_activite',date_debut_activite,
                        'date_fermeture',date_fermeture,
                        'distribution_speciale',distribution_speciale,
                        'enseigne_1',enseigne_1,
                        'enseigne_2',enseigne_2,
                        'enseigne_3',enseigne_3,
                        'est_siege',est_siege,
                        'etat_administratif',etat_administratif_etablissement,
                        'from_insee',from_insee,
                        'from_rne',from_rne,
                        'geo_adresse',geo_adresse,
                        'geo_id',geo_id,
                        'geo_score',geo_score,
                        'indice_repetition',indice_repetition,
                        'latitude',latitude,
                        'libelle_cedex',libelle_cedex,
                        'libelle_commune',libelle_commune,
                        'libelle_commune_etranger',libelle_commune_etranger,
                        'libelle_pays_etranger',libelle_pays_etranger,
                        'libelle_voie',libelle_voie,
                        'liste_finess_geographique',liste_finess_geographique,
                        'liste_id_bio',liste_id_bio,
                        'liste_idcc',liste_idcc,
                        'liste_rge',liste_rge,
                        'liste_uai',liste_uai,
                        'longitude',longitude,
                        'nom_commercial',nom_commercial,
                        'numero_voie',numero_voie,
                        'dernier_numero_voie',dernier_numero_voie,
                        'siren',siren,
                        'siret',siret,
                        'statut_diffusion_etablissement',
                        statut_diffusion_etablissement,
                        'tranche_effectif_salarie',tranche_effectif_salarie,
                        'annee_tranche_effectif_salarie',
                        annee_tranche_effectif_salarie,
                        'type_voie',type_voie,
                        'date_mise_a_jour_insee',date_mise_a_jour_insee,
                        'date_mise_a_jour_rne',date_mise_a_jour_rne,
                        'x',x,
                        'y',y,
                        'successions',json_object(
                            'predecesseurs',successions_predecesseurs,
                            'successeurs',successions_successeurs
                        )
                        )
                    FROM
                    (
                        SELECT
                        s.activite_principale as activite_principale,
                        s.activite_principale_naf25 as activite_principale_naf25,
                        s.activite_principale_registre_metier as
                        activite_principale_registre_metier,
                        s.caractere_employeur as caractere_employeur,
                        s.cedex as cedex,
                        s.code_pays_etranger as code_pays_etranger,
                        s.code_postal as code_postal,
                        s.commune as commune,
                        s.complement_adresse as complement_adresse,
                        s.date_creation as date_creation,
                        s.date_debut_activite as date_debut_activite,
                        s.date_fermeture_etablissement as date_fermeture,
                        s.distribution_speciale as distribution_speciale,
                        s.enseigne_1 as enseigne_1,
                        s.enseigne_2 as enseigne_2,
                        s.enseigne_3 as enseigne_3,
                        s.est_siege as est_siege,
                        s.etat_administratif_etablissement as
                        etat_administratif_etablissement,
                        NULL as geo_adresse,
                        NULL as geo_id,
                        NULL as geo_score,
                        s.indice_repetition as indice_repetition,
                        s.latitude as latitude,
                        s.libelle_cedex as libelle_cedex,
                        s.libelle_commune as libelle_commune,
                        s.libelle_commune_etranger as libelle_commune_etranger,
                        s.libelle_pays_etranger as libelle_pays_etranger,
                        s.libelle_voie as libelle_voie,
                        (SELECT liste_finess_geographique FROM finess_geographique WHERE siret = s.siret) as
                        liste_finess_geographique,
                        (SELECT liste_id_bio FROM agence_bio WHERE siret = s.siret) as
                        liste_id_bio,
                        (SELECT liste_idcc_etablissement FROM convention_collective
                        WHERE siret = s.siret) as liste_idcc,
                        (SELECT liste_rge FROM rge WHERE siret = s.siret) as liste_rge,
                        (SELECT liste_uai FROM uai WHERE siret = s.siret) as liste_uai,
                        s.longitude as longitude,
                        s.nom_commercial as nom_commercial,
                        s.numero_voie as numero_voie,
                        s.dernier_numero_voie as dernier_numero_voie,
                        s.siren as siren,
                        s.siret as siret,
                        s.statut_diffusion_etablissement as
                        statut_diffusion_etablissement,
                        s.tranche_effectif_salarie as tranche_effectif_salarie,
                        s.annee_tranche_effectif_salarie as
                        annee_tranche_effectif_salarie,
                        s.type_voie as type_voie,
                        s.date_mise_a_jour_insee as date_mise_a_jour_insee,
                        s.date_mise_a_jour_rne as date_mise_a_jour_rne,
                        s.x as x,
                        s.y as y,
                        (SELECT json_group_array(json_object(
                            'siret', siret_predecesseur,
                            'date_lien_succession', date_lien_succession,
                            'transfert_siege', transfert_siege,
                            'continuite_economique', continuite_economique
                            ))
                        FROM liens_succession
                        WHERE siret_successeur = s.siret
                        ) as successions_predecesseurs,
                        (SELECT json_group_array(json_object(
                            'siret', siret_successeur,
                            'date_lien_succession', date_lien_succession,
                            'transfert_siege', transfert_siege,
                            'continuite_economique', continuite_economique
                            ))
                        FROM liens_succession
                        WHERE siret_predecesseur = s.siret
                        ) as successions_successeurs
                        FROM siege as s
                        WHERE s.siren = st.siren
                    )
                ) as siege,
            sp.est_entrepreneur_spectacle as est_entrepreneur_spectacle,
            sp.statut_entrepreneur_spectacle as statut_entrepreneur_spectacle,
            fj.liste_finess_juridique as liste_finess_juridique,
            eg.egapro_renseignee as egapro_renseignee,
            bg.bilan_ges_renseigne as bilan_ges_renseigne,
            ar.est_achats_responsables as est_achats_responsables,
            ac.est_alim_confiance as est_alim_confiance,
            pv.est_patrimoine_vivant as est_patrimoine_vivant,
            amin.aide_de_minimis_renseignee as aide_de_minimis_renseignee,
            aade.aide_ademe_renseignee as aide_ademe_renseignee,
            av.est_avocat as est_avocat,
            col.colter_code_insee as colter_code_insee,
            col.colter_code as colter_code,
            col.colter_niveau as colter_niveau,
            ef.est_ess_france as est_ess_france,
            (SELECT json_group_array(
                json_object(
                    'siren', siren,
                    'nom', nom,
                    'prenom', prenom,
                    'date_naissance', date_naissance,
                    'sexe', sexe,
                    'fonction', fonction
                    )
                ) FROM
                (
                    SELECT DISTINCT siren, nom, prenom, date_naissance,
                    sexe, fonction
                    FROM elus
                    WHERE siren = ul.siren
                )
            ) as colter_elus,
            orgf.est_qualiopi as est_qualiopi,
            orgf.liste_id_organisme_formation as liste_id_organisme_formation,
            mi.est_siae AS est_siae,
            mi.type_siae AS type_siae,
            tva.liste_tva as liste_tva,
            CASE WHEN fo.siren IS NOT NULL THEN json_object(
                'numero_rnf', fo.numero_rnf,
                'denomination', fo.denomination,
                'type_organisme', fo.type_organisme,
                'date_creation', fo.date_creation,
                'siren', fo.siren,
                'siret', fo.siret,
                'adresse', fo.adresse,
                'code_postal', fo.code_postal,
                'ville', fo.ville
            ) END as fondation,
            (
                SELECT json_object(
                    'date_immatriculation', date_immatriculation,
                    'date_radiation', date_radiation,
                    'indicateur_associe_unique', indicateur_associe_unique,
                    'capital_social', capital_social,
                    'date_cloture_exercice', date_cloture_exercice,
                    'duree_personne_morale', duree_personne_morale,
                    'date_fin_existence', date_fin_existence,
                    'nature_entreprise', nature_entreprise,
                    'date_debut_activite', date_debut_activite,
                    'capital_variable', capital_variable,
                    'devise_capital', devise_capital
                )
                FROM
                (
                    SELECT date_immatriculation, date_radiation,
                    indicateur_associe_unique, capital_social,
                    date_cloture_exercice, duree_personne_morale, date_fin_existence, nature_entreprise,
                    date_debut_activite, capital_variable, devise_capital
                    FROM immatriculation
                    WHERE siren = ul.siren
                )
            ) as immatriculation,
            json_object(
                'radiation',
                    CASE WHEN r.siren IS NOT NULL AND r.visibility THEN json_object(
                        'est_radie', r.est_radie,
                        'id_annonce', r.id_annonce,
                        'date', r.date
                    ) END,
                'procedure_collective',
                    CASE WHEN pc.siren IS NOT NULL THEN json_object(
                        'statut', pc.statut,
                        'id_annonce', pc.id_annonce,
                        'date', pc.date
                    ) END
            ) as bodacc
            FROM
                unite_legale ul
            LEFT JOIN siege st ON ul.siren = st.siren
            -- One indexed seek per table instead of one per column, and measurably
            -- cheaper than the correlated subqueries these replace. Only tables
            -- verified unique on siren are joined: convention_collective (several rows
            -- per siren) and immatriculation (duplicates) stay scalar subqueries above.
            LEFT JOIN count_etablissement ce ON ce.siren = ul.siren
            LEFT JOIN count_etablissement_ouvert ceo ON ceo.siren = ul.siren
            LEFT JOIN bilan_financier bf ON bf.siren = ul.siren
            LEFT JOIN spectacle sp ON sp.siren = ul.siren
            LEFT JOIN finess_juridique fj ON fj.siren = ul.siren
            LEFT JOIN egapro eg ON eg.siren = ul.siren
            LEFT JOIN bilan_ges bg ON bg.siren = ul.siren
            LEFT JOIN achats_responsables ar ON ar.siren = ul.siren
            LEFT JOIN alim_confiance ac ON ac.siren = ul.siren
            LEFT JOIN patrimoine_vivant pv ON pv.siren = ul.siren
            LEFT JOIN aides_minimis amin ON amin.siren = ul.siren
            LEFT JOIN aides_ademe aade ON aade.siren = ul.siren
            LEFT JOIN avocat av ON av.siren = ul.siren
            LEFT JOIN colter col ON col.siren = ul.siren
            LEFT JOIN ess_france ef ON ef.siren = ul.siren
            LEFT JOIN organisme_formation orgf ON orgf.siren = ul.siren
            LEFT JOIN marche_inclusion mi ON mi.siren = ul.siren
            LEFT JOIN tva ON tva.siren = ul.siren
            LEFT JOIN fondation fo ON fo.siren = ul.siren
            LEFT JOIN bodacc_radiations r ON r.siren = ul.siren
            LEFT JOIN bodacc_procedures_collectives pc ON pc.siren = ul.siren
            WHERE ul.siren IS NOT NULL
    """


def select_fields_to_index_query(siren_start=None, siren_end=None):
    """Return the query and its parameters, restricted to a range of siren.

    Both bounds are optional: the first shard has no lower bound and the last no upper
    one. The predicates are appended only for the bounds that exist rather than being
    neutralised with `? IS NULL OR ...`, because the OR form costs the range scan —
    EXPLAIN QUERY PLAN then falls back to walking the whole index instead of
    `SEARCH ul USING COVERING INDEX index_unite_legale_siren (siren>? AND siren<?)`.

    siren is fixed-width TEXT, so the lexicographic order of the unique index is the
    numeric order and a string comparison is a correct range.
    """
    predicates = []
    params = []
    if siren_start is not None:
        predicates.append("AND ul.siren >= ?")
        params.append(siren_start)
    if siren_end is not None:
        predicates.append("AND ul.siren < ?")
        params.append(siren_end)

    return f"{SELECT_FIELDS_TO_INDEX_QUERY} {' '.join(predicates)}", params
