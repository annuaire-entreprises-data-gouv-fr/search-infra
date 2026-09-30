SELECT_FIELDS_TO_INDEX_QUERY = """SELECT
            unite_legale.activite_principale_unite_legale as activite_principale_unite_legale,
            unite_legale.activite_principale_naf25_unite_legale as activite_principale_naf25_unite_legale,
            unite_legale.caractere_employeur as caractere_employeur,
            unite_legale.categorie_entreprise as categorie_entreprise,
            unite_legale.date_creation_unite_legale as date_creation_unite_legale,
            unite_legale.date_fermeture_unite_legale as date_fermeture,
            unite_legale.date_mise_a_jour_insee as date_mise_a_jour_insee,
            unite_legale.date_mise_a_jour_rne as date_mise_a_jour_rne,
            unite_legale.denomination_usuelle_1 as denomination_usuelle_1_unite_legale,
            unite_legale.denomination_usuelle_2 as denomination_usuelle_2_unite_legale,
            unite_legale.denomination_usuelle_3 as denomination_usuelle_3_unite_legale,
            unite_legale.economie_sociale_solidaire_unite_legale as
            economie_sociale_solidaire_unite_legale,
            unite_legale.etat_administratif_unite_legale as etat_administratif_unite_legale,
            unite_legale.from_insee as from_insee,
            unite_legale.from_rne as from_rne,
            unite_legale.identifiant_association_unite_legale as
            identifiant_association_unite_legale,
            unite_legale.nature_juridique_unite_legale as nature_juridique_unite_legale,
            unite_legale.nom as nom,
            unite_legale.nom_raison_sociale as nom_raison_sociale,
            unite_legale.nom_usage as nom_usage,
            unite_legale.prenom as prenom,
            unite_legale.sigle as sigle,
            unite_legale.siren,
            (SELECT siren_conserve FROM doublons WHERE siren_doublon = unite_legale.siren) as siren_conserve,
            (SELECT siret FROM etablissement WHERE siren = unite_legale.siren
                AND est_siege = 'true') as siret_siege,
            unite_legale.tranche_effectif_salarie_unite_legale as
            tranche_effectif_salarie_unite_legale,
            unite_legale.statut_diffusion_unite_legale as
            statut_diffusion_unite_legale,
            unite_legale.est_societe_mission as est_societe_mission,
            unite_legale.annee_categorie_entreprise as annee_categorie_entreprise,
            unite_legale.annee_tranche_effectif_salarie as annee_tranche_effectif_salarie,
            (SELECT sirets_par_idcc FROM convention_collective WHERE
                        siren = unite_legale.siren) as sirets_par_idcc,
            (SELECT liste_idcc_unite_legale FROM convention_collective WHERE
                        siren = unite_legale.siren) as liste_idcc_unite_legale,
            count_etablissement."count" as nombre_etablissements,
            count_etablissement_ouvert."count" as nombre_etablissements_ouverts,
            CASE WHEN bilan_financier.siren IS NOT NULL THEN json_object(
                'ca', bilan_financier.ca,
                'resultat_net', bilan_financier.resultat_net,
                'date_cloture_exercice', bilan_financier.date_cloture_exercice,
                'annee_cloture_exercice', bilan_financier.annee_cloture_exercice
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
                    WHERE siren = unite_legale.siren
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
                        WHERE siren = unite_legale.siren
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
                        'date_mise_a_jour_rne',date_mise_a_jour_rne,
                        'type_voie',type_voie,
                        'x',x,
                        'y',y,
                        -- json() is required, not decorative: SQLite only carries the
                        -- JSON subtype of json_group_array() across a subquery boundary
                        -- on some versions. Without it these two come back as escaped
                        -- strings and the transform iterates over their characters.
                        'successions',json_object(
                            'predecesseurs',json(successions_predecesseurs),
                            'successeurs',json(successions_successeurs)
                        )
                        )
                    ) FROM
                    (
                        SELECT
                        etablissement.activite_principale as activite_principale,
                        etablissement.activite_principale_naf25 as activite_principale_naf25,
                        etablissement.activite_principale_registre_metier as
                        activite_principale_registre_metier,
                        CASE
                            WHEN EXISTS (
                                SELECT 1
                                FROM ancien_siege
                                WHERE siret = etablissement.siret
                                )
                                THEN TRUE
                            ELSE FALSE
                        END AS ancien_siege,
                        etablissement.caractere_employeur as caractere_employeur,
                        etablissement.cedex as cedex,
                        etablissement.code_pays_etranger as code_pays_etranger,
                        etablissement.code_postal as code_postal,
                        etablissement.commune as commune,
                        etablissement.complement_adresse as complement_adresse,
                        etablissement.date_creation as date_creation,
                        etablissement.date_debut_activite as date_debut_activite,
                        etablissement.date_fermeture_etablissement as date_fermeture,
                        etablissement.distribution_speciale as distribution_speciale,
                        etablissement.enseigne_1 as enseigne_1,
                        etablissement.enseigne_2 as enseigne_2,
                        etablissement.enseigne_3 as enseigne_3,
                        etablissement.est_siege as est_siege,
                        etablissement.etat_administratif_etablissement as
                        etat_administratif_etablissement,
                        NULL as geo_adresse,
                        NULL as geo_id,
                        NULL as geo_score,
                        etablissement.indice_repetition as indice_repetition,
                        etablissement.latitude as latitude,
                        etablissement.libelle_cedex as libelle_cedex,
                        etablissement.libelle_commune as libelle_commune,
                        etablissement.libelle_commune_etranger as libelle_commune_etranger,
                        etablissement.libelle_pays_etranger as libelle_pays_etranger,
                        etablissement.libelle_voie as libelle_voie,
                        etablissement.longitude as longitude,
                        (SELECT liste_finess_geographique FROM finess_geographique WHERE siret = etablissement.siret) as
                        liste_finess_geographique,
                        (SELECT liste_id_bio FROM agence_bio WHERE siret = etablissement.siret) as
                        liste_id_bio,
                        (SELECT liste_idcc_etablissement FROM convention_collective
                        WHERE siret = etablissement.siret) as liste_idcc,
                        (SELECT liste_rge FROM rge WHERE siret = etablissement.siret) as liste_rge,
                        (SELECT liste_uai FROM uai WHERE siret = etablissement.siret) as liste_uai,
                        etablissement.nom_commercial as nom_commercial,
                        etablissement.numero_voie as numero_voie,
                        etablissement.dernier_numero_voie as dernier_numero_voie,
                        etablissement.siren as siren,
                        etablissement.siret as siret,
                        etablissement.statut_diffusion_etablissement as
                        statut_diffusion_etablissement,
                        etablissement.tranche_effectif_salarie as
                        tranche_effectif_salarie,
                        etablissement.annee_tranche_effectif_salarie as
                        annee_tranche_effectif_salarie,
                        etablissement.date_mise_a_jour_insee as date_mise_a_jour_insee,
                        etablissement.date_mise_a_jour_rne as date_mise_a_jour_rne,
                        etablissement.type_voie as type_voie,
                        etablissement.x as x,
                        etablissement.y as y,
                        (SELECT json_group_array(json_object(
                            'siret', siret_predecesseur,
                            'date_lien_succession', date_lien_succession,
                            'transfert_siege', transfert_siege,
                            'continuite_economique', continuite_economique
                            ))
                        FROM liens_succession
                        WHERE siret_successeur = etablissement.siret
                        ) as successions_predecesseurs,
                        (SELECT json_group_array(json_object(
                            'siret', siret_successeur,
                            'date_lien_succession', date_lien_succession,
                            'transfert_siege', transfert_siege,
                            'continuite_economique', continuite_economique
                            ))
                        FROM liens_succession
                        WHERE siret_predecesseur = etablissement.siret
                        ) as successions_successeurs
                        FROM etablissement
                        WHERE etablissement.siren = unite_legale.siren
                    )
                ) as etablissements,
            spectacle.est_entrepreneur_spectacle as est_entrepreneur_spectacle,
            spectacle.statut_entrepreneur_spectacle as statut_entrepreneur_spectacle,
            finess_juridique.liste_finess_juridique as liste_finess_juridique,
            egapro.egapro_renseignee as egapro_renseignee,
            bilan_ges.bilan_ges_renseigne as bilan_ges_renseigne,
            achats_responsables.est_achats_responsables as est_achats_responsables,
            alim_confiance.est_alim_confiance as est_alim_confiance,
            patrimoine_vivant.est_patrimoine_vivant as est_patrimoine_vivant,
            aides_minimis.aide_de_minimis_renseignee as aide_de_minimis_renseignee,
            aides_ademe.aide_ademe_renseignee as aide_ademe_renseignee,
            avocat.est_avocat as est_avocat,
            colter.colter_code_insee as colter_code_insee,
            colter.colter_code as colter_code,
            colter.colter_niveau as colter_niveau,
            ess_france.est_ess_france as est_ess_france,
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
                    WHERE siren = unite_legale.siren
                )
            ) as colter_elus,
            organisme_formation.est_qualiopi as est_qualiopi,
            organisme_formation.liste_id_organisme_formation as liste_id_organisme_formation,
            marche_inclusion.est_siae AS est_siae,
            marche_inclusion.type_siae AS type_siae,
            tva.liste_tva as liste_tva,
            CASE WHEN fondation.siren IS NOT NULL THEN json_object(
                'numero_rnf', fondation.numero_rnf,
                'denomination', fondation.denomination,
                'type_organisme', fondation.type_organisme,
                'date_creation', fondation.date_creation,
                'siren', fondation.siren,
                'siret', fondation.siret,
                'adresse', fondation.adresse,
                'code_postal', fondation.code_postal,
                'ville', fondation.ville
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
                    WHERE siren = unite_legale.siren
                )
            ) as immatriculation,
            json_object(
                'radiation',
                    CASE WHEN bodacc_radiations.siren IS NOT NULL AND bodacc_radiations.visibility THEN json_object(
                        'est_radie', bodacc_radiations.est_radie,
                        'id_annonce', bodacc_radiations.id_annonce,
                        'date', bodacc_radiations.date
                    ) END,
                'procedure_collective',
                    CASE WHEN bodacc_procedures_collectives.siren IS NOT NULL THEN json_object(
                        'statut', bodacc_procedures_collectives.statut,
                        'id_annonce', bodacc_procedures_collectives.id_annonce,
                        'date', bodacc_procedures_collectives.date
                    ) END
            ) as bodacc
            FROM
                unite_legale
            LEFT JOIN count_etablissement ON count_etablissement.siren = unite_legale.siren
            LEFT JOIN count_etablissement_ouvert ON count_etablissement_ouvert.siren = unite_legale.siren
            LEFT JOIN bilan_financier ON bilan_financier.siren = unite_legale.siren
            LEFT JOIN spectacle ON spectacle.siren = unite_legale.siren
            LEFT JOIN finess_juridique ON finess_juridique.siren = unite_legale.siren
            LEFT JOIN egapro ON egapro.siren = unite_legale.siren
            LEFT JOIN bilan_ges ON bilan_ges.siren = unite_legale.siren
            LEFT JOIN achats_responsables ON achats_responsables.siren = unite_legale.siren
            LEFT JOIN alim_confiance ON alim_confiance.siren = unite_legale.siren
            LEFT JOIN patrimoine_vivant ON patrimoine_vivant.siren = unite_legale.siren
            LEFT JOIN aides_minimis ON aides_minimis.siren = unite_legale.siren
            LEFT JOIN aides_ademe ON aides_ademe.siren = unite_legale.siren
            LEFT JOIN avocat ON avocat.siren = unite_legale.siren
            LEFT JOIN colter ON colter.siren = unite_legale.siren
            LEFT JOIN ess_france ON ess_france.siren = unite_legale.siren
            LEFT JOIN organisme_formation ON organisme_formation.siren = unite_legale.siren
            LEFT JOIN marche_inclusion ON marche_inclusion.siren = unite_legale.siren
            LEFT JOIN tva ON tva.siren = unite_legale.siren
            LEFT JOIN fondation ON fondation.siren = unite_legale.siren
            LEFT JOIN bodacc_radiations ON bodacc_radiations.siren = unite_legale.siren
            LEFT JOIN bodacc_procedures_collectives ON bodacc_procedures_collectives.siren = unite_legale.siren
            WHERE unite_legale.siren IS NOT NULL
    """
