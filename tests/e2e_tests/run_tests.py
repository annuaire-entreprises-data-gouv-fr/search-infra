from dag_datalake_sirene.tests.e2e_tests.test_api import (
    test_fetch_company,
    test_personne_filter,
    test_format_date_naissance,
    test_error_query,
    test_accept_three_characters,
    test_query_too_short,
    test_short_query_with_filter,
    test_terms_empty_only,
    test_bool_filters,
    test_est_service_public,
    test_min_per_page,
    test_page_number,
    test_siren_search,
    test_banned_param,
)


def run_e2e_tests():
    test_fetch_company()
    test_personne_filter()
    test_format_date_naissance()
    test_error_query()
    test_accept_three_characters()
    test_query_too_short()
    test_short_query_with_filter()
    test_terms_empty_only()
    test_bool_filters()
    test_est_service_public()
    test_min_per_page()
    test_page_number()
    test_siren_search()
    test_banned_param()
