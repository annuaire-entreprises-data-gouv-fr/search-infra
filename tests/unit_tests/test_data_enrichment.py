"""
import pytest
from dag_datalake_sirene.workflows.data_pipelines.elasticsearch.data_enrichment import (
    is_service_public,
)

# Sample URSSAF SIREN numbers for testing
urssaf_siren_numbers = {"123456789", "987654321"}


@pytest.mark.parametrize(
    "nature_juridique, siren, expected",
    [
        ("4711", "123456789", True),  # Valid prefix
        ("72", "987654321", True),  # Valid prefix
        ("1234", "320252489", True),  # BPI France
        ("1234", "123456789", True),  # URSSAF SIREN
        ("4120", "775663438", False),  # RATP should be excluded
        ("1234", "111111111", False),  # Non-public SIREN
        ("", "123456789", False),  # Empty nature juridique
        (None, "123456789", False),  # None nature juridique
    ],
)
def test_is_service_public(nature_juridique, siren, expected):
    assert is_service_public(nature_juridique, siren) == expected
"""
