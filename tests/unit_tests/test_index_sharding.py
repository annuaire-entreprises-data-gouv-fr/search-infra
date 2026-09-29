from itertools import pairwise

import pytest

from data_pipelines_annuaire.helpers.sqlite_client import SqliteClient
from data_pipelines_annuaire.workflows.data_pipelines.elasticsearch.sqlite.fields_to_index import (
    select_fields_to_index_query,
)
from data_pipelines_annuaire.workflows.data_pipelines.elasticsearch.task_functions.index import (
    compute_siren_ranges,
)

SHARD_COUNT = 4
UNITES_LEGALES_COUNT = 100


@pytest.fixture
def sqlite_client(tmp_path):
    client = SqliteClient(str(tmp_path / "sirene.db"))
    client.execute("CREATE TABLE unite_legale (siren TEXT)")
    client.execute(
        "CREATE UNIQUE INDEX index_unite_legale_siren ON unite_legale (siren)"
    )
    client.db_cursor.executemany(
        "INSERT INTO unite_legale VALUES (?)",
        [(f"{siren:09d}",) for siren in range(UNITES_LEGALES_COUNT)],
    )
    yield client
    client.db_conn.close()


def count_in_range(sqlite_client, siren_start, siren_end):
    predicates, params = [], []
    if siren_start is not None:
        predicates.append("AND siren >= ?")
        params.append(siren_start)
    if siren_end is not None:
        predicates.append("AND siren < ?")
        params.append(siren_end)
    query = f"SELECT count(*) FROM unite_legale WHERE 1 {' '.join(predicates)}"
    return sqlite_client.execute(query, params).fetchone()[0]


# compute_siren_ranges()


def test_siren_ranges_tile_the_table_without_gap_or_overlap(sqlite_client):
    _, siren_ranges = compute_siren_ranges(sqlite_client, SHARD_COUNT)

    assert len(siren_ranges) == SHARD_COUNT
    assert siren_ranges[0]["siren_start"] is None
    assert siren_ranges[-1]["siren_end"] is None
    for current, following in pairwise(siren_ranges):
        assert current["siren_end"] == following["siren_start"]


def test_siren_ranges_are_balanced_and_cover_every_row(sqlite_client):
    _, siren_ranges = compute_siren_ranges(sqlite_client, SHARD_COUNT)

    counts = [
        count_in_range(sqlite_client, **siren_range) for siren_range in siren_ranges
    ]

    assert sum(counts) == UNITES_LEGALES_COUNT
    assert counts == [UNITES_LEGALES_COUNT // SHARD_COUNT] * SHARD_COUNT


def test_siren_ranges_never_lose_rows_when_there_are_fewer_rows_than_shards(
    sqlite_client,
):
    sqlite_client.execute("DELETE FROM unite_legale WHERE siren > '000000001'")

    unites_legales_count, siren_ranges = compute_siren_ranges(
        sqlite_client, SHARD_COUNT
    )

    assert unites_legales_count == 2
    counts = [
        count_in_range(sqlite_client, **siren_range) for siren_range in siren_ranges
    ]
    assert sum(counts) == 2


def test_siren_ranges_of_an_empty_table_is_a_single_unbounded_range(sqlite_client):
    sqlite_client.execute("DELETE FROM unite_legale")

    unites_legales_count, siren_ranges = compute_siren_ranges(
        sqlite_client, SHARD_COUNT
    )

    assert unites_legales_count == 0
    assert siren_ranges == [{"siren_start": None, "siren_end": None}]


# select_fields_to_index_query()


def test_query_without_bounds_has_no_predicate_and_no_parameter():
    query, params = select_fields_to_index_query()

    assert "ul.siren >=" not in query
    assert "ul.siren <" not in query
    assert params == []


def test_query_bounds_are_appended_in_parameter_order():
    query, params = select_fields_to_index_query(
        siren_start="356000000", siren_end="552100554"
    )

    assert query.index("ul.siren >= ?") < query.index("ul.siren < ?")
    assert params == ["356000000", "552100554"]


@pytest.mark.parametrize(
    "siren_range, expected_predicate, expected_params",
    [
        ({"siren_start": "356000000"}, "ul.siren >= ?", ["356000000"]),
        ({"siren_end": "356000000"}, "ul.siren < ?", ["356000000"]),
    ],
)
def test_query_with_a_single_bound(siren_range, expected_predicate, expected_params):
    query, params = select_fields_to_index_query(**siren_range)

    assert expected_predicate in query
    assert params == expected_params


def test_query_ranges_are_valid_sql_and_select_their_shard(sqlite_client):
    """The generated SQL must run: a typo in the predicate would only show up on a
    3-hour run otherwise."""
    _, siren_ranges = compute_siren_ranges(sqlite_client, SHARD_COUNT)

    for siren_range in siren_ranges:
        _, params = select_fields_to_index_query(**siren_range)
        predicates = " ".join(
            predicate
            for predicate, bound in (
                ("AND ul.siren >= ?", siren_range["siren_start"]),
                ("AND ul.siren < ?", siren_range["siren_end"]),
            )
            if bound is not None
        )
        query = f"SELECT count(*) FROM unite_legale ul WHERE 1 {predicates}"

        assert sqlite_client.execute(query, params).fetchone()[0] > 0
