import pytest

from data_pipelines_annuaire.helpers.sqlite_client import SqliteClient


@pytest.fixture
def sqlite_client(tmp_path):
    client = SqliteClient(str(tmp_path / "test.db"))
    yield client
    client.db_conn.close()


# SqliteClient.to_csv()


def test_to_csv_writes_header_and_rows(sqlite_client, tmp_path):
    sqlite_client.execute("CREATE TABLE people (id INTEGER, name TEXT)")
    sqlite_client.execute("INSERT INTO people VALUES (1, 'Alice'), (2, 'Bob')")
    sqlite_client.db_conn.commit()

    output = tmp_path / "people.csv"
    local_file = sqlite_client.to_csv("SELECT * FROM people ORDER BY id", str(output))

    assert output.read_text().splitlines() == ["id,name", "1,Alice", "2,Bob"]
    assert local_file.lines_count() == 3  # header + 2 rows


def test_to_csv_empty_result_writes_header_only(sqlite_client, tmp_path):
    sqlite_client.execute("CREATE TABLE people (id INTEGER, name TEXT)")
    sqlite_client.db_conn.commit()

    output = tmp_path / "empty.csv"
    local_file = sqlite_client.to_csv("SELECT * FROM people", str(output))

    assert output.read_text().splitlines() == ["id,name"]
    assert local_file.lines_count() == 1
