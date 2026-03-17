from ingest.date_utils import parse_publication_date


def test_parse_epoch_10_digits() -> None:
    assert parse_publication_date("1728284400") == "2024-10-07"


def test_parse_epoch_9_digits() -> None:
    assert parse_publication_date("883641600") == "1998-01-01"


def test_parse_none_like() -> None:
    assert parse_publication_date("None") is None


def test_parse_invalid_date_string() -> None:
    assert parse_publication_date("2019 02 31") is None


def test_parse_valid_date_string() -> None:
    assert parse_publication_date("2024-02-29") == "2024-02-29"
