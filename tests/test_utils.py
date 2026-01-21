from src.db import Database
from src.utils import format_duration, truncate_text


def test_normalize_query() -> None:
    assert Database.normalize_query("  Hello   world ") == "hello world"


def test_format_duration() -> None:
    assert format_duration(None) == "?"
    assert format_duration(-1) == "?"
    assert format_duration(59) == "00:59"
    assert format_duration(61) == "01:01"
    assert format_duration(3601) == "1:00:01"


def test_truncate_text() -> None:
    assert truncate_text("hello", 10) == "hello"
    assert truncate_text("hello world", 5) == "he..."
    assert truncate_text("hello", 3) == "hel"
