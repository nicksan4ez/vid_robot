from src.main import build_prepare_text, build_switch_pm_text
from src.db import YtCandidate


def test_build_switch_pm_text_length() -> None:
    text = build_switch_pm_text("очень длинный запрос " * 10)
    assert len(text) <= 64


def test_build_prepare_text_includes_query() -> None:
    candidates = [
        YtCandidate(
            youtube_id="abc",
            title="Test video",
            duration=42,
            thumbnail_url=None,
            source_url="https://youtube.com/watch?v=abc",
            rank=1,
        )
    ]
    text = build_prepare_text(candidates, "кот падает")
    assert "кот падает" in text
    assert "Test video" in text
