from src.main import build_switch_pm_text


def test_build_switch_pm_text_length() -> None:
    text = build_switch_pm_text()
    assert text == "Найти и подготовить 🎬 ≈ 10 сек"

