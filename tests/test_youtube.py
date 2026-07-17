import asyncio
from pathlib import Path

import pytest

from src import youtube


FORMAT_ERROR = "ERROR: [youtube] video-id: Requested format is not available"


def _configure_cookies(monkeypatch, tmp_path: Path) -> None:
    cookie_file = tmp_path / "cookies.txt"
    cookie_file.write_text("# Netscape HTTP Cookie File\n", encoding="utf-8")
    monkeypatch.setenv("YTDLP_COOKIES_FILE", str(cookie_file))
    monkeypatch.setenv("YTDLP_TMP_DIR", str(tmp_path / "yt_tmp"))


def _format_arg(args: list[str]) -> str:
    return args[args.index("-f") + 1]


def _write_result(args: list[str], content: bytes) -> None:
    output_template = Path(args[args.index("-o") + 1])
    output_file = Path(str(output_template).replace("%(ext)s", "mp4"))
    output_file.write_bytes(content)


def test_download_tries_all_authenticated_selectors_before_anonymous_fallback(
    monkeypatch, tmp_path: Path
) -> None:
    _configure_cookies(monkeypatch, tmp_path)
    calls: list[list[str]] = []
    authenticated_calls = 0

    async def fake_run(
        args: list[str], timeout_seconds: float | None = None
    ) -> tuple[int, str, str]:
        nonlocal authenticated_calls
        calls.append(args)
        if "--cookies" in args:
            authenticated_calls += 1
            if authenticated_calls == 1:
                return 1, "", FORMAT_ERROR
            _write_result(args, b"authenticated-video")
            return 0, "", ""
        _write_result(args, b"public-preview")
        return 0, "", ""

    monkeypatch.setattr(youtube, "_run_yt_dlp", fake_run)

    result = asyncio.run(
        youtube.download("https://youtu.be/video-id", tmp_path, "authenticated")
    )

    assert result.file_path.read_bytes() == b"authenticated-video"
    assert len(calls) == 2
    assert all("--cookies" in args for args in calls)


def test_download_retries_same_selectors_without_cookies_after_cookie_phase(
    monkeypatch, tmp_path: Path
) -> None:
    _configure_cookies(monkeypatch, tmp_path)
    calls: list[list[str]] = []

    async def fake_run(
        args: list[str], timeout_seconds: float | None = None
    ) -> tuple[int, str, str]:
        calls.append(args)
        if "--cookies" in args:
            # Marker in stdout must still be recognized when stderr is non-empty.
            return 1, FORMAT_ERROR, "WARNING: no downloadable formats"
        _write_result(args, b"video")
        return 0, "", ""

    monkeypatch.setattr(youtube, "_run_yt_dlp", fake_run)

    result = asyncio.run(
        youtube.download("https://youtu.be/video-id", tmp_path, "regression")
    )

    cookie_calls = [args for args in calls if "--cookies" in args]
    anonymous_calls = [args for args in calls if "--cookies" not in args]
    assert result.file_path.read_bytes() == b"video"
    assert [_format_arg(args) for args in cookie_calls] == list(
        youtube.FORMAT_CANDIDATES
    )
    assert len(anonymous_calls) == 1
    assert _format_arg(anonymous_calls[0]) == youtube.FORMAT_CANDIDATES[0]


def test_download_does_not_drop_cookies_after_unrelated_errors(
    monkeypatch, tmp_path: Path
) -> None:
    _configure_cookies(monkeypatch, tmp_path)
    calls: list[list[str]] = []

    async def fake_run(
        args: list[str], timeout_seconds: float | None = None
    ) -> tuple[int, str, str]:
        calls.append(args)
        return 1, "", "ERROR: network unavailable"

    monkeypatch.setattr(youtube, "_run_yt_dlp", fake_run)

    with pytest.raises(youtube.YtDlpError, match="network unavailable"):
        asyncio.run(
            youtube.download("https://youtu.be/video-id", tmp_path, "network-error")
        )

    assert calls
    assert all("--cookies" in args for args in calls)
