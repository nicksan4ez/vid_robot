import asyncio
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from .db import YtCandidate

MAX_FILESIZE = "49M"


class YtDlpError(RuntimeError):
    pass


@dataclass(frozen=True)
class DownloadResult:
    file_path: Path


async def _run_yt_dlp(args: list[str]) -> tuple[int, str, str]:
    try:
        process = await asyncio.create_subprocess_exec(
            *args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
    except FileNotFoundError as exc:
        raise YtDlpError("yt-dlp is not installed or not in PATH") from exc

    stdout, stderr = await process.communicate()
    return process.returncode, stdout.decode("utf-8", errors="replace"), stderr.decode(
        "utf-8", errors="replace"
    )


async def search(query: str, limit: int) -> list[YtCandidate]:
    args = [
        "yt-dlp",
        f"ytsearch{limit}:{query}",
        "--skip-download",
        "--dump-json",
        "--no-warnings",
        "--no-playlist",
    ]
    code, out, err = await _run_yt_dlp(args)
    if code != 0:
        raise YtDlpError(err.strip() or "yt-dlp search failed")

    candidates: list[YtCandidate] = []
    for idx, line in enumerate(out.splitlines(), start=1):
        if not line.strip():
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue

        youtube_id = payload.get("id") or ""
        title = payload.get("title") or ""
        duration = payload.get("duration")
        duration_value = None
        if isinstance(duration, (int, float)):
            duration_value = int(duration)
        thumbnail = payload.get("thumbnail")
        webpage_url = payload.get("webpage_url") or payload.get("original_url") or ""

        if not youtube_id or not title or not webpage_url:
            continue

        candidates.append(
            YtCandidate(
                youtube_id=youtube_id,
                title=title,
                duration=duration_value,
                thumbnail_url=thumbnail if isinstance(thumbnail, str) else None,
                source_url=webpage_url,
                rank=idx,
            )
        )

    return candidates


def _cleanup_prefix(directory: Path, prefix: str) -> None:
    for path in directory.glob(f"{prefix}.*"):
        try:
            path.unlink()
        except FileNotFoundError:
            continue


def _find_downloaded_file(directory: Path, prefix: str) -> Optional[Path]:
    matches = list(directory.glob(f"{prefix}.*"))
    if not matches:
        return None
    matches.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return matches[0]


async def download(source_url: str, output_dir: Path, job_id: str) -> DownloadResult:
    output_dir.mkdir(parents=True, exist_ok=True)

    format_candidates = [
        "bestvideo[height<=720]+bestaudio/best[height<=720]",
        "bestvideo[height<=480]+bestaudio/best[height<=480]",
        "bestvideo[height<=360]+bestaudio/best[height<=360]",
        "best[height<=720]",
        "best[height<=480]",
        "best[height<=360]",
    ]

    output_template = str(output_dir / f"{job_id}.%(ext)s")

    last_error = ""
    for fmt in format_candidates:
        _cleanup_prefix(output_dir, job_id)
        args = [
            "yt-dlp",
            source_url,
            "-f",
            fmt,
            "--max-filesize",
            MAX_FILESIZE,
            "--merge-output-format",
            "mp4",
            "--no-playlist",
            "--no-warnings",
            "-o",
            output_template,
        ]
        code, out, err = await _run_yt_dlp(args)
        if code == 0:
            path = _find_downloaded_file(output_dir, job_id)
            if path is not None and path.exists():
                return DownloadResult(file_path=path)
            last_error = "download finished but file is missing"
        else:
            last_error = err.strip() or out.strip() or "yt-dlp download failed"

    raise YtDlpError(last_error)
