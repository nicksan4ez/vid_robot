import asyncio
import json
import logging
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from .db import YtCandidate

MAX_FILESIZE = "49M"
logger = logging.getLogger("vid_robot.ytdlp")


def _debug_enabled() -> tuple[bool, int]:
    enabled = os.getenv("YTDLP_DEBUG", "").strip().lower() in {"1", "true", "yes", "y", "on"}
    lines_raw = os.getenv("YTDLP_DEBUG_LINES", "5")
    try:
        lines = max(1, int(lines_raw))
    except ValueError:
        lines = 5
    return enabled, lines


class YtDlpError(RuntimeError):
    pass


@dataclass(frozen=True)
class DownloadResult:
    file_path: Path


def _get_timeout(default: float) -> float:
    timeout_raw = os.getenv("YTDLP_TIMEOUT_SECONDS", "").strip()
    if not timeout_raw:
        return default
    try:
        return max(default, float(timeout_raw))
    except ValueError:
        return default


def _common_yt_dlp_args() -> list[str]:
    args: list[str] = []
    cookies_file = os.getenv("YTDLP_COOKIES_FILE", "").strip()
    if cookies_file:
        args.extend(["--cookies", cookies_file])
    extractor_args = os.getenv("YTDLP_EXTRACTOR_ARGS", "").strip()
    if extractor_args:
        args.extend(["--extractor-args", extractor_args])
    return args


async def _run_yt_dlp(args: list[str], timeout_seconds: float | None = None) -> tuple[int, str, str]:
    try:
        process = await asyncio.create_subprocess_exec(
            *args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
    except FileNotFoundError as exc:
        raise YtDlpError("yt-dlp is not installed or not in PATH") from exc

    if timeout_seconds is None:
        timeout = _get_timeout(6.0)
    else:
        timeout = max(1.0, timeout_seconds)

    try:
        stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=timeout)
    except asyncio.TimeoutError:
        process.kill()
        stdout, stderr = await process.communicate()
        returncode = process.returncode if process.returncode is not None else -9
        return (
            returncode,
            stdout.decode("utf-8", errors="replace"),
            stderr.decode("utf-8", errors="replace") + "\nyt-dlp timeout",
        )

    return process.returncode, stdout.decode("utf-8", errors="replace"), stderr.decode(
        "utf-8", errors="replace"
    )


async def fetch_video_info(video_id: str) -> Optional[YtCandidate]:
    socket_timeout = os.getenv("YTDLP_SOCKET_TIMEOUT", "").strip()
    args = [
        "yt-dlp",
        f"https://www.youtube.com/watch?v={video_id}",
        "--skip-download",
        "--dump-json",
        "--no-warnings",
    ]
    args.extend(_common_yt_dlp_args())
    if socket_timeout:
        args.extend(["--socket-timeout", socket_timeout])
    start = time.monotonic()
    code, out, err = await _run_yt_dlp(args, timeout_seconds=_get_timeout(30.0))
    debug, lines = _debug_enabled()
    if debug:
        elapsed = time.monotonic() - start
        logger.info("yt-dlp info args=%s elapsed=%.2fs code=%s", args, elapsed, code)
        if err.strip():
            logger.info(
                "yt-dlp info stderr (first %s lines):\n%s",
                lines,
                "\n".join(err.splitlines()[:lines]),
            )
        if out.strip():
            logger.info(
                "yt-dlp info stdout (first %s lines):\n%s",
                lines,
                "\n".join(out.splitlines()[:lines]),
            )
    if code != 0:
        raise YtDlpError(err.strip() or "yt-dlp info failed")

    try:
        payload = json.loads(out.strip().splitlines()[-1])
    except (json.JSONDecodeError, IndexError):
        return None

    youtube_id = payload.get("id") or video_id
    title = payload.get("title") or ""
    if not title:
        return None
    duration = _extract_duration(payload)
    thumbnail = payload.get("thumbnail")
    webpage_url = payload.get("webpage_url") or payload.get("original_url") or ""
    view_count_value = payload.get("view_count")
    view_count = int(view_count_value) if isinstance(view_count_value, int) else None

    return YtCandidate(
        youtube_id=youtube_id,
        title=title,
        duration=duration,
        view_count=view_count,
        thumbnail_url=thumbnail if isinstance(thumbnail, str) else None,
        source_url=webpage_url or f"https://www.youtube.com/watch?v={youtube_id}",
        rank=1,
    )


async def fetch_media_info(url: str) -> Optional[YtCandidate]:
    args = [
        "yt-dlp",
        url,
        "--skip-download",
        "--dump-json",
        "--no-warnings",
    ]
    args.extend(_common_yt_dlp_args())
    code, out, err = await _run_yt_dlp(args, timeout_seconds=_get_timeout(30.0))
    debug, lines = _debug_enabled()
    if debug:
        logger.info("yt-dlp info args=%s code=%s", args, code)
        if err.strip():
            logger.info(
                "yt-dlp info stderr (first %s lines):\n%s",
                lines,
                "\n".join(err.splitlines()[:lines]),
            )
    if code != 0:
        raise YtDlpError(err.strip() or "yt-dlp info failed")

    try:
        payload = json.loads(out.strip().splitlines()[-1])
    except (json.JSONDecodeError, IndexError):
        return None

    title = payload.get("title") or ""
    if not title:
        return None
    duration = _extract_duration(payload)
    thumbnail = payload.get("thumbnail")
    webpage_url = payload.get("webpage_url") or payload.get("original_url") or url
    view_count_value = payload.get("view_count")
    view_count = int(view_count_value) if isinstance(view_count_value, int) else None
    media_id = payload.get("id") or ""

    return YtCandidate(
        youtube_id=media_id,
        title=title,
        duration=duration,
        view_count=view_count,
        thumbnail_url=thumbnail if isinstance(thumbnail, str) else None,
        source_url=webpage_url,
        rank=1,
    )


def _extract_duration(payload: dict) -> Optional[int]:
    duration_value = payload.get("duration")
    if isinstance(duration_value, (int, float)):
        return int(duration_value)
    duration_string = payload.get("duration_string")
    if isinstance(duration_string, str):
        parsed = _parse_duration_string(duration_string)
        if parsed is not None:
            return parsed
    return None


def _parse_duration_string(value: str) -> Optional[int]:
    parts = value.strip().split(":")
    if not parts or any(not part.isdigit() for part in parts):
        return None
    if len(parts) == 2:
        minutes, seconds = parts
        return int(minutes) * 60 + int(seconds)
    if len(parts) == 3:
        hours, minutes, seconds = parts
        return int(hours) * 3600 + int(minutes) * 60 + int(seconds)
    return None


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
        "bestvideo[ext=mp4][vcodec^=avc1][height<=720]+bestaudio[ext=m4a]/best[ext=mp4][height<=720]",
        "bestvideo[ext=mp4][vcodec^=avc1][height<=480]+bestaudio[ext=m4a]/best[ext=mp4][height<=480]",
        "bestvideo[ext=mp4][vcodec^=avc1][height<=360]+bestaudio[ext=m4a]/best[ext=mp4][height<=360]",
        "bestvideo[vcodec^=avc1][height<=720]+bestaudio/best[height<=720]",
        "bestvideo[vcodec^=avc1][height<=480]+bestaudio/best[height<=480]",
        "bestvideo[vcodec^=avc1][height<=360]+bestaudio/best[height<=360]",
        "best[ext=mp4][height<=720]",
        "best[ext=mp4][height<=480]",
        "best[ext=mp4][height<=360]",
        "best[height<=720]",
        "best[height<=480]",
        "best[height<=360]",
        "best",
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
            "--remux-video",
            "mp4",
            "--recode-video",
            "mp4",
            "--postprocessor-args",
            "FFmpegVideoConvertor:-c:v libx264 -profile:v main -level 4.0 -pix_fmt yuv420p -c:a aac -b:a 128k -movflags +faststart",
            "--no-playlist",
            "--no-warnings",
            "-o",
            output_template,
        ]
        args.extend(_common_yt_dlp_args())
        code, out, err = await _run_yt_dlp(args, timeout_seconds=_get_timeout(120.0))
        if code == 0:
            path = _find_downloaded_file(output_dir, job_id)
            if path is not None and path.exists():
                return DownloadResult(file_path=path)
            last_error = "download finished but file is missing"
        else:
            last_error = err.strip() or out.strip() or "yt-dlp download failed"

    raise YtDlpError(last_error)
