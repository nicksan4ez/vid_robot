import asyncio
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import aiohttp

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


_DURATION_RE = re.compile(r"^PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?$")


def _parse_iso_duration(value: str) -> Optional[int]:
    match = _DURATION_RE.match(value)
    if not match:
        return None
    hours = int(match.group(1) or 0)
    minutes = int(match.group(2) or 0)
    seconds = int(match.group(3) or 0)
    return hours * 3600 + minutes * 60 + seconds


def _pick_thumbnail(snippet: dict) -> Optional[str]:
    thumbnails = snippet.get("thumbnails") or {}
    for key in ("high", "medium", "default"):
        entry = thumbnails.get(key) or {}
        url = entry.get("url")
        if isinstance(url, str):
            return url
    return None


async def search_via_api(query: str, limit: int, api_key: str) -> list[YtCandidate]:
    params = {
        "part": "snippet",
        "type": "video",
        "maxResults": str(limit),
        "q": query,
        "key": api_key,
    }
    timeout = aiohttp.ClientTimeout(total=5)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.get(
            "https://www.googleapis.com/youtube/v3/search", params=params
        ) as response:
            if response.status != 200:
                text = await response.text()
                raise YtDlpError(f"YouTube API search failed: {response.status} {text}")
            payload = await response.json()

    items = payload.get("items") or []
    if not items:
        return []

    video_ids: list[str] = []
    raw_candidates: list[tuple[str, str, Optional[str]]] = []
    for item in items:
        video_id = ((item.get("id") or {}).get("videoId")) or ""
        snippet = item.get("snippet") or {}
        title = snippet.get("title") or ""
        if not video_id or not title:
            continue
        thumb = _pick_thumbnail(snippet)
        video_ids.append(video_id)
        raw_candidates.append((video_id, title, thumb))

    durations: dict[str, Optional[int]] = {}
    view_counts: dict[str, Optional[int]] = {}
    if video_ids:
        params = {
            "part": "contentDetails,statistics",
            "id": ",".join(video_ids),
            "key": api_key,
        }
        timeout = aiohttp.ClientTimeout(total=5)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(
                "https://www.googleapis.com/youtube/v3/videos", params=params
            ) as response:
                if response.status == 200:
                    details = await response.json()
                    for item in details.get("items") or []:
                        vid = item.get("id") or ""
                        content = item.get("contentDetails") or {}
                        duration_value = content.get("duration") or ""
                        if vid and isinstance(duration_value, str):
                            durations[vid] = _parse_iso_duration(duration_value)
                        stats = item.get("statistics") or {}
                        view_count = stats.get("viewCount")
                        if vid and isinstance(view_count, str) and view_count.isdigit():
                            view_counts[vid] = int(view_count)

    candidates: list[YtCandidate] = []
    for idx, (video_id, title, thumb) in enumerate(raw_candidates, start=1):
        candidates.append(
            YtCandidate(
                youtube_id=video_id,
                title=title,
                duration=durations.get(video_id),
                view_count=view_counts.get(video_id),
                thumbnail_url=thumb,
                source_url=f"https://www.youtube.com/watch?v={video_id}",
                rank=idx,
            )
        )

    return candidates


async def fetch_video_info(video_id: str, api_key: str) -> Optional[YtCandidate]:
    params = {
        "part": "snippet,contentDetails,statistics",
        "id": video_id,
        "key": api_key,
    }
    timeout = aiohttp.ClientTimeout(total=5)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.get(
            "https://www.googleapis.com/youtube/v3/videos", params=params
        ) as response:
            if response.status != 200:
                text = await response.text()
                raise YtDlpError(f"YouTube API videos failed: {response.status} {text}")
            payload = await response.json()

    items = payload.get("items") or []
    if not items:
        return None

    item = items[0]
    snippet = item.get("snippet") or {}
    content = item.get("contentDetails") or {}
    title = snippet.get("title") or ""
    if not title:
        return None

    duration_raw = content.get("duration") or ""
    duration = _parse_iso_duration(duration_raw) if isinstance(duration_raw, str) else None
    thumb = _pick_thumbnail(snippet)
    stats = item.get("statistics") or {}
    view_count = stats.get("viewCount")
    if isinstance(view_count, str) and view_count.isdigit():
        view_count_value = int(view_count)
    else:
        view_count_value = None

    return YtCandidate(
        youtube_id=video_id,
        title=title,
        duration=duration,
        view_count=view_count_value,
        thumbnail_url=thumb,
        source_url=f"https://www.youtube.com/watch?v={video_id}",
        rank=1,
    )


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
        code, out, err = await _run_yt_dlp(args)
        if code == 0:
            path = _find_downloaded_file(output_dir, job_id)
            if path is not None and path.exists():
                return DownloadResult(file_path=path)
            last_error = "download finished but file is missing"
        else:
            last_error = err.strip() or out.strip() or "yt-dlp download failed"

    raise YtDlpError(last_error)
