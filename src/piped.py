import asyncio
import json
import logging
import os
import re
from dataclasses import dataclass
from typing import Optional

import aiohttp

from .db import YtCandidate

logger = logging.getLogger("vid_robot.piped")


def _debug_enabled() -> bool:
    return os.getenv("PIPED_DEBUG", "").strip().lower() in {"1", "true", "yes", "y", "on"}


class PipedError(RuntimeError):
    pass


@dataclass(frozen=True)
class StreamInfo:
    video_id: str
    title: str
    duration: Optional[int]
    thumbnail_url: Optional[str]
    livestream: bool


_VIDEO_ID_RE = re.compile(r"[?&]v=([A-Za-z0-9_-]{6,})")


def _extract_video_id(item: dict) -> Optional[str]:
    video_id = item.get("videoId") or item.get("id")
    if isinstance(video_id, str) and video_id:
        return video_id
    url = item.get("url")
    if isinstance(url, str) and url:
        match = _VIDEO_ID_RE.search(url)
        if match:
            return match.group(1)
        if "watch?v=" in url:
            return url.split("watch?v=", 1)[1].split("&", 1)[0]
    return None


def _parse_duration(value: object) -> Optional[int]:
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
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


def _parse_view_count(value: object) -> Optional[int]:
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.isdigit():
        return int(value)
    return None


class PipedClient:
    def __init__(self, base_url: str, timeout_seconds: float) -> None:
        self._base_url = base_url.rstrip("/")
        self._timeout = aiohttp.ClientTimeout(total=timeout_seconds)
        self._search_params: dict[str, dict] = {
            "q_key": "q",
            "params": {"filter": "videos"},
        }

    async def _get_json(self, base_url: str, path: str, params: dict) -> object:
        url = f"{base_url}{path}"
        try:
            async with aiohttp.ClientSession(timeout=self._timeout) as session:
                async with session.get(url, params=params) as response:
                    if response.status != 200:
                        text = await response.text()
                        raise PipedError(f"{path} failed: {response.status} {text}")
                    raw = await response.read()
                    try:
                        text = raw.decode("utf-8", errors="replace")
                        return json.loads(text)
                    except json.JSONDecodeError as exc:
                        raise PipedError(f"{path} invalid JSON") from exc
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            raise PipedError(f"{path} failed: {exc}") from exc

    async def search(self, query: str, limit: int) -> list[YtCandidate]:
        params = dict(self._search_params["params"])
        q_key = self._search_params["q_key"]
        params[q_key] = query
        if _debug_enabled():
            logger.info("Piped search base=%s params=%s", self._base_url, params)
        data = await self._get_json(self._base_url, "/search", params)
        if _debug_enabled():
            sample = json.dumps(data, ensure_ascii=False)[:800]
            logger.info("Piped search payload sample=%s", sample)
        candidates = parse_search_items(data, limit)
        if not candidates and params.get("filter") == "videos":
            fallback_params = dict(params)
            fallback_params.pop("filter", None)
            if _debug_enabled():
                logger.info("Piped search fallback params=%s", fallback_params)
            data = await self._get_json(self._base_url, "/search", fallback_params)
            if _debug_enabled():
                sample = json.dumps(data, ensure_ascii=False)[:800]
                logger.info("Piped search fallback payload sample=%s", sample)
            candidates = parse_search_items(data, limit)
        return candidates

    async def streams(self, video_id: str) -> StreamInfo:
        data = await self._get_json(self._base_url, f"/streams/{video_id}", {})
        if not isinstance(data, dict):
            raise PipedError("Invalid streams response")
        title = data.get("title")
        if not isinstance(title, str) or not title:
            raise PipedError("Missing title in streams response")
        duration = _parse_duration(data.get("duration"))
        thumb = data.get("thumbnailUrl") or data.get("thumbnail")
        thumbnail_url = thumb if isinstance(thumb, str) else None
        livestream = bool(data.get("livestream"))
        return StreamInfo(
            video_id=video_id,
            title=title,
            duration=duration,
            thumbnail_url=thumbnail_url,
            livestream=livestream,
        )


def parse_search_items(data: object, limit: int) -> list[YtCandidate]:
    items = data.get("items") if isinstance(data, dict) else data
    if not isinstance(items, list):
        if _debug_enabled():
            logger.info(
                "Piped search unexpected payload type=%s keys=%s",
                type(data),
                getattr(data, "keys", lambda: [])(),
            )
        return []

    candidates: list[YtCandidate] = []
    if _debug_enabled() and items:
        logger.info(
            "Piped search items count=%s first_keys=%s",
            len(items),
            list(items[0].keys()) if isinstance(items[0], dict) else type(items[0]),
        )
    for idx, item in enumerate(items, start=1):
        if idx > limit:
            break
        if not isinstance(item, dict):
            continue
        if item.get("type") not in (None, "video", "stream"):
            continue
        video_id = _extract_video_id(item)
        title = item.get("title")
        if not isinstance(title, str) or not title:
            continue
        duration = _parse_duration(item.get("duration"))
        thumb = item.get("thumbnail") or item.get("thumbnailUrl")
        thumbnail_url = thumb if isinstance(thumb, str) else None
        view_count = _parse_view_count(item.get("views") or item.get("viewCount"))
        is_short = item.get("isShort")
        if not isinstance(is_short, bool):
            is_short = None

        if not video_id:
            continue
        candidates.append(
            YtCandidate(
                youtube_id=video_id,
                title=title,
                duration=duration,
                view_count=view_count,
                thumbnail_url=thumbnail_url,
                source_url=f"https://www.youtube.com/watch?v={video_id}",
                rank=idx,
                is_short=is_short,
            )
        )

    return candidates
