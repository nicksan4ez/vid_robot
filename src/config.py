import os
from dataclasses import dataclass
from pathlib import Path


def _get_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def _get_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    return int(raw)


@dataclass(frozen=True)
class Settings:
    bot_token: str
    db_path: Path
    download_dir: Path
    max_inline_results: int
    max_yt_results: int
    max_concurrent_jobs: int
    pm_token_ttl_seconds: int
    inline_show_yt_cards: bool
    youtube_api_key: str


def load_settings() -> Settings:
    token = os.getenv("BOT_TOKEN")
    if not token:
        raise RuntimeError("BOT_TOKEN is required")
    youtube_key = os.getenv("YOUTUBE_API_KEY")
    if not youtube_key:
        raise RuntimeError("YOUTUBE_API_KEY is required")

    db_path = Path(os.getenv("DB_PATH", "./data/vid_robot.db"))
    download_dir = Path(os.getenv("DOWNLOAD_DIR", "/tmp/vid_robot"))

    return Settings(
        bot_token=token,
        db_path=db_path,
        download_dir=download_dir,
        max_inline_results=_get_int("MAX_INLINE_RESULTS", 10),
        max_yt_results=_get_int("MAX_YT_RESULTS", 5),
        max_concurrent_jobs=_get_int("MAX_CONCURRENT_JOBS", 2),
        pm_token_ttl_seconds=_get_int("PM_TOKEN_TTL_SECONDS", 3600),
        inline_show_yt_cards=_get_bool("INLINE_SHOW_YT_CARDS", False),
        youtube_api_key=youtube_key,
    )
