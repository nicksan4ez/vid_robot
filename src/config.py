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


def _get_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


@dataclass(frozen=True)
class Settings:
    bot_token: str
    db_path: Path
    download_dir: Path
    max_inline_results: int
    popular_inline_results: int
    max_yt_results: int
    yt_inline_results: int
    max_concurrent_jobs: int
    pm_token_ttl_seconds: int
    inline_show_yt_cards: bool
    piped_api_base_url: str
    piped_timeout_seconds: float
    piped_api_base_urls: list[str]
    piped_cache_ttl_seconds: int


def load_settings() -> Settings:
    token = os.getenv("BOT_TOKEN")
    if not token:
        raise RuntimeError("BOT_TOKEN is required")
    piped_base = os.getenv("PIPED_API_BASE_URL", "").strip()
    if not piped_base:
        raise RuntimeError("PIPED_API_BASE_URL is required")
    piped_bases = os.getenv("PIPED_API_BASE_URLS", "").strip()

    db_path = Path(os.getenv("DB_PATH", "./data/vid_robot.db"))
    download_dir = Path(os.getenv("DOWNLOAD_DIR", "/tmp/vid_robot"))

    return Settings(
        bot_token=token,
        db_path=db_path,
        download_dir=download_dir,
        max_inline_results=_get_int("MAX_INLINE_RESULTS", 10),
        popular_inline_results=_get_int("POPULAR_INLINE_RESULTS", 20),
        max_yt_results=_get_int("MAX_YT_RESULTS", 5),
        yt_inline_results=_get_int("YT_INLINE_RESULTS", 10),
        max_concurrent_jobs=_get_int("MAX_CONCURRENT_JOBS", 2),
        pm_token_ttl_seconds=_get_int("PM_TOKEN_TTL_SECONDS", 3600),
        inline_show_yt_cards=_get_bool("INLINE_SHOW_YT_CARDS", False),
        piped_api_base_url=piped_base,
        piped_timeout_seconds=_get_float("PIPED_TIMEOUT_SECONDS", 4.0),
        piped_api_base_urls=[
            base.strip()
            for base in piped_bases.split(",")
            if base.strip()
        ],
        piped_cache_ttl_seconds=_get_int("PIPED_CACHE_TTL_SECONDS", 120),
    )
