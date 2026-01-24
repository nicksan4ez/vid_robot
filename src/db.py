import secrets
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional

import aiosqlite


@dataclass(frozen=True)
class PmToken:
    token: str
    query_text: str
    query_norm: str
    created_at: int
    expires_at: int


@dataclass(frozen=True)
class YtCandidate:
    youtube_id: str
    title: str
    duration: Optional[int]
    view_count: Optional[int]
    thumbnail_url: Optional[str]
    source_url: str
    rank: int
    is_short: Optional[bool] = None


class Database:
    def __init__(self, path: Path) -> None:
        self._path = path
        self._conn: Optional[aiosqlite.Connection] = None

    async def connect(self) -> None:
        self._conn = await aiosqlite.connect(self._path)
        self._conn.row_factory = aiosqlite.Row
        await self._conn.execute("PRAGMA journal_mode=WAL;")
        await self._conn.execute("PRAGMA foreign_keys=ON;")
        await self._conn.commit()

    async def close(self) -> None:
        if self._conn is not None:
            await self._conn.close()
            self._conn = None

    async def init(self) -> None:
        assert self._conn is not None
        await self._conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS videos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                file_id TEXT,
                file_unique_id TEXT,
                youtube_id TEXT,
                source_url TEXT,
                title TEXT,
                duration INTEGER,
                width INTEGER,
                height INTEGER,
                size INTEGER,
                thumb_url TEXT,
                use_count INTEGER NOT NULL DEFAULT 0,
                created_at INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS video_queries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                query_norm TEXT NOT NULL,
                video_id INTEGER NOT NULL,
                created_at INTEGER NOT NULL,
                UNIQUE(query_norm, video_id),
                FOREIGN KEY(video_id) REFERENCES videos(id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_video_queries_query
                ON video_queries(query_norm);

            CREATE TABLE IF NOT EXISTS pm_tokens (
                token TEXT PRIMARY KEY,
                query_text TEXT NOT NULL,
                query_norm TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                expires_at INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS yt_candidates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                token TEXT NOT NULL,
                rank INTEGER NOT NULL,
                youtube_id TEXT NOT NULL,
                title TEXT NOT NULL,
                duration INTEGER,
                view_count INTEGER,
                thumbnail_url TEXT,
                source_url TEXT NOT NULL,
                FOREIGN KEY(token) REFERENCES pm_tokens(token) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_yt_candidates_token
                ON yt_candidates(token);

            CREATE TABLE IF NOT EXISTS user_video_stats (
                user_id INTEGER NOT NULL,
                video_id INTEGER NOT NULL,
                use_count INTEGER NOT NULL,
                last_used_at INTEGER NOT NULL,
                PRIMARY KEY (user_id, video_id)
            );

            CREATE INDEX IF NOT EXISTS idx_user_video_stats_user
                ON user_video_stats(user_id, use_count DESC, last_used_at DESC);
            """
        )
        await self._ensure_columns()
        await self._conn.commit()

    async def _ensure_columns(self) -> None:
        assert self._conn is not None
        await self._ensure_video_columns()
        await self._ensure_candidate_columns()

    async def _ensure_video_columns(self) -> None:
        assert self._conn is not None
        cursor = await self._conn.execute("PRAGMA table_info(videos)")
        rows = await cursor.fetchall()
        await cursor.close()
        columns = {row["name"] for row in rows}
        if "use_count" not in columns:
            await self._conn.execute(
                "ALTER TABLE videos ADD COLUMN use_count INTEGER NOT NULL DEFAULT 0"
            )

    async def _ensure_candidate_columns(self) -> None:
        assert self._conn is not None
        cursor = await self._conn.execute("PRAGMA table_info(yt_candidates)")
        rows = await cursor.fetchall()
        await cursor.close()
        columns = {row["name"] for row in rows}
        if "view_count" not in columns:
            await self._conn.execute(
                "ALTER TABLE yt_candidates ADD COLUMN view_count INTEGER"
            )

    @staticmethod
    def normalize_query(query: str) -> str:
        return " ".join(query.strip().lower().split())

    async def purge_expired_tokens(self, now: Optional[int] = None) -> None:
        assert self._conn is not None
        now_ts = now or int(time.time())
        await self._conn.execute(
            "DELETE FROM pm_tokens WHERE expires_at <= ?", (now_ts,)
        )
        await self._conn.commit()

    async def create_pm_token(self, query_text: str, query_norm: str, ttl_seconds: int) -> PmToken:
        assert self._conn is not None
        token = secrets.token_hex(8)
        now_ts = int(time.time())
        expires = now_ts + ttl_seconds
        await self._conn.execute(
            "INSERT INTO pm_tokens (token, query_text, query_norm, created_at, expires_at) VALUES (?, ?, ?, ?, ?)",
            (token, query_text, query_norm, now_ts, expires),
        )
        await self._conn.commit()
        return PmToken(
            token=token,
            query_text=query_text,
            query_norm=query_norm,
            created_at=now_ts,
            expires_at=expires,
        )

    async def get_pm_token(self, token: str) -> Optional[PmToken]:
        assert self._conn is not None
        cursor = await self._conn.execute(
            "SELECT token, query_text, query_norm, created_at, expires_at FROM pm_tokens WHERE token = ?",
            (token,),
        )
        row = await cursor.fetchone()
        await cursor.close()
        if row is None:
            return None
        return PmToken(
            token=row["token"],
            query_text=row["query_text"],
            query_norm=row["query_norm"],
            created_at=row["created_at"],
            expires_at=row["expires_at"],
        )

    async def store_candidates(self, token: str, candidates: Iterable[YtCandidate]) -> None:
        assert self._conn is not None
        await self._conn.execute("DELETE FROM yt_candidates WHERE token = ?", (token,))
        await self._conn.executemany(
            """
            INSERT INTO yt_candidates
                (token, rank, youtube_id, title, duration, view_count, thumbnail_url, source_url)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    token,
                    cand.rank,
                    cand.youtube_id,
                    cand.title,
                    cand.duration,
                    cand.view_count,
                    cand.thumbnail_url,
                    cand.source_url,
                )
                for cand in candidates
            ],
        )
        await self._conn.commit()

    async def get_candidates(self, token: str) -> list[YtCandidate]:
        assert self._conn is not None
        cursor = await self._conn.execute(
            """
            SELECT youtube_id, title, duration, thumbnail_url, source_url, rank
            FROM yt_candidates
            WHERE token = ?
            ORDER BY rank ASC
            """,
            (token,),
        )
        rows = await cursor.fetchall()
        await cursor.close()
        return [
            YtCandidate(
                youtube_id=row["youtube_id"],
                title=row["title"],
                duration=row["duration"],
                view_count=row["view_count"],
                thumbnail_url=row["thumbnail_url"],
                source_url=row["source_url"],
                rank=row["rank"],
            )
            for row in rows
        ]

    async def create_video(
        self,
        *,
        file_id: str,
        file_unique_id: str,
        youtube_id: str,
        source_url: str,
        title: str,
        duration: Optional[int],
        width: Optional[int],
        height: Optional[int],
        size: Optional[int],
        thumb_url: Optional[str],
    ) -> int:
        assert self._conn is not None
        now_ts = int(time.time())
        cursor = await self._conn.execute(
            """
            INSERT INTO videos
                (file_id, file_unique_id, youtube_id, source_url, title, duration, width, height, size, thumb_url, use_count, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                file_id,
                file_unique_id,
                youtube_id,
                source_url,
                title,
                duration,
                width,
                height,
                size,
                thumb_url,
                0,
                now_ts,
            ),
        )
        await self._conn.commit()
        return int(cursor.lastrowid)

    async def link_query_to_video(self, query_norm: str, video_id: int) -> None:
        assert self._conn is not None
        now_ts = int(time.time())
        await self._conn.execute(
            "INSERT OR IGNORE INTO video_queries (query_norm, video_id, created_at) VALUES (?, ?, ?)",
            (query_norm, video_id, now_ts),
        )
        await self._conn.commit()

    async def find_cached_videos(self, query_norm: str, limit: int) -> list[dict]:
        assert self._conn is not None
        like_value = f"%{query_norm}%"
        cursor = await self._conn.execute(
            """
            SELECT v.id, v.file_id, v.title, v.thumb_url
            FROM videos v
            JOIN video_queries q ON q.video_id = v.id
            WHERE q.query_norm LIKE ? AND v.file_id IS NOT NULL
            ORDER BY q.created_at DESC
            LIMIT ?
            """,
            (like_value, limit),
        )
        rows = await cursor.fetchall()
        await cursor.close()
        return [dict(row) for row in rows]

    async def find_cached_videos_by_title(
        self, query_norm: str, exclude_ids: Iterable[int], limit: int
    ) -> list[dict]:
        assert self._conn is not None
        exclude = list(exclude_ids)
        like_value = f"%{query_norm}%"

        if exclude:
            placeholders = ",".join("?" for _ in exclude)
            sql = (
                "SELECT id, file_id, title, thumb_url "
                "FROM videos "
                "WHERE file_id IS NOT NULL AND lower(title) LIKE ? "
                f"AND id NOT IN ({placeholders}) "
                "ORDER BY use_count DESC, created_at DESC "
                "LIMIT ?"
            )
            params = [like_value, *exclude, limit]
        else:
            sql = (
                "SELECT id, file_id, title, thumb_url "
                "FROM videos "
                "WHERE file_id IS NOT NULL AND lower(title) LIKE ? "
                "ORDER BY use_count DESC, created_at DESC "
                "LIMIT ?"
            )
            params = [like_value, limit]

        cursor = await self._conn.execute(sql, params)
        rows = await cursor.fetchall()
        await cursor.close()
        return [dict(row) for row in rows]

    async def get_popular_videos(self, limit: int, exclude_ids: Optional[Iterable[int]] = None) -> list[dict]:
        assert self._conn is not None
        exclude = list(exclude_ids) if exclude_ids else []
        if exclude:
            placeholders = ",".join("?" for _ in exclude)
            sql = (
                "SELECT id, file_id, title, thumb_url "
                "FROM videos "
                "WHERE file_id IS NOT NULL AND id NOT IN ("
                f"{placeholders}"
                ") "
                "ORDER BY use_count DESC, created_at DESC "
                "LIMIT ?"
            )
            params = [*exclude, limit]
        else:
            sql = (
                "SELECT id, file_id, title, thumb_url "
                "FROM videos "
                "WHERE file_id IS NOT NULL "
                "ORDER BY use_count DESC, created_at DESC "
                "LIMIT ?"
            )
            params = [limit]
        cursor = await self._conn.execute(sql, params)
        rows = await cursor.fetchall()
        await cursor.close()
        return [dict(row) for row in rows]

    async def increment_usage(self, video_id: int) -> None:
        assert self._conn is not None
        await self._conn.execute(
            "UPDATE videos SET use_count = use_count + 1 WHERE id = ?",
            (video_id,),
        )
        await self._conn.commit()

    async def upsert_user_video_stat(self, user_id: int, video_id: int) -> None:
        assert self._conn is not None
        now_ts = int(time.time())
        await self._conn.execute(
            """
            INSERT INTO user_video_stats (user_id, video_id, use_count, last_used_at)
            VALUES (?, ?, 1, ?)
            ON CONFLICT(user_id, video_id)
            DO UPDATE SET use_count = use_count + 1, last_used_at = excluded.last_used_at
            """,
            (user_id, video_id, now_ts),
        )
        await self._conn.commit()

    async def get_user_top_videos(self, user_id: int, limit: int) -> list[dict]:
        assert self._conn is not None
        cursor = await self._conn.execute(
            """
            SELECT v.id, v.file_id, v.title, v.thumb_url
            FROM user_video_stats s
            JOIN videos v ON v.id = s.video_id
            WHERE s.user_id = ? AND v.file_id IS NOT NULL
            ORDER BY s.use_count DESC, s.last_used_at DESC
            LIMIT ?
            """,
            (user_id, limit),
        )
        rows = await cursor.fetchall()
        await cursor.close()
        return [dict(row) for row in rows]

    async def get_user_ranked_video_ids(
        self, user_id: int, video_ids: Iterable[int]
    ) -> list[int]:
        assert self._conn is not None
        ids = list(video_ids)
        if not ids:
            return []
        placeholders = ",".join("?" for _ in ids)
        cursor = await self._conn.execute(
            f"""
            SELECT video_id
            FROM user_video_stats
            WHERE user_id = ? AND video_id IN ({placeholders})
            ORDER BY use_count DESC, last_used_at DESC
            """,
            (user_id, *ids),
        )
        rows = await cursor.fetchall()
        await cursor.close()
        return [int(row["video_id"]) for row in rows]

    async def get_video_by_id(self, video_id: int) -> Optional[dict]:
        assert self._conn is not None
        cursor = await self._conn.execute(
            """
            SELECT id, file_id, title, thumb_url
            FROM videos
            WHERE id = ?
            """,
            (video_id,),
        )
        row = await cursor.fetchone()
        await cursor.close()
        if row is None:
            return None
        return dict(row)
