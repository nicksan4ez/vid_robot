import asyncio
import contextlib
import logging
import os
import re
import time
from pathlib import Path

from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, F
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command, CommandStart
from aiogram.filters.command import CommandObject
from aiogram.types import CallbackQuery
from aiogram.types import (
    ChosenInlineResult,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InlineQuery,
    InlineQueryResultArticle,
    InlineQueryResultCachedVideo,
    InputMediaVideo,
    InputTextMessageContent,
    Message,
    ReplyKeyboardMarkup,
    KeyboardButton,
)
from aiogram.types.input_file import FSInputFile

from .config import load_settings
from .db import Database, YtCandidate
from .utils import format_duration
from .piped import PipedClient, PipedError
from .youtube import YtDlpError, download as yt_download
from .utils import parse_time_range
from .youtube import fetch_media_info, fetch_video_info


logger = logging.getLogger("vid_robot")

AGE_RESTRICTED_MARKERS = (
    "sign in to confirm your age",
    "age-restricted",
    "age restricted",
)

YOUTUBE_ID_RE = re.compile(
    r"(?:v=|/shorts/|youtu\.be/)([A-Za-z0-9_-]{6,})",
    re.IGNORECASE,
)

def is_age_restricted_error(message: str) -> bool:
    lowered = message.lower()
    return any(marker in lowered for marker in AGE_RESTRICTED_MARKERS)


class PrepManager:
    def __init__(
        self,
        bot: Bot,
        db: Database,
        download_dir: Path,
        max_concurrent: int,
    ) -> None:
        self._bot = bot
        self._db = db
        self._download_dir = download_dir
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._lock = asyncio.Lock()
        self._in_progress: set[str] = set()

    async def start_youtube(
        self,
        youtube_id: str,
        chat_id: int,
        query_norm: str | None,
        inline_message_id: str | None,
        candidate: YtCandidate | None,
        source_url: str | None,
        status_message_id: int | None = None,
        status_keywords: str | None = None,
    ) -> bool:
        key = f"{chat_id}:{source_url or youtube_id}"
        async with self._lock:
            if key in self._in_progress:
                return False
            self._in_progress.add(key)
        asyncio.create_task(
            self._run_youtube(
                youtube_id,
                chat_id,
                query_norm,
                inline_message_id,
                candidate,
                source_url,
                status_message_id,
                status_keywords,
                key,
            )
        )
        return True

    async def _run_youtube(
        self,
        youtube_id: str,
        chat_id: int,
        query_norm: str | None,
        inline_message_id: str | None,
        candidate: YtCandidate | None,
        source_url: str | None,
        status_message_id: int | None,
        status_keywords: str | None,
        key: str,
    ) -> None:
        async with self._semaphore:
            try:
                await self._process_youtube(
                    youtube_id,
                    chat_id,
                    query_norm,
                    inline_message_id,
                    candidate,
                    source_url,
                    status_message_id,
                    status_keywords,
                )
            except Exception:
                logger.exception("Preparation failed for youtube_id=%s", youtube_id)
                await self._bot.send_message(
                    chat_id,
                    "Не удалось подготовить видео. Попробуйте позже.",
                )
            finally:
                async with self._lock:
                    self._in_progress.discard(key)

    async def _process_youtube(
        self,
        youtube_id: str,
        chat_id: int,
        query_norm: str | None,
        inline_message_id: str | None,
        candidate: YtCandidate | None,
        source_url: str | None,
        status_message_id: int | None,
        status_keywords: str | None,
    ) -> None:
        duration = candidate.duration if candidate else None
        download_url = source_url or f"https://www.youtube.com/watch?v={youtube_id}"
        if duration is not None and duration > 60:
            await self._bot.send_message(
                chat_id,
                "Видео длиннее 1 минуты, выбери другое.",
            )
            return
        if await self._db.is_video_blocked_by_source(youtube_id, download_url):
            await self._bot.send_message(
                chat_id,
                "Это видео заблокированно администратором, выберите другое",
            )
            return
        # Do not send extra status messages; user already sees the inline placeholder.

        job_id = f"yt-{youtube_id or 'media'}"
        try:
            result = await yt_download(
                download_url,
                self._download_dir,
                job_id,
            )
        except YtDlpError as exc:
            if is_age_restricted_error(str(exc)):
                await self._bot.send_message(
                    chat_id,
                    "🔞Бот слишком молод для такого видео, выбери другое",
                )
            else:
                await self._bot.send_message(chat_id, f"Не удалось скачать видео: {exc}")
            return

        title = candidate.title if candidate else "Видео"
        thumb_url = candidate.thumbnail_url if candidate else None
        if status_message_id and status_keywords is not None:
            try:
                await self._bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=status_message_id,
                    text=f"Видео \"{title}\" загружено, ключевые слова: `{status_keywords}`",
                    parse_mode="Markdown",
                    reply_markup=build_upload_cancel_keyboard(),
                )
            except TelegramBadRequest:
                pass

        caption = (
            "✅ Готово! Отправь видео обратно в чат, нажав на кнопку 💬 "
            "или добавь к видео свои теги ⌨️ (ключевые слова) для более удобного поиска"
        )
        try:
            upload_message = await self._bot.send_video(
                chat_id,
                FSInputFile(result.file_path),
                caption=caption,
                disable_notification=True,
                parse_mode="Markdown",
            )
        finally:
            try:
                result.file_path.unlink(missing_ok=True)
            except Exception:
                logger.warning("Failed to remove file %s", result.file_path)

        if upload_message.video is None:
            await self._bot.send_message(
                chat_id,
                "Не удалось отправить видео в нужном формате. "
                "Проверьте, что установлен ffmpeg, и повторите попытку.",
            )
            return

        video = upload_message.video
        stored_id = youtube_id or (candidate.youtube_id if candidate else "")
        video_id = await self._db.create_video(
            file_id=video.file_id,
            file_unique_id=video.file_unique_id,
            youtube_id=stored_id,
            source_url=download_url,
            title=title,
            duration=video.duration,
            width=video.width,
            height=video.height,
            size=video.file_size,
            thumb_url=thumb_url,
            uploader_id=chat_id,
        )
        if query_norm:
            await self._db.link_query_to_video(query_norm, video_id)

        keyboard = build_video_ready_keyboard(video_id)

        if inline_message_id:
            try:
                await self._bot.edit_message_media(
                    inline_message_id=inline_message_id,
                    media=InputMediaVideo(
                        media=video.file_id,
                        caption=caption,
                        parse_mode="Markdown",
                    ),
                )
                await self._bot.edit_message_reply_markup(
                    inline_message_id=inline_message_id,
                    reply_markup=keyboard,
                )
            except TelegramBadRequest:
                await self._bot.send_message(
                    chat_id,
                    "Готово! Можно отправить в чат.",
                    reply_markup=keyboard,
                )
        else:
            try:
                await self._bot.edit_message_reply_markup(
                    chat_id=chat_id,
                    message_id=upload_message.message_id,
                    reply_markup=keyboard,
                )
            except TelegramBadRequest:
                await self._bot.send_message(
                    chat_id,
                    "Готово! Можно отправить в чат.",
                    reply_markup=keyboard,
                )

        if inline_message_id:
            try:
                await self._bot.delete_message(chat_id, upload_message.message_id)
            except TelegramBadRequest:
                pass


def build_switch_pm_text() -> str:
    return "Сделать свой видеостикер🎬 ≈ 10 сек"


def format_views(value: int | None) -> str:
    if value is None:
        return "—"
    if value < 1000:
        return str(value)
    if value < 1_000_000:
        return f"{value / 1000:.1f}K".replace(".0", "")
    return f"{value / 1_000_000:.1f}M".replace(".0", "")


def build_inline_search_keyboard(query_text: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="Выбрать видео:",
                    switch_inline_query_current_chat=f"yt:{query_text}",
                )
            ]
        ]
    )


def build_main_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [
                KeyboardButton(text="🔍 Найти"),
                KeyboardButton(text="⬇️Загрузить свое"),
                KeyboardButton(text="✂️ Обрезать"),
            ],
            [
                KeyboardButton(text="🆘Помощь"),
                KeyboardButton(text="🚩Пожаловаться"),
            ]
        ],
        resize_keyboard=True,
        is_persistent=True,
    )


def build_upload_cancel_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="Отмена",
                    callback_data="upload_cancel",
                )
            ]
        ]
    )


def build_video_ready_keyboard(video_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="💬 Отправить в чат..",
                    switch_inline_query=f"ready:{video_id}",
                ),
                InlineKeyboardButton(
                    text="✂️ Обрезать",
                    callback_data=f"cut:{video_id}",
                ),
                InlineKeyboardButton(
                    text="⌨️ Добавить теги",
                    callback_data=f"addtags:{video_id}",
                ),
            ]
        ]
    )


def build_cut_pick_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="Выбрать видео в @vid_robot",
                    switch_inline_query_current_chat="",
                )
            ]
        ]
    )


def build_cut_confirm_keyboard(cut_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="✅ Подтвердить",
                    callback_data=f"cutconfirm:{cut_id}",
                ),
                InlineKeyboardButton(
                    text="❌ Отменить",
                    callback_data=f"cutcancel:{cut_id}",
                ),
            ]
        ]
    )


def build_report_pick_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="Выбрать видео в @vid_robot",
                    switch_inline_query_current_chat="",
                )
            ]
        ]
    )


def build_inline_search_button() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🔍 Найти",
                    switch_inline_query_current_chat="yt: ",
                )
            ]
        ]
    )


def extract_youtube_id(text: str) -> str | None:
    match = YOUTUBE_ID_RE.search(text)
    if not match:
        return None
    return match.group(1)


def extract_first_url(text: str) -> str | None:
    match = re.search(r"https?://\S+", text)
    if not match:
        return None
    return match.group(0).rstrip(").,]>")


def format_user_link(user: object) -> str:
    first = getattr(user, "first_name", "") or ""
    last = getattr(user, "last_name", "") or ""
    username = getattr(user, "username", None)
    full_name = " ".join(part for part in [first, last] if part).strip() or "Пользователь"
    if username:
        return f"{full_name}, @{username}, https://t.me/{username}"
    user_id = getattr(user, "id", None)
    if user_id:
        return f"{full_name}, tg://user?id={user_id}"
    return full_name


def format_user_html(user: object) -> str:
    import html

    first = getattr(user, "first_name", "") or ""
    last = getattr(user, "last_name", "") or ""
    username = getattr(user, "username", None)
    full_name = " ".join(part for part in [first, last] if part).strip() or "Пользователь"
    safe_name = html.escape(full_name)
    if username:
        safe_username = html.escape(username)
        profile_url = f"https://t.me/{safe_username}"
        return f"{safe_name}, @{safe_username}, <a href=\"{profile_url}\">профиль</a>"
    user_id = getattr(user, "id", None)
    if user_id:
        return f"{safe_name}, <a href=\"tg://user?id={user_id}\">профиль</a>"
    return safe_name


def parse_hhmm(value: str) -> tuple[int, int] | None:
    raw = value.strip()
    if not raw:
        return None
    parts = raw.split(":")
    if len(parts) != 2 or not parts[0].isdigit() or not parts[1].isdigit():
        return None
    hour = int(parts[0])
    minute = int(parts[1])
    if hour < 0 or hour > 23 or minute < 0 or minute > 59:
        return None
    return hour, minute


def format_stats_text(stats: dict, top_videos: list[dict], schedule_value: str) -> str:
    lines = [
        "Статистика сервиса",
        "",
        f"Видео: всего {stats.get('videos_total', 0)}, готовых {stats.get('videos_ready', 0)}, заблокировано {stats.get('videos_blocked', 0)}",
        f"Загрузки пользователей: {stats.get('uploads_total', 0)}",
        f"Новых видео за 24ч: {stats.get('videos_24h', 0)}",
        f"Отправок в чаты (total use_count): {stats.get('sends_total', 0)}",
        f"Пользователей (по использованию): {stats.get('users_total', 0)}, активных за 24ч: {stats.get('users_24h', 0)}",
        f"Связок пользователь-видео: {stats.get('user_video_pairs', 0)}",
        f"Тегов (video_queries): {stats.get('tags_total', 0)}",
        (
            "Жалобы: всего "
            f"{stats.get('complaints_total', 0)}, pending {stats.get('complaints_pending', 0)}, "
            f"blocked {stats.get('complaints_blocked', 0)}, skipped {stats.get('complaints_skipped', 0)}, "
            f"ban {stats.get('complaints_banned', 0)}"
        ),
        f"Заблокированных стукачей: {stats.get('banned_reporters', 0)}",
        f"Расписание: {schedule_value}",
        "",
        "TOP видео:",
    ]
    if not top_videos:
        lines.append("Нет данных")
    else:
        for idx, row in enumerate(top_videos, start=1):
            title = (row.get("title") or "Видео").replace("\n", " ").strip()
            lines.append(f"{idx}. {title} — {row.get('use_count', 0)}")
    return "\n".join(lines)


async def main() -> None:
    logging.basicConfig(level=logging.INFO)
    load_dotenv()

    settings = load_settings()
    settings.db_path.parent.mkdir(parents=True, exist_ok=True)
    settings.download_dir.mkdir(parents=True, exist_ok=True)

    db = Database(settings.db_path)
    await db.connect()
    await db.init()
    await db.purge_expired_tokens()

    piped = PipedClient(
        settings.piped_api_base_url,
        settings.piped_timeout_seconds,
    )

    bot = Bot(token=settings.bot_token)
    dp = Dispatcher()
    prep_manager = PrepManager(
        bot,
        db,
        settings.download_dir,
        settings.max_concurrent_jobs,
    )
    yt_cache: dict[str, tuple[float, YtCandidate]] = {}
    yt_cache_ttl = 600.0
    upload_state: dict[int, dict] = {}
    tag_state: dict[int, dict] = {}
    report_state: dict[int, dict] = {}
    cut_state: dict[int, dict] = {}
    cut_jobs: dict[str, dict] = {}

    async def get_stat_schedule_value() -> str:
        configured = await db.get_setting("stat_schedule")
        if configured is None or configured.strip() == "":
            return settings.stat_schedule_default
        return configured.strip()

    async def build_stats_message() -> str:
        stats = await db.get_service_stats()
        top_videos = await db.get_top_videos(limit=10)
        schedule_value = await get_stat_schedule_value()
        return format_stats_text(stats, top_videos, schedule_value)

    async def send_stats_to_admin() -> None:
        if settings.admin_id <= 0:
            return
        text = await build_stats_message()
        await bot.send_message(settings.admin_id, text)

    @dp.inline_query()
    async def inline_query_handler(inline_query: InlineQuery) -> None:
        query = (inline_query.query or "").strip()
        if query in {"🚩Пожаловаться", "🚩пожаловаться", "yt:🚩Пожаловаться", "yt:🚩пожаловаться"}:
            await inline_query.answer(
                [],
                is_personal=True,
                cache_time=1,
                switch_pm_text="Отправить жалобу",
                switch_pm_parameter="report",
            )
            return
        if not query:
            total_limit = settings.empty_inline_total
            page_size = min(settings.popular_inline_results, 10)
            try:
                offset = int(inline_query.offset or 0)
            except ValueError:
                offset = 0
            offset = max(0, offset)

            user_id = inline_query.from_user.id
            personal = await db.get_user_top_videos(user_id, total_limit)
            personal_ids = {int(item["id"]) for item in personal}
            remaining = total_limit - len(personal)
            popular = []
            if remaining > 0:
                popular = await db.get_popular_videos(remaining, exclude_ids=personal_ids)

            combined: list[tuple[dict, str]] = []
            seen_ids: set[int] = set()
            for item in personal:
                vid = int(item["id"])
                if vid in seen_ids:
                    continue
                combined.append((item, "Часто используемое"))
                seen_ids.add(vid)
            for item in popular:
                vid = int(item["id"])
                if vid in seen_ids:
                    continue
                combined.append((item, "Популярное"))
                seen_ids.add(vid)

            page = combined[offset : offset + page_size]
            results = [
                InlineQueryResultCachedVideo(
                    id=f"vid:{item['id']}",
                    video_file_id=item["file_id"],
                    title=item.get("title") or "Видео",
                    description=label,
                    thumbnail_url=item.get("thumb_url"),
                )
                for item, label in page
            ]
            next_offset = ""
            if offset + page_size < len(combined):
                next_offset = str(offset + page_size)
            await db.purge_expired_tokens()
            token = await db.create_pm_token(query, "", settings.pm_token_ttl_seconds)
            switch_pm_text = build_switch_pm_text()
            switch_pm_parameter = f"pm-{token.token}"
            await inline_query.answer(
                results,
                is_personal=True,
                cache_time=1,
                next_offset=next_offset,
                switch_pm_text=switch_pm_text,
                switch_pm_parameter=switch_pm_parameter,
            )
            return

        if query.startswith("ready:"):
            raw_id = query.split(":", 1)[1]
            if not raw_id.isdigit():
                await inline_query.answer([], is_personal=True, cache_time=1)
                return
            video = await db.get_video_by_id(int(raw_id))
            if video is None or not video.get("file_id") or video.get("blocked"):
                await inline_query.answer([], is_personal=True, cache_time=1)
                return
            result = InlineQueryResultCachedVideo(
                id=f"vid:{video['id']}",
                video_file_id=video["file_id"],
                title=video.get("title") or "Видео",
                description="Готовое",
                thumbnail_url=video.get("thumb_url"),
            )
            await inline_query.answer([result], is_personal=True, cache_time=1)
            return

        if query.startswith("yt:"):
            query_text = query.split(":", 1)[1].strip()
            if not query_text:
                await inline_query.answer([], is_personal=True, cache_time=1)
                return
            try:
                yt_candidates = await piped.search(
                    query_text,
                    settings.max_inline_results,
                )
            except PipedError as exc:
                logger.warning("piped search failed: %s", exc)
                yt_candidates = []

            if os.getenv("PIPED_DEBUG", "").strip().lower() in {"1", "true", "yes", "y", "on"}:
                logger.info(
                    "Piped inline candidates: total=%s first_id=%s",
                    len(yt_candidates),
                    yt_candidates[0].youtube_id if yt_candidates else None,
                )

            results = []
            filtered = [
                cand
                for cand in yt_candidates
                if (
                    cand.is_short is True
                    or (cand.duration is not None and cand.duration <= 60)
                )
            ]
            for cand in filtered[: settings.max_inline_results]:
                yt_cache[cand.youtube_id] = (time.monotonic(), cand)
                duration = format_duration(cand.duration)
                views = format_views(cand.view_count)
                results.append(
                    InlineQueryResultArticle(
                        id=f"yt:{cand.youtube_id}",
                        title=cand.title,
                        description=f"YouTube • {duration} • {views}",
                        thumbnail_url=cand.thumbnail_url,
                        input_message_content=InputTextMessageContent(
                            message_text="⏳ Готовлю видео..."
                        ),
                    )
                )
            if os.getenv("PIPED_DEBUG", "").strip().lower() in {"1", "true", "yes", "y", "on"}:
                logger.info(
                    "Inline results count=%s first_title=%s",
                    len(results),
                    results[0].title if results else None,
                )
            try:
                await inline_query.answer(
                    results,
                    is_personal=True,
                    cache_time=1,
                )
            except TelegramBadRequest as exc:
                if "query is too old" in str(exc).lower():
                    logger.info("Inline query expired before response")
                    return
                raise
            return

        query_norm = db.normalize_query(query)
        cached = await db.find_cached_videos(query_norm, settings.max_inline_results)
        results: list = []
        cached_ids: list[int] = []
        cached_items: list[dict] = []

        for item in cached:
            cached_ids.append(int(item["id"]))
            cached_items.append(item)

        if len(results) < settings.max_inline_results:
            remaining = settings.max_inline_results - len(results)
            title_matches = await db.find_cached_videos_by_title(
                query_norm, cached_ids, remaining
            )
            for item in title_matches:
                cached_items.append(item)

        if cached_items:
            user_id = inline_query.from_user.id
            item_map = {int(item["id"]): item for item in cached_items}
            ordered_ids = await db.get_user_ranked_video_ids(
                user_id, item_map.keys()
            )
            personal_ids = set(ordered_ids)
            ordered_items = [item_map[vid] for vid in ordered_ids if vid in item_map]
            for item in cached_items:
                if int(item["id"]) not in personal_ids:
                    ordered_items.append(item)
            seen_ids: set[int] = set()
            for item in ordered_items:
                vid = int(item["id"])
                if vid in seen_ids:
                    continue
                seen_ids.add(vid)
                is_personal = int(item["id"]) in personal_ids
                results.append(
                    InlineQueryResultCachedVideo(
                        id=f"vid:{item['id']}",
                        video_file_id=item["file_id"],
                        title=item.get("title") or "Видео",
                        description="Часто используемое" if is_personal else "Готовое",
                        thumbnail_url=item.get("thumb_url"),
                    )
                )

        await db.purge_expired_tokens()
        token = await db.create_pm_token(query, query_norm, settings.pm_token_ttl_seconds)
        switch_pm_text = build_switch_pm_text()
        switch_pm_parameter = f"pm-{token.token}"

        try:
            await inline_query.answer(
                results,
                is_personal=True,
                cache_time=1,
                switch_pm_text=switch_pm_text,
                switch_pm_parameter=switch_pm_parameter,
            )
        except TelegramBadRequest as exc:
            if "query is too old" in str(exc).lower():
                logger.info("Inline query expired before response")
                return
            raise

    @dp.message(CommandStart())
    async def start_handler(message: Message, command: CommandObject) -> None:
        if not command.args:
            await message.answer(
                "Привет! Используйте inline-режим: `@vid_robot` _запрос_\n"
                "Для подготовки видео нажмите кнопку «Найти и подготовить».",
                reply_markup=build_main_keyboard(),
                parse_mode="Markdown",
            )
            return

        param = command.args
        if not param.startswith("pm-"):
            if param == "report":
                report_state[message.from_user.id] = {"stage": "await_video"}
                await message.answer(
                    "Выберите видео, на которое хотите пожаловаться:",
                    reply_markup=build_report_pick_keyboard(),
                )
            else:
                await message.answer(
                    "Неизвестный параметр. Повторите поиск в inline."
                )
            return

        token = param.split("-", 1)[1]
        token_info = await db.get_pm_token(token)
        if token_info is None:
            await message.answer("Ссылка устарела. Повторите поиск в inline.")
            return
        if token_info.expires_at <= int(time.time()):
            await message.answer("Ссылка устарела. Повторите поиск в inline.")
            return
        keyboard = build_inline_search_keyboard(token_info.query_text)
        await message.answer(
            f"Нажми на кнопку и выбери нужное видео 👇",
            reply_markup=keyboard,
        )

    @dp.message(Command("help"))
    async def help_handler(message: Message) -> None:
        await message.answer(
            settings.help_button_text,
            reply_markup=build_main_keyboard(),
            parse_mode="Markdown",
            disable_web_page_preview=True,
        )

    @dp.message(Command("upload"))
    async def upload_handler(message: Message) -> None:
        prompt = (
            "Для загрузки своего видео пришлите ссылку формата "
            "`https://www.youtube.com/watch?v=dQw4w9WgXcQ` "
            "`https://www.tiktok.com/@vid_robot/video/1234567890123456789` "
            "\nВидео должно быть короче *60 секунд.*"
        )
        sent = await message.answer(
            prompt,
            reply_markup=build_upload_cancel_keyboard(),
            parse_mode="Markdown",
        )
        upload_state[message.chat.id] = {
            "stage": "await_link",
            "message_id": sent.message_id,
        }

    @dp.message(Command("cut"))
    async def cut_handler(message: Message) -> None:
        cut_state[message.from_user.id] = {"stage": "await_video"}
        await message.answer(
            "Выберите видео, которое хотите обрезать:",
            reply_markup=build_cut_pick_keyboard(),
        )

    @dp.message(Command("report"))
    async def report_handler(message: Message) -> None:
        report_state[message.from_user.id] = {"stage": "await_video"}
        await message.answer(
            "Выберите видео, на которое хотите пожаловаться:",
            reply_markup=build_report_pick_keyboard(),
        )

    @dp.message(Command("stat"))
    async def stat_handler(message: Message) -> None:
        if message.from_user.id != settings.admin_id:
            return
        await message.answer(await build_stats_message())

    @dp.message(Command("stat_schedule"))
    async def stat_schedule_handler(message: Message, command: CommandObject) -> None:
        if message.from_user.id != settings.admin_id:
            return
        args = (command.args or "").strip()
        if not args:
            current = await get_stat_schedule_value()
            await message.answer(
                "Текущее расписание статистики: "
                f"`{current}`\n"
                "Изменить: `/stat_schedule HH:MM`\n"
                "Отключить: `/stat_schedule off`",
                parse_mode="Markdown",
            )
            return
        if args.lower() in {"off", "disable", "none", "выкл"}:
            await db.set_setting("stat_schedule", "off")
            await message.answer("Расписание статистики отключено.")
            return
        parsed = parse_hhmm(args)
        if parsed is None:
            await message.answer("Неверный формат. Используйте `HH:MM`, например `09:30`.", parse_mode="Markdown")
            return
        hour, minute = parsed
        normalized = f"{hour:02d}:{minute:02d}"
        await db.set_setting("stat_schedule", normalized)
        await message.answer(f"Новое расписание статистики: `{normalized}`", parse_mode="Markdown")

    @dp.callback_query(F.data == "upload_cancel")
    async def upload_cancel_handler(callback: CallbackQuery) -> None:
        if callback.message is None:
            return
        chat_id = callback.message.chat.id
        upload_state.pop(chat_id, None)
        try:
            await callback.message.edit_text("Отменено.")
        except TelegramBadRequest:
            await callback.message.answer("Отменено.")
        await callback.answer()

    @dp.callback_query(F.data.startswith("addtags:"))
    async def add_tags_handler(callback: CallbackQuery) -> None:
        raw_id = callback.data.split(":", 1)[1]
        if not raw_id.isdigit():
            await callback.answer()
            return
        user_id = callback.from_user.id
        if callback.message is not None:
            chat_id = callback.message.chat.id
            message_id = callback.message.message_id
            tag_state[user_id] = {"video_id": int(raw_id), "chat_id": chat_id, "message_id": message_id}
            try:
                await bot.edit_message_caption(
                    chat_id=chat_id,
                    message_id=message_id,
                    caption="⌨️ Напиши ключевые слова, чтобы легко найти это видео, к примеру: `кот хакер`",
                    parse_mode="Markdown",
                    reply_markup=callback.message.reply_markup,
                )
            except TelegramBadRequest:
                pass
        elif callback.inline_message_id:
            tag_state[user_id] = {
                "video_id": int(raw_id),
                "inline_message_id": callback.inline_message_id,
            }
            try:
                await bot.edit_message_caption(
                    inline_message_id=callback.inline_message_id,
                    caption="⌨️ Напиши ключевые слова, чтобы легко найти это видео, к примеру: `кот хакер`",
                    parse_mode="Markdown",
                    reply_markup=build_video_ready_keyboard(int(raw_id)),
                )
            except TelegramBadRequest:
                pass
        await callback.answer()

    @dp.callback_query(F.data.startswith("cut:"))
    async def cut_request_handler(callback: CallbackQuery) -> None:
        raw_id = callback.data.split(":", 1)[1]
        if not raw_id.isdigit():
            await callback.answer()
            return
        video_id = int(raw_id)
        video = await db.get_video_by_id(video_id)
        if not video:
            await callback.answer("Видео не найдено", show_alert=True)
            return
        user_id = callback.from_user.id
        uploader_id = video.get("uploader_id")
        if user_id != settings.admin_id and uploader_id and uploader_id != user_id:
            await callback.answer("Обрезать можно только свои видео", show_alert=True)
            return
        if uploader_id is None and user_id != settings.admin_id:
            await callback.answer("Обрезать можно только свои видео", show_alert=True)
            return
        cut_state[user_id] = {"stage": "await_range", "video_id": video_id}
        if callback.message is not None:
            await callback.message.answer(
                "Чтобы обрезать видео отправь мне сообщение с какой по какую секунду надо обрезать: `00-05` или `0-5`",
                parse_mode="Markdown",
            )
        else:
            await bot.send_message(
                user_id,
                "Чтобы обрезать видео отправь мне сообщение с какой по какую секунду надо обрезать: `00-05` или `0-5`",
                parse_mode="Markdown",
            )
        await callback.answer()

    @dp.callback_query(F.data.startswith("cutconfirm:"))
    async def cut_confirm_handler(callback: CallbackQuery) -> None:
        cut_id = callback.data.split(":", 1)[1]
        job = cut_jobs.get(cut_id)
        if not job:
            await callback.answer("Обрезка устарела", show_alert=True)
            return
        if callback.from_user.id != job.get("user_id"):
            await callback.answer()
            return
        video_id = job["video_id"]
        msg_id = job.get("message_id")
        if msg_id is None:
            await callback.answer()
            return
        try:
            await bot.edit_message_reply_markup(
                chat_id=callback.message.chat.id,
                message_id=msg_id,
                reply_markup=None,
            )
        except TelegramBadRequest:
            pass
        try:
            await bot.edit_message_media(
                chat_id=callback.message.chat.id,
                message_id=msg_id,
                media=InputMediaVideo(media=job["file_id"]),
            )
            await bot.edit_message_caption(
                chat_id=callback.message.chat.id,
                message_id=msg_id,
                caption=(
                    "✅ Готово! Отправь видео обратно в чат, нажав на кнопку 💬 "
                    "или добавь к видео свои теги ⌨️ (ключевые слова) для более удобного поиска"
                ),
                parse_mode="Markdown",
                reply_markup=build_video_ready_keyboard(video_id),
            )
        except TelegramBadRequest:
            await bot.send_video(
                callback.message.chat.id,
                job["file_id"],
                caption=(
                    "✅ Готово! Отправь видео обратно в чат, нажав на кнопку 💬 "
                    "или добавь к видео свои теги ⌨️ (ключевые слова) для более удобного поиска"
                ),
                parse_mode="Markdown",
                reply_markup=build_video_ready_keyboard(video_id),
            )
        await db.update_video_media(
            video_id,
            file_id=job["file_id"],
            file_unique_id=job["file_unique_id"],
            duration=job["duration"],
            width=job["width"],
            height=job["height"],
            size=job["size"],
            thumb_url=job.get("thumb_url"),
        )
        cut_jobs.pop(cut_id, None)
        await callback.answer("Обрезка применена")

    @dp.callback_query(F.data.startswith("cutcancel:"))
    async def cut_cancel_handler(callback: CallbackQuery) -> None:
        cut_id = callback.data.split(":", 1)[1]
        job = cut_jobs.pop(cut_id, None)
        if not job:
            await callback.answer()
            return
        if callback.from_user.id != job.get("user_id"):
            await callback.answer()
            return
        msg_id = job.get("message_id")
        if msg_id is not None:
            try:
                await bot.edit_message_reply_markup(
                    chat_id=callback.message.chat.id,
                    message_id=msg_id,
                    reply_markup=None,
                )
            except TelegramBadRequest:
                pass
        await callback.answer("Обрезка отменена")

    @dp.callback_query(F.data.startswith("complaint:"))
    async def complaint_admin_handler(callback: CallbackQuery) -> None:
        parts = callback.data.split(":", 2)
        if len(parts) != 3:
            await callback.answer()
            return
        action = parts[1]
        if callback.from_user.id != settings.admin_id:
            await callback.answer()
            return
        if not parts[2].isdigit():
            await callback.answer()
            return
        complaint_id = int(parts[2])
        complaint = await db.get_complaint(complaint_id)
        if complaint is None or complaint.get("status") != "pending":
            await callback.answer()
            return
        reporter_id = int(complaint["reporter_id"])
        video_id = int(complaint["video_id"])
        if action == "block":
            await db.set_video_blocked(video_id, True)
            await db.update_complaint_status(complaint_id, "blocked")
            await bot.send_message(
                reporter_id,
                "Ваша жалоба рассмотрена. Видео заблокировано. Спасибо 🤝",
            )
        elif action == "skip":
            await db.update_complaint_status(complaint_id, "skipped")
            await bot.send_message(
                reporter_id,
                "Ваша жалоба рассмотрена. Видео НЕ заблокировано. Спасибо 🤝",
            )
        elif action == "ban":
            await db.ban_reporter(reporter_id)
            await db.update_complaint_status(complaint_id, "banned")
            await bot.send_message(
                reporter_id,
                "Вам запрещено отправлять жалобы",
            )
        try:
            await callback.message.edit_reply_markup(reply_markup=None)
        except TelegramBadRequest:
            pass
        await callback.answer()

    async def _safe_delete_message(chat_id: int, message_id: int) -> None:
        try:
            await bot.delete_message(chat_id, message_id)
        except TelegramBadRequest:
            pass

    @dp.message(F.chat.type == "private", F.text, ~F.text.startswith("/"))
    async def private_query_handler(message: Message) -> None:
        if message.from_user and message.from_user.is_bot:
            return
        text = message.text.strip()
        if not text:
            return
        if text.startswith("⏳ Готовлю видео"):
            return
        tag_info = tag_state.get(message.from_user.id)
        if tag_info:
            tags = text.strip()
            if not tags:
                return
            await _safe_delete_message(message.chat.id, message.message_id)
            await db.link_query_to_video(db.normalize_query(tags), tag_info["video_id"])
            try:
                if "inline_message_id" in tag_info:
                    await bot.edit_message_caption(
                        inline_message_id=tag_info["inline_message_id"],
                        caption="✅ Готово! Отправь видео обратно в чат, нажав на кнопку 💬 "
                        "или добавь к видео свои теги ⌨️ (ключевые слова) для более удобного поиска",
                        parse_mode="Markdown",
                        reply_markup=build_video_ready_keyboard(tag_info["video_id"]),
                    )
                else:
                    await bot.edit_message_caption(
                        chat_id=tag_info["chat_id"],
                        message_id=tag_info["message_id"],
                        caption="✅ Готово! Отправь видео обратно в чат, нажав на кнопку 💬 "
                        "или добавь к видео свои теги ⌨️ (ключевые слова) для более удобного поиска",
                        parse_mode="Markdown",
                        reply_markup=build_video_ready_keyboard(tag_info["video_id"]),
                    )
            except TelegramBadRequest:
                pass
            tag_state.pop(message.from_user.id, None)
            return
        cut_info = cut_state.get(message.from_user.id)
        if cut_info and cut_info.get("stage") == "await_range":
            range_text = text.strip()
            if "-" not in range_text:
                return
            parsed = parse_time_range(range_text)
            if not parsed:
                if not cut_info.get("hinted"):
                    await message.answer(
                        "Неверный формат. Пример: `00-05` или `0-5`",
                        parse_mode="Markdown",
                    )
                    cut_info["hinted"] = True
                return
            start_sec, end_sec = parsed
            if start_sec < 0 or end_sec <= start_sec:
                await message.answer("Неверный диапазон.")
                return
            video_id = int(cut_info["video_id"])
            video = await db.get_video_by_id(video_id)
            if not video:
                cut_state.pop(message.from_user.id, None)
                await message.answer("Видео не найдено.")
                return
            uploader_id = video.get("uploader_id")
            if message.from_user.id != settings.admin_id and uploader_id != message.from_user.id:
                cut_state.pop(message.from_user.id, None)
                await message.answer("Обрезать можно только свои видео.")
                return
            if video.get("duration") and end_sec > int(video["duration"]):
                await message.answer("Диапазон больше длительности видео.")
                return
            await _safe_delete_message(message.chat.id, message.message_id)
            status_msg = await message.answer("Обрезаю видео...")
            try:
                await bot.edit_message_text(
                    chat_id=message.chat.id,
                    message_id=status_msg.message_id,
                    text="Обрезаю видео...",
                )
            except TelegramBadRequest:
                pass
            job_id = f"cut-{video_id}-{int(time.time())}"
            source_url = video.get("source_url")
            try:
                result = await yt_download(
                    source_url,
                    settings.download_dir,
                    job_id,
                    start_time=start_sec,
                    end_time=end_sec,
                )
            except YtDlpError as exc:
                await bot.edit_message_text(
                    chat_id=message.chat.id,
                    message_id=status_msg.message_id,
                    text=f"Не удалось обрезать видео: {exc}",
                )
                cut_state.pop(message.from_user.id, None)
                return
            try:
                sent = await bot.send_video(
                    message.chat.id,
                    FSInputFile(result.file_path),
                    caption="Обрезка готова. Подтвердить?",
                    reply_markup=build_cut_confirm_keyboard(job_id),
                )
            finally:
                try:
                    result.file_path.unlink(missing_ok=True)
                except Exception:
                    logger.warning("Failed to remove file %s", result.file_path)
            if sent.video is None:
                await bot.edit_message_text(
                    chat_id=message.chat.id,
                    message_id=status_msg.message_id,
                    text="Не удалось отправить обрезанное видео.",
                )
                cut_state.pop(message.from_user.id, None)
                return
            cut_jobs[job_id] = {
                "user_id": message.from_user.id,
                "video_id": video_id,
                "message_id": sent.message_id,
                "file_id": sent.video.file_id,
                "file_unique_id": sent.video.file_unique_id,
                "duration": sent.video.duration,
                "width": sent.video.width,
                "height": sent.video.height,
                "size": sent.video.file_size,
                "thumb_url": video.get("thumb_url"),
            }
            try:
                await bot.delete_message(message.chat.id, status_msg.message_id)
            except TelegramBadRequest:
                pass
            cut_state.pop(message.from_user.id, None)
            return
        report_info = report_state.get(message.from_user.id)
        if report_info and report_info.get("stage") == "await_reason":
            reason = text.strip()
            if not reason:
                return
            await _safe_delete_message(message.chat.id, message.message_id)
            complaint_id = await db.create_complaint(
                message.from_user.id, report_info["video_id"], reason
            )
            await message.answer("Жалоба отправлена на рассмотрение")
            video = await db.get_video_by_id(report_info["video_id"])
            if video and settings.admin_id:
                import html

                reporter = format_user_html(message.from_user)
                title = html.escape(video.get("title") or "Видео")
                source_url = html.escape(video.get("source_url") or "—")
                reason_safe = html.escape(reason)
                ready_code = html.escape(f"@vid_robot ready:{video['id']}")
                text_block = (
                    f"Поступила жалоба от {reporter} на видео \"{title}\" ({source_url})\n"
                    f"Прямая ссылка на видео: <code>{ready_code}</code>\n"
                    f"Причина: \"{reason_safe}\""
                )
                await bot.send_message(
                    settings.admin_id,
                    text_block,
                    parse_mode="HTML",
                    disable_web_page_preview=True,
                )
                await bot.send_video(
                    settings.admin_id,
                    video["file_id"],
                    reply_markup=InlineKeyboardMarkup(
                        inline_keyboard=[
                            [
                                InlineKeyboardButton(
                                    text="🚫Заблокировать",
                                    callback_data=f"complaint:block:{complaint_id}",
                                ),
                                InlineKeyboardButton(
                                    text="💤Пропустить",
                                    callback_data=f"complaint:skip:{complaint_id}",
                                ),
                                InlineKeyboardButton(
                                    text="Блок. стукача",
                                    callback_data=f"complaint:ban:{complaint_id}",
                                ),
                            ]
                        ]
                    ),
                )
            report_state.pop(message.from_user.id, None)
            return
        lowered = text.lower()
        if lowered in {"help", "помощь", "🆘помощь"}:
            await message.answer(
                settings.help_button_text,
                reply_markup=build_main_keyboard(),
                parse_mode="Markdown",
                disable_web_page_preview=True,
            )
            return
        if lowered in {"cut", "обрезать", "✂️ обрезать", "✂️обрезать"}:
            cut_state[message.from_user.id] = {"stage": "await_video"}
            await message.answer(
                "Выберите видео, которое хотите обрезать:",
                reply_markup=build_cut_pick_keyboard(),
            )
            return
        if lowered in {"/report", "report", "жалоба", "пожаловаться", "🚩пожаловаться"}:
            if await db.is_report_banned(message.from_user.id):
                await message.answer("Вам запрещено отправлять жалобы")
                return
            report_state[message.from_user.id] = {"stage": "await_video"}
            await message.answer(
                "Выберите видео, на которое хотите пожаловаться:",
                reply_markup=build_report_pick_keyboard(),
            )
            return
        if lowered in {"🔍 найти", "найти"}:
            await message.answer(
                "Открой inline и начни поиск: `@vid_robot yt:`\n\nЛибо нажми кнопку ниже 👇",
                reply_markup=build_inline_search_button(),
                parse_mode="Markdown",
            )
            return
        if lowered in {"upload", "загрузить свое", "загрузить своё", "⬇️загрузить свое", "⬇️загрузить своё"}:
            prompt = (
                "Для загрузки своего видео пришлите ссылку формата "
                "`https://www.youtube.com/watch?v=dQw4w9WgXcQ` "
                "`https://www.tiktok.com/@vid_robot/video/1234567890123456789` "
                "\nВидео должно быть короче *60 секунд.*"
            )
            sent = await message.answer(
                prompt,
                reply_markup=build_upload_cancel_keyboard(),
                disable_web_page_preview=True,
                parse_mode="Markdown",
            )
            upload_state[message.chat.id] = {
                "stage": "await_link",
                "message_id": sent.message_id,
            }
            return

        state = upload_state.get(message.chat.id)
        if state:
            if state["stage"] == "await_link":
                raw_url = extract_first_url(text)
                youtube_id = extract_youtube_id(text) if raw_url else None
                if not raw_url:
                    try:
                        await bot.edit_message_text(
                            chat_id=message.chat.id,
                            message_id=state["message_id"],
                            text=(
                                "Неверная ссылка. Пришлите ссылку формата "
                                "https://www.youtube.com/watch?v=dQw4w9WgXcQ"
                            ),
                            reply_markup=build_upload_cancel_keyboard(),
                            disable_web_page_preview=True,
                        )
                    except TelegramBadRequest:
                        await message.answer(
                            "Неверная ссылка. Пришлите ссылку формата "
                            "https://www.youtube.com/watch?v=dQw4w9WgXcQ",
                            reply_markup=build_upload_cancel_keyboard(),
                            disable_web_page_preview=True,
                        )
                    return
                source_url = raw_url
                if youtube_id:
                    source_url = f"https://www.youtube.com/watch?v={youtube_id}"
                await _safe_delete_message(message.chat.id, message.message_id)
                try:
                    await bot.edit_message_text(
                        chat_id=message.chat.id,
                        message_id=state["message_id"],
                        text=f"Проверю ваше видео:\n{source_url}",
                        reply_markup=build_upload_cancel_keyboard(),
                        disable_web_page_preview=True,
                    )
                except TelegramBadRequest:
                    pass
                info = None
                try:
                    info = await fetch_media_info(source_url)
                except YtDlpError as exc:
                    logger.warning("yt-dlp info failed for %s: %s", youtube_id, exc)
                if info is None or info.duration is None:
                    try:
                        await bot.edit_message_text(
                            chat_id=message.chat.id,
                            message_id=state["message_id"],
                            text="Не удалось получить данные видео. Попробуйте другую ссылку.",
                            reply_markup=build_upload_cancel_keyboard(),
                            disable_web_page_preview=True,
                        )
                    except TelegramBadRequest:
                        await message.answer(
                            "Не удалось получить данные видео. Попробуйте другую ссылку.",
                            reply_markup=build_upload_cancel_keyboard(),
                            disable_web_page_preview=True,
                        )
                    return
                if info.duration > 60:
                    try:
                        await bot.edit_message_text(
                            chat_id=message.chat.id,
                            message_id=state["message_id"],
                            text="Видео длиннее 1 минуты, выберите другое.",
                            reply_markup=build_upload_cancel_keyboard(),
                            disable_web_page_preview=True,
                        )
                    except TelegramBadRequest:
                        await message.answer(
                            "Видео длиннее 1 минуты, выберите другое.",
                            reply_markup=build_upload_cancel_keyboard(),
                            disable_web_page_preview=True,
                        )
                    return
                upload_state[message.chat.id] = {
                    "stage": "await_keywords",
                    "message_id": state["message_id"],
                    "youtube_id": youtube_id or info.youtube_id or "",
                    "source_url": source_url,
                    "candidate": info,
                }
                try:
                    await bot.edit_message_text(
                        chat_id=message.chat.id,
                        message_id=state["message_id"],
                        text="Готово ☑️ Теперь напиши ключевые слова, чтобы легко найти это видео",
                        reply_markup=build_upload_cancel_keyboard(),
                    )
                except TelegramBadRequest:
                    await message.answer(
                        "Готово ☑️ Теперь напиши ключевые слова, чтобы легко найти это видео",
                        reply_markup=build_upload_cancel_keyboard(),
                    )
                return

            if state["stage"] == "await_keywords":
                keywords = text.strip()
                if not keywords:
                    return
                await _safe_delete_message(message.chat.id, message.message_id)
                query_norm = db.normalize_query(keywords)
                try:
                    await bot.edit_message_text(
                        chat_id=message.chat.id,
                        message_id=state["message_id"],
                        text=(
                            f"Видео \"{state['candidate'].title}\" загружено, "
                            f"ключевые слова: `{keywords}`\n\n"
                            f"*Подожди немного ⏳*"
                        ),
                        reply_markup=build_upload_cancel_keyboard(),
                        parse_mode="Markdown",
                    )
                except TelegramBadRequest:
                    await message.answer(
                        (
                            f"Видео \"{state['candidate'].title}\" загружено, "
                            f"ключевые слова: `{keywords}`\n\n"
                            f"*Подожди немного ⏳*"
                        ),
                        reply_markup=build_upload_cancel_keyboard(),
                        parse_mode="Markdown",
                    )
                started = await prep_manager.start_youtube(
                    state["youtube_id"],
                    message.chat.id,
                    query_norm,
                    None,
                    state.get("candidate"),
                    state.get("source_url"),
                    status_message_id=state["message_id"],
                    status_keywords=keywords,
                )
                if not started:
                    try:
                        await bot.edit_message_text(
                            chat_id=message.chat.id,
                            message_id=state["message_id"],
                            text="Подготовка уже запущена для этого видео.",
                        )
                    except TelegramBadRequest:
                        await message.answer("Подготовка уже запущена для этого видео.")
                upload_state.pop(message.chat.id, None)
                return

        if text.startswith("⏳ Готовлю видео"):
            return
        await message.answer(
            "Нажми на кнопки ниже или пришли ссылку",
            reply_markup=build_main_keyboard(),
        )

    @dp.chosen_inline_result()
    async def chosen_inline_handler(chosen: ChosenInlineResult) -> None:
        result_id = chosen.result_id or ""
        if result_id.startswith("vid:"):
            raw_id = result_id.split(":", 1)[1]
            if raw_id.isdigit():
                video_id = int(raw_id)
                report_info = report_state.get(chosen.from_user.id)
                if report_info and report_info.get("stage") == "await_video":
                    if await db.is_report_banned(chosen.from_user.id):
                        await bot.send_message(
                            chosen.from_user.id,
                            "Вам запрещено отправлять жалобы",
                        )
                        report_state.pop(chosen.from_user.id, None)
                        return
                    report_state[chosen.from_user.id] = {
                        "stage": "await_reason",
                        "video_id": video_id,
                    }
                    await bot.send_message(
                        chosen.from_user.id,
                        "📝 Напишите причину жалобы на это видео.",
                    )
                    return
                cut_info = cut_state.get(chosen.from_user.id)
                if cut_info and cut_info.get("stage") == "await_video":
                    video = await db.get_video_by_id(video_id)
                    if not video:
                        await bot.send_message(chosen.from_user.id, "Видео не найдено.")
                        cut_state.pop(chosen.from_user.id, None)
                        return
                    uploader_id = video.get("uploader_id")
                    if chosen.from_user.id != settings.admin_id and uploader_id != chosen.from_user.id:
                        await bot.send_message(
                            chosen.from_user.id,
                            "Обрезать можно только свои видео.",
                        )
                        cut_state.pop(chosen.from_user.id, None)
                        return
                    cut_state[chosen.from_user.id] = {
                        "stage": "await_range",
                        "video_id": video_id,
                    }
                    await bot.send_message(
                        chosen.from_user.id,
                        "Чтобы обрезать видео отправь мне сообщение с какой по какую секунду надо обрезать: `00-05` или `0-5`",
                        parse_mode="Markdown",
                    )
                    return
                await db.increment_usage(video_id)
                await db.upsert_user_video_stat(chosen.from_user.id, video_id)
            return
        if result_id.startswith("yt:"):
            youtube_id = result_id.split(":", 1)[1]
            query_text = ""
            if chosen.query and chosen.query.startswith("yt:"):
                query_text = chosen.query.split(":", 1)[1].strip()
            query_norm = db.normalize_query(query_text) if query_text else None
            candidate = None
            cached = yt_cache.get(youtube_id)
            if cached:
                ts, item = cached
                if time.monotonic() - ts <= yt_cache_ttl:
                    candidate = item
                else:
                    yt_cache.pop(youtube_id, None)
            started = await prep_manager.start_youtube(
                youtube_id,
                chosen.from_user.id,
                query_norm,
                chosen.inline_message_id,
                candidate,
                candidate.source_url if candidate else None,
            )
            if not started:
                await bot.send_message(
                    chosen.from_user.id,
                    "Подготовка уже запущена для этого видео.",
                )
            return

    async def stat_scheduler_loop() -> None:
        last_sent_key = ""
        while True:
            try:
                schedule_value = (await get_stat_schedule_value()).strip().lower()
                parsed = parse_hhmm(schedule_value)
                if settings.admin_id > 0 and parsed is not None:
                    hour, minute = parsed
                    now = time.localtime()
                    if now.tm_hour == hour and now.tm_min == minute:
                        key = f"{now.tm_year}:{now.tm_yday}:{hour:02d}:{minute:02d}"
                        if key != last_sent_key:
                            await send_stats_to_admin()
                            last_sent_key = key
            except Exception:
                logger.exception("Stat scheduler failed")
            await asyncio.sleep(max(5, settings.stat_scheduler_tick_seconds))

    scheduler_task = asyncio.create_task(stat_scheduler_loop())

    try:
        await dp.start_polling(bot)
    finally:
        scheduler_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await scheduler_task
        await db.close()


if __name__ == "__main__":
    asyncio.run(main())
