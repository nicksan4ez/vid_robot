import asyncio
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
        if duration is not None and duration > 60:
            await self._bot.send_message(
                chat_id,
                "Видео длиннее 1 минуты, выбери другое.",
            )
            return
        # Do not send extra status messages; user already sees the inline placeholder.

        job_id = f"yt-{youtube_id or 'media'}"
        download_url = source_url or f"https://www.youtube.com/watch?v={youtube_id}"
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

        caption = None
        try:
            upload_message = await self._bot.send_video(
                chat_id,
                FSInputFile(result.file_path),
                caption=caption,
                disable_notification=True,
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
        )
        if query_norm:
            await self._db.link_query_to_video(query_norm, video_id)

        keyboard = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="📤 Отправить обратно в чат…",
                        switch_inline_query=f"ready:{video_id}",
                    )
                ]
            ]
        )

        if inline_message_id:
            try:
                await self._bot.edit_message_media(
                    inline_message_id=inline_message_id,
                    media=InputMediaVideo(
                        media=video.file_id,
                        caption=caption,
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
    return "Найти и подготовить 🎬 ≈ 10 сек"


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
                KeyboardButton(text="🆘Помощь"),
                KeyboardButton(text="🔍 Найти"),
                KeyboardButton(text="⬇️Загрузить свое"),
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

    @dp.inline_query()
    async def inline_query_handler(inline_query: InlineQuery) -> None:
        query = (inline_query.query or "").strip()
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
            for item in personal:
                combined.append((item, "Часто используемое"))
            for item in popular:
                combined.append((item, "Популярное"))

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
            await inline_query.answer(
                results,
                is_personal=True,
                cache_time=1,
                next_offset=next_offset,
            )
            return

        if query.startswith("ready:"):
            raw_id = query.split(":", 1)[1]
            if not raw_id.isdigit():
                await inline_query.answer([], is_personal=True, cache_time=1)
                return
            video = await db.get_video_by_id(int(raw_id))
            if video is None or not video.get("file_id"):
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
            for item in ordered_items:
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
        lowered = text.lower()
        if lowered in {"help", "помощь", "🆘помощь"}:
            await message.answer(
                settings.help_button_text,
                reply_markup=build_main_keyboard(),
                parse_mode="Markdown",
                disable_web_page_preview=True,
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
        keyboard = build_inline_search_keyboard(text)
        await message.answer(
            f"Вот результаты по запросу: {text}",
            reply_markup=keyboard,
        )

    @dp.chosen_inline_result()
    async def chosen_inline_handler(chosen: ChosenInlineResult) -> None:
        result_id = chosen.result_id or ""
        if result_id.startswith("vid:"):
            raw_id = result_id.split(":", 1)[1]
            if raw_id.isdigit():
                video_id = int(raw_id)
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

    try:
        await dp.start_polling(bot)
    finally:
        await db.close()


if __name__ == "__main__":
    asyncio.run(main())
