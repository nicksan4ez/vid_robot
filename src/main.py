import asyncio
import logging
import time
from pathlib import Path

from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, F
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import CommandStart
from aiogram.filters.command import CommandObject
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
)
from aiogram.types.input_file import FSInputFile

from .config import load_settings
from .db import Database
from .utils import format_duration
from .youtube import (
    YtDlpError,
    download as yt_download,
    fetch_video_info,
    search_via_api,
)


logger = logging.getLogger("vid_robot")

AGE_RESTRICTED_MARKERS = (
    "sign in to confirm your age",
    "age-restricted",
    "age restricted",
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
        youtube_api_key: str,
    ) -> None:
        self._bot = bot
        self._db = db
        self._download_dir = download_dir
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._lock = asyncio.Lock()
        self._in_progress: set[str] = set()
        self._youtube_api_key = youtube_api_key

    async def start_youtube(
        self,
        youtube_id: str,
        chat_id: int,
        query_norm: str | None,
        inline_message_id: str | None,
    ) -> bool:
        key = f"{chat_id}:{youtube_id}"
        async with self._lock:
            if key in self._in_progress:
                return False
            self._in_progress.add(key)
        asyncio.create_task(
            self._run_youtube(youtube_id, chat_id, query_norm, inline_message_id, key)
        )
        return True

    async def _run_youtube(
        self,
        youtube_id: str,
        chat_id: int,
        query_norm: str | None,
        inline_message_id: str | None,
        key: str,
    ) -> None:
        async with self._semaphore:
            try:
                await self._process_youtube(youtube_id, chat_id, query_norm, inline_message_id)
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
    ) -> None:
        try:
            candidate = await fetch_video_info(youtube_id, self._youtube_api_key)
        except YtDlpError as exc:
            await self._bot.send_message(chat_id, f"Не удалось получить данные: {exc}")
            return

        if candidate is None:
            await self._bot.send_message(chat_id, "Не удалось получить данные видео.")
            return

        # Do not send extra status messages; user already sees the inline placeholder.

        job_id = f"yt-{youtube_id}"
        try:
            result = await yt_download(candidate.source_url, self._download_dir, job_id)
        except YtDlpError as exc:
            if is_age_restricted_error(str(exc)):
                await self._bot.send_message(
                    chat_id,
                    "🔞Бот слишком молод для такого видео, выбери другое",
                )
            else:
                await self._bot.send_message(chat_id, f"Не удалось скачать видео: {exc}")
            return

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
        video_id = await self._db.create_video(
            file_id=video.file_id,
            file_unique_id=video.file_unique_id,
            youtube_id=candidate.youtube_id,
            source_url=candidate.source_url,
            title=candidate.title,
            duration=video.duration,
            width=video.width,
            height=video.height,
            size=video.file_size,
            thumb_url=candidate.thumbnail_url,
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

    bot = Bot(token=settings.bot_token)
    dp = Dispatcher()
    prep_manager = PrepManager(
        bot,
        db,
        settings.download_dir,
        settings.max_concurrent_jobs,
        settings.youtube_api_key,
    )

    @dp.inline_query()
    async def inline_query_handler(inline_query: InlineQuery) -> None:
        query = (inline_query.query or "").strip()
        if not query:
            popular = await db.get_popular_videos(settings.popular_inline_results)
            results = [
                InlineQueryResultCachedVideo(
                    id=str(item["id"]),
                    video_file_id=item["file_id"],
                    title=item.get("title") or "Видео",
                    description="Готовое",
                    thumbnail_url=item.get("thumb_url"),
                )
                for item in popular
            ]
            await inline_query.answer(results, is_personal=True, cache_time=1)
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
                id=str(video["id"]),
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
                yt_candidates = await asyncio.wait_for(
                    search_via_api(
                        query_text,
                        settings.yt_inline_results,
                        api_key=settings.youtube_api_key,
                    ),
                    timeout=5.0,
                )
            except asyncio.TimeoutError:
                logger.warning("YouTube API search timed out")
                yt_candidates = []
            except YtDlpError as exc:
                logger.warning("YouTube API search failed: %s", exc)
                yt_candidates = []

            yt_candidates = [
                cand
                for cand in yt_candidates
                if cand.duration is not None and cand.duration <= 60
            ]

            results = []
            for cand in yt_candidates:
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
            await inline_query.answer(results, is_personal=True, cache_time=1)
            return

        query_norm = db.normalize_query(query)
        cached = await db.find_cached_videos(query_norm, settings.max_inline_results)
        results: list = []
        cached_ids: list[int] = []

        for item in cached:
            cached_ids.append(int(item["id"]))
            results.append(
                InlineQueryResultCachedVideo(
                    id=str(item["id"]),
                    video_file_id=item["file_id"],
                    title=item.get("title") or "Видео",
                    description="Готовое",
                    thumbnail_url=item.get("thumb_url"),
                )
            )

        if len(results) < settings.max_inline_results:
            remaining = settings.max_inline_results - len(results)
            title_matches = await db.find_cached_videos_by_title(
                query_norm, cached_ids, remaining
            )
            for item in title_matches:
                results.append(
                    InlineQueryResultCachedVideo(
                        id=str(item["id"]),
                        video_file_id=item["file_id"],
                        title=item.get("title") or "Видео",
                        description="Готовое",
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
                "Привет! Используйте inline-режим: @vid_robot <запрос>\n"
                "Для подготовки видео нажмите кнопку «Найти и подготовить»."
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

    @dp.message(F.chat.type == "private", F.text, ~F.text.startswith("/"))
    async def private_query_handler(message: Message) -> None:
        if message.from_user and message.from_user.is_bot:
            return
        query_text = message.text.strip()
        if not query_text:
            return
        if query_text.startswith("⏳ Готовлю видео"):
            return

        keyboard = build_inline_search_keyboard(query_text)
        await message.answer(
            f"Вот результаты по запросу: {query_text}",
            reply_markup=keyboard,
        )

    @dp.chosen_inline_result()
    async def chosen_inline_handler(chosen: ChosenInlineResult) -> None:
        result_id = chosen.result_id or ""
        if result_id.isdigit():
            await db.increment_usage(int(result_id))
            return
        if result_id.startswith("yt:"):
            youtube_id = result_id.split(":", 1)[1]
            query_text = ""
            if chosen.query and chosen.query.startswith("yt:"):
                query_text = chosen.query.split(":", 1)[1].strip()
            query_norm = db.normalize_query(query_text) if query_text else None
            started = await prep_manager.start_youtube(
                youtube_id,
                chosen.from_user.id,
                query_norm,
                chosen.inline_message_id,
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
