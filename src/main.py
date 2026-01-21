import asyncio
import logging
import time
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, F
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import CommandStart
from aiogram.filters.command import CommandObject
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InlineQuery,
    InlineQueryResultArticle,
    InlineQueryResultCachedVideo,
    InputMediaPhoto,
    InputTextMessageContent,
    Message,
)
from aiogram.types.input_file import FSInputFile

from .config import load_settings
from .db import Database, YtCandidate
from .utils import format_duration, truncate_text
from .youtube import YtDlpError, download as yt_download, search_via_api


logger = logging.getLogger("vid_robot")


class PrepManager:
    def __init__(self, bot: Bot, db: Database, download_dir: Path, max_concurrent: int) -> None:
        self._bot = bot
        self._db = db
        self._download_dir = download_dir
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._lock = asyncio.Lock()
        self._in_progress: set[str] = set()

    async def start(self, token: str, chat_id: int) -> bool:
        async with self._lock:
            if token in self._in_progress:
                return False
            self._in_progress.add(token)
        asyncio.create_task(self._run(token, chat_id))
        return True

    async def _run(self, token: str, chat_id: int) -> None:
        async with self._semaphore:
            try:
                await self._process(token, chat_id)
            except Exception:
                logger.exception("Preparation failed for token=%s", token)
                await self._bot.send_message(
                    chat_id,
                    "Не удалось подготовить видео. Попробуйте позже.",
                )
            finally:
                async with self._lock:
                    self._in_progress.discard(token)

    async def _process(self, token: str, chat_id: int) -> None:
        token_info = await self._db.get_pm_token(token)
        if token_info is None:
            await self._bot.send_message(chat_id, "Ссылка устарела. Повторите поиск в inline.")
            return
        now_ts = int(time.time())
        if token_info.expires_at <= now_ts:
            await self._bot.send_message(chat_id, "Ссылка устарела. Повторите поиск в inline.")
            return

        candidates = await self._db.get_candidates(token)
        if not candidates:
            try:
                candidates = await search_via_api(
                    token_info.query_text, limit=3, api_key=settings.youtube_api_key
                )
                await self._db.store_candidates(token, candidates)
            except YtDlpError as exc:
                await self._bot.send_message(
                    chat_id, f"Не удалось найти видео: {exc}"
                )
                return

        if not candidates:
            await self._bot.send_message(chat_id, "Ничего не нашлось по запросу.")
            return

        candidate = candidates[0]
        await self._bot.send_message(chat_id, "⏳ Готовлю видео...")

        job_id = f"{token}-{candidate.youtube_id}"
        try:
            result = await yt_download(candidate.source_url, self._download_dir, job_id)
        except YtDlpError as exc:
            await self._bot.send_message(chat_id, f"Не удалось скачать видео: {exc}")
            return

        caption = candidate.title or None
        if caption:
            caption = truncate_text(caption, 1024)
        try:
            video_message = await self._bot.send_video(
                chat_id,
                FSInputFile(result.file_path),
                caption=caption,
            )
        finally:
            try:
                result.file_path.unlink(missing_ok=True)
            except Exception:
                logger.warning("Failed to remove file %s", result.file_path)

        if video_message.video is None:
            await self._bot.send_message(chat_id, "Не удалось получить данные видео.")
            return

        video = video_message.video
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
        await self._db.link_query_to_video(token_info.query_norm, video_id)

        keyboard = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="📤 Отправить в чат…",
                        switch_inline_query=f"ready:{video_id}",
                    )
                ]
            ]
        )

        try:
            await self._bot.edit_message_reply_markup(
                chat_id=chat_id,
                message_id=video_message.message_id,
                reply_markup=keyboard,
            )
        except TelegramBadRequest:
            await self._bot.send_message(
                chat_id,
                "Готово! Можно отправить в чат.",
                reply_markup=keyboard,
            )


def build_prepare_text(candidates: list[YtCandidate], query_text: str) -> str:
    lines = ["Подготовка видео", "", f"Запрос: {query_text}", ""]

    for idx, cand in enumerate(candidates[:3], start=1):
        duration = format_duration(cand.duration)
        lines.append(f"{idx}. {cand.title}")
        lines.append(f"⏱ {duration}")
        lines.append(cand.source_url)
        lines.append("")

    return "\n".join(lines).strip()

def build_prepare_keyboard(token: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="Начать подготовку (≈ 10–30 сек)",
                    callback_data=f"prep:{token}",
                )
            ]
        ]
    )


async def send_prepare_prompt(
    bot: Bot,
    *,
    chat_id: int,
    candidates: list[YtCandidate],
    query_text: str,
    token: str,
) -> None:
    keyboard = build_prepare_keyboard(token)

    media: list[InputMediaPhoto] = []
    for cand in candidates[:3]:
        if not cand.thumbnail_url:
            continue
        duration = format_duration(cand.duration)
        caption = truncate_text(f"{cand.title}\n⏱ {duration}", 1024)
        media.append(InputMediaPhoto(media=cand.thumbnail_url, caption=caption))

    if media:
        try:
            await bot.send_media_group(chat_id, media)
            await bot.send_message(
                chat_id,
                f"Подготовка видео\n\nЗапрос: {query_text}",
                reply_markup=keyboard,
            )
            return
        except TelegramBadRequest:
            media = []

    text = build_prepare_text(candidates, query_text)
    await bot.send_message(
        chat_id,
        text,
        reply_markup=keyboard,
        disable_web_page_preview=True,
    )


def build_switch_pm_text(query: str) -> str:
    base = f"🎬 Подготовить видео по запросу “{query}” (≈ 10–30 сек)"
    return truncate_text(base, 64)


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
    prep_manager = PrepManager(bot, db, settings.download_dir, settings.max_concurrent_jobs)

    @dp.inline_query()
    async def inline_query_handler(inline_query: InlineQuery) -> None:
        query = (inline_query.query or "").strip()
        if not query:
            await inline_query.answer([], is_personal=True, cache_time=1)
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
            )
            await inline_query.answer([result], is_personal=True, cache_time=1)
            return

        query_norm = db.normalize_query(query)
        cached = await db.find_cached_videos(query_norm, settings.max_inline_results)
        results: list = []

        for item in cached:
            results.append(
                InlineQueryResultCachedVideo(
                    id=str(item["id"]),
                    video_file_id=item["file_id"],
                    title=item.get("title") or "Видео",
                    description="Готовое",
                )
            )

        switch_pm_text: Optional[str] = None
        switch_pm_parameter: Optional[str] = None

        try:
            yt_candidates = await asyncio.wait_for(
                search_via_api(
                    query,
                    settings.max_yt_results,
                    api_key=settings.youtube_api_key,
                ),
                timeout=3.0,
            )
        except asyncio.TimeoutError:
            logger.warning("yt-dlp search timed out")
            yt_candidates = []
        except YtDlpError as exc:
            logger.warning("yt-dlp search failed: %s", exc)
            yt_candidates = []

        if yt_candidates:
            await db.purge_expired_tokens()
            token = await db.create_pm_token(query, query_norm, settings.pm_token_ttl_seconds)
            await db.store_candidates(token.token, yt_candidates)

            switch_pm_text = build_switch_pm_text(query)
            switch_pm_parameter = f"pm-{token.token}"

            if settings.inline_show_yt_cards:
                for cand in yt_candidates[: settings.max_yt_results]:
                    results.append(
                        InlineQueryResultArticle(
                            id=f"yt-{cand.youtube_id}",
                            title=cand.title,
                            description="Приготовить ≈ 10–30 сек",
                            thumbnail_url=cand.thumbnail_url,
                            input_message_content=InputTextMessageContent(
                                message_text="Подготовка видео…"
                            ),
                        )
                    )

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
                "Для подготовки видео нажмите кнопку в inline-выдаче."
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
        candidates = await db.get_candidates(token)
        if not candidates:
            try:
                candidates = await search_via_api(
                    token_info.query_text, limit=3, api_key=settings.youtube_api_key
                )
                await db.store_candidates(token, candidates)
            except YtDlpError as exc:
                await message.answer(f"Не удалось найти видео: {exc}")
                return

        if not candidates:
            await message.answer("Ничего не нашлось по запросу.")
            return

        await send_prepare_prompt(
            bot,
            chat_id=message.chat.id,
            candidates=candidates,
            query_text=token_info.query_text,
            token=token,
        )

    @dp.message(F.chat.type == "private", F.text, ~F.text.startswith("/"))
    async def private_query_handler(message: Message) -> None:
        query_text = message.text.strip()
        if not query_text:
            return

        query_norm = db.normalize_query(query_text)
        try:
            yt_candidates = await search_via_api(
                query_text, settings.max_yt_results, api_key=settings.youtube_api_key
            )
        except YtDlpError as exc:
            await message.answer(f"Не удалось найти видео: {exc}")
            return

        if not yt_candidates:
            await message.answer("Ничего не нашлось по запросу.")
            return

        token = await db.create_pm_token(query_text, query_norm, settings.pm_token_ttl_seconds)
        await db.store_candidates(token.token, yt_candidates)

        await send_prepare_prompt(
            bot,
            chat_id=message.chat.id,
            candidates=yt_candidates,
            query_text=query_text,
            token=token.token,
        )

    @dp.callback_query(F.data.startswith("prep:"))
    async def prepare_callback(callback: CallbackQuery) -> None:
        token = callback.data.split(":", 1)[1]
        await callback.answer()
        if callback.message is None:
            return

        started = await prep_manager.start(token, callback.message.chat.id)
        if not started:
            await callback.message.answer("Подготовка уже запущена.")

    try:
        await dp.start_polling(bot)
    finally:
        await db.close()


if __name__ == "__main__":
    asyncio.run(main())
