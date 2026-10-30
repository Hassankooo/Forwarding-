# -*- coding: utf-8 -*-

import os
import asyncio
import threading
import time
import logging

from telethon import TelegramClient, functions

from telethon.sessions import StringSession

from telethon.tl.types import (
    MessageMediaPhoto,
    MessageMediaDocument,
    DocumentAttributeFilename
)


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

logger = logging.getLogger("telegram_client")


# ============================================================
# CONFIG
# ============================================================

API_ID = int(
    os.environ.get(
        "TELEGRAM_API_ID",
        "0"
    )
)

API_HASH = os.environ.get(
    "TELEGRAM_API_HASH",
    ""
)

SESSION_STRING = os.environ.get(
    "TELEGRAM_SESSION_STRING",
    ""
)

BOT_TOKEN = os.environ.get(
    "TELEGRAM_BOT_TOKEN",
    ""
)


# ============================================================
# EVENT LOOP
# ============================================================

loop = asyncio.new_event_loop()


def start_loop(event_loop):

    asyncio.set_event_loop(
        event_loop
    )

    event_loop.run_forever()


loop_thread = threading.Thread(
    target=start_loop,
    args=(loop,),
    daemon=True
)

loop_thread.start()


# ============================================================
# TELEGRAM CLIENT
# ============================================================

if SESSION_STRING:

    client = TelegramClient(
        StringSession(
            SESSION_STRING
        ),
        API_ID,
        API_HASH,
        loop=loop
    )

elif BOT_TOKEN:

    client = TelegramClient(
        "bot_session",
        API_ID,
        API_HASH,
        loop=loop
    )

else:

    client = TelegramClient(
        "anon_session",
        API_ID,
        API_HASH,
        loop=loop
    )


# ============================================================
# TELEGRAM REQUEST RATE CONTROL
# ============================================================

TELEGRAM_MIN_INTERVAL = 0.5

telegram_lock = threading.Lock()

last_telegram_request = 0.0


def wait_telegram_slot():

    global last_telegram_request

    with telegram_lock:

        now = time.monotonic()

        elapsed = (
            now
            - last_telegram_request
        )

        if elapsed < TELEGRAM_MIN_INTERVAL:

            time.sleep(
                TELEGRAM_MIN_INTERVAL
                - elapsed
            )

        last_telegram_request = (
            time.monotonic()
        )


# ============================================================
# START
# ============================================================

_started = False
_start_lock = threading.Lock()


def start_telegram():
    """
    يتصل بتيليجرام ويسجل الدخول إذا لزم الأمر.
    آمن للاستدعاء أكثر من مرة (idempotent).
    """

    global _started

    with _start_lock:

        if _started:
            logger.info("Telegram client already started, skipping.")
            return

        async def _connect():

            await client.connect()

            if not await client.is_user_authorized():

                if BOT_TOKEN:

                    await client.login(
                        bot_token=BOT_TOKEN
                    )

                else:

                    raise RuntimeError(
                        "Telegram client is not authorized and no "
                        "BOT_TOKEN / valid SESSION_STRING was provided."
                    )

            logger.info("✅ تم الاتصال بتيليجرام بنجاح!")

        future = asyncio.run_coroutine_threadsafe(
            _connect(),
            loop
        )

        try:

            future.result(
                timeout=30
            )

            _started = True

        except Exception:

            logger.exception("❌ فشل الاتصال بتيليجرام عند الإقلاع")

            raise


# ============================================================
# GET DIALOGS (CHATS LIST)
# ============================================================

def get_dialogs(limit=None):
    """
    جلب جميع المحادثات المتاحة للحساب: المجموعات والقنوات
    والمنتديات والمحادثات الخاصة، مع تضمين الرسائل المحفوظة.
    """
    wait_telegram_slot()

    async def _get_dialogs():
        dialogs = []
        seen_ids = set()

        # limit=None يعني متابعة جلب المحادثات حتى نهايتها.
        async for dialog in client.iter_dialogs(limit=limit):
            item = {
                "id": dialog.id,
                "title": dialog.name,
                "is_group": dialog.is_group,
                "is_channel": dialog.is_channel,
                "is_user": dialog.is_user,
                "is_saved_messages": False
            }
            dialogs.append(item)
            seen_ids.add(dialog.id)

        # التأكد من ظهور محادثة الرسائل المحفوظة حتى إن لم تُرجعها
        # قائمة الحوارات لأي سبب.
        me = await client.get_me()
        if me and me.id not in seen_ids:
            dialogs.insert(0, {
                "id": me.id,
                "title": "الرسائل المحفوظة",
                "is_group": False,
                "is_channel": False,
                "is_user": True,
                "is_saved_messages": True
            })
        else:
            for item in dialogs:
                if me and item["id"] == me.id:
                    item["title"] = "الرسائل المحفوظة"
                    item["is_saved_messages"] = True
                    break

        return dialogs

    try:

        future = asyncio.run_coroutine_threadsafe(
            _get_dialogs(),
            loop
        )

        return future.result(
            timeout=30
        )

    except Exception:

        logger.exception("get_dialogs failed")

        raise


# ============================================================
# CHECK IF CHAT IS A FORUM
# ============================================================

def is_forum_chat(chat_id):

    wait_telegram_slot()

    async def _check():

        entity = await client.get_entity(
            chat_id
        )

        return getattr(
            entity,
            "forum",
            False
        )

    try:

        future = asyncio.run_coroutine_threadsafe(
            _check(),
            loop
        )

        return future.result(
            timeout=30
        )

    except Exception:

        logger.exception("is_forum_chat failed for chat_id=%s", chat_id)

        return False


# ============================================================
# TOPICS
# ============================================================

def get_topics(chat_id):

    wait_telegram_slot()

    async def _get_topics():

        entity = await client.get_entity(
            chat_id
        )

        result = await client(
            functions.messages.GetForumTopicsRequest(
                peer=entity,
                offset_date=0,
                offset_id=0,
                offset_topic=0,
                limit=100,
                q=""
            )
        )

        topics = []

        for topic in result.topics:

            topics.append(
                {
                    "id": topic.id,
                    "title": topic.title,
                    "closed": getattr(
                        topic,
                        "closed",
                        False
                    ),
                    "hidden": getattr(
                        topic,
                        "hidden",
                        False
                    ),
                    "is_general": getattr(
                        topic,
                        "my",
                        False
                    )
                }
            )

        return topics

    try:

        future = asyncio.run_coroutine_threadsafe(
            _get_topics(),
            loop
        )

        return future.result(
            timeout=30
        )

    except Exception:

        logger.exception("get_topics failed for chat_id=%s", chat_id)

        raise


# ============================================================
# MEDIA INFO
# ============================================================

def extract_media_info(msg):

    if not msg.media:
        return None

    media_type = "file"
    filename = None
    size = 0

    if isinstance(
        msg.media,
        MessageMediaPhoto
    ):

        media_type = "photo"

        filename = (
            f"photo_{msg.id}.jpg"
        )

    elif (
        isinstance(
            msg.media,
            MessageMediaDocument
        )
        and msg.media.document
    ):

        doc = msg.media.document

        size = getattr(
            doc,
            "size",
            0
        )

        mime = (
            getattr(
                doc,
                "mime_type",
                ""
            )
            or ""
        ).lower()

        for attr in doc.attributes:

            attr_name = type(attr).__name__

            if isinstance(
                attr,
                DocumentAttributeFilename
            ):

                filename = attr.file_name

            elif attr_name == "DocumentAttributeVideo":

                media_type = "video"

            elif attr_name in (
                "DocumentAttributeAudio",
                "DocumentAttributeVoice"
            ):

                media_type = "audio"

        if media_type == "file":

            if mime.startswith("video/"):
                media_type = "video"

            elif mime.startswith("audio/"):
                media_type = "audio"

            elif mime.startswith("image/"):
                media_type = "photo"

    if (
        not filename
        and hasattr(msg, "file")
        and msg.file
    ):

        filename = getattr(
            msg.file,
            "name",
            None
        )

    if (
        not filename
        or filename.startswith("Document from")
        or filename.startswith("مستند من")
    ):

        ext = ""

        if (
            isinstance(
                msg.media,
                MessageMediaDocument
            )
            and msg.media.document
        ):

            mime = (
                getattr(
                    msg.media.document,
                    "mime_type",
                    ""
                )
                or ""
            ).lower()

            if (
                "presentation" in mime
                or "powerpoint" in mime
            ):

                ext = ".pptx"

            elif "pdf" in mime:

                ext = ".pdf"

            elif (
                "word" in mime
                or "document" in mime
            ):

                ext = ".docx"

            elif "zip" in mime:

                ext = ".zip"

            elif "rar" in mime:

                ext = ".rar"

            elif "text/" in mime:

                ext = ".txt"

        filename = (
            f"file_{msg.id}{ext}"
        )

    return {
        "type": media_type,
        "name": filename,
        "size": size
    }


# ============================================================
# GET CHAT / CHANNEL MEDIA MESSAGES (NON-FORUM)
# ============================================================

def get_chat_media_messages(
    chat_id,
    limit=50
):

    wait_telegram_slot()

    async def _get_messages():

        entity = await client.get_entity(
            chat_id
        )

        messages = []

        async for msg in client.iter_messages(
            entity,
            limit=200
        ):

            if not msg.media:
                continue

            media_info = extract_media_info(
                msg
            )

            if not media_info:
                continue

            messages.append(
                {
                    "id": msg.id,
                    "text": "",
                    "media": media_info
                }
            )

            if len(messages) >= limit:
                break

        return messages

    try:

        future = asyncio.run_coroutine_threadsafe(
            _get_messages(),
            loop
        )

        return future.result(
            timeout=30
        )

    except Exception:

        logger.exception(
            "get_chat_media_messages failed for chat_id=%s",
            chat_id
        )

        raise


# ============================================================
# TOPIC FILES ONLY
# ============================================================

def get_topic_messages(
    chat_id,
    topic_id,
    limit=10
):

    wait_telegram_slot()

    async def _get_messages():

        entity = await client.get_entity(
            chat_id
        )

        messages = []

        async for msg in client.iter_messages(
            entity,
            limit=200
        ):

            is_in_topic = False

            if msg.id == topic_id:

                is_in_topic = True

            elif (
                hasattr(msg, "reply_to")
                and msg.reply_to
            ):

                top_id = getattr(
                    msg.reply_to,
                    "reply_to_top_id",
                    None
                )

                reply_id = getattr(
                    msg.reply_to,
                    "reply_to_msg_id",
                    None
                )

                if (
                    top_id == topic_id
                    or reply_id == topic_id
                ):

                    is_in_topic = True

            if not is_in_topic:
                continue

            if not msg.media:
                continue

            media_info = extract_media_info(
                msg
            )

            if not media_info:
                continue

            messages.append(
                {
                    "id": msg.id,
                    "text": "",
                    "media": media_info
                }
            )

            if len(messages) >= limit:
                break

        return messages

    try:

        future = asyncio.run_coroutine_threadsafe(
            _get_messages(),
            loop
        )

        return future.result(
            timeout=30
        )

    except Exception:

        logger.exception(
            "get_topic_messages failed for chat_id=%s topic_id=%s",
            chat_id,
            topic_id
        )

        raise


# ============================================================
# DOWNLOAD
# ============================================================

def download_media(
    chat_id,
    message_id,
    destination
):

    wait_telegram_slot()

    async def _download():

        entity = await client.get_entity(
            chat_id
        )

        msg = await client.get_messages(
            entity,
            ids=message_id
        )

        if (
            not msg
            or not msg.media
        ):

            raise Exception(
                "الرسالة لا تحتوي على ميديا."
            )

        path = await client.download_media(
            msg,
            file=destination
        )

        if not path:

            raise Exception(
                "فشل تنزيل الملف."
            )

        media_info = extract_media_info(
            msg
        )

        return {
            "path": path,
            "type": (
                media_info["type"]
                if media_info
                else "file"
            )
        }

    try:

        future = asyncio.run_coroutine_threadsafe(
            _download(),
            loop
        )

        return future.result(
            timeout=120
        )

    except Exception:

        logger.exception(
            "download_media failed for chat_id=%s message_id=%s",
            chat_id,
            message_id
        )

        raise
        