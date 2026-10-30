# -*- coding: utf-8 -*-

import os
import uuid
import time
import threading
import queue
import logging
import json
import mimetypes
import requests

from flask import Flask, request, abort, send_file

import telegram_client


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

logger = logging.getLogger("app")

app = Flask(__name__)


# ============================================================
# CONFIG
# ============================================================

PAGE_ACCESS_TOKEN = os.environ.get("PAGE_ACCESS_TOKEN", "")
VERIFY_TOKEN = os.environ.get("VERIFY_TOKEN", "")

GRAPH_VERSION = os.environ.get("GRAPH_VERSION", "v20.0")

GRAPH_URL = (
    f"https://graph.facebook.com/"
    f"{GRAPH_VERSION}/me/messages"
)

ATTACHMENT_UPLOAD_URL = (
    f"https://graph.facebook.com/"
    f"{GRAPH_VERSION}/me/message_attachments"
)

PAGE_SIZE = 10
CHAT_PAGE_SIZE = 20


# ============================================================
# TELEGRAM STARTUP
# ============================================================

try:

    telegram_client.start_telegram()

except Exception:

    logger.exception(
        "❌ فشل الاتصال بتيليجرام عند الإقلاع — "
        "تأكد من TELEGRAM_API_ID / TELEGRAM_API_HASH / "
        "TELEGRAM_SESSION_STRING أو TELEGRAM_BOT_TOKEN"
    )


# ============================================================
# MESSENGER SEND QUEUE
# ============================================================

SEND_INTERVAL = 5.0

send_queue = queue.Queue()

last_send_time = 0.0
send_time_lock = threading.Lock()


def wait_for_send_slot():

    global last_send_time

    with send_time_lock:

        now = time.monotonic()

        elapsed = (
            now
            - last_send_time
        )

        if elapsed < SEND_INTERVAL:

            time.sleep(
                SEND_INTERVAL
                - elapsed
            )

        last_send_time = (
            time.monotonic()
        )


# ============================================================
# MESSENGER JSON REQUEST
# ============================================================

def messenger_request(
    payload,
    timeout=120
):
    if not PAGE_ACCESS_TOKEN:
        logger.error("❌ PAGE_ACCESS_TOKEN غير موجود")
        return None

    wait_for_send_slot()

    try:

        response = requests.post(
            GRAPH_URL,
            params={
                "access_token":
                    PAGE_ACCESS_TOKEN
            },
            json=payload,
            timeout=timeout
        )

        logger.info(
            "📤 FB SEND STATUS: %s",
            response.status_code
        )

        if not response.ok:
            logger.error(
                "MESSENGER SEND ERROR: %s",
                response.text
            )

        return response

    except requests.RequestException as exc:
        logger.error(
            "MESSENGER REQUEST ERROR: %r",
            exc
        )
        return None


# ============================================================
# ATTACHMENT UPLOAD
# ============================================================

def upload_attachment(
    path,
    attachment_type,
    filename
):
    if not PAGE_ACCESS_TOKEN:
        logger.error("❌ PAGE_ACCESS_TOKEN غير موجود")
        return None

    if not path or not os.path.exists(path):
        logger.error("❌ الملف غير موجود للرفع: %s", path)
        return None

    file_size = os.path.getsize(path)

    mime_type, _ = (
        mimetypes.guess_type(filename)
    )

    if not mime_type:
        mime_type = "application/octet-stream"

    logger.info(
        "📤 بدء رفع الملف إلى Messenger: "
        "%s | %s bytes | %s | type=%s",
        filename,
        file_size,
        mime_type,
        attachment_type
    )

    message_json = json.dumps(
        {
            "attachment": {
                "type": attachment_type,
                "payload": {
                    "is_reusable": False
                }
            }
        },
        ensure_ascii=False
    )

    wait_for_send_slot()

    try:

        with open(path, "rb") as file_handle:

            files = {
                "filedata": (
                    filename,
                    file_handle,
                    mime_type
                )
            }

            data = {
                "message": message_json
            }

            response = requests.post(
                ATTACHMENT_UPLOAD_URL,
                params={
                    "access_token":
                        PAGE_ACCESS_TOKEN
                },
                data=data,
                files=files,
                timeout=300
            )

        logger.info(
            "📤 FB ATTACHMENT UPLOAD STATUS: %s",
            response.status_code
        )

        if not response.ok:
            logger.error(
                "❌ ATTACHMENT UPLOAD ERROR: %s",
                response.text
            )
            return None

        try:
            result = response.json()
        except Exception:
            logger.error(
                "❌ Messenger returned non-JSON response: %s",
                response.text
            )
            return None

        attachment_id = result.get(
            "attachment_id"
        )

        if not attachment_id:
            logger.error(
                "❌ لم يتم الحصول على attachment_id: %s",
                result
            )
            return None

        logger.info(
            "✅ ATTACHMENT UPLOAD SUCCESS: %s",
            attachment_id
        )

        return attachment_id

    except requests.RequestException as exc:
        logger.error(
            "❌ ATTACHMENT UPLOAD REQUEST ERROR: %r",
            exc
        )
        return None

    except Exception:
        logger.exception(
            "❌ ATTACHMENT UPLOAD UNKNOWN ERROR"
        )
        return None


# ============================================================
# SEND ATTACHMENT ID
# ============================================================

def send_attachment_id(
    psid,
    attachment_type,
    attachment_id
):
    payload = {
        "recipient": {
            "id": psid
        },
        "message": {
            "attachment": {
                "type": attachment_type,
                "payload": {
                    "attachment_id":
                        attachment_id
                }
            }
        }
    }

    response = messenger_request(
        payload,
        timeout=120
    )

    if (
        response is not None
        and response.ok
    ):
        logger.info(
            "✅ ATTACHMENT SENT SUCCESSFULLY: %s",
            attachment_id
        )
        return True

    logger.error(
        "❌ FAILED TO SEND ATTACHMENT ID: %s",
        attachment_id
    )

    return False


# ============================================================
# MESSAGE DUPLICATE PROTECTION
# ============================================================

SEND_DEDUP_TTL = 60

recent_sends = {}
recent_sends_lock = threading.Lock()


def cleanup_recent_sends():
    now = time.time()
    expired = [
        key
        for key, timestamp
        in recent_sends.items()
        if now - timestamp
        > SEND_DEDUP_TTL
    ]
    for key in expired:
        recent_sends.pop(key, None)


def already_sent(dedup_key):
    now = time.time()
    with recent_sends_lock:
        cleanup_recent_sends()
        if dedup_key in recent_sends:
            return True
        recent_sends[dedup_key] = now
    return False


# ============================================================
# ATTACHMENT PENDING / SUCCESS STATE
# ============================================================

attachment_states = {}
attachment_states_lock = threading.Lock()

ATTACHMENT_STATE_TTL = 300


def cleanup_attachment_states():
    now = time.time()
    expired = []
    for key, info in attachment_states.items():
        timestamp = info.get("time", now)
        if now - timestamp > ATTACHMENT_STATE_TTL:
            expired.append(key)
    for key in expired:
        attachment_states.pop(key, None)


def attachment_is_pending_or_sent(
    dedup_key
):
    now = time.time()
    with attachment_states_lock:
        cleanup_attachment_states()
        info = attachment_states.get(
            dedup_key
        )
        if info:
            status = info.get("status")
            if status in ["pending", "sent"]:
                return True

        attachment_states[
            dedup_key
        ] = {
            "status": "pending",
            "time": now
        }

    return False


def attachment_mark_success(
    dedup_key
):
    with attachment_states_lock:
        attachment_states[
            dedup_key
        ] = {
            "status": "sent",
            "time": time.time()
        }


def attachment_mark_failed(
    dedup_key
):
    with attachment_states_lock:
        attachment_states.pop(
            dedup_key,
            None
        )


# ============================================================
# SEND QUEUE WORKER
# ============================================================

def send_worker():
    while True:
        job = send_queue.get()
        try:
            if not job:
                continue

            job_type = job.get(
                "type",
                "json"
            )

            if job_type == "json":
                payload = job["payload"]
                timeout = job.get("timeout", 120)
                messenger_request(payload, timeout=timeout)

            elif job_type == "attachment":
                psid = job["psid"]
                path = job["path"]
                filename = job["filename"]
                attachment_type = job["attachment_type"]
                dedup_key = job["dedup_key"]

                try:
                    attachment_id = upload_attachment(
                        path,
                        attachment_type,
                        filename
                    )

                    if not attachment_id:
                        attachment_mark_failed(dedup_key)
                        continue

                    success = send_attachment_id(
                        psid,
                        attachment_type,
                        attachment_id
                    )

                    if success:
                        attachment_mark_success(dedup_key)
                    else:
                        attachment_mark_failed(dedup_key)

                except Exception:
                    attachment_mark_failed(dedup_key)
                    logger.exception("❌ ATTACHMENT WORKER ERROR")

        except Exception:
            logger.exception("SEND WORKER ERROR")

        finally:
            send_queue.task_done()


send_thread = threading.Thread(
    target=send_worker,
    daemon=True
)

send_thread.start()


# ============================================================
# QUEUE TEXT
# ============================================================

def queue_text(
    psid,
    text,
    dedup_key=None
):
    if not text:
        return

    text = str(text)

    if dedup_key is None:
        dedup_key = f"text:{psid}:{text}"

    if already_sent(dedup_key):
        logger.warning("⚠️ DUPLICATE TEXT BLOCKED: %s", dedup_key)
        return

    chunks = []

    while len(text) > 1900:
        cut = text.rfind("\n", 0, 1900)
        if cut < 500:
            cut = 1900
        chunks.append(text[:cut])
        text = text[cut:].lstrip()

    if text:
        chunks.append(text)

    for index, chunk in enumerate(chunks):
        chunk_key = f"{dedup_key}:chunk:{index}"
        if already_sent(chunk_key):
            continue

        payload = {
            "recipient": {
                "id": psid
            },
            "message": {
                "text": chunk
            }
        }

        send_queue.put(
            {
                "type": "json",
                "payload": payload,
                "timeout": 60
            }
        )


# ============================================================
# QUEUE DIRECT ATTACHMENT
# ============================================================

def queue_attachment(
    psid,
    attachment_type,
    path,
    filename,
    file_id=None
):
    if file_id is None:
        file_id = path

    dedup_key = (
        f"attachment:"
        f"{psid}:"
        f"{attachment_type}:"
        f"{file_id}"
    )

    if attachment_is_pending_or_sent(dedup_key):
        logger.warning("⚠️ DUPLICATE ATTACHMENT BLOCKED: %s", dedup_key)
        return False

    if not path or not os.path.exists(path):
        logger.error("❌ ATTACHMENT FILE NOT FOUND: %s", path)
        attachment_mark_failed(dedup_key)
        return False

    send_queue.put(
        {
            "type": "attachment",
            "psid": psid,
            "path": path,
            "filename": filename,
            "attachment_type": attachment_type,
            "dedup_key": dedup_key
        }
    )

    logger.info("📦 ATTACHMENT ADDED TO QUEUE: %s", filename)
    return True


# ============================================================
# USER STATE
# ============================================================

user_states = {}
state_lock = threading.Lock()


def get_state(psid):
    with state_lock:
        if psid not in user_states:
            user_states[psid] = {
                "section": "chats",  # البدء بقائمة المحادثات
                "chats": [],
                "chat_index": None,
                "target_chat_id": None,
                "chat_title": "",
                "topics": [],
                "topic_index": None,
                "topic_id": None,
                "topic_title": "",
                "messages": [],
                "page": 0,
                "chat_page": 0
            }
        return user_states[psid]


# ============================================================
# WEBHOOK DUPLICATE PROTECTION
# ============================================================

WEBHOOK_TTL = 600

processed_events = {}
processed_events_lock = threading.Lock()


def event_already_processed(event):
    event_id = event.get("mid")
    if not event_id:
        return False

    now = time.time()
    with processed_events_lock:
        expired = [
            key
            for key, timestamp
            in processed_events.items()
            if now - timestamp
            > WEBHOOK_TTL
        ]
        for key in expired:
            processed_events.pop(key, None)

        if event_id in processed_events:
            return True

        processed_events[event_id] = now
    return False


# ============================================================
# COMMAND DUPLICATE PROTECTION
# ============================================================

COMMAND_TTL = 3

recent_commands = {}
recent_commands_lock = threading.Lock()


def command_is_duplicate(psid, text):
    key = f"{psid}:{text}"
    now = time.time()
    with recent_commands_lock:
        old = recent_commands.get(key)
        if old and now - old < COMMAND_TTL:
            return True
        recent_commands[key] = now
    return False


# ============================================================
# MEDIA STORAGE
# ============================================================

MEDIA_DIR = "/tmp/messenger_media"

os.makedirs(MEDIA_DIR, exist_ok=True)

media_files = {}
media_lock = threading.Lock()

MEDIA_LIFETIME = 3600


def cleanup_media():
    now = time.time()
    with media_lock:
        expired = []
        for token, info in media_files.items():
            if now - info.get("created", now) > MEDIA_LIFETIME:
                expired.append((token, info.get("path")))

        for token, path in expired:
            media_files.pop(token, None)
            try:
                if path and os.path.exists(path):
                    os.remove(path)
            except Exception:
                pass


def media_cleanup_worker():
    while True:
        try:
            cleanup_media()
        except Exception:
            logger.exception("MEDIA CLEANUP ERROR")
        time.sleep(300)


threading.Thread(
    target=media_cleanup_worker,
    daemon=True
).start()


# ============================================================
# HELPERS
# ============================================================

def media_icon(media):
    if not media:
        return "📎"

    icons = {
        "photo": "🖼️",
        "video": "🎬",
        "document": "📄",
        "audio": "🎵",
        "voice": "🎙️"
    }

    return icons.get(media.get("type"), "📎")


def human_size(size):
    if not size:
        return ""

    size = float(size)

    if size < 1024:
        return f"{int(size)} B"

    if size < 1024 * 1024:
        return f"{size / 1024:.1f} KB"

    return f"{size / (1024 * 1024):.1f} MB"


# ============================================================
# FORMAT MEDIA LIST
# ============================================================

def format_messages_page(messages, page, title):
    start = page * PAGE_SIZE
    end = start + PAGE_SIZE

    current = messages[start:end]

    if not current:
        return f"📁 {title}\n\nلا توجد ملفات في هذه الصفحة."

    total_pages = (len(messages) + PAGE_SIZE - 1) // PAGE_SIZE

    lines = [
        f"📁 {title}",
        f"الصفحة {page + 1} / {total_pages}",
        ""
    ]

    for i, msg in enumerate(current, start=1):
        media = msg.get("media")
        icon = media_icon(media)
        filename = media.get("name") if media else "ملف"
        size = ""

        if media and media.get("size"):
            size = f" ({human_size(media['size'])})"

        lines.append(f"{i}. {icon} {filename}{size}")

    lines.extend(
        [
            "",
            "أرسل رقم الملف لإرساله.",
            "التالي — الصفحة التالية",
            "السابق — الصفحة السابقة",
            "رجوع — العودة للقائمة"
        ]
    )

    return "\n".join(lines)


# ============================================================
# SHOW CHATS (DIALOGS)
# ============================================================

def format_chats_page(chats, page):
    start = page * CHAT_PAGE_SIZE
    current = chats[start:start + CHAT_PAGE_SIZE]
    total_pages = max(1, (len(chats) + CHAT_PAGE_SIZE - 1) // CHAT_PAGE_SIZE)

    lines = [
        "📌 اختر محادثة أو قناة:",
        f"الصفحة {page + 1} / {total_pages}",
        ""
    ]

    for i, chat in enumerate(current, start=1):
        is_channel = chat.get("is_channel")
        is_group = chat.get("is_group")
        is_saved = chat.get("is_saved_messages")
        icon = "⭐" if is_saved else ("📢" if is_channel else ("👥" if is_group else "👤"))
        title = chat.get("title", "بدون اسم")
        lines.append(f"{i}. {icon} {title}")

    lines.extend([
        "",
        "أرسل رقم المحادثة لفتحها.",
        "التالي — الصفحة التالية",
        "السابق — الصفحة السابقة"
    ])
    return "\n".join(lines)


def show_chats(psid):
    try:
        chats = telegram_client.get_dialogs(limit=None)
    except Exception:
        logger.exception("GET DIALOGS ERROR")
        queue_text(psid, "❌ تعذر جلب المحادثات من تيليجرام.")
        return

    state = get_state(psid)
    state.update({
        "section": "chats",
        "chats": chats,
        "chat_page": 0,
        "chat_index": None,
        "target_chat_id": None,
        "chat_title": "",
        "topics": [],
        "topic_index": None,
        "topic_id": None,
        "topic_title": "",
        "messages": [],
        "page": 0
    })

    if not chats:
        queue_text(psid, "📭 لا توجد محادثات متاحة.")
        return

    queue_text(psid, format_chats_page(chats, 0))


# ============================================================
# OPEN SELECTED CHAT
# ============================================================

def open_selected_chat(psid, chat_index):
    state = get_state(psid)
    chats = state.get("chats", [])

    if not (0 <= chat_index < len(chats)):
        queue_text(psid, "❌ رقم المحادثة غير صحيح.")
        return

    chat = chats[chat_index]
    chat_id = chat.get("id")
    chat_title = chat.get("title", "محادثة")

    state["chat_index"] = chat_index
    state["target_chat_id"] = chat_id
    state["chat_title"] = chat_title

    # التحقق مما إذا كانت المحادثة منتدى (Forum)
    try:
        is_forum = telegram_client.is_forum_chat(chat_id)
    except Exception:
        is_forum = False

    if not is_forum:
        open_channel_messages_for_chat(psid, chat_id, chat_title)
        return

    try:
        topics = telegram_client.get_topics(chat_id)
    except Exception:
        logger.exception("GET TOPICS ERROR")
        queue_text(psid, "❌ تعذر جلب مواضيع المنتدى.")
        return

    state.update({
        "section": "topics",
        "topics": topics,
        "messages": [],
        "page": 0,
        "topic_index": None,
        "topic_id": None,
        "topic_title": ""
    })

    if not topics:
        queue_text(
            psid,
            f"📁 {chat_title}\n\n📭 لا توجد مواضيع متاحة في هذا المنتدى."
        )
        return

    lines = [
        f"📁 {chat_title}",
        "📌 مواضيع المنتدى:",
        ""
    ]

    for i, topic in enumerate(topics, start=1):
        closed = " 🔒" if topic.get("closed") else ""
        lines.append(f"{i}. {topic.get('title', 'بدون اسم')}{closed}")

    lines.extend(
        [
            "",
            "أرسل رقم الموضوع لعرض ملفاته."
        ]
    )

    queue_text(psid, "\n".join(lines))


# ============================================================
# OPEN CHANNEL / GROUP MESSAGES FOR CHAT
# ============================================================

def open_channel_messages_for_chat(psid, chat_id, chat_title):
    try:
        messages = telegram_client.get_chat_media_messages(
            chat_id,
            limit=50
        )
    except Exception:
        logger.exception("GET CHANNEL MESSAGES ERROR")
        queue_text(psid, "❌ تعذر جلب ملفات المحادثة.")
        return

    state = get_state(psid)
    state.update({
        "section": "messages",
        "topics": [],
        "messages": messages,
        "page": 0,
        "topic_id": None,
        "topic_title": chat_title
    })

    if not messages:
        queue_text(psid, f"📁 {chat_title}\n\nلا توجد ملفات متاحة.")
        return

    queue_text(
        psid,
        format_messages_page(messages, 0, chat_title)
    )


# ============================================================
# OPEN TOPIC
# ============================================================

def open_topic_messages(psid, topic_index):
    state = get_state(psid)
    target_chat_id = state.get("target_chat_id")
    topics = state.get("topics", [])

    if not (0 <= topic_index < len(topics)):
        queue_text(psid, "❌ رقم الموضوع غير صحيح.")
        return

    topic = topics[topic_index]
    topic_id = topic.get("id")
    topic_title = topic.get("title", "موضوع")

    state["topic_index"] = topic_index
    state["topic_id"] = topic_id

    try:
        messages = telegram_client.get_topic_messages(
            target_chat_id,
            topic_id,
            limit=50
        )
    except Exception:
        logger.exception("GET TOPIC ERROR")
        queue_text(psid, "❌ تعذر جلب ملفات الموضوع.")
        return

    state.update({
        "section": "messages",
        "messages": messages,
        "page": 0,
        "topic_title": topic_title
    })

    if not messages:
        queue_text(
            psid,
            f"📁 {topic_title}\n\nلا توجد ملفات في هذا الموضوع."
        )
        return

    queue_text(
        psid,
        format_messages_page(messages, 0, topic_title)
    )


# ============================================================
# GET ATTACHMENT TYPE
# ============================================================

def get_attachment_type(media, filename):
    media_type = ""

    if media:
        media_type = (media.get("type", "") or "").lower()

    if media_type == "photo":
        return "image"

    if media_type == "video":
        return "video"

    if media_type in ["audio", "voice"]:
        return "audio"

    mime_type, _ = mimetypes.guess_type(filename)

    if mime_type:
        if mime_type.startswith("image/"):
            return "image"
        if mime_type.startswith("video/"):
            return "video"
        if mime_type.startswith("audio/"):
            return "audio"

    return "file"


# ============================================================
# DOWNLOAD + QUEUE FILE
# ============================================================

def download_and_send_file(psid, item_number):
    state = get_state(psid)
    page = state.get("page", 0)
    messages = state.get("messages", [])
    target_chat_id = state.get("target_chat_id")

    actual_index = page * PAGE_SIZE + item_number - 1

    if not (0 <= actual_index < len(messages)):
        queue_text(psid, "❌ رقم الملف غير صحيح.")
        return

    msg = messages[actual_index]
    media = msg.get("media")

    if not media:
        queue_text(psid, "❌ هذا العنصر لا يحتوي على ملف.")
        return

    file_id = msg.get("id")

    if not file_id:
        queue_text(psid, "❌ تعذر تحديد الملف.")
        return

    filename = media.get("name") or "file"
    filename = os.path.basename(filename)

    destination = os.path.join(
        MEDIA_DIR,
        f"{uuid.uuid4().hex}_{filename}"
    )

    logger.info("📥 بدء تحميل الملف من Telegram: %s", filename)

    try:
        result = telegram_client.download_media(
            target_chat_id,
            file_id,
            destination
        )
    except Exception:
        logger.exception("DOWNLOAD ERROR")
        queue_text(psid, "❌ فشل تحميل الملف من Telegram.")
        return

    path = result.get("path")

    if not path or not os.path.exists(path):
        queue_text(psid, "❌ الملف غير موجود بعد التحميل.")
        return

    file_size = os.path.getsize(path)

    logger.info(
        "✅ تم تحميل الملف: %s | %s bytes | %s",
        filename,
        file_size,
        human_size(file_size)
    )

    attachment_type = get_attachment_type(media, filename)

    queue_attachment(
        psid,
        attachment_type,
        path,
        filename,
        file_id=file_id
    )


# ============================================================
# MEDIA ROUTE
# ============================================================

@app.route("/media/<token>", methods=["GET"])
def media(token):
    with media_lock:
        info = media_files.get(token)

    if not info:
        abort(404)

    path = info.get("path")

    if not path or not os.path.exists(path):
        abort(404)

    response = send_file(
        path,
        as_attachment=False,
        conditional=False
    )
    response.headers["Cache-Control"] = "public, max-age=3600"
    return response


# ============================================================
# WEBHOOK VERIFY
# ============================================================

@app.route("/webhook", methods=["GET"])
def verify():
    mode = request.args.get("hub.mode")
    token = request.args.get("hub.verify_token")
    challenge = request.args.get("hub.challenge")

    if mode == "subscribe" and token == VERIFY_TOKEN:
        return challenge, 200

    return "Forbidden", 403


# ============================================================
# WEBHOOK
# ============================================================

@app.route("/webhook", methods=["POST"])
def webhook():
    try:
        return _handle_webhook()
    except Exception:
        logger.exception("❌ UNHANDLED WEBHOOK ERROR")
        return "OK", 200


def _handle_webhook():
    data = request.get_json(silent=True) or {}

    if data.get("object") != "page":
        return "OK", 200

    for entry in data.get("entry", []):
        for event in entry.get("messaging", []):
            if event_already_processed(event):
                continue

            psid = event.get("sender", {}).get("id")
            message = event.get("message")

            if not psid or not message or message.get("is_echo"):
                continue

            text = (message.get("text") or "").strip().lower()

            if not text:
                continue

            if command_is_duplicate(psid, text):
                continue

            state = get_state(psid)
            section = state.get("section")

            # ------------------------------------------------
            # START / LIST CHATS
            # ------------------------------------------------

            if text in [
                "/start",
                "بدء",
                "قائمة",
                "المحادثات",
                "المواضيع"
            ]:
                show_chats(psid)

            # ------------------------------------------------
            # BACK
            # ------------------------------------------------

            elif text in ["رجوع", "back"]:
                if section == "messages" and state.get("topic_id") is not None:
                    # العودة لقائمة المواضيع داخل نفس المحادثة
                    chat_idx = state.get("chat_index")
                    if chat_idx is not None:
                        open_selected_chat(psid, chat_idx)
                    else:
                        show_chats(psid)
                else:
                    # العودة للقائمة الرئيسية (قائمة المحادثات)
                    show_chats(psid)

            # ------------------------------------------------
            # NEXT
            # ------------------------------------------------

            elif text in ["التالي", "next"]:
                if section == "chats":
                    chats = state.get("chats", [])
                    max_page = (len(chats) - 1) // CHAT_PAGE_SIZE if chats else 0
                    if state.get("chat_page", 0) < max_page:
                        state["chat_page"] = state.get("chat_page", 0) + 1
                        queue_text(psid, format_chats_page(chats, state["chat_page"]))
                    else:
                        queue_text(psid, "هذه آخر صفحة من المحادثات.")
                    continue

                if section != "messages":
                    queue_text(psid, "اختر محادثة أو موضوعاً أولاً.")
                    continue

                messages = state.get("messages", [])
                if not messages:
                    queue_text(psid, "لا توجد ملفات.")
                    continue

                max_page = (len(messages) - 1) // PAGE_SIZE

                if state["page"] < max_page:
                    state["page"] += 1
                    queue_text(
                        psid,
                        format_messages_page(
                            messages,
                            state["page"],
                            state["topic_title"] or state["chat_title"]
                        )
                    )
                else:
                    queue_text(psid, "هذه آخر صفحة.")

            # ------------------------------------------------
            # PREVIOUS
            # ------------------------------------------------

            elif text in ["السابق", "previous"]:
                if section == "chats":
                    chats = state.get("chats", [])
                    if state.get("chat_page", 0) > 0:
                        state["chat_page"] -= 1
                        queue_text(psid, format_chats_page(chats, state["chat_page"]))
                    else:
                        queue_text(psid, "هذه أول صفحة من المحادثات.")
                    continue

                if section != "messages":
                    queue_text(psid, "اختر محادثة أو موضوعاً أولاً.")
                    continue

                if state["page"] > 0:
                    state["page"] -= 1
                    queue_text(
                        psid,
                        format_messages_page(
                            state["messages"],
                            state["page"],
                            state["topic_title"] or state["chat_title"]
                        )
                    )
                else:
                    queue_text(psid, "هذه أول صفحة.")

            # ------------------------------------------------
            # NUMBER SELECTION
            # ------------------------------------------------

            elif text.isdigit():
                number = int(text)

                if section == "chats":
                    actual_chat_index = state.get("chat_page", 0) * CHAT_PAGE_SIZE + number - 1
                    open_selected_chat(psid, actual_chat_index)

                elif section == "topics":
                    open_topic_messages(psid, number - 1)

                elif section == "messages":
                    download_and_send_file(psid, number)

            # ------------------------------------------------
            # UNKNOWN COMMAND
            # ------------------------------------------------

            else:
                queue_text(
                    psid,
                    "❓ أمر غير معروف.\n"
                    "أرسل «قائمة» أو «بدء» لعرض المحادثات المتاحة."
                )

    return "OK", 200


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=int(os.environ.get("PORT", 5000))
    )