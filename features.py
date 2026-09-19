import os
import re
import random
import asyncio
import hashlib
import asyncpg
import logging
from datetime import datetime, timedelta
from typing import Optional
from dotenv import load_dotenv

from aiogram import Router, F, Bot
from aiogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    ReplyKeyboardMarkup,
    KeyboardButton,
    ChatMemberUpdated,
    ChatJoinRequest,
)
from aiogram.filters import (
    CommandStart,
    Command,
    CommandObject,
    StateFilter,
    ChatMemberUpdatedFilter,
    KICKED,
    LEFT,
    RESTRICTED,
    MEMBER,
    ADMINISTRATOR,
)
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.exceptions import TelegramRetryAfter, TelegramAPIError

load_dotenv()

router = Router()
logger = logging.getLogger(__name__)

# ==================== DATABASE CONFIGURATION & POOL ====================

def get_database_url() -> str:
    url = os.getenv("DATABASE_URL") or os.getenv("DATABASE_PATH") or ""
    return str(url).strip().strip("'").strip('"')

DATABASE_URL = get_database_url()

def get_admin_id() -> int:
    raw = os.getenv("ADMIN_ID", "6427894095")
    clean = str(raw).strip().strip("'").strip('"')
    return int(clean) if clean.isdigit() else 6427894095

ADMIN_ID = get_admin_id()

# Global asyncpg connection pool & init guard
db_pool: Optional[asyncpg.Pool] = None
_db_initialized: bool = False

# Active queue playback, join-accept & ad broadcast tasks
active_tasks: dict[int, asyncio.Task] = {}
active_join_tasks: dict[str, asyncio.Task] = {}
active_ad_task: Optional[asyncio.Task] = None

# Live broadcast progress tracking for stats
live_broadcast_stats: dict[int, dict] = {}


async def get_pool() -> asyncpg.Pool:
    global db_pool
    if db_pool is None:
        db_pool = await asyncpg.create_pool(
            dsn=DATABASE_URL,
            min_size=1,
            max_size=10,
            ssl="require" if "neon.tech" in DATABASE_URL or "sslmode=require" in DATABASE_URL else None
        )
    return db_pool


async def init_db():
    global _db_initialized
    if _db_initialized:
        return
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS bot_settings (
                key TEXT PRIMARY KEY,
                value TEXT
            );
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS queues (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL UNIQUE,
                destination TEXT,
                notify_destination TEXT,
                delay_sec INT DEFAULT 5,
                delay_min INT DEFAULT 5,
                delay_max INT DEFAULT 15,
                delay_type TEXT DEFAULT 'fixed',
                mode TEXT DEFAULT 'sequence',
                run_count INT DEFAULT 0,
                caption_header TEXT DEFAULT '',
                caption_footer TEXT DEFAULT '',
                replace_link_from TEXT DEFAULT '',
                replace_link_target TEXT DEFAULT ''
            );
        """)
        await conn.execute("ALTER TABLE queues ADD COLUMN IF NOT EXISTS delay_min INT DEFAULT 5;")
        await conn.execute("ALTER TABLE queues ADD COLUMN IF NOT EXISTS delay_max INT DEFAULT 15;")
        await conn.execute("ALTER TABLE queues ADD COLUMN IF NOT EXISTS delay_type TEXT DEFAULT 'fixed';")
        await conn.execute("ALTER TABLE queues ADD COLUMN IF NOT EXISTS run_count INT DEFAULT 0;")
        await conn.execute("ALTER TABLE queues ADD COLUMN IF NOT EXISTS caption_header TEXT DEFAULT '';")
        await conn.execute("ALTER TABLE queues ADD COLUMN IF NOT EXISTS caption_footer TEXT DEFAULT '';")
        await conn.execute("ALTER TABLE queues ADD COLUMN IF NOT EXISTS replace_link_from TEXT DEFAULT '';")
        await conn.execute("ALTER TABLE queues ADD COLUMN IF NOT EXISTS replace_link_target TEXT DEFAULT '';")

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS uploaders (
                user_id BIGINT PRIMARY KEY,
                name TEXT DEFAULT 'Flezen uploader',
                queue_id INT REFERENCES queues(id) ON DELETE SET NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS destinations (
                chat_id TEXT PRIMARY KEY,
                title TEXT,
                chat_type TEXT,
                is_unmatured BOOLEAN DEFAULT FALSE,
                accept_requests BOOLEAN DEFAULT FALSE,
                join_delay_min INT DEFAULT 3,
                join_delay_max INT DEFAULT 10,
                assigned_queue_id INT REFERENCES queues(id) ON DELETE SET NULL,
                posts_delivered INT DEFAULT 0,
                total_accepted INT DEFAULT 0
            );
        """)
        await conn.execute("ALTER TABLE destinations ADD COLUMN IF NOT EXISTS is_unmatured BOOLEAN DEFAULT FALSE;")
        await conn.execute("ALTER TABLE destinations ADD COLUMN IF NOT EXISTS accept_requests BOOLEAN DEFAULT FALSE;")
        await conn.execute("ALTER TABLE destinations ADD COLUMN IF NOT EXISTS join_delay_min INT DEFAULT 3;")
        await conn.execute("ALTER TABLE destinations ADD COLUMN IF NOT EXISTS join_delay_max INT DEFAULT 10;")
        await conn.execute("ALTER TABLE destinations ADD COLUMN IF NOT EXISTS assigned_queue_id INT REFERENCES queues(id) ON DELETE SET NULL;")
        await conn.execute("ALTER TABLE destinations ADD COLUMN IF NOT EXISTS posts_delivered INT DEFAULT 0;")
        await conn.execute("ALTER TABLE destinations ADD COLUMN IF NOT EXISTS total_accepted INT DEFAULT 0;")

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS join_requests (
                id SERIAL PRIMARY KEY,
                chat_id TEXT NOT NULL,
                user_id BIGINT NOT NULL,
                status TEXT DEFAULT 'pending',
                approved_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (chat_id, user_id)
            );
        """)
        await conn.execute("ALTER TABLE join_requests ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'pending';")
        await conn.execute("ALTER TABLE join_requests ADD COLUMN IF NOT EXISTS approved_at TIMESTAMP;")

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS posts (
                id SERIAL PRIMARY KEY,
                queue_id INT REFERENCES queues(id) ON DELETE CASCADE,
                user_id BIGINT,
                from_chat_id BIGINT,
                message_id BIGINT,
                content_hash TEXT,
                caption TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        await conn.execute("ALTER TABLE posts ADD COLUMN IF NOT EXISTS caption TEXT;")
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_queue_content_hash ON posts(queue_id, content_hash);
        """)

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS ad_posts (
                id SERIAL PRIMARY KEY,
                from_chat_id BIGINT,
                message_id BIGINT,
                content_hash TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS user_logs (
                id SERIAL PRIMARY KEY,
                user_id BIGINT,
                queue_id INT,
                is_duplicate BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        await conn.execute("ALTER TABLE user_logs ADD COLUMN IF NOT EXISTS is_duplicate BOOLEAN DEFAULT FALSE;")
        await conn.execute("ALTER TABLE user_logs ADD COLUMN IF NOT EXISTS queue_id INT;")

    _db_initialized = True


# ==================== HELPER FUNCTIONS ====================

def parse_destination_input(text: str) -> Optional[tuple[str, str]]:
    """
    Parses input text to extract (numerical_id, nickname).
    Supports:
      - "Channel Name -1001234567890"
      - "-1001234567890 Channel Name"
      - "Channel_Name -1001234567890"
    """
    parts = text.strip().split()
    if len(parts) < 2:
        return None

    # Check if first token is numerical ID
    clean_first = parts[0].lstrip("-")
    if clean_first.isdigit() and len(clean_first) >= 5:
        numerical_id = parts[0]
        nickname = " ".join(parts[1:]).strip()
        return numerical_id, nickname

    # Check if last token is numerical ID
    clean_last = parts[-1].lstrip("-")
    if clean_last.isdigit() and len(clean_last) >= 5:
        numerical_id = parts[-1]
        nickname = " ".join(parts[:-1]).strip()
        return numerical_id, nickname

    # Check any middle token
    for i, p in enumerate(parts):
        clean = p.lstrip("-")
        if clean.isdigit() and len(clean) >= 5:
            numerical_id = p
            other_parts = parts[:i] + parts[i+1:]
            nickname = " ".join(other_parts).strip()
            return numerical_id, nickname

    return None


def get_message_content_hash(message: Message) -> str:
    if message.photo:
        return f"photo_{message.photo[-1].file_unique_id}"
    elif message.video:
        return f"video_{message.video.file_unique_id}"
    elif message.document:
        return f"doc_{message.document.file_unique_id}"
    elif message.animation:
        return f"anim_{message.animation.file_unique_id}"
    elif message.audio:
        return f"audio_{message.audio.file_unique_id}"
    elif message.voice:
        return f"voice_{message.voice.file_unique_id}"
    elif message.video_note:
        return f"vnote_{message.video_note.file_unique_id}"
    elif message.text:
        return f"text_{hashlib.md5(message.text.strip().encode('utf-8')).hexdigest()}"
    elif message.caption:
        return f"caption_{hashlib.md5(message.caption.strip().encode('utf-8')).hexdigest()}"
    return f"msg_{message.message_id}"


def format_eta(seconds: float) -> str:
    seconds = max(0, int(seconds))
    if seconds == 0:
        return "0 sec"
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    secs = seconds % 60
    parts = []
    if hours > 0:
        parts.append(f"{hours} hr")
    if minutes > 0 or hours > 0:
        parts.append(f"{minutes} min")
    if secs > 0 or len(parts) == 0:
        parts.append(f"{secs} sec")
    return " ".join(parts)


def apply_caption_rules(
    original_text: Optional[str],
    header: Optional[str],
    footer: Optional[str],
    replace_link_target: Optional[str],
    replace_link_from: Optional[str] = ""
) -> Optional[str]:
    text = original_text or ""
    header = (header or "").strip()
    footer = (footer or "").strip()
    replace_link_target = (replace_link_target or "").strip()
    replace_link_from = (replace_link_from or "").strip()

    if not text and not header and not footer:
        return None

    if replace_link_target and text:
        if replace_link_from:
            text = re.sub(re.escape(replace_link_from), replace_link_target, text, flags=re.IGNORECASE)
        else:
            tg_link_pattern = r"(?:https?://)?(?:www\.)?(?:t\.me|telegram\.me)/(?:\+[a-zA-Z0-9_\-]+|[a-zA-Z0-9_\-]+)"
            text = re.sub(tg_link_pattern, replace_link_target, text)

    parts = []
    if header:
        parts.append(header)
    if text.strip():
        parts.append(text.strip())
    if footer:
        parts.append(footer)

    res = "\n\n".join(parts)
    return res if res else None


async def check_bot_broadcast_permission(bot: Bot, chat_id: str) -> str:
    try:
        me = await bot.get_me()
        member = await bot.get_chat_member(chat_id=chat_id, user_id=me.id)
        status = member.status
        if status in ["administrator", "creator"]:
            can_post = getattr(member, "can_post_messages", None)
            can_send = getattr(member, "can_send_messages", None)
            if can_post is False and can_send is False:
                return "🔴 Admin (Posting Restricted)"
            return "🟢 Can Broadcast (Admin/Creator)"
        elif status == "member":
            return "🟡 Member status (May lack direct channel post rights)"
        return f"🔴 Restricted status ({status})"
    except Exception as e:
        err_msg = str(e)
        if "chat not found" in err_msg.lower():
            return "🔴 Chat not found / Bot not added"
        return f"🔴 Permission check error ({err_msg[:25]})"


# ==================== RATE-LIMITED NOTIFICATION DISPATCHER ====================

async def safe_send_message(bot: Bot, chat_id: str | int, text: str, reply_markup=None) -> Optional[Message]:
    try:
        return await bot.send_message(chat_id=chat_id, text=text, parse_mode="HTML", reply_markup=reply_markup)
    except TelegramRetryAfter as e:
        await asyncio.sleep(e.retry_after + 0.5)
        try:
            return await bot.send_message(chat_id=chat_id, text=text, parse_mode="HTML", reply_markup=reply_markup)
        except Exception:
            return None
    except Exception:
        return None


async def safe_edit_message(bot: Bot, chat_id: str | int, message_id: int, text: str, reply_markup=None):
    try:
        await bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=text, parse_mode="HTML", reply_markup=reply_markup)
    except TelegramRetryAfter as e:
        await asyncio.sleep(e.retry_after + 0.5)
        try:
            await bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=text, parse_mode="HTML", reply_markup=reply_markup)
        except Exception:
            pass
    except Exception:
        pass


async def safe_delete_message(bot: Bot, chat_id: int | str, message_id: int):
    try:
        await bot.delete_message(chat_id=chat_id, message_id=message_id)
    except Exception:
        pass


async def dispatch_notification(bot: Bot, message_text: str, queue_id: Optional[int] = None):
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            master_dest = await conn.fetchval(
                "SELECT value FROM bot_settings WHERE key = 'master_log_chat_id'"
            )
            queue_dest = None
            if queue_id:
                queue_dest = await conn.fetchval(
                    "SELECT notify_destination FROM queues WHERE id = $1", queue_id
                )

        if master_dest:
            await safe_send_message(bot, master_dest, message_text)
        if queue_dest and queue_dest != master_dest:
            await safe_send_message(bot, queue_dest, message_text)
    except Exception:
        pass


# ==================== CHAT JOIN REQUEST LISTENER & WORKER ====================

@router.chat_join_request()
async def handle_join_request(event: ChatJoinRequest, bot: Bot):
    chat_id = str(event.chat.id)
    user_id = event.from_user.id

    pool = await get_pool()
    async with pool.acquire() as conn:
        master_dest = await conn.fetchval("SELECT value FROM bot_settings WHERE key = 'master_log_chat_id'")
        if master_dest and str(master_dest) == chat_id:
            return

        dest = await conn.fetchrow(
            "SELECT accept_requests, is_unmatured, title FROM destinations WHERE chat_id = $1", chat_id
        )
        await conn.execute(
            "INSERT INTO join_requests (chat_id, user_id, status) VALUES ($1, $2, 'pending') ON CONFLICT (chat_id, user_id) DO NOTHING",
            chat_id, user_id
        )

    if dest:
        should_process = (not dest["is_unmatured"]) or dest["accept_requests"]
        if should_process:
            if chat_id not in active_join_tasks or active_join_tasks[chat_id].done():
                task = asyncio.create_task(join_request_worker(bot, chat_id))
                active_join_tasks[chat_id] = task


async def join_request_worker(bot: Bot, chat_id: str):
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            dest = await conn.fetchrow(
                "SELECT title, is_unmatured, accept_requests, join_delay_min, join_delay_max FROM destinations WHERE chat_id = $1",
                chat_id
            )

        if not dest:
            return

        if dest["is_unmatured"] and not dest["accept_requests"]:
            return

        chat_title = dest["title"] or chat_id
        min_delay = dest["join_delay_min"] or 3
        max_delay = dest["join_delay_max"] or 10
        accepted_count = 0

        while True:
            pool = await get_pool()
            async with pool.acquire() as conn:
                current_dest = await conn.fetchrow(
                    "SELECT is_unmatured, accept_requests FROM destinations WHERE chat_id = $1", chat_id
                )
                if not current_dest:
                    break
                if current_dest["is_unmatured"] and not current_dest["accept_requests"]:
                    break

                row = await conn.fetchrow(
                    "SELECT id, user_id FROM join_requests WHERE chat_id = $1 AND status = 'pending' ORDER BY id ASC LIMIT 1",
                    chat_id
                )

            if not row:
                break

            req_id, user_id = row["id"], row["user_id"]

            try:
                await bot.approve_chat_join_request(chat_id=chat_id, user_id=user_id)
                accepted_count += 1
                async with pool.acquire() as conn:
                    await conn.execute("UPDATE destinations SET total_accepted = total_accepted + 1 WHERE chat_id = $1", chat_id)
            except Exception:
                pass

            pool = await get_pool()
            async with pool.acquire() as conn:
                await conn.execute("UPDATE join_requests SET status = 'accepted', approved_at = CURRENT_TIMESTAMP WHERE id = $1", req_id)

            sleep_duration = random.randint(min(min_delay, max_delay), max(min_delay, max_delay))
            await asyncio.sleep(sleep_duration)

        if accepted_count > 0:
            type_label = "Unmatured Channel" if dest["is_unmatured"] else "Matured Channel"
            await dispatch_notification(
                bot,
                f"🤝 <b>Join Requests Auto-Approved</b>\n\n"
                f"• <b>Destination:</b> <b>{chat_title}</b> ({type_label})\n"
                f"• <b>ID:</b> <code>{chat_id}</code>\n"
                f"• <b>Total Accepted in Run:</b> <code>{accepted_count}</code> member(s)\n"
                f"• <b>Random Delay:</b> <code>{min_delay}s - {max_delay}s</code>"
            )

    except asyncio.CancelledError:
        pass
    finally:
        active_join_tasks.pop(chat_id, None)


# ==================== LIVE BATCH AGGREGATOR ====================

class UploadBatchSession:
    def __init__(self, user_id: int, user_chat_id: int, queue_id: int, uploader_name: str, queue_name: str, bot: Bot):
        self.user_id = user_id
        self.user_chat_id = user_chat_id
        self.queue_id = queue_id
        self.uploader_name = uploader_name
        self.queue_name = queue_name
        self.bot = bot
        
        self.queued_count = 0
        self.duplicate_count = 0
        self.pending_posts = []
        self.pending_dup_logs = 0
        self.user_msg_id: Optional[int] = None
        
        self.log_messages: dict[str, int] = {}
        self.debounce_task: Optional[asyncio.Task] = None
        self.last_sync_time = 0.0
        self.lock = asyncio.Lock()

    async def add_post(self, from_chat_id: int, message_id: int, content_hash: str, caption: Optional[str] = None):
        async with self.lock:
            pool = await get_pool()
            async with pool.acquire() as conn:
                is_duplicate = await conn.fetchval(
                    "SELECT 1 FROM posts WHERE queue_id = $1 AND content_hash = $2 LIMIT 1",
                    self.queue_id, content_hash
                )

            in_memory_dup = any(h == content_hash for _, _, h, _ in self.pending_posts)

            if is_duplicate or in_memory_dup:
                self.duplicate_count += 1
                self.pending_dup_logs += 1
            else:
                self.queued_count += 1
                self.pending_posts.append((from_chat_id, message_id, content_hash, caption))

            if self.debounce_task and not self.debounce_task.done():
                self.debounce_task.cancel()
            self.debounce_task = asyncio.create_task(self._debounce_flush())

    async def _flush_to_db(self):
        to_insert = list(self.pending_posts)
        self.pending_posts.clear()
        dup_logs = self.pending_dup_logs
        self.pending_dup_logs = 0

        pool = await get_pool()
        async with pool.acquire() as conn:
            if to_insert:
                await conn.executemany(
                    "INSERT INTO posts (queue_id, user_id, from_chat_id, message_id, content_hash, caption) VALUES ($1, $2, $3, $4, $5, $6)",
                    [(self.queue_id, self.user_id, fc, mi, ch, cap) for fc, mi, ch, cap in to_insert]
                )
                await conn.executemany(
                    "INSERT INTO user_logs (user_id, queue_id, is_duplicate) VALUES ($1, $2, FALSE)",
                    [(self.user_id, self.queue_id) for _ in to_insert]
                )
            if dup_logs > 0:
                await conn.executemany(
                    "INSERT INTO user_logs (user_id, queue_id, is_duplicate) VALUES ($1, $2, TRUE)",
                    [(self.user_id, self.queue_id) for _ in range(dup_logs)]
                )

    async def _update_ui(self, is_final: bool = False):
        now = asyncio.get_event_loop().time()
        if not is_final and (now - self.last_sync_time < 2.5):
            return

        self.last_sync_time = now
        await self._flush_to_db()

        if is_final:
            if self.user_msg_id:
                await safe_delete_message(self.bot, self.user_chat_id, self.user_msg_id)
                self.user_msg_id = None
            
            dup_text = f"\n⚠️ Total Duplicates Skipped: <code>{self.duplicate_count}</code>" if self.duplicate_count > 0 else ""
            await safe_send_message(
                self.bot,
                self.user_chat_id,
                f"✅ <b>Upload Completed!</b>\nSuccessfully queued <code>{self.queued_count}</code> post(s) into <b>{self.queue_name}</b>.{dup_text}"
            )
        else:
            dup_line = f"\n• ⚠️ Duplicates Skipped: <code>{self.duplicate_count}</code>" if self.duplicate_count > 0 else ""
            status_text = (
                f"📥 <b>Auto-detecting posts...</b>\n"
                f"• Queue: <b>{self.queue_name}</b>\n"
                f"• New Posts Queued: <code>{self.queued_count}</code>{dup_line}"
            )
            if self.user_msg_id:
                await safe_edit_message(self.bot, self.user_chat_id, self.user_msg_id, status_text)
            else:
                msg = await safe_send_message(self.bot, self.user_chat_id, status_text)
                if msg:
                    self.user_msg_id = msg.message_id

        pool = await get_pool()
        async with pool.acquire() as conn:
            master_dest = await conn.fetchval("SELECT value FROM bot_settings WHERE key = 'master_log_chat_id'")
            q_dest = await conn.fetchval("SELECT notify_destination FROM queues WHERE id = $1", self.queue_id)

        target_channels = set()
        if master_dest:
            target_channels.add(master_dest)
        if q_dest:
            target_channels.add(q_dest)

        status_tag = "✅ <b>Batch Upload Completed</b>" if is_final else "📥 <b>Batch Upload in Progress...</b>"
        dup_log_line = f"\n• <b>Duplicates Skipped:</b> <code>{self.duplicate_count}</code>" if self.duplicate_count > 0 else ""
        log_text = (
            f"{status_tag}\n\n"
            f"• <b>Flezen Uploader:</b> {self.uploader_name} (<code>{self.user_id}</code>)\n"
            f"• <b>Target Queue:</b> <code>{self.queue_name}</code>\n"
            f"• <b>New Posts Queued:</b> <code>{self.queued_count}</code>"
            f"{dup_log_line}\n"
            f"• <b>Status:</b> {'Finalized' if is_final else 'Receiving & filtering...'}"
        )

        for cid in target_channels:
            if cid in self.log_messages:
                await safe_edit_message(self.bot, cid, self.log_messages[cid], log_text)
            else:
                sent = await safe_send_message(self.bot, cid, log_text)
                if sent:
                    self.log_messages[cid] = sent.message_id

    async def _debounce_flush(self):
        try:
            while True:
                await asyncio.sleep(3.0)
                async with self.lock:
                    if not self.pending_posts and self.pending_dup_logs == 0:
                        break
                    await self._update_ui(is_final=False)

            async with self.lock:
                await self._update_ui(is_final=True)
        except asyncio.CancelledError:
            pass
        finally:
            upload_batches.pop(self.user_id, None)


upload_batches: dict[int, UploadBatchSession] = {}


# ==================== FSM STATES ====================

class AdminStates(StatesGroup):
    waiting_for_queue_name = State()
    waiting_for_master_log_manual = State()
    waiting_for_destination_manual = State()
    waiting_for_fixed_delay = State()
    waiting_for_random_delay = State()
    waiting_for_join_delay = State()
    waiting_for_uploader_id = State()
    waiting_for_uploader_name = State()
    
    # Ad Management states
    waiting_for_ad_post = State()
    waiting_for_ad_delay = State()

    # Caption Editor states
    waiting_for_caption_header = State()
    waiting_for_caption_footer = State()
    waiting_for_caption_link = State()


# ==================== AUTO-DISCOVERY & INTERACTIVE NOTIFICATION ====================

@router.my_chat_member(ChatMemberUpdatedFilter(member_status_changed=ADMINISTRATOR))
async def bot_added_as_admin(event: ChatMemberUpdated, bot: Bot):
    chat = event.chat
    chat_id = str(chat.id)
    chat_title = chat.title or chat.username or "Unnamed Destination"
    chat_type = chat.type

    pool = await get_pool()
    async with pool.acquire() as conn:
        master_dest = await conn.fetchval("SELECT value FROM bot_settings WHERE key = 'master_log_chat_id'")
        
        if master_dest and master_dest == chat_id:
            await dispatch_notification(bot, f"ℹ️ Bot refreshed as Admin in Master Log channel: <b>{chat_title}</b>")
            return

        await conn.execute(
            """
            INSERT INTO destinations (chat_id, title, chat_type, is_unmatured, accept_requests)
            VALUES ($1, $2, $3, FALSE, TRUE)
            ON CONFLICT(chat_id) DO UPDATE SET title = EXCLUDED.title, chat_type = EXCLUDED.chat_type
            """,
            chat_id, chat_title, chat_type
        )

    log_msg = (
        f"📢 <b>New Destination Detected!</b>\n\n"
        f"• <b>Title:</b> {chat_title}\n"
        f"• <b>ID:</b> <code>{chat_id}</code>\n"
        f"• <b>Type:</b> {chat_type.capitalize()}\n\n"
        "Choose how this channel should function:"
    )

    setup_kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🟢 Set as Matured Channel", callback_data=f"auto_setup_dest:{chat_id}:matured")],
            [InlineKeyboardButton(text="⚪ Set as Unmatured Channel", callback_data=f"auto_setup_dest:{chat_id}:unmatured")],
            [InlineKeyboardButton(text="📋 Set as Master Log (Process Only)", callback_data=f"auto_setup_dest:{chat_id}:master")],
            [InlineKeyboardButton(text="🗑 Remove Channel", callback_data=f"dest_delete:{chat_id}")]
        ]
    )

    admin_target = get_admin_id()
    if admin_target:
        await safe_send_message(bot, admin_target, log_msg, reply_markup=setup_kb)

    await dispatch_notification(bot, log_msg)


@router.callback_query(F.data.startswith("auto_setup_dest:"))
async def admin_auto_setup_dest(callback: CallbackQuery, bot: Bot):
    await callback.answer()
    if callback.from_user.id != get_admin_id():
        return
    parts = callback.data.split(":")
    chat_id = parts[1]
    choice = parts[2]

    pool = await get_pool()
    if choice == "matured":
        async with pool.acquire() as conn:
            await conn.execute("DELETE FROM bot_settings WHERE key = 'master_log_chat_id' AND value = $1", chat_id)
            await conn.execute(
                "UPDATE destinations SET is_unmatured = FALSE, accept_requests = TRUE WHERE chat_id = $1",
                chat_id
            )
        if chat_id not in active_join_tasks or active_join_tasks[chat_id].done():
            task = asyncio.create_task(join_request_worker(bot, chat_id))
            active_join_tasks[chat_id] = task
        await callback.answer("Configured as Matured Channel!", show_alert=True)
        await render_dest_actions(callback, chat_id)

    elif choice == "unmatured":
        async with pool.acquire() as conn:
            await conn.execute("DELETE FROM bot_settings WHERE key = 'master_log_chat_id' AND value = $1", chat_id)
            await conn.execute(
                "UPDATE destinations SET is_unmatured = TRUE, accept_requests = FALSE WHERE chat_id = $1",
                chat_id
            )
        if chat_id in active_join_tasks:
            active_join_tasks[chat_id].cancel()
            del active_join_tasks[chat_id]
        await callback.answer("Configured as Unmatured Channel!", show_alert=True)
        await render_dest_actions(callback, chat_id)

    elif choice == "master":
        if chat_id in active_join_tasks:
            active_join_tasks[chat_id].cancel()
            del active_join_tasks[chat_id]

        async with pool.acquire() as conn:
            await conn.execute("DELETE FROM destinations WHERE chat_id = $1", chat_id)
            await conn.execute(
                """
                INSERT INTO bot_settings (key, value)
                VALUES ('master_log_chat_id', $1)
                ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
                """,
                chat_id
            )
        await callback.answer("Set as Master Log! Excluded from regular broadcasts.", show_alert=True)
        await admin_set_master_log_screen(callback)


@router.my_chat_member(ChatMemberUpdatedFilter(member_status_changed=(KICKED | LEFT | RESTRICTED | MEMBER)))
async def bot_removed_from_admin(event: ChatMemberUpdated, bot: Bot):
    chat_id = str(event.chat.id)
    if chat_id in active_join_tasks:
        active_join_tasks[chat_id].cancel()
        del active_join_tasks[chat_id]

    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM destinations WHERE chat_id = $1", chat_id)
        await conn.execute(
            "DELETE FROM bot_settings WHERE key = 'master_log_chat_id' AND value = $1", chat_id
        )

    await dispatch_notification(
        bot,
        f"⚠️ <b>Bot Removed as Admin</b>\nRemoved from chat ID: <code>{chat_id}</code>."
    )


# ==================== KEYBOARDS ====================

def get_user_main_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🚀 Start Task"), KeyboardButton(text="📊 Check Stats")],
            [KeyboardButton(text="⏹ Finish Uploading")]
        ],
        resize_keyboard=True
    )

async def get_admin_main_kb() -> InlineKeyboardMarkup:
    pool = await get_pool()
    async with pool.acquire() as conn:
        master_log = await conn.fetchval("SELECT value FROM bot_settings WHERE key = 'master_log_chat_id'")
        matured_count = await conn.fetchval(
            "SELECT COUNT(*) FROM destinations WHERE is_unmatured = FALSE AND (chat_id != $1 OR $1 IS NULL)", master_log
        ) or 0
        unmatured_count = await conn.fetchval(
            "SELECT COUNT(*) FROM destinations WHERE is_unmatured = TRUE AND (chat_id != $1 OR $1 IS NULL)", master_log
        ) or 0

    master_label = f"📋 Master Log: {master_log[:15]}..." if master_log else "📋 Set Master Log"

    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📂 Manage Queues", callback_data="admin_manage_hub")],
            [InlineKeyboardButton(text="📢 Advertisement Hub", callback_data="admin_ads_hub")],
            [InlineKeyboardButton(text="✏️ Edit Queue Captions", callback_data="admin_caption_hub")],
            [InlineKeyboardButton(text=f"📡 Available Destinations (🟢{matured_count} | ⚪{unmatured_count})", callback_data="admin_view_destinations")],
            [InlineKeyboardButton(text="📈 Matrix & Performance", callback_data="admin_matrix_hub")],
            [InlineKeyboardButton(text="👥 Manage Uploaders (Add/Delete)", callback_data="admin_uploaders_menu")],
            [InlineKeyboardButton(text=master_label, callback_data="admin_set_master_log_screen")],
            [InlineKeyboardButton(text="❌ Close Menu", callback_data="admin_close")]
        ]
    )


# ==================== COMMAND: ADD DESTINATION ====================

@router.message(Command("adddestination", "addest", "add_destination", "addchannel"))
async def cmd_adddestination(message: Message, command: CommandObject):
    if message.from_user.id != get_admin_id():
        return

    args = command.args
    if not args:
        await message.answer(
            "⚠️ <b>Add Destination Command Format:</b>\n\n"
            "<b>Usage:</b>\n"
            "<code>/adddestination [Channel Name] [Numerical ID]</code>\n"
            "or\n"
            "<code>/adddestination [Numerical ID] [Channel Name]</code>\n\n"
            "<b>Examples:</b>\n"
            "• <code>/adddestination VIP Channel -1001234567890</code>\n"
            "• <code>/adddestination -1001234567890 VIP Channel</code>\n"
            "• <code>/addest Marketing_Hub -1009876543210</code>",
            parse_mode="HTML"
        )
        return

    parsed = parse_destination_input(args)
    if not parsed:
        await message.answer(
            "⚠️ <b>Missing or Invalid Arguments!</b>\n\n"
            "Please provide both a channel name and its numerical ID (minimum 5 digits).\n"
            "<b>Example:</b> <code>/adddestination VIP Channel -1001234567890</code>",
            parse_mode="HTML"
        )
        return

    numerical_id, nickname = parsed

    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO destinations (chat_id, title, chat_type, is_unmatured, accept_requests)
            VALUES ($1, $2, 'channel', FALSE, TRUE)
            ON CONFLICT (chat_id) DO UPDATE SET title = EXCLUDED.title
            """,
            numerical_id, nickname
        )

    buttons = [
        [
            InlineKeyboardButton(text="🟢 Set Matured", callback_data=f"dest_convert_matured:{numerical_id}"),
            InlineKeyboardButton(text="⚪ Set Unmatured", callback_data=f"dest_convert_unmatured:{numerical_id}")
        ],
        [InlineKeyboardButton(text="📋 Set as Master Log (Process Only)", callback_data=f"dest_set_master:{numerical_id}")],
        [InlineKeyboardButton(text="⚙️ Open Channel Actions", callback_data=f"dest_actions:{numerical_id}")]
    ]

    await message.answer(
        f"✅ <b>Destination Registered Successfully!</b>\n\n"
        f"• <b>Name:</b> [{nickname}]\n"
        f"• <b>ID:</b> <code>{numerical_id}</code>\n"
        f"• <b>Category:</b> 🟢 <b>Matured Channel</b> (Default)\n\n"
        "Configure category or designate as Master Log below:",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons)
    )


# ==================== HUBS: MANAGE & MATRIX ====================

@router.callback_query(F.data == "admin_manage_hub")
async def admin_manage_hub(callback: CallbackQuery):
    await callback.answer()
    if callback.from_user.id != get_admin_id():
        return
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📂 List & Manage Queues", callback_data="admin_list_queues")],
        [InlineKeyboardButton(text="🚀 Running Queues Hub", callback_data="admin_running_queues")],
        [InlineKeyboardButton(text="➕ Create Queue", callback_data="admin_create_queue")],
        [InlineKeyboardButton(text="🔙 Back to Admin Menu", callback_data="admin_back")]
    ])
    await safe_edit_message(callback.message.bot, callback.message.chat.id, callback.message.message_id, "📂 <b>Manage Queues Hub</b>\n\nChoose an option:", reply_markup=kb)


@router.callback_query(F.data == "admin_matrix_hub")
async def admin_matrix_hub(callback: CallbackQuery):
    await callback.answer()
    if callback.from_user.id != get_admin_id():
        return
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Global Process Stats", callback_data="admin_global_process_stats")],
        [InlineKeyboardButton(text="👥 Manage Uploaders", callback_data="admin_uploaders_menu")],
        [InlineKeyboardButton(text="📡 Destination Broadcast Permission Check", callback_data="admin_matrix_dest_perm")],
        [InlineKeyboardButton(text="🤝 Destination Joining Stats", callback_data="admin_matrix_joining")],
        [InlineKeyboardButton(text="🔙 Back to Admin Menu", callback_data="admin_back")]
    ])
    await safe_edit_message(callback.message.bot, callback.message.chat.id, callback.message.message_id, "📈 <b>Admin Matrix Hub</b>\n\nSelect a metric/status category:", reply_markup=kb)


@router.callback_query(F.data == "admin_matrix_dest_perm")
async def admin_matrix_dest_perm(callback: CallbackQuery, bot: Bot):
    await callback.answer("Checking broadcast permissions...")
    if callback.from_user.id != get_admin_id():
        return
    pool = await get_pool()
    async with pool.acquire() as conn:
        master_dest = await conn.fetchval("SELECT value FROM bot_settings WHERE key = 'master_log_chat_id'")
        dests = await conn.fetch(
            "SELECT chat_id, title, is_unmatured, posts_delivered FROM destinations WHERE (chat_id != $1 OR $1 IS NULL) ORDER BY is_unmatured ASC, title ASC",
            master_dest
        )

    if not dests:
        kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Back", callback_data="admin_matrix_hub")]])
        await safe_edit_message(bot, callback.message.chat.id, callback.message.message_id, "📡 <b>Broadcast Permission Check</b>\n\nNo broadcast destinations registered.", reply_markup=kb)
        return

    report = ["📡 <b>Broadcast Destinations Permission Check:</b>\n"]
    for d in dests:
        cat_badge = "⚪ Unmatured" if d["is_unmatured"] else "🟢 Matured"
        status_str = await check_bot_broadcast_permission(bot, d["chat_id"])
        delivered = d["posts_delivered"] or 0
        report.append(f"• <b>[{d['title'] or d['chat_id']}] {{{delivered}}}</b> [{cat_badge}]: {status_str}")

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Re-check Permissions", callback_data="admin_matrix_dest_perm")],
        [InlineKeyboardButton(text="🔙 Back to Matrix Hub", callback_data="admin_matrix_hub")]
    ])
    await safe_edit_message(bot, callback.message.chat.id, callback.message.message_id, "\n".join(report), reply_markup=kb)


@router.callback_query(F.data == "admin_matrix_joining")
async def admin_matrix_joining(callback: CallbackQuery):
    await callback.answer()
    if callback.from_user.id != get_admin_id():
        return
    now = datetime.utcnow()
    day_ago = now - timedelta(days=1)
    week_ago = now - timedelta(days=7)
    month_ago = now - timedelta(days=30)

    pool = await get_pool()
    async with pool.acquire() as conn:
        master_dest = await conn.fetchval("SELECT value FROM bot_settings WHERE key = 'master_log_chat_id'")
        destinations = await conn.fetch(
            "SELECT * FROM destinations WHERE (chat_id != $1 OR $1 IS NULL) ORDER BY is_unmatured ASC, title ASC",
            master_dest
        )
        pending_map = {row["chat_id"]: row["count"] for row in await conn.fetch("SELECT chat_id, COUNT(*) as count FROM join_requests WHERE status='pending' GROUP BY chat_id")}

    report = ["🤝 <b>Destination Joining & Approval Stats:</b>\n"]
    for d in destinations:
        cid = d["chat_id"]
        title = d["title"] or cid
        is_unm = d["is_unmatured"]
        accepting = d["accept_requests"]
        pending = pending_map.get(cid, 0)
        posts_sent = d["posts_delivered"] or 0
        
        cat_tag = "⚪ Unmatured" if is_unm else "🟢 Matured"
        if is_unm:
            status_tag = "🟢 Accepting Joins" if accepting else "⚪ Paused Joining"
        else:
            status_tag = "🟢 Auto-Accept Active"

        d_app = await pool.fetchval("SELECT COUNT(*) FROM join_requests WHERE chat_id=$1 AND status='accepted' AND approved_at >= $2", cid, day_ago)
        w_app = await pool.fetchval("SELECT COUNT(*) FROM join_requests WHERE chat_id=$1 AND status='accepted' AND approved_at >= $2", cid, week_ago)
        m_app = await pool.fetchval("SELECT COUNT(*) FROM join_requests WHERE chat_id=$1 AND status='accepted' AND approved_at >= $2", cid, month_ago)

        report.append(
            f"• <b>[{title}] {{{posts_sent}}}</b> [{cat_tag}] (ID: <code>{cid}</code>)\n"
            f"  ├ Posts Delivered: <code>{posts_sent}</code>\n"
            f"  ├ Status: {status_tag}\n"
            f"  ├ Pending Requests: <code>{pending}</code>\n"
            f"  └ Approved | Day: <code>{d_app}</code> | Week: <code>{w_app}</code> | Month: <code>{m_app}</code>\n"
        )

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Refresh", callback_data="admin_matrix_joining")],
        [InlineKeyboardButton(text="🔙 Back to Matrix Hub", callback_data="admin_matrix_hub")]
    ])
    await safe_edit_message(callback.message.bot, callback.message.chat.id, callback.message.message_id, "\n".join(report), reply_markup=kb)


# ==================== UNIVERSAL BROADCAST WORKER ====================

async def broadcast_worker(bot: Bot, queue_id: int, target_scope: str = "both"):
    admin_chat_id = get_admin_id()
    admin_msg_id: Optional[int] = None
    master_msg_id: Optional[int] = None
    master_dest: Optional[str] = None
    qname = f"Queue_{queue_id}"

    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            await conn.execute("UPDATE queues SET run_count = run_count + 1 WHERE id = $1", queue_id)
            q = await conn.fetchrow(
                "SELECT name, delay_sec, delay_min, delay_max, delay_type, mode, caption_header, caption_footer, replace_link_from, replace_link_target FROM queues WHERE id = $1",
                queue_id
            )
            master_dest = await conn.fetchval("SELECT value FROM bot_settings WHERE key = 'master_log_chat_id'")

            if target_scope == "matured":
                target_dests = await conn.fetch(
                    "SELECT chat_id, title FROM destinations WHERE is_unmatured = FALSE AND (chat_id != $1 OR $1 IS NULL)",
                    master_dest
                )
            elif target_scope == "unmatured":
                target_dests = await conn.fetch(
                    "SELECT chat_id, title FROM destinations WHERE is_unmatured = TRUE AND (chat_id != $1 OR $1 IS NULL)",
                    master_dest
                )
            else:  
                target_dests = await conn.fetch(
                    "SELECT chat_id, title, is_unmatured FROM destinations WHERE (chat_id != $1 OR $1 IS NULL)",
                    master_dest
                )

            items = await conn.fetch(
                "SELECT from_chat_id, message_id, caption FROM posts WHERE queue_id = $1 ORDER BY id ASC",
                queue_id
            )

        if not q or not items:
            return

        if not target_dests:
            no_dest_text = f"⚠️ <b>Broadcast Halted:</b> No broadcast channels found under category <b>{target_scope.upper()}</b>."
            if admin_chat_id:
                await safe_send_message(bot, admin_chat_id, no_dest_text)
            return

        target_chat_ids = [d["chat_id"] for d in target_dests]
        target_titles = [d["title"] or d["chat_id"] for d in target_dests]

        qname = q["name"]
        delay_type = q["delay_type"] or "fixed"
        mode = q["mode"] or "sequence"
        c_header = q["caption_header"]
        c_footer = q["caption_footer"]
        c_from = q["replace_link_from"]
        c_target = q["replace_link_target"]

        dest_display = f"{len(target_chat_ids)} channel(s) ({', '.join(target_titles[:3])}{'...' if len(target_titles) > 3 else ''})"

        items = list(items)
        if mode == "shuffle":
            random.shuffle(items)

        total_posts = len(items)

        live_broadcast_stats[queue_id] = {
            "name": qname,
            "sent": 0,
            "total": total_posts,
            "eta_seconds": 0.0,
            "destination": dest_display,
            "mode": mode,
            "delay_type": delay_type,
            "scope": target_scope.upper()
        }

        init_text = (
            f"🚀 <b>Broadcast Started</b>\n\n"
            f"• <b>Queue:</b> <code>{qname}</code>\n"
            f"• <b>Category Scope:</b> <code>{target_scope.upper()}</code>\n"
            f"• <b>Total Targets:</b> <b>{len(target_chat_ids)} channels</b>\n"
            f"• <b>Total Posts:</b> <code>{total_posts}</code>\n"
            f"• <b>Order:</b> <code>{mode.upper()}</code>\n"
            f"• <b>Delay:</b> <code>{delay_type.upper()}</code>\n"
            f"• <b>Progress:</b> 0 out of {total_posts}\n"
            f"• <b>ETA:</b> Calculating..."
        )

        if admin_chat_id:
            m = await safe_send_message(bot, admin_chat_id, init_text)
            if m:
                admin_msg_id = m.message_id
        if master_dest:
            m = await safe_send_message(bot, master_dest, init_text)
            if m:
                master_msg_id = m.message_id

        sent_count = 0
        for post in items:
            final_caption = apply_caption_rules(post["caption"], c_header, c_footer, c_target, c_from)
            kwargs = {}
            if final_caption is not None:
                kwargs["caption"] = final_caption
                kwargs["parse_mode"] = "HTML"

            for cid in target_chat_ids:
                try:
                    await bot.copy_message(
                        chat_id=cid,
                        from_chat_id=post["from_chat_id"],
                        message_id=post["message_id"],
                        **kwargs
                    )
                    async with pool.acquire() as conn:
                        await conn.execute("UPDATE destinations SET posts_delivered = posts_delivered + 1 WHERE chat_id = $1", cid)
                except Exception:
                    pass

            sent_count += 1

            if delay_type == "random":
                min_d = q["delay_min"] or 5
                max_d = q["delay_max"] or 15
                chosen_delay = random.randint(min(min_d, max_d), max(min_d, max_d))
                avg_delay = (min_d + max_d) / 2
            else:
                chosen_delay = q["delay_sec"] or 5
                avg_delay = chosen_delay

            remaining_posts = total_posts - sent_count
            eta_seconds = remaining_posts * avg_delay
            eta_str = format_eta(eta_seconds)

            live_broadcast_stats[queue_id]["sent"] = sent_count
            live_broadcast_stats[queue_id]["eta_seconds"] = eta_seconds

            if remaining_posts > 0:
                progress_text = (
                    f"🚀 <b>Running Queue: {qname}</b> [{target_scope.upper()}]\n\n"
                    f"• <b>Delivering to:</b> <b>{len(target_chat_ids)} channels</b>\n"
                    f"• <b>Progress:</b> Completed <code>{sent_count}</code> out of <code>{total_posts}</code>\n"
                    f"• <b>ETA Remaining:</b> <code>{eta_str}</code>\n"
                    f"• <b>Mode:</b> {mode.upper()} | <b>Delay:</b> {chosen_delay}s ({delay_type})"
                )

                if admin_chat_id and admin_msg_id:
                    await safe_edit_message(bot, admin_chat_id, admin_msg_id, progress_text)
                if master_dest and master_msg_id:
                    await safe_edit_message(bot, master_dest, master_msg_id, progress_text)

                await asyncio.sleep(chosen_delay)

        final_text = (
            f"🏁 <b>Broadcast Completed Successfully!</b>\n\n"
            f"• <b>Queue:</b> <code>{qname}</code>\n"
            f"• <b>Category Delivered:</b> <code>{target_scope.upper()}</code>\n"
            f"• <b>Channels Reached:</b> <b>{len(target_chat_ids)}</b>\n"
            f"• <b>Total Posts Sent:</b> <code>{sent_count} / {total_posts}</code>\n"
            f"• <b>Status:</b> Completed & Idle"
        )

        if admin_chat_id and admin_msg_id:
            await safe_edit_message(bot, admin_chat_id, admin_msg_id, final_text)
        if master_dest and master_msg_id:
            await safe_edit_message(bot, master_dest, master_msg_id, final_text)

    except asyncio.CancelledError:
        halt_text = f"⏹ <b>Broadcast Stopped:</b> Queue <code>{qname}</code> was halted by admin."
        if admin_chat_id and admin_msg_id:
            await safe_edit_message(bot, admin_chat_id, admin_msg_id, halt_text)
        if master_dest and master_msg_id:
            await safe_edit_message(bot, master_dest, master_msg_id, halt_text)
    finally:
        active_tasks.pop(queue_id, None)
        live_broadcast_stats.pop(queue_id, None)


# ==================== ADVERTISEMENT BROADCAST WORKER ====================

async def ad_broadcast_worker(bot: Bot, target_scope: str = "both"):
    global active_ad_task
    admin_chat_id = get_admin_id()
    admin_msg_id: Optional[int] = None
    master_msg_id: Optional[int] = None

    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            ad_items = await conn.fetch("SELECT from_chat_id, message_id FROM ad_posts ORDER BY id ASC")
            master_dest = await conn.fetchval("SELECT value FROM bot_settings WHERE key = 'master_log_chat_id'")
            min_d_val = await conn.fetchval("SELECT value FROM bot_settings WHERE key = 'ad_delay_min'")
            max_d_val = await conn.fetchval("SELECT value FROM bot_settings WHERE key = 'ad_delay_max'")

            min_d = int(min_d_val) if min_d_val and min_d_val.isdigit() else 5
            max_d = int(max_d_val) if max_d_val and max_d_val.isdigit() else 15

            if target_scope == "matured":
                target_dests = await conn.fetch(
                    "SELECT chat_id, title FROM destinations WHERE is_unmatured = FALSE AND (chat_id != $1 OR $1 IS NULL)",
                    master_dest
                )
            elif target_scope == "unmatured":
                target_dests = await conn.fetch(
                    "SELECT chat_id, title FROM destinations WHERE is_unmatured = TRUE AND (chat_id != $1 OR $1 IS NULL)",
                    master_dest
                )
            else:
                target_dests = await conn.fetch(
                    "SELECT chat_id, title FROM destinations WHERE (chat_id != $1 OR $1 IS NULL)",
                    master_dest
                )

        if not ad_items or not target_dests:
            return

        target_chat_ids = [d["chat_id"] for d in target_dests]
        total_ads = len(ad_items)

        start_text = (
            f"📢 <b>Advertisement Broadcast Started</b>\n\n"
            f"• <b>Scope:</b> <code>{target_scope.upper()}</code>\n"
            f"• <b>Target Channels:</b> <code>{len(target_chat_ids)}</code>\n"
            f"• <b>Total Ads:</b> <code>{total_ads}</code>\n"
            f"• <b>Random Interval:</b> <code>{min_d}s - {max_d}s</code>"
        )
        if admin_chat_id:
            m = await safe_send_message(bot, admin_chat_id, start_text)
            if m:
                admin_msg_id = m.message_id
        if master_dest:
            m = await safe_send_message(bot, master_dest, start_text)
            if m:
                master_msg_id = m.message_id

        sent_count = 0
        for ad in ad_items:
            for cid in target_chat_ids:
                try:
                    await bot.copy_message(chat_id=cid, from_chat_id=ad["from_chat_id"], message_id=ad["message_id"])
                    async with pool.acquire() as conn:
                        await conn.execute("UPDATE destinations SET posts_delivered = posts_delivered + 1 WHERE chat_id = $1", cid)
                except Exception:
                    pass

            sent_count += 1
            chosen_delay = random.randint(min(min_d, max_d), max(min_d, max_d))

            if sent_count < total_ads:
                progress_text = (
                    f"📢 <b>Broadcasting Ads...</b>\n\n"
                    f"• <b>Progress:</b> <code>{sent_count} / {total_ads}</code> sent\n"
                    f"• <b>Scope:</b> {target_scope.upper()} ({len(target_chat_ids)} channels)\n"
                    f"• <b>Next interval:</b> <code>{chosen_delay}s</code>"
                )
                if admin_chat_id and admin_msg_id:
                    await safe_edit_message(bot, admin_chat_id, admin_msg_id, progress_text)
                if master_dest and master_msg_id:
                    await safe_edit_message(bot, master_dest, master_msg_id, progress_text)

                await asyncio.sleep(chosen_delay)

        finish_text = (
            f"🏁 <b>Advertisement Broadcast Completed!</b>\n\n"
            f"• <b>Total Ads Sent:</b> <code>{sent_count}</code>\n"
            f"• <b>Scope Delivered:</b> <code>{target_scope.upper()}</code>"
        )
        if admin_chat_id and admin_msg_id:
            await safe_edit_message(bot, admin_chat_id, admin_msg_id, finish_text)
        if master_dest and master_msg_id:
            await safe_edit_message(bot, master_dest, master_msg_id, finish_text)

    except asyncio.CancelledError:
        pass
    finally:
        active_ad_task = None


# ==================== ADVERTISEMENT HUB UI ====================

@router.callback_query(F.data == "admin_ads_hub")
async def admin_ads_hub(callback: CallbackQuery):
    await callback.answer()
    if callback.from_user.id != get_admin_id():
        return

    pool = await get_pool()
    async with pool.acquire() as conn:
        ad_count = await conn.fetchval("SELECT COUNT(*) FROM ad_posts") or 0
        min_d = await conn.fetchval("SELECT value FROM bot_settings WHERE key = 'ad_delay_min'") or "5"
        max_d = await conn.fetchval("SELECT value FROM bot_settings WHERE key = 'ad_delay_max'") or "15"

    is_running = active_ad_task is not None and not active_ad_task.done()
    status_str = "🟢 Broadcasting Now" if is_running else "⚪ Idle"

    buttons = [
        [InlineKeyboardButton(text="➕ Add Advertisement Post", callback_data="admin_add_ad_post")],
        [InlineKeyboardButton(text=f"⏱ Set Random Delay ({min_d}s - {max_d}s)", callback_data="admin_ad_delay_prompt")],
        [InlineKeyboardButton(
            text="▶️ Broadcast Ads" if not is_running else "⏹ Halt Ad Broadcast",
            callback_data="admin_ad_scope_prompt" if not is_running else "admin_halt_ads"
        )],
        [InlineKeyboardButton(text="🗑 Clear Stored Ads", callback_data="admin_clear_ads")],
        [InlineKeyboardButton(text="🔙 Back to Admin Menu", callback_data="admin_back")]
    ]

    card = (
        "📢 <b>Advertisement Control Hub</b>\n\n"
        f"• <b>Stored Ads:</b> <code>{ad_count}</code>\n"
        f"• <b>Status:</b> {status_str}\n"
        f"• <b>Random Interval Range:</b> <code>{min_d}s - {max_d}s</code>\n\n"
        "Broadcast promotional messages universally without touching Master Log."
    )
    await safe_edit_message(callback.message.bot, callback.message.chat.id, callback.message.message_id, card, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))


@router.callback_query(F.data == "admin_add_ad_post")
async def admin_add_ad_post(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    if callback.from_user.id != get_admin_id():
        return
    await state.set_state(AdminStates.waiting_for_ad_post)
    await safe_edit_message(
        callback.message.bot,
        callback.message.chat.id,
        callback.message.message_id,
        "📢 <b>Add Advertisement Post:</b>\n\n"
        "Send the message (photo, video, album, text, or graphic) to save into the advertisement library.\n\n"
        "<i>Send /cancel to return.</i>"
    )


@router.message(AdminStates.waiting_for_ad_post)
async def admin_save_ad_post(message: Message, state: FSMContext):
    if message.from_user.id != get_admin_id():
        return

    if message.text == "/cancel":
        await state.clear()
        kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Back to Ads Hub", callback_data="admin_ads_hub")]])
        await message.answer("❌ Cancelled.", reply_markup=kb)
        return

    content_hash = get_message_content_hash(message)
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO ad_posts (from_chat_id, message_id, content_hash) VALUES ($1, $2, $3)",
            message.chat.id, message.message_id, content_hash
        )
        total_ads = await conn.fetchval("SELECT COUNT(*) FROM ad_posts")

    await state.clear()
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="➕ Add Another Ad", callback_data="admin_add_ad_post")],
            [InlineKeyboardButton(text="📢 Open Ads Hub", callback_data="admin_ads_hub")]
        ]
    )
    await message.answer(f"✅ Advertisement creative saved! Total ads in library: <code>{total_ads}</code>", parse_mode="HTML", reply_markup=kb)


@router.callback_query(F.data == "admin_ad_delay_prompt")
async def admin_ad_delay_prompt(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    if callback.from_user.id != get_admin_id():
        return
    await state.set_state(AdminStates.waiting_for_ad_delay)
    await safe_edit_message(
        callback.message.bot,
        callback.message.chat.id,
        callback.message.message_id,
        "⏱ <b>Set Random Interval for Ads:</b>\n\nSend minimum and maximum seconds separated by a dash (e.g. <code>10-30</code>):"
    )


@router.message(AdminStates.waiting_for_ad_delay, F.text)
async def admin_ad_delay_save(message: Message, state: FSMContext):
    if message.from_user.id != get_admin_id():
        return
    raw = message.text.strip().replace("-", " ").split()
    if len(raw) != 2 or not raw[0].isdigit() or not raw[1].isdigit():
        await message.answer("⚠️ Invalid format. Example: <code>10-30</code>.")
        return

    min_d, max_d = int(raw[0]), int(raw[1])
    if min_d < 1 or max_d < min_d:
        await message.answer("⚠️ Minimum must be >= 1 and Maximum must be >= Minimum.")
        return

    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("INSERT INTO bot_settings (key, value) VALUES ('ad_delay_min', $1) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value", str(min_d))
        await conn.execute("INSERT INTO bot_settings (key, value) VALUES ('ad_delay_max', $1) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value", str(max_d))

    await state.clear()
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Back to Ads Hub", callback_data="admin_ads_hub")]])
    await message.answer(f"✅ Ad interval set to <code>{min_d}s - {max_d}s</code>.", parse_mode="HTML", reply_markup=kb)


@router.callback_query(F.data == "admin_ad_scope_prompt")
async def admin_ad_scope_prompt(callback: CallbackQuery):
    await callback.answer()
    if callback.from_user.id != get_admin_id():
        return

    pool = await get_pool()
    async with pool.acquire() as conn:
        ad_count = await conn.fetchval("SELECT COUNT(*) FROM ad_posts") or 0
        master_dest = await conn.fetchval("SELECT value FROM bot_settings WHERE key = 'master_log_chat_id'")
        m_count = await conn.fetchval("SELECT COUNT(*) FROM destinations WHERE is_unmatured = FALSE AND (chat_id != $1 OR $1 IS NULL)", master_dest) or 0
        u_count = await conn.fetchval("SELECT COUNT(*) FROM destinations WHERE is_unmatured = TRUE AND (chat_id != $1 OR $1 IS NULL)", master_dest) or 0

    if ad_count == 0:
        await callback.answer("⚠️ No advertisement posts saved yet! Add one first.", show_alert=True)
        return

    buttons = [
        [InlineKeyboardButton(text=f"🟢 Matured Channels ({m_count})", callback_data="run_ad_broadcast:matured")],
        [InlineKeyboardButton(text=f"⚪ Unmatured Channels ({u_count})", callback_data="run_ad_broadcast:unmatured")],
        [InlineKeyboardButton(text=f"🌐 Both Matured & Unmatured ({m_count + u_count})", callback_data="run_ad_broadcast:both")],
        [InlineKeyboardButton(text="🔙 Back", callback_data="admin_ads_hub")]
    ]
    await safe_edit_message(
        callback.message.bot,
        callback.message.chat.id,
        callback.message.message_id,
        "📢 <b>Choose Target Scope for Ad Broadcast:</b>\n\n(Master Log is excluded automatically)",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons)
    )


@router.callback_query(F.data.startswith("run_ad_broadcast:"))
async def admin_run_ad_broadcast(callback: CallbackQuery, bot: Bot):
    global active_ad_task
    await callback.answer()
    if callback.from_user.id != get_admin_id():
        return
    scope = callback.data.split(":")[1]
    active_ad_task = asyncio.create_task(ad_broadcast_worker(bot, target_scope=scope))
    await callback.answer(f"🚀 Ad broadcast launched on {scope.upper()} channels!", show_alert=True)
    await admin_ads_hub(callback)


@router.callback_query(F.data == "admin_halt_ads")
async def admin_halt_ads(callback: CallbackQuery):
    global active_ad_task
    await callback.answer()
    if callback.from_user.id != get_admin_id():
        return
    if active_ad_task and not active_ad_task.done():
        active_ad_task.cancel()
        active_ad_task = None
        await callback.answer("Ad broadcast halted.", show_alert=True)
    await admin_ads_hub(callback)


@router.callback_query(F.data == "admin_clear_ads")
async def admin_clear_ads(callback: CallbackQuery):
    await callback.answer("Purged ads")
    if callback.from_user.id != get_admin_id():
        return
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM ad_posts")
    await callback.answer("All advertisement creatives cleared.", show_alert=True)
    await admin_ads_hub(callback)


# ==================== CAPTION MODIFIER HUB (QUEUE-WISE) ====================

@router.callback_query(F.data == "admin_caption_hub")
async def admin_caption_hub(callback: CallbackQuery):
    await callback.answer()
    if callback.from_user.id != get_admin_id():
        return

    pool = await get_pool()
    async with pool.acquire() as conn:
        queues = await conn.fetch("SELECT id, name FROM queues ORDER BY id ASC")

    if not queues:
        kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Back", callback_data="admin_back")]])
        await safe_edit_message(callback.message.bot, callback.message.chat.id, callback.message.message_id, "⚠️ No queues created yet.", reply_markup=kb)
        return

    buttons = [
        [InlineKeyboardButton(text=f"📁 Edit Caption: {q['name']}", callback_data=f"q_caption_edit:{q['id']}")]
        for q in queues
    ]
    buttons.append([InlineKeyboardButton(text="🔙 Back to Admin Menu", callback_data="admin_back")])

    text = (
        "✏️ <b>Queue Caption Editor Hub</b>\n\n"
        "Select a queue to configure automated <b>Header</b>, <b>Footer</b>, or <b>Telegram Link Replacement</b>:"
    )
    await safe_edit_message(callback.message.bot, callback.message.chat.id, callback.message.message_id, text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))


async def render_queue_caption_screen(callback: CallbackQuery, queue_id: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        q = await conn.fetchrow(
            "SELECT name, caption_header, caption_footer, replace_link_from, replace_link_target FROM queues WHERE id = $1",
            queue_id
        )

    if not q:
        await callback.answer("Queue not found.", show_alert=True)
        return

    h = q["caption_header"] or "<i>None</i>"
    f = q["caption_footer"] or "<i>None</i>"
    target = q["replace_link_target"] or "<i>None</i>"
    source = q["replace_link_from"] or "<i>All Telegram Links (*)</i>"

    buttons = [
        [InlineKeyboardButton(text="🏷 Set Header", callback_data=f"set_q_header:{queue_id}")],
        [InlineKeyboardButton(text="📝 Set Footer", callback_data=f"set_q_footer:{queue_id}")],
        [InlineKeyboardButton(text="🔗 Set Link Replacement", callback_data=f"set_q_link:{queue_id}")],
        [InlineKeyboardButton(text="👁 Check Real-Life Sample", callback_data=f"preview_q_sample:{queue_id}")],
        [InlineKeyboardButton(text="🗑 Reset Caption Rules", callback_data=f"reset_q_caption:{queue_id}")],
        [InlineKeyboardButton(text="🔙 Back to Queues", callback_data="admin_caption_hub")]
    ]

    card = (
        f"✏️ <b>Caption Rules for:</b> <code>{q['name']}</code>\n\n"
        f"• <b>Header (Prepended):</b>\n{h}\n\n"
        f"• <b>Footer (Appended):</b>\n{f}\n\n"
        f"• <b>Target To Find:</b>\n{source}\n\n"
        f"• <b>Replace With:</b>\n{target}\n\n"
        "All broadcasts originating from this queue will automatically adopt these formatting rules."
    )
    await safe_edit_message(callback.message.bot, callback.message.chat.id, callback.message.message_id, card, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))


@router.callback_query(F.data.startswith("q_caption_edit:"))
async def admin_q_caption_edit(callback: CallbackQuery):
    await callback.answer()
    if callback.from_user.id != get_admin_id():
        return
    queue_id = int(callback.data.split(":")[1])
    await render_queue_caption_screen(callback, queue_id)


@router.callback_query(F.data.startswith("set_q_header:"))
async def admin_set_q_header_prompt(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    if callback.from_user.id != get_admin_id():
        return
    queue_id = int(callback.data.split(":")[1])
    await state.update_data(active_caption_qid=queue_id)
    await state.set_state(AdminStates.waiting_for_caption_header)
    await safe_edit_message(
        callback.message.bot,
        callback.message.chat.id,
        callback.message.message_id,
        "🏷 <b>Send Caption Header text:</b>\n\n(Added at the top of every post. Send <code>/clear</code> to remove):"
    )


@router.message(AdminStates.waiting_for_caption_header, F.text)
async def admin_save_q_header(message: Message, state: FSMContext):
    if message.from_user.id != get_admin_id():
        return
    data = await state.get_data()
    queue_id = data["active_caption_qid"]
    val = "" if message.text.strip() == "/clear" else message.text.strip()

    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("UPDATE queues SET caption_header = $1 WHERE id = $2", val, queue_id)

    await state.clear()
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👁 Check Sample", callback_data=f"preview_q_sample:{queue_id}")],
        [InlineKeyboardButton(text="🔙 Back to Caption Rules", callback_data=f"q_caption_edit:{queue_id}")]
    ])
    await message.answer("✅ Header updated successfully.", reply_markup=kb)


@router.callback_query(F.data.startswith("set_q_footer:"))
async def admin_set_q_footer_prompt(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    if callback.from_user.id != get_admin_id():
        return
    queue_id = int(callback.data.split(":")[1])
    await state.update_data(active_caption_qid=queue_id)
    await state.set_state(AdminStates.waiting_for_caption_footer)
    await safe_edit_message(
        callback.message.bot,
        callback.message.chat.id,
        callback.message.message_id,
        "📝 <b>Send Caption Footer text:</b>\n\n(Appended at the end of every post. Send <code>/clear</code> to remove):"
    )


@router.message(AdminStates.waiting_for_caption_footer, F.text)
async def admin_save_q_footer(message: Message, state: FSMContext):
    if message.from_user.id != get_admin_id():
        return
    data = await state.get_data()
    queue_id = data["active_caption_qid"]
    val = "" if message.text.strip() == "/clear" else message.text.strip()

    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("UPDATE queues SET caption_footer = $1 WHERE id = $2", val, queue_id)

    await state.clear()
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👁 Check Sample", callback_data=f"preview_q_sample:{queue_id}")],
        [InlineKeyboardButton(text="🔙 Back to Caption Rules", callback_data=f"q_caption_edit:{queue_id}")]
    ])
    await message.answer("✅ Footer updated successfully.", reply_markup=kb)


@router.callback_query(F.data.startswith("set_q_link:"))
async def admin_set_q_link_prompt(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    if callback.from_user.id != get_admin_id():
        return
    queue_id = int(callback.data.split(":")[1])
    await state.update_data(active_caption_qid=queue_id)
    await state.set_state(AdminStates.waiting_for_caption_link)
    
    prompt_text = (
        "🔗 <b>Set Link / Text Replacement</b>\n\n"
        "You can specify both the link/text to find and what to replace it with, OR provide just the new link to replace all Telegram links.\n\n"
        "<b>Option 1: Specific Link/Text Replacement</b>\n"
        "Format: <code>old_link -> new_link</code>\n"
        "Example: <code>https://t.me/+xyzzz -> https://t.me/MyTargetChannel</code>\n\n"
        "<b>Option 2: Replace All Telegram Links</b>\n"
        "Send just the new link:\n"
        "Example: <code>https://t.me/MyTargetChannel</code>\n\n"
        "<i>Send <code>/clear</code> to disable link replacement.</i>"
    )
    await safe_edit_message(
        callback.message.bot,
        callback.message.chat.id,
        callback.message.message_id,
        prompt_text
    )


@router.message(AdminStates.waiting_for_caption_link, F.text)
async def admin_save_q_link(message: Message, state: FSMContext):
    if message.from_user.id != get_admin_id():
        return
    data = await state.get_data()
    queue_id = data["active_caption_qid"]
    raw = message.text.strip()

    if raw == "/clear":
        from_target = ""
        to_target = ""
    else:
        sep = None
        for s in ["->", "=>", "|", "\n"]:
            if s in raw:
                sep = s
                break
        if sep:
            parts = raw.split(sep, 1)
            from_target = parts[0].strip()
            to_target = parts[1].strip()
        else:
            from_target = ""
            to_target = raw

    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE queues SET replace_link_from = $1, replace_link_target = $2 WHERE id = $3",
            from_target, to_target, queue_id
        )

    await state.clear()
    
    preview_kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👁 Check Real-Life Sample", callback_data=f"preview_q_sample:{queue_id}")],
        [InlineKeyboardButton(text="⚙️ Caption Settings", callback_data=f"q_caption_edit:{queue_id}")],
        [InlineKeyboardButton(text="🚀 Back to Queue", callback_data=f"run_hub_q:{queue_id}")]
    ])

    if not to_target:
        await message.answer("✅ Link replacement disabled.", reply_markup=preview_kb)
    else:
        match_desc = f"<code>{from_target}</code>" if from_target else "<i>All Telegram links (*.t.me/...)</i>"
        await message.answer(
            f"✅ <b>Link Replacement Configured Successfully!</b>\n\n"
            f"• <b>Target To Find:</b> {match_desc}\n"
            f"• <b>Replace With:</b> <code>{to_target}</code>\n\n"
            "Tap below to verify with a live real-life sample preview:",
            parse_mode="HTML",
            reply_markup=preview_kb
        )


# ==================== REAL-LIFE SAMPLE PREVIEW ====================

@router.callback_query(F.data.startswith("preview_q_sample:"))
async def admin_preview_q_sample(callback: CallbackQuery):
    await callback.answer()
    if callback.from_user.id != get_admin_id():
        return
    queue_id = int(callback.data.split(":")[1])

    pool = await get_pool()
    async with pool.acquire() as conn:
        q = await conn.fetchrow(
            "SELECT name, caption_header, caption_footer, replace_link_from, replace_link_target FROM queues WHERE id = $1",
            queue_id
        )
        sample_post = await conn.fetchrow(
            "SELECT caption FROM posts WHERE queue_id = $1 AND caption IS NOT NULL AND caption != '' ORDER BY id DESC LIMIT 1",
            queue_id
        )

    if not q:
        await callback.answer("Queue not found.", show_alert=True)
        return

    header = q["caption_header"]
    footer = q["caption_footer"]
    target = q["replace_link_target"]
    source = q["replace_link_from"]

    if sample_post and sample_post["caption"]:
        orig_caption = sample_post["caption"]
        is_real = True
    else:
        test_link = source if source else "https://t.me/+xyzzz"
        orig_caption = (
            f"🔥 Exclusive Daily News & Updates!\n\n"
            f"Join our private chat room right now: {test_link}\n"
            f"Contact admin support for help."
        )
        is_real = False

    transformed_caption = apply_caption_rules(orig_caption, header, footer, target, source)

    buttons = [
        [InlineKeyboardButton(text="🔄 Refresh Sample", callback_data=f"preview_q_sample:{queue_id}")],
        [InlineKeyboardButton(text="✏️ Edit Caption Rules", callback_data=f"q_caption_edit:{queue_id}")],
        [InlineKeyboardButton(text="🔙 Back to Queue Scheduler", callback_data=f"run_hub_q:{queue_id}")]
    ]

    preview_text = (
        f"👁 <b>Real-Life Caption Sample Preview</b>\n"
        f"• <b>Queue:</b> <code>{q['name']}</code>\n"
        f"• <b>Data Source:</b> {'<i>Stored post from database</i>' if is_real else '<i>Simulated post</i>'}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"<b>📥 ORIGINAL CAPTION:</b>\n"
        f"{orig_caption}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"<b>📤 TRANSFORMED OUTPUT:</b>\n"
        f"{transformed_caption or '<i>(No text/caption generated)</i>'}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"• <b>Target To Find:</b> <code>{source or 'All Telegram links (*)'}</code>\n"
        f"• <b>Replace With:</b> <code>{target or 'None'}</code>"
    )

    await safe_edit_message(
        callback.message.bot,
        callback.message.chat.id,
        callback.message.message_id,
        preview_text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons)
    )


@router.callback_query(F.data.startswith("reset_q_caption:"))
async def admin_reset_q_caption(callback: CallbackQuery):
    await callback.answer("Resetting...")
    if callback.from_user.id != get_admin_id():
        return
    queue_id = int(callback.data.split(":")[1])

    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE queues SET caption_header = '', caption_footer = '', replace_link_from = '', replace_link_target = '' WHERE id = $1",
            queue_id
        )

    await render_queue_caption_screen(callback, queue_id)


# ==================== GLOBAL PROCESS STATS DASHBOARD ====================

@router.callback_query(F.data == "admin_global_process_stats")
async def admin_global_process_stats(callback: CallbackQuery):
    await callback.answer("Loading global stats...")
    if callback.from_user.id != get_admin_id():
        return

    pool = await get_pool()
    async with pool.acquire() as conn:
        master_log_id = await conn.fetchval("SELECT value FROM bot_settings WHERE key = 'master_log_chat_id'")
        all_queues = await conn.fetch("SELECT id, name, delay_sec, mode, run_count FROM queues ORDER BY id ASC")
        destinations = await conn.fetch(
            "SELECT chat_id, title, is_unmatured, accept_requests, join_delay_min, join_delay_max, posts_delivered, total_accepted FROM destinations WHERE (chat_id != $1 OR $1 IS NULL) ORDER BY is_unmatured ASC, title ASC",
            master_log_id
        )
        queue_post_counts = await conn.fetch("SELECT queue_id, COUNT(*) as count FROM posts GROUP BY queue_id")
        join_pending_counts = await conn.fetch("SELECT chat_id, COUNT(*) as count FROM join_requests WHERE status = 'pending' GROUP BY chat_id")

    posts_map = {row["queue_id"]: row["count"] for row in queue_post_counts}
    join_pending_map = {row["chat_id"]: row["count"] for row in join_pending_counts}

    report = ["📊 <b>GLOBAL PROCESS & PERFORMANCE DASHBOARD</b>\n"]

    report.append(f"📋 <b>Master Log Channel (Audit / Notifications Only):</b>\n• <code>{master_log_id or 'Not Configured'}</code>\n")

    report.append("🚀 <b>Active Broadcast Queues:</b>")
    active_broadcast_count = 0

    for qid, task in list(active_tasks.items()):
        if not task.done() and qid in live_broadcast_stats:
            active_broadcast_count += 1
            info = live_broadcast_stats[qid]
            eta_str = format_eta(info["eta_seconds"])
            scope_str = info.get("scope", "BOTH")
            report.append(
                f"• <b>{info['name']}</b> [{scope_str}]\n"
                f"  ├ Progress: <code>{info['sent']} / {info['total']}</code> posts\n"
                f"  ├ Targets: <code>{info['destination']}</code>\n"
                f"  └ ⏳ <b>ETA:</b> <code>{eta_str}</code>\n"
            )

    if active_broadcast_count == 0:
        report.append("<i>No broadcast queues are currently running.</i>\n")

    report.append("🟢 <b>Matured Channels (Auto-Accepting Joins):</b>")
    matured_dests = [d for d in destinations if not d["is_unmatured"]]
    if matured_dests:
        for d in matured_dests:
            cid = d["chat_id"]
            title = d["title"] or cid
            pending = join_pending_map.get(cid, 0)
            accepted = d["total_accepted"] or 0
            delivered = d["posts_delivered"] or 0
            report.append(
                f"• <b>[{title}] {{{delivered}}}</b> (<code>{cid}</code>)\n"
                f"  ├ Posts Delivered: <code>{delivered}</code> | Approved Joins: <code>{accepted}</code>\n"
                f"  └ Pending Joins: <code>{pending}</code> ({d['join_delay_min']}s-{d['join_delay_max']}s delay)\n"
            )
    else:
        report.append("<i>No Matured channels connected.</i>\n")

    report.append("⚪ <b>Unmatured Channels:</b>")
    unmatured_dests = [d for d in destinations if d["is_unmatured"]]
    if unmatured_dests:
        for d in unmatured_dests:
            cid = d["chat_id"]
            title = d["title"] or cid
            accepting = d["accept_requests"]
            pending = join_pending_map.get(cid, 0)
            accepted = d["total_accepted"] or 0
            delivered = d["posts_delivered"] or 0
            status_badge = "🟢 Accepting Joins" if accepting else "⚪ Paused"
            report.append(
                f"• <b>[{title}] {{{delivered}}}</b> — {status_badge}\n"
                f"  ├ Posts Delivered: <code>{delivered}</code>\n"
                f"  ├ Requests: <code>{accepted}</code> approved, <code>{pending}</code> pending\n"
                f"  └ Random Interval: <code>{d['join_delay_min']}s - {d['join_delay_max']}s</code>\n"
            )
    else:
        report.append("<i>No Unmatured channels connected.</i>\n")

    report.append("📁 <b>Queues:</b>")
    if all_queues:
        for q in all_queues:
            qid = q["id"]
            qname = q["name"]
            curr_posts = posts_map.get(qid, 0)
            runs = q["run_count"] or 0
            report.append(
                f"• <b>{qname}</b>: <code>{curr_posts}</code> stored posts | <code>{runs}</code> runs"
            )
    else:
        report.append("<i>No queues created yet.</i>")

    buttons = [
        [InlineKeyboardButton(text="🔄 Refresh Live Stats", callback_data="admin_global_process_stats")],
        [InlineKeyboardButton(text="🚀 Running Queues Hub", callback_data="admin_running_queues")],
        [InlineKeyboardButton(text="🔙 Back to Matrix Hub", callback_data="admin_matrix_hub")]
    ]

    await safe_edit_message(callback.message.bot, callback.message.chat.id, callback.message.message_id, "\n".join(report), reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))


# ==================== RUNNING QUEUE HUB ====================

async def render_run_hub_detail(callback: CallbackQuery, queue_id: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        q = await conn.fetchrow("SELECT * FROM queues WHERE id = $1", queue_id)
        post_count = await conn.fetchval("SELECT COUNT(*) FROM posts WHERE queue_id = $1", queue_id)
        master_dest = await conn.fetchval("SELECT value FROM bot_settings WHERE key = 'master_log_chat_id'")
        m_count = await conn.fetchval("SELECT COUNT(*) FROM destinations WHERE is_unmatured = FALSE AND (chat_id != $1 OR $1 IS NULL)", master_dest) or 0
        u_count = await conn.fetchval("SELECT COUNT(*) FROM destinations WHERE is_unmatured = TRUE AND (chat_id != $1 OR $1 IS NULL)", master_dest) or 0

    if not q:
        await callback.answer("Queue not found.", show_alert=True)
        return

    is_running = queue_id in active_tasks and not active_tasks[queue_id].done()
    status_str = "🟢 Active (Broadcasting)" if is_running else "⚪ Idle (Ready)"
    mode = q["mode"] or "sequence"
    delay_type = q["delay_type"] or "fixed"

    if delay_type == "random":
        delay_str = f"Random ({q['delay_min']}s - {q['delay_max']}s)"
    else:
        delay_str = f"Fixed ({q['delay_sec']}s)"

    toggle_mode = "sequence" if mode == "shuffle" else "shuffle"

    buttons = [
        [
            InlineKeyboardButton(text=f"🔀 Mode: {mode.upper()}", callback_data=f"set_mode_hub:{queue_id}:{toggle_mode}"),
            InlineKeyboardButton(text=f"⏱ {delay_str}", callback_data=f"open_delay_menu:{queue_id}")
        ],
        [InlineKeyboardButton(text="✏️ Configure Caption Rules", callback_data=f"q_caption_edit:{queue_id}")],
        [
            InlineKeyboardButton(
                text="▶️ Start Sending Queue" if not is_running else "⏹ Halt Running Queue",
                callback_data=f"prompt_run_scope:{queue_id}" if not is_running else f"toggle_run_hub:{queue_id}:stop"
            )
        ],
        [InlineKeyboardButton(text="🔙 Back to Running Hub", callback_data="admin_running_queues")]
    ]

    detail_card = (
        f"🚀 <b>Queue Scheduler:</b> <code>{q['name']}</code>\n\n"
        f"• <b>Status:</b> {status_str}\n"
        f"• <b>Stored Posts:</b> <code>{post_count}</code>\n"
        f"• <b>Playback Order:</b> <code>{mode.capitalize()}</code>\n"
        f"• <b>Delay Profile:</b> <code>{delay_str}</code>\n"
        f"• <b>Lifetime Launches:</b> <code>{q['run_count']}</code>\n\n"
        f"💡 <b>Broadcast Targets (Master Log is excluded):</b>\n"
        f"• 🟢 All Matured Channels: <code>{m_count}</code>\n"
        f"• ⚪ All Unmatured Channels: <code>{u_count}</code>\n\n"
        "Tap <b>Start Sending Queue</b> to broadcast."
    )
    await safe_edit_message(callback.message.bot, callback.message.chat.id, callback.message.message_id, detail_card, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))


@router.callback_query(F.data == "admin_running_queues")
async def admin_running_queues(callback: CallbackQuery):
    await callback.answer()
    if callback.from_user.id != get_admin_id():
        return

    pool = await get_pool()
    async with pool.acquire() as conn:
        queues = await conn.fetch("SELECT id, name, run_count FROM queues ORDER BY id ASC")

    if not queues:
        kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Back", callback_data="admin_manage_hub")]])
        await safe_edit_message(callback.message.bot, callback.message.chat.id, callback.message.message_id, "🚀 <b>Running Queues Hub</b>\n\nNo queues created yet.", reply_markup=kb)
        return

    buttons = []
    text_lines = ["🚀 <b>Running Queues Control Hub:</b>\n"]

    for q in queues:
        qid = q["id"]
        qname = q["name"]
        is_running = qid in active_tasks and not active_tasks[qid].done()
        status_symbol = "🟢 Running" if is_running else "⚪ Idle"
        text_lines.append(f"• <b>{qname}</b> — {status_symbol} (Runs: {q['run_count']})")
        buttons.append([InlineKeyboardButton(text=f"⚙️ Schedule/Control: {qname} ({status_symbol})", callback_data=f"run_hub_q:{qid}")])

    buttons.append([InlineKeyboardButton(text="🔙 Back to Manage Hub", callback_data="admin_manage_hub")])
    await safe_edit_message(callback.message.bot, callback.message.chat.id, callback.message.message_id, "\n".join(text_lines), reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))


@router.callback_query(F.data.startswith("run_hub_q:"))
async def admin_run_hub_detail(callback: CallbackQuery):
    await callback.answer()
    if callback.from_user.id != get_admin_id():
        return
    queue_id = int(callback.data.split(":")[1])
    await render_run_hub_detail(callback, queue_id)


@router.callback_query(F.data.startswith("set_mode_hub:"))
async def admin_set_mode_hub(callback: CallbackQuery):
    await callback.answer("Mode updated")
    if callback.from_user.id != get_admin_id():
        return
    _, qid_str, new_mode = callback.data.split(":")
    queue_id = int(qid_str)

    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("UPDATE queues SET mode = $1 WHERE id = $2", new_mode, queue_id)

    await render_run_hub_detail(callback, queue_id)


# ==================== PROMPT BROADCAST DESTINATION SCOPE ====================

@router.callback_query(F.data.startswith("prompt_run_scope:"))
async def admin_prompt_run_scope(callback: CallbackQuery):
    await callback.answer()
    if callback.from_user.id != get_admin_id():
        return
    queue_id = int(callback.data.split(":")[1])

    pool = await get_pool()
    async with pool.acquire() as conn:
        q = await conn.fetchrow("SELECT name FROM queues WHERE id = $1", queue_id)
        post_count = await conn.fetchval("SELECT COUNT(*) FROM posts WHERE queue_id = $1", queue_id)
        master_dest = await conn.fetchval("SELECT value FROM bot_settings WHERE key = 'master_log_chat_id'")
        m_count = await conn.fetchval("SELECT COUNT(*) FROM destinations WHERE is_unmatured = FALSE AND (chat_id != $1 OR $1 IS NULL)", master_dest) or 0
        u_count = await conn.fetchval("SELECT COUNT(*) FROM destinations WHERE is_unmatured = TRUE AND (chat_id != $1 OR $1 IS NULL)", master_dest) or 0

    if post_count == 0:
        await callback.answer("⚠️ Queue is empty! No posts to broadcast.", show_alert=True)
        return
    if (m_count + u_count) == 0:
        await callback.answer("⚠️ No broadcast destinations registered! Add one with /adddestination.", show_alert=True)
        return

    buttons = [
        [InlineKeyboardButton(text=f"🟢 All Matured Channels ({m_count})", callback_data=f"toggle_run_hub:{queue_id}:matured")],
        [InlineKeyboardButton(text=f"⚪ All Unmatured Channels ({u_count})", callback_data=f"toggle_run_hub:{queue_id}:unmatured")],
        [InlineKeyboardButton(text=f"🌐 All Channels (Both: {m_count + u_count})", callback_data=f"toggle_run_hub:{queue_id}:both")],
        [InlineKeyboardButton(text="🔙 Cancel", callback_data=f"run_hub_q:{queue_id}")]
    ]

    prompt_text = (
        f"🚀 <b>Select Broadcast Targets for {q['name']}:</b>\n\n"
        "Posts will deliver to the selected channel category. The Master Log destination will only receive audit updates.\n\n"
        f"• 🟢 <b>Matured Channels:</b> <code>{m_count}</code>\n"
        f"• ⚪ <b>Unmatured Channels:</b> <code>{u_count}</code>\n\n"
        "Choose target scope:"
    )
    await safe_edit_message(callback.message.bot, callback.message.chat.id, callback.message.message_id, prompt_text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))


@router.callback_query(F.data.startswith("toggle_run_hub:"))
async def admin_toggle_run_hub(callback: CallbackQuery, bot: Bot):
    await callback.answer()
    if callback.from_user.id != get_admin_id():
        return
    parts = callback.data.split(":")
    queue_id = int(parts[1])
    action_or_scope = parts[2] if len(parts) > 2 else "both"

    if action_or_scope == "stop":
        if queue_id in active_tasks and not active_tasks[queue_id].done():
            active_tasks[queue_id].cancel()
            del active_tasks[queue_id]
            live_broadcast_stats.pop(queue_id, None)
            await callback.answer("Broadcast halted.", show_alert=True)
    else:
        task = asyncio.create_task(broadcast_worker(bot, queue_id, target_scope=action_or_scope))
        active_tasks[queue_id] = task
        await callback.answer(f"🚀 Broadcast started ({action_or_scope.upper()})!", show_alert=True)

    await render_run_hub_detail(callback, queue_id)


# ==================== DELAY CONFIGURATION ====================

@router.callback_query(F.data.startswith("open_delay_menu:"))
async def admin_open_delay_menu(callback: CallbackQuery):
    await callback.answer()
    if callback.from_user.id != get_admin_id():
        return
    queue_id = int(callback.data.split(":")[1])

    buttons = [
        [InlineKeyboardButton(text="⏱ Set Fixed Delay (e.g. 15s)", callback_data=f"set_fixed_prompt:{queue_id}")],
        [InlineKeyboardButton(text="🎲 Set Random Delay Range (e.g. 10s - 30s)", callback_data=f"set_random_prompt:{queue_id}")],
        [InlineKeyboardButton(text="🔙 Back", callback_data=f"run_hub_q:{queue_id}")]
    ]
    await safe_edit_message(callback.message.bot, callback.message.chat.id, callback.message.message_id, "⏱ <b>Select Delay Configuration:</b>\n\n• <b>Fixed Delay:</b> Constant interval between posts.\n• <b>Random Delay Range:</b> Picks random interval between min and max seconds.", reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))


@router.callback_query(F.data.startswith("set_fixed_prompt:"))
async def admin_set_fixed_prompt(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    if callback.from_user.id != get_admin_id():
        return
    queue_id = int(callback.data.split(":")[1])
    await state.update_data(current_queue_id=queue_id)
    await state.set_state(AdminStates.waiting_for_fixed_delay)
    await safe_edit_message(callback.message.bot, callback.message.chat.id, callback.message.message_id, "⏱ <b>Set Fixed Delay:</b>\n\nSend delay in seconds (e.g. <code>15</code>):")


@router.message(AdminStates.waiting_for_fixed_delay, F.text)
async def admin_set_fixed_save(message: Message, state: FSMContext):
    if message.from_user.id != get_admin_id():
        return
    val = message.text.strip()
    if not val.isdigit() or int(val) < 1:
        await message.answer("⚠️ Please enter a valid number of seconds (1 or higher).")
        return

    delay = int(val)
    data = await state.get_data()
    queue_id = data["current_queue_id"]

    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE queues SET delay_sec = $1, delay_type = 'fixed' WHERE id = $2",
            delay, queue_id
        )

    await state.clear()
    kb = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="🔙 Back to Queue Scheduler", callback_data=f"run_hub_q:{queue_id}")]]
    )
    await message.answer(f"✅ Fixed delay updated to <code>{delay}</code> seconds.", parse_mode="HTML", reply_markup=kb)


@router.callback_query(F.data.startswith("set_random_prompt:"))
async def admin_set_random_prompt(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    if callback.from_user.id != get_admin_id():
        return
    queue_id = int(callback.data.split(":")[1])
    await state.update_data(current_queue_id=queue_id)
    await state.set_state(AdminStates.waiting_for_random_delay)
    await safe_edit_message(callback.message.bot, callback.message.chat.id, callback.message.message_id, "🎲 <b>Set Random Delay Range:</b>\n\nSend minimum and maximum seconds separated by dash/space (e.g. <code>10-30</code>):")


@router.message(AdminStates.waiting_for_random_delay, F.text)
async def admin_set_random_save(message: Message, state: FSMContext):
    if message.from_user.id != get_admin_id():
        return
    raw = message.text.strip().replace("-", " ").split()
    if len(raw) != 2 or not raw[0].isdigit() or not raw[1].isdigit():
        await message.answer("⚠️ Invalid format. Example: <code>10-30</code>.")
        return

    min_d = int(raw[0])
    max_d = int(raw[1])
    if min_d < 1 or max_d < min_d:
        await message.answer("⚠️ Minimum must be at least 1 and Maximum must be >= Minimum.")
        return

    data = await state.get_data()
    queue_id = data["current_queue_id"]

    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE queues SET delay_min = $1, delay_max = $2, delay_type = 'random' WHERE id = $3",
            min_d, max_d, queue_id
        )

    await state.clear()
    kb = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="🔙 Back to Queue Scheduler", callback_data=f"run_hub_q:{queue_id}")]]
    )
    await message.answer(f"✅ Random delay range set to <code>{min_d}s - {max_d}s</code>.", parse_mode="HTML", reply_markup=kb)


# ==================== SEPARATED AVAILABLE DESTINATIONS DASHBOARD ====================

@router.callback_query(F.data == "admin_view_destinations")
async def admin_view_destinations(callback: CallbackQuery):
    await callback.answer()
    if callback.from_user.id != get_admin_id():
        return

    pool = await get_pool()
    async with pool.acquire() as conn:
        master_dest = await conn.fetchval("SELECT value FROM bot_settings WHERE key = 'master_log_chat_id'")
        matured_count = await conn.fetchval(
            "SELECT COUNT(*) FROM destinations WHERE is_unmatured = FALSE AND (chat_id != $1 OR $1 IS NULL)", master_dest
        ) or 0
        unmatured_count = await conn.fetchval(
            "SELECT COUNT(*) FROM destinations WHERE is_unmatured = TRUE AND (chat_id != $1 OR $1 IS NULL)", master_dest
        ) or 0

    text = (
        "📡 <b>Available Destinations Hub</b>\n\n"
        "Broadcast destinations are strictly divided into two categories:\n\n"
        f"• 🟢 <b>Matured Channels ({matured_count}):</b>\n"
        "  Target channels where join requests are automatically approved with a safe random delay.\n\n"
        f"• ⚪ <b>Unmatured Channels ({unmatured_count}):</b>\n"
        "  Growth channels where join requests remain paused until activated.\n\n"
        "<i>Note: The Master Log is entirely separate and never receives broadcasts.</i>"
    )

    buttons = [
        [InlineKeyboardButton(text="➕ Add Destination (Command / Manual)", callback_data="admin_add_dest_prompt")],
        [InlineKeyboardButton(text=f"🟢 Matured Channels ({matured_count})", callback_data="admin_dest_list:matured")],
        [InlineKeyboardButton(text=f"⚪ Unmatured Channels ({unmatured_count})", callback_data="admin_dest_list:unmatured")],
        [InlineKeyboardButton(text="🔙 Back to Admin Menu", callback_data="admin_back")]
    ]
    await safe_edit_message(callback.message.bot, callback.message.chat.id, callback.message.message_id, text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))


@router.callback_query(F.data == "admin_add_dest_prompt")
async def admin_add_dest_prompt(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    if callback.from_user.id != get_admin_id():
        return

    await state.set_state(AdminStates.waiting_for_destination_manual)
    text = (
        "➕ <b>Add Destination Channel:</b>\n\n"
        "You can register a channel in two ways:\n\n"
        "<b>1. Use Command directly:</b>\n"
        "<code>/adddestination [Channel Name] [Numerical ID]</code>\n"
        "<i>Example:</i> <code>/adddestination VIP Channel -1001234567890</code>\n\n"
        "<b>2. Or reply here now:</b>\n"
        "Send the <b>Channel Name</b> and <b>Numerical ID</b> in a single message.\n\n"
        "<i>Send /cancel to discard.</i>"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Back to Destinations", callback_data="admin_view_destinations")]])
    await safe_edit_message(callback.message.bot, callback.message.chat.id, callback.message.message_id, text, reply_markup=kb)


@router.message(AdminStates.waiting_for_destination_manual, F.text)
async def admin_add_dest_manual_save(message: Message, state: FSMContext):
    if message.from_user.id != get_admin_id():
        return

    if message.text.strip() == "/cancel":
        await state.clear()
        kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Back to Destinations", callback_data="admin_view_destinations")]])
        await message.answer("❌ Cancelled.", reply_markup=kb)
        return

    parsed = parse_destination_input(message.text.strip())
    if not parsed:
        await message.answer(
            "⚠️ <b>Invalid Format!</b>\n\n"
            "Please provide both channel name and numerical ID.\n"
            "<b>Example:</b> <code>VIP Channel -1001234567890</code>\n\n"
            "<i>Send /cancel to discard.</i>",
            parse_mode="HTML"
        )
        return

    numerical_id, nickname = parsed
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO destinations (chat_id, title, chat_type, is_unmatured, accept_requests)
            VALUES ($1, $2, 'channel', FALSE, TRUE)
            ON CONFLICT (chat_id) DO UPDATE SET title = EXCLUDED.title
            """,
            numerical_id, nickname
        )

    await state.clear()
    buttons = [
        [
            InlineKeyboardButton(text="🟢 Set Matured", callback_data=f"dest_convert_matured:{numerical_id}"),
            InlineKeyboardButton(text="⚪ Set Unmatured", callback_data=f"dest_convert_unmatured:{numerical_id}")
        ],
        [InlineKeyboardButton(text="📋 Set as Master Log (Process Only)", callback_data=f"dest_set_master:{numerical_id}")],
        [InlineKeyboardButton(text="⚙️ Open Channel Actions", callback_data=f"dest_actions:{numerical_id}")]
    ]

    await message.answer(
        f"✅ <b>Destination Registered Successfully!</b>\n\n"
        f"• <b>Name:</b> [{nickname}]\n"
        f"• <b>ID:</b> <code>{numerical_id}</code>\n"
        f"• <b>Category:</b> 🟢 <b>Matured Channel</b> (Default)\n\n"
        "Configure category or designate as Master Log below:",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons)
    )


@router.callback_query(F.data.startswith("admin_dest_list:"))
async def admin_dest_list_category(callback: CallbackQuery):
    await callback.answer()
    if callback.from_user.id != get_admin_id():
        return
    category = callback.data.split(":")[1]
    is_unm = (category == "unmatured")

    pool = await get_pool()
    async with pool.acquire() as conn:
        master_dest = await conn.fetchval("SELECT value FROM bot_settings WHERE key = 'master_log_chat_id'")
        destinations = await conn.fetch(
            "SELECT * FROM destinations WHERE is_unmatured = $1 AND (chat_id != $2 OR $2 IS NULL) ORDER BY title ASC",
            is_unm, master_dest
        )

    cat_label = "⚪ Unmatured Channels" if is_unm else "🟢 Matured Channels"
    text = f"📡 <b>{cat_label} ({len(destinations)} Total):</b>\n\n"

    if not destinations:
        text += f"<i>No {category} broadcast destinations found. Register using /adddestination or menu.</i>"
        buttons = [
            [InlineKeyboardButton(text="➕ Add Destination", callback_data="admin_add_dest_prompt")],
            [InlineKeyboardButton(text="🔙 Back to Available Destinations", callback_data="admin_view_destinations")]
        ]
        await safe_edit_message(callback.message.bot, callback.message.chat.id, callback.message.message_id, text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))
        return

    buttons = []
    for d in destinations:
        cid = d["chat_id"]
        title = d["title"] or cid
        posts_count = d["posts_delivered"] or 0
        
        status_info = []
        if is_unm:
            status_info.append("Accepting 🟢" if d["accept_requests"] else "Paused ⚪")
        else:
            status_info.append("Auto-Accept 🟢")

        meta_str = f" [{', '.join(status_info)}]" if status_info else ""
        text += f"• <b>[{title}] {{{posts_count}}}</b> (<code>{cid}</code>){meta_str}\n"
        # MENTION MESSAGE SENT COUNT BESIDE CHANNELS IN MANAGE: [CHANNEL NAME] {POST SEND COUNT}
        buttons.append([InlineKeyboardButton(text=f"⚙️ Manage: [{title}] {{{posts_count}}}", callback_data=f"dest_actions:{cid}")])

    buttons.append([InlineKeyboardButton(text="➕ Add Destination", callback_data="admin_add_dest_prompt")])
    buttons.append([InlineKeyboardButton(text="🔙 Back to Available Destinations", callback_data="admin_view_destinations")])
    await safe_edit_message(callback.message.bot, callback.message.chat.id, callback.message.message_id, text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))


# ==================== CHANNEL / DESTINATION ACTIONS ====================

async def render_dest_actions(callback: CallbackQuery, chat_id: str):
    pool = await get_pool()
    async with pool.acquire() as conn:
        dest = await conn.fetchrow("SELECT * FROM destinations WHERE chat_id = $1", chat_id)
        pending_joins = await conn.fetchval(
            "SELECT COUNT(*) FROM join_requests WHERE chat_id = $1 AND status = 'pending'", chat_id
        ) or 0
        total_joins = await conn.fetchval(
            "SELECT COUNT(*) FROM join_requests WHERE chat_id = $1", chat_id
        ) or 0

    if not dest:
        await callback.answer("Destination not found in broadcast lists.", show_alert=True)
        return

    title = dest["title"]
    is_unmatured = dest["is_unmatured"]
    accept_requests = dest["accept_requests"]
    join_min = dest["join_delay_min"] or 3
    join_max = dest["join_delay_max"] or 10
    delivered = dest["posts_delivered"] or 0

    buttons = []

    if is_unmatured:
        buttons.append([InlineKeyboardButton(text="🟢 Convert to Matured Channel", callback_data=f"dest_convert_matured:{chat_id}")])
        join_btn_text = "⏹ Pause Accepting Requests" if accept_requests else "▶️ Start Accepting Join Requests"
        buttons.append([InlineKeyboardButton(text=join_btn_text, callback_data=f"dest_toggle_accept:{chat_id}")])
    else:
        buttons.append([InlineKeyboardButton(text="⚪ Convert to Unmatured Channel", callback_data=f"dest_convert_unmatured:{chat_id}")])

    buttons.append([InlineKeyboardButton(text=f"⏱ Join Delay: {join_min}s - {join_max}s", callback_data=f"dest_prompt_join_delay:{chat_id}")])
    buttons.append([InlineKeyboardButton(text="📋 Move to Master Log (Exclude Broadcasts)", callback_data=f"dest_set_master:{chat_id}")])
    buttons.append([InlineKeyboardButton(text="🗑 Remove Channel", callback_data=f"dest_delete:{chat_id}")])
    
    back_target = "unmatured" if is_unmatured else "matured"
    buttons.append([InlineKeyboardButton(text="🔙 Back to Channels", callback_data=f"admin_dest_list:{back_target}")])

    cat_label = "⚪ Unmatured Channel" if is_unmatured else "🟢 Matured Channel"
    join_flow_status = "🟢 Auto-Accepted (Running)" if not is_unmatured else ("🟢 Accepting Requests" if accept_requests else "⚪ Paused")

    card = (
        f"⚙️ <b>Broadcast Channel:</b> <b>[{title}] {{{delivered}}}</b> (<code>{chat_id}</code>)\n\n"
        f"• <b>Category:</b> <code>{cat_label}</code>\n"
        f"• <b>Total Messages/Posts Sent:</b> <code>{delivered}</code>\n"
        f"• <b>Join Requests Mode:</b> <code>{join_flow_status}</code>\n"
        f"• <b>Approved Joins:</b> <code>{total_joins - pending_joins} / {total_joins}</code> (<b>{pending_joins} pending</b>)\n"
        f"• <b>Random Join Delay:</b> <code>{join_min}s - {join_max}s</code>"
    )
    await safe_edit_message(callback.message.bot, callback.message.chat.id, callback.message.message_id, card, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))


@router.callback_query(F.data.startswith("dest_actions:"))
async def admin_dest_actions(callback: CallbackQuery):
    await callback.answer()
    if callback.from_user.id != get_admin_id():
        return
    chat_id = callback.data.split(":")[1]
    await render_dest_actions(callback, chat_id)


# ==================== REMOVE DESTINATION CHANNEL ====================

@router.callback_query(F.data.startswith("dest_delete:"))
async def admin_dest_delete(callback: CallbackQuery):
    await callback.answer()
    if callback.from_user.id != get_admin_id():
        return
    chat_id = callback.data.split(":")[1]

    if chat_id in active_join_tasks:
        active_join_tasks[chat_id].cancel()
        del active_join_tasks[chat_id]

    pool = await get_pool()
    async with pool.acquire() as conn:
        dest = await conn.fetchrow("SELECT title FROM destinations WHERE chat_id = $1", chat_id)
        await conn.execute("DELETE FROM destinations WHERE chat_id = $1", chat_id)
        await conn.execute("DELETE FROM join_requests WHERE chat_id = $1", chat_id)

    title = dest["title"] if dest else chat_id
    await callback.answer(f"Channel {title} removed!", show_alert=True)
    await admin_view_destinations(callback)


# ==================== CONVERSION: MATURED <-> UNMATURED ====================

@router.callback_query(F.data.startswith("dest_convert_matured:"))
async def admin_dest_convert_matured(callback: CallbackQuery, bot: Bot):
    await callback.answer("Converted to Matured Channel!")
    if callback.from_user.id != get_admin_id():
        return
    chat_id = callback.data.split(":")[1]

    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE destinations 
            SET is_unmatured = FALSE, accept_requests = TRUE 
            WHERE chat_id = $1
            """,
            chat_id
        )

    if chat_id not in active_join_tasks or active_join_tasks[chat_id].done():
        task = asyncio.create_task(join_request_worker(bot, chat_id))
        active_join_tasks[chat_id] = task

    await render_dest_actions(callback, chat_id)


@router.callback_query(F.data.startswith("dest_convert_unmatured:"))
async def admin_dest_convert_unmatured(callback: CallbackQuery):
    await callback.answer("Converted to Unmatured Channel!")
    if callback.from_user.id != get_admin_id():
        return
    chat_id = callback.data.split(":")[1]

    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE destinations 
            SET is_unmatured = TRUE, accept_requests = FALSE 
            WHERE chat_id = $1
            """,
            chat_id
        )

    if chat_id in active_join_tasks:
        active_join_tasks[chat_id].cancel()
        del active_join_tasks[chat_id]

    await render_dest_actions(callback, chat_id)


# ==================== JOIN REQUESTS TOGGLE & INTERVAL ====================

@router.callback_query(F.data.startswith("dest_toggle_accept:"))
async def admin_dest_toggle_accept(callback: CallbackQuery, bot: Bot):
    await callback.answer()
    if callback.from_user.id != get_admin_id():
        return
    chat_id = callback.data.split(":")[1]

    pool = await get_pool()
    async with pool.acquire() as conn:
        curr = await conn.fetchval("SELECT accept_requests FROM destinations WHERE chat_id = $1", chat_id)
        new_state = not curr
        await conn.execute("UPDATE destinations SET accept_requests = $1 WHERE chat_id = $2", new_state, chat_id)

    if new_state:
        if chat_id not in active_join_tasks or active_join_tasks[chat_id].done():
            task = asyncio.create_task(join_request_worker(bot, chat_id))
            active_join_tasks[chat_id] = task
        await callback.answer("🟢 Accepting join requests started!", show_alert=True)
    else:
        if chat_id in active_join_tasks:
            active_join_tasks[chat_id].cancel()
            del active_join_tasks[chat_id]
        await callback.answer("⏹ Accepting join requests paused.", show_alert=True)

    await render_dest_actions(callback, chat_id)


@router.callback_query(F.data.startswith("dest_prompt_join_delay:"))
async def admin_dest_prompt_join_delay(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    if callback.from_user.id != get_admin_id():
        return
    chat_id = callback.data.split(":")[1]
    await state.update_data(current_join_chat_id=chat_id)
    await state.set_state(AdminStates.waiting_for_join_delay)
    await safe_edit_message(callback.message.bot, callback.message.chat.id, callback.message.message_id, "🎲 <b>Set Random Join Delay Range:</b>\n\nSend min and max seconds separated by a dash (e.g. <code>3-10</code>):")


@router.message(AdminStates.waiting_for_join_delay, F.text)
async def admin_dest_save_join_delay(message: Message, state: FSMContext):
    if message.from_user.id != get_admin_id():
        return
    raw = message.text.strip().replace("-", " ").split()
    if len(raw) != 2 or not raw[0].isdigit() or not raw[1].isdigit():
        await message.answer("⚠️ Invalid format. Example: <code>3-10</code> or <code>5 15</code>.")
        return

    min_d = int(raw[0])
    max_d = int(raw[1])
    if min_d < 1 or max_d < min_d:
        await message.answer("⚠️ Minimum must be >= 1 and Maximum must be >= Minimum.")
        return

    data = await state.get_data()
    chat_id = data["current_join_chat_id"]

    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE destinations SET join_delay_min = $1, join_delay_max = $2 WHERE chat_id = $3",
            min_d, max_d, chat_id
        )

    await state.clear()
    kb = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="🔙 Back to Destination", callback_data=f"dest_actions:{chat_id}")]]
    )
    await message.answer(f"✅ Random join delay set to <code>{min_d}s - {max_d}s</code>.", parse_mode="HTML", reply_markup=kb)


# ==================== DEDICATED MASTER LOG CONTROLS ====================

@router.callback_query(F.data.startswith("dest_set_master:"))
async def admin_dest_set_master(callback: CallbackQuery, bot: Bot):
    await callback.answer()
    if callback.from_user.id != get_admin_id():
        return
    chat_id = callback.data.split(":")[1]

    if chat_id in active_join_tasks:
        active_join_tasks[chat_id].cancel()
        del active_join_tasks[chat_id]

    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM destinations WHERE chat_id = $1", chat_id)
        await conn.execute(
            """
            INSERT INTO bot_settings (key, value)
            VALUES ('master_log_chat_id', $1)
            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
            """,
            chat_id
        )

    await dispatch_notification(bot, "📋 <b>Channel Activated as Master Log Destination</b> (Audit Logs & Process Updates Only)")
    await callback.answer("Promoted to Master Log! It is now excluded from broadcast lists.", show_alert=True)
    await admin_set_master_log_screen(callback)


@router.callback_query(F.data == "admin_set_master_log_screen")
async def admin_set_master_log_screen(callback: CallbackQuery):
    await callback.answer()
    if callback.from_user.id != get_admin_id():
        return

    pool = await get_pool()
    async with pool.acquire() as conn:
        current_master = await conn.fetchval("SELECT value FROM bot_settings WHERE key = 'master_log_chat_id'")

    current_str = f"<code>{current_master}</code>" if current_master else "<i>Not Configured</i>"
    buttons = [
        [InlineKeyboardButton(text="✏️ Enter ID/Username Manually", callback_data="admin_master_log_manual")]
    ]

    if current_master:
        buttons.append([InlineKeyboardButton(text="🗑 Disconnect Master Log", callback_data="admin_clear_master_log")])

    buttons.append([InlineKeyboardButton(text="🔙 Back to Admin Menu", callback_data="admin_back")])

    text = (
        "📋 <b>Master Log Channel Configuration</b>\n\n"
        f"• <b>Current Master Log:</b> {current_str}\n\n"
        "<b>Important Role:</b>\n"
        "• Receives all operational audit trails, new user alerts, upload batch statuses, and ETA broadcasts.\n"
        "• <b>Completely excluded</b> from Matured & Unmatured broadcast destination lists."
    )
    await safe_edit_message(callback.message.bot, callback.message.chat.id, callback.message.message_id, text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))


@router.callback_query(F.data == "admin_clear_master_log")
async def admin_clear_master_log(callback: CallbackQuery):
    await callback.answer("Master log disconnected")
    if callback.from_user.id != get_admin_id():
        return

    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM bot_settings WHERE key = 'master_log_chat_id'")

    await admin_set_master_log_screen(callback)


@router.callback_query(F.data == "admin_master_log_manual")
async def admin_master_log_manual_prompt(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    if callback.from_user.id != get_admin_id():
        return
    await state.set_state(AdminStates.waiting_for_master_log_manual)
    await safe_edit_message(callback.message.bot, callback.message.chat.id, callback.message.message_id, "📋 <b>Enter Master Log Destination:</b>\n\nSend numeric chat ID (<code>-100...</code>) or username (<code>@ChannelName</code>):\n\n<i>Send /cancel to discard.</i>")


@router.message(AdminStates.waiting_for_master_log_manual, F.text)
async def admin_master_log_manual_save(message: Message, state: FSMContext, bot: Bot):
    if message.from_user.id != get_admin_id():
        return
    if message.text.strip() == "/cancel":
        await state.clear()
        kb = await get_admin_main_kb()
        await message.answer("❌ Cancelled.", reply_markup=kb)
        return

    target = message.text.strip()
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM destinations WHERE chat_id = $1", target)
        await conn.execute(
            """
            INSERT INTO bot_settings (key, value)
            VALUES ('master_log_chat_id', $1)
            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
            """,
            target
        )

    await state.clear()
    await dispatch_notification(
        bot,
        f"📋 <b>Master Log Destination Activated:</b> Linked to <code>{target}</code> (Excluded from broadcasts)."
    )
    kb = await get_admin_main_kb()
    await message.answer(f"✅ Master log destination saved as <code>{target}</code>.", parse_mode="HTML", reply_markup=kb)


# ==================== ADMIN STATE HANDLERS ====================

@router.message(AdminStates.waiting_for_queue_name, F.text)
async def admin_create_queue_save(message: Message, state: FSMContext, bot: Bot):
    if message.from_user.id != get_admin_id():
        return

    if message.text.strip() == "/cancel":
        await state.clear()
        kb = await get_admin_main_kb()
        await message.answer("❌ Queue creation cancelled.", reply_markup=kb)
        return

    queue_name = message.text.strip()
    pool = await get_pool()
    async with pool.acquire() as conn:
        try:
            new_id = await conn.fetchval(
                "INSERT INTO queues (name, delay_sec, delay_min, delay_max, delay_type, mode, run_count) VALUES ($1, 5, 5, 15, 'fixed', 'sequence', 0) RETURNING id",
                queue_name
            )

            await dispatch_notification(
                bot,
                f"📁 <b>New Queue Established</b>\n"
                f"• <b>Name:</b> <code>{queue_name}</code>\n"
                f"• <b>Queue ID:</b> <code>{new_id}</code>",
                queue_id=new_id
            )

            await state.clear()
            confirm_kb = InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="🚀 Open in Running Hub", callback_data=f"run_hub_q:{new_id}")],
                    [InlineKeyboardButton(text="🔙 Back to Admin Menu", callback_data="admin_back")]
                ]
            )

            confirmation_card = (
                "✅ <b>Queue Created Successfully!</b>\n\n"
                f"• <b>Queue Name:</b> <code>{queue_name}</code>\n"
                f"• <b>Queue ID:</b> <code>{new_id}</code>\n\n"
                "Tap below to schedule it in the Running Hub."
            )
            await message.answer(confirmation_card, parse_mode="HTML", reply_markup=confirm_kb)

        except Exception:
            kb = await get_admin_main_kb()
            await message.answer(
                f"❌ <b>Error:</b> A queue named <code>{queue_name}</code> already exists. Please choose a different name.",
                parse_mode="HTML",
                reply_markup=kb
            )
            await state.clear()


@router.message(AdminStates.waiting_for_uploader_id, F.text)
async def admin_uploader_id_received(message: Message, state: FSMContext):
    if message.from_user.id != get_admin_id():
        return
    val = message.text.strip()
    if not val.isdigit():
        await message.answer("⚠️ Please provide a valid numeric User ID.")
        return

    await state.update_data(new_uploader_user_id=int(val))
    await state.set_state(AdminStates.waiting_for_uploader_name)
    await message.answer("🏷 Enter a label/name for this uploader (or type <code>Flezen uploader</code>):", parse_mode="HTML")


@router.message(AdminStates.waiting_for_uploader_name, F.text)
async def admin_uploader_name_received(message: Message, state: FSMContext, bot: Bot):
    if message.from_user.id != get_admin_id():
        return
    alias = message.text.strip() or "Flezen uploader"
    data = await state.get_data()
    uid = data["new_uploader_user_id"]
    qid = data["new_uploader_queue_id"]

    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO uploaders (user_id, name, queue_id)
            VALUES ($1, $2, $3)
            ON CONFLICT(user_id) DO UPDATE SET name = EXCLUDED.name, queue_id = EXCLUDED.queue_id
            """,
            uid, alias, qid
        )
        q_row = await conn.fetchrow("SELECT name FROM queues WHERE id = $1", qid)
        q_name = q_row["name"] if q_row else "Queue"

    await dispatch_notification(
        bot,
        f"👤 <b>Flezen Uploader Configured</b>\n• <b>Name:</b> {alias}\n• <b>User ID:</b> <code>{uid}</code>\n• <b>Assigned Queue:</b> <code>{q_name}</code>",
        queue_id=qid
    )

    try:
        await bot.send_message(
            chat_id=uid,
            text=f"🎉 You have been added as a <b>{alias}</b> for queue <code>{q_name}</code>. You can start uploading directly anytime!",
            parse_mode="HTML",
            reply_markup=get_user_main_kb()
        )
    except Exception:
        pass

    await state.clear()
    kb = await get_admin_main_kb()
    await message.answer(
        f"✅ <b>{alias}</b> (<code>{uid}</code>) assigned to queue <b>{q_name}</b>.",
        parse_mode="HTML",
        reply_markup=kb
    )


# ==================== USER HANDLERS & ACCESS REQUESTS ====================

@router.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext, bot: Bot):
    await state.clear()
    user = message.from_user
    current_admin = get_admin_id()
    
    if user.id == current_admin:
        kb = await get_admin_main_kb()
        await message.answer(
            "👋 <b>Welcome Admin</b>. Use /admin to access the control panel.",
            parse_mode="HTML",
            reply_markup=kb
        )
        return

    pool = await get_pool()
    async with pool.acquire() as conn:
        uploader = await conn.fetchrow(
            """
            SELECT u.name, q.name AS q_name 
            FROM uploaders u 
            LEFT JOIN queues q ON u.queue_id = q.id 
            WHERE u.user_id = $1
            """,
            user.id
        )

    if uploader and uploader["q_name"]:
        await message.answer(
            f"👋 <b>Welcome back, {uploader['name']}!</b>\n"
            f"📁 <b>Assigned Queue:</b> <code>{uploader['q_name']}</code>\n\n"
            "💡 <b>Auto-detect & Deduplication Enabled:</b> Send posts/media directly anytime. Duplicates will be skipped automatically!",
            parse_mode="HTML",
            reply_markup=get_user_main_kb()
        )
    else:
        await message.answer(
            "🔒 <b>Access Restricted</b>\n\n"
            "Your account is not yet authorized. An access request has been sent to the administrator.",
            parse_mode="HTML"
        )
        
        user_mention = f"@{user.username}" if user.username else user.full_name
        req_text = (
            "🚨 <b>New Access Request</b>\n\n"
            f"• <b>User:</b> {user.full_name} ({user_mention})\n"
            f"• <b>User ID:</b> <code>{user.id}</code>\n\n"
            "Click below to select a queue and grant access."
        )

        await dispatch_notification(bot, req_text)

        if current_admin:
            grant_kb = InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="✅ Grant Access as Flezen Uploader", callback_data=f"grant_req:{user.id}")]
                ]
            )
            await safe_send_message(bot, current_admin, req_text, reply_markup=grant_kb)


@router.callback_query(F.data.startswith("grant_req:"))
async def admin_grant_request(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    if callback.from_user.id != get_admin_id():
        return
    user_id = int(callback.data.split(":")[1])
    await state.update_data(new_uploader_user_id=user_id)

    pool = await get_pool()
    async with pool.acquire() as conn:
        queues = await conn.fetch("SELECT id, name FROM queues ORDER BY id ASC")

    if not queues:
        await callback.answer("⚠️ Create at least one queue first!", show_alert=True)
        return

    buttons = [
        [InlineKeyboardButton(text=f"📁 {row['name']}", callback_data=f"auto_grant_assign:{row['id']}")]
        for row in queues
    ]
    buttons.append([InlineKeyboardButton(text="🔙 Back", callback_data="admin_back")])
    await safe_edit_message(callback.message.bot, callback.message.chat.id, callback.message.message_id, f"Select the queue to grant user <code>{user_id}</code> access to:", reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))


@router.callback_query(F.data.startswith("auto_grant_assign:"))
async def admin_auto_grant_finish(callback: CallbackQuery, state: FSMContext, bot: Bot):
    await callback.answer("Access granted!")
    if callback.from_user.id != get_admin_id():
        return
    queue_id = int(callback.data.split(":")[1])
    data = await state.get_data()
    user_id = data["new_uploader_user_id"]

    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO uploaders (user_id, name, queue_id)
            VALUES ($1, 'Flezen uploader', $2)
            ON CONFLICT(user_id) DO UPDATE SET queue_id = EXCLUDED.queue_id
            """,
            user_id, queue_id
        )
        q_row = await conn.fetchrow("SELECT name FROM queues WHERE id = $1", queue_id)
        q_name = q_row["name"] if q_row else "Queue"

    try:
        await bot.send_message(
            chat_id=user_id,
            text=(
                f"🎉 <b>Access Approved!</b>\n\n"
                f"You are now registered as a <b>Flezen uploader</b> for queue: <code>{q_name}</code>.\n"
                "You can directly upload posts/media into this chat at any time!"
            ),
            parse_mode="HTML",
            reply_markup=get_user_main_kb()
        )
    except Exception:
        pass

    await dispatch_notification(
        bot,
        f"👤 <b>New Flezen Uploader Approved</b>\n• User: <code>{user_id}</code>\n• Queue: <code>{q_name}</code>",
        queue_id=queue_id
    )

    await state.clear()
    kb = await get_admin_main_kb()
    await safe_edit_message(callback.message.bot, callback.message.chat.id, callback.message.message_id, f"✅ Access granted to user <code>{user_id}</code> for queue <b>{q_name}</b>.", reply_markup=kb)


# ==================== CHECK STATS ====================

@router.message(F.text == "📊 Check Stats")
async def user_stats_handler(message: Message):
    user_id = message.from_user.id
    now = datetime.utcnow()
    
    day_ago = now - timedelta(days=1)
    week_ago = now - timedelta(days=7)
    month_ago = now - timedelta(days=30)

    pool = await get_pool()
    async with pool.acquire() as conn:
        day_count = await conn.fetchval(
            "SELECT COUNT(*) FROM user_logs WHERE user_id = $1 AND is_duplicate = FALSE AND created_at >= $2",
            user_id, day_ago
        )
        week_count = await conn.fetchval(
            "SELECT COUNT(*) FROM user_logs WHERE user_id = $1 AND is_duplicate = FALSE AND created_at >= $2",
            user_id, week_ago
        )
        month_count = await conn.fetchval(
            "SELECT COUNT(*) FROM user_logs WHERE user_id = $1 AND is_duplicate = FALSE AND created_at >= $2",
            user_id, month_ago
        )

        day_dups = await conn.fetchval(
            "SELECT COUNT(*) FROM user_logs WHERE user_id = $1 AND is_duplicate = TRUE AND created_at >= $2",
            user_id, day_ago
        )
        week_dups = await conn.fetchval(
            "SELECT COUNT(*) FROM user_logs WHERE user_id = $1 AND is_duplicate = TRUE AND created_at >= $2",
            user_id, week_ago
        )
        month_dups = await conn.fetchval(
            "SELECT COUNT(*) FROM user_logs WHERE user_id = $1 AND is_duplicate = TRUE AND created_at >= $2",
            user_id, month_ago
        )

    stats_text = (
        "📊 <b>Your Upload Statistics:</b>\n\n"
        f"• <b>Past 24 Hours:</b> <code>{day_count}</code> posts"
        + (f" <i>(⚠️ Duplicates: {day_dups})</i>\n" if day_dups > 0 else "\n")
        + f"• <b>Past 7 Days:</b> <code>{week_count}</code> posts"
        + (f" <i>(⚠️ Duplicates: {week_dups})</i>\n" if week_dups > 0 else "\n")
        + f"• <b>Past 30 Days:</b> <code>{month_count}</code> posts"
        + (f" <i>(⚠️ Duplicates: {month_dups})</i>" if month_dups > 0 else "")
    )
    await message.answer(stats_text, parse_mode="HTML")


@router.message(F.text == "🚀 Start Task")
async def user_start_task(message: Message):
    user_id = message.from_user.id

    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT q.name AS q_name, u.name AS u_name 
            FROM uploaders u 
            JOIN queues q ON u.queue_id = q.id 
            WHERE u.user_id = $1
            """,
            user_id
        )

    if not row:
        await message.answer(
            "⚠️ You haven't been assigned to an active queue yet. Please contact the administrator.",
            reply_markup=get_user_main_kb()
        )
        return

    queue_name, uploader_alias = row["q_name"], row["u_name"]
    await message.answer(
        f"📥 <b>Uploader Profile:</b> <code>{uploader_alias}</code>\n"
        f"📁 <b>Connected Queue:</b> <code>{queue_name}</code>\n\n"
        "Send your posts (text, photos, albums, videos, files) anytime.\n"
        "• Automatic duplicate detection is active.",
        parse_mode="HTML",
        reply_markup=get_user_main_kb()
    )


# ==================== FINISH UPLOAD SUMMARY ====================

@router.message(F.text.in_(["⏹ Finish Uploading", "⏹ Completed Uploading"]))
async def user_finish_uploading(message: Message, bot: Bot):
    user_id = message.from_user.id

    session_queued = 0
    session_dups = 0
    session = upload_batches.get(user_id)
    if session:
        session_queued = session.queued_count
        session_dups = session.duplicate_count
        if session.debounce_task and not session.debounce_task.done():
            session.debounce_task.cancel()
        await session._update_ui(is_final=True)

    now = datetime.utcnow()
    start_of_today = datetime(now.year, now.month, now.day)
    start_of_month = datetime(now.year, now.month, 1)

    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT u.name AS u_name, u.queue_id, q.name AS q_name 
            FROM uploaders u 
            JOIN queues q ON u.queue_id = q.id 
            WHERE u.user_id = $1
            """,
            user_id
        )

        today_valid = await conn.fetchval(
            "SELECT COUNT(*) FROM user_logs WHERE user_id = $1 AND is_duplicate = FALSE AND created_at >= $2",
            user_id, start_of_today
        )
        today_dups = await conn.fetchval(
            "SELECT COUNT(*) FROM user_logs WHERE user_id = $1 AND is_duplicate = TRUE AND created_at >= $2",
            user_id, start_of_today
        )
        month_valid = await conn.fetchval(
            "SELECT COUNT(*) FROM user_logs WHERE user_id = $1 AND is_duplicate = FALSE AND created_at >= $2",
            user_id, start_of_month
        )

    if not row:
        await message.answer("⚠️ No active queue found for your profile.", reply_markup=get_user_main_kb())
        return

    uploader_name = row["u_name"]
    qid = row["queue_id"]
    qname = row["q_name"]

    dup_summary = f"• <b>Duplicates Skipped:</b> <code>{session_dups}</code>\n" if session_dups > 0 else ""

    summary_text = (
        "🏁 <b>Upload Session Completed!</b>\n\n"
        f"• <b>Session New Uploads:</b> <code>{session_queued}</code> post(s)\n"
        f"{dup_summary}"
        f"• <b>Today's Total Uploads:</b> <code>{today_valid}</code> post(s)\n"
        f"• <b>Today's Total Duplicate Uploads:</b> <code>{today_dups}</code>\n"
        f"• <b>This Month's Total Uploads:</b> <code>{month_valid}</code> post(s)\n\n"
        f"<i>Saved in queue <b>{qname}</b>.</i>"
    )
    await message.answer(summary_text, parse_mode="HTML", reply_markup=get_user_main_kb())

    await dispatch_notification(
        bot,
        f"⏹ <b>Flezen Uploader Finished Uploading</b>\n\n"
        f"• <b>Uploader:</b> {uploader_name} (<code>{user_id}</code>)\n"
        f"• <b>Queue:</b> <code>{qname}</code>\n"
        f"• <b>Session New Posts:</b> <code>{session_queued}</code>\n"
        f"• <b>Today's Valid Uploads:</b> <code>{today_valid}</code>\n"
        f"• <b>Today's Duplicate Uploads:</b> <code>{today_dups}</code>\n"
        f"• <b>Month's Total Uploads:</b> <code>{month_valid}</code>",
        queue_id=qid
    )


# ==================== AUTO-DETECT INCOMING POSTS ====================

@router.message(StateFilter(None), ~F.text.startswith("/"))
async def handle_auto_detect_posts(message: Message, bot: Bot, state: FSMContext):
    user_id = message.from_user.id

    if message.text in ["🚀 Start Task", "📊 Check Stats", "⏹ Finish Uploading", "⏹ Completed Uploading"]:
        return

    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT u.queue_id, u.name AS u_name, q.name AS q_name 
            FROM uploaders u 
            JOIN queues q ON u.queue_id = q.id 
            WHERE u.user_id = $1
            """,
            user_id
        )

    if not row or not row["queue_id"]:
        if user_id == get_admin_id():
            return
        await cmd_start(message, state, bot)
        return

    session = upload_batches.get(user_id)
    if not session:
        session = UploadBatchSession(
            user_id=user_id,
            user_chat_id=message.chat.id,
            queue_id=row["queue_id"],
            uploader_name=row["u_name"],
            queue_name=row["q_name"],
            bot=bot
        )
        upload_batches[user_id] = session

    content_hash = get_message_content_hash(message)
    caption_content = message.caption or message.text or ""
    await session.add_post(message.chat.id, message.message_id, content_hash, caption=caption_content)


# ==================== ADMIN PANEL & QUEUES ====================

@router.message(Command("admin"))
async def admin_panel(message: Message, state: FSMContext):
    if message.from_user.id != get_admin_id():
        return
    await state.clear()
    kb = await get_admin_main_kb()
    await message.answer("⚙️ <b>Admin Control Panel</b>", parse_mode="HTML", reply_markup=kb)


@router.callback_query(F.data == "admin_close")
async def admin_close_callback(callback: CallbackQuery, state: FSMContext):
    await callback.answer("Closed")
    await state.clear()
    await callback.message.delete()


@router.callback_query(F.data == "admin_back")
async def admin_back(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    if callback.from_user.id != get_admin_id():
        return
    await state.clear()
    kb = await get_admin_main_kb()
    await safe_edit_message(callback.message.bot, callback.message.chat.id, callback.message.message_id, "⚙️ <b>Admin Control Panel</b>", reply_markup=kb)


@router.callback_query(F.data == "admin_create_queue")
async def admin_create_queue_start(callback: CallbackQuery, state: FSMContext):
    await callback.answer("Opening queue creator...")
    if callback.from_user.id != get_admin_id():
        return
    await state.set_state(AdminStates.waiting_for_queue_name)
    await safe_edit_message(callback.message.bot, callback.message.chat.id, callback.message.message_id, "➕ <b>Create New Post Queue</b>\n\nSend title/name (e.g. <code>Marketing Pipeline</code>):\n\n<i>Send /cancel to discard.</i>")


@router.callback_query(F.data == "admin_list_queues")
async def admin_list_queues(callback: CallbackQuery):
    await callback.answer()
    if callback.from_user.id != get_admin_id():
        return
    pool = await get_pool()
    async with pool.acquire() as conn:
        queues = await conn.fetch("SELECT id, name FROM queues ORDER BY id ASC")

    if not queues:
        kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Back", callback_data="admin_manage_hub")]])
        await safe_edit_message(callback.message.bot, callback.message.chat.id, callback.message.message_id, "📂 No queues found. Create one first.", reply_markup=kb)
        return

    buttons = [
        [InlineKeyboardButton(text=f"📁 {row['name']}", callback_data=f"q_view:{row['id']}")]
        for row in queues
    ]
    buttons.append([InlineKeyboardButton(text="🔙 Back", callback_data="admin_manage_hub")])
    await safe_edit_message(callback.message.bot, callback.message.chat.id, callback.message.message_id, "📂 <b>Select a Queue to Configure:</b>", reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))


@router.callback_query(F.data.startswith("q_view:"))
async def admin_queue_detail(callback: CallbackQuery):
    await callback.answer()
    if callback.from_user.id != get_admin_id():
        return
    queue_id = int(callback.data.split(":")[1])

    pool = await get_pool()
    async with pool.acquire() as conn:
        q = await conn.fetchrow("SELECT * FROM queues WHERE id = $1", queue_id)
        post_count = await conn.fetchval("SELECT COUNT(*) FROM posts WHERE queue_id = $1", queue_id)

    if not q:
        await callback.answer("Queue not found.", show_alert=True)
        return

    name = q["name"]
    is_running = queue_id in active_tasks and not active_tasks[queue_id].done()
    status_str = "🟢 Active (Broadcasting)" if is_running else "⚪ Idle"

    buttons = [
        [InlineKeyboardButton(text="🚀 Open in Running Hub", callback_data=f"run_hub_q:{queue_id}")],
        [InlineKeyboardButton(text="✏️ Edit Caption Rules", callback_data=f"q_caption_edit:{queue_id}")],
        [
            InlineKeyboardButton(text="🗑 Clear Posts", callback_data=f"q_clear_posts:{queue_id}"),
            InlineKeyboardButton(text="❌ Delete Queue", callback_data=f"q_delete:{queue_id}")
        ],
        [InlineKeyboardButton(text="🔙 Back to Queues", callback_data="admin_list_queues")]
    ]

    text = (
        f"📁 <b>Queue:</b> <code>{name}</code>\n\n"
        f"• <b>Status:</b> {status_str}\n"
        f"• <b>Total Stored Posts:</b> <code>{post_count}</code>\n"
        f"• <b>Lifetime Launches:</b> <code>{q['run_count']}</code> time(s)\n"
    )
    await safe_edit_message(callback.message.bot, callback.message.chat.id, callback.message.message_id, text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))


@router.callback_query(F.data.startswith("q_clear_posts:"))
async def admin_clear_posts(callback: CallbackQuery, bot: Bot):
    await callback.answer("Purged posts")
    if callback.from_user.id != get_admin_id():
        return
    queue_id = int(callback.data.split(":")[1])
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM posts WHERE queue_id = $1", queue_id)

    await dispatch_notification(bot, "🗑 <b>Queue Cleared:</b> All posts purged by admin.", queue_id=queue_id)
    await admin_queue_detail(callback)


@router.callback_query(F.data.startswith("q_delete:"))
async def admin_delete_queue(callback: CallbackQuery):
    await callback.answer("Deleted")
    if callback.from_user.id != get_admin_id():
        return
    queue_id = int(callback.data.split(":")[1])
    if queue_id in active_tasks:
        active_tasks[queue_id].cancel()
        del active_tasks[queue_id]

    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM queues WHERE id = $1", queue_id)

    await admin_list_queues(callback)


# ==================== UPLOADER CONFIGURATION ====================

@router.callback_query(F.data == "admin_uploaders_menu")
async def admin_uploaders_menu(callback: CallbackQuery):
    await callback.answer()
    if callback.from_user.id != get_admin_id():
        return
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Add Flezen Uploader", callback_data="admin_add_uploader")],
        [InlineKeyboardButton(text="📊 View & Delete Uploaders", callback_data="admin_view_uploaders")],
        [InlineKeyboardButton(text="🔙 Back to Admin Menu", callback_data="admin_back")]
    ])
    await safe_edit_message(
        callback.message.bot,
        callback.message.chat.id,
        callback.message.message_id,
        "👥 <b>Manage Flezen Uploaders</b>\n\nChoose an action below to add or remove uploaders:",
        reply_markup=kb
    )


@router.callback_query(F.data == "admin_add_uploader")
async def admin_add_uploader_start(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    if callback.from_user.id != get_admin_id():
        return
    pool = await get_pool()
    async with pool.acquire() as conn:
        queues = await conn.fetch("SELECT id, name FROM queues ORDER BY id ASC")

    if not queues:
        kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Back", callback_data="admin_back")]])
        await safe_edit_message(callback.message.bot, callback.message.chat.id, callback.message.message_id, "⚠️ Create at least one queue before registering uploaders.", reply_markup=kb)
        return

    buttons = [
        [InlineKeyboardButton(text=f"📁 {row['name']}", callback_data=f"sel_q_uploader:{row['id']}")]
        for row in queues
    ]
    buttons.append([InlineKeyboardButton(text="🔙 Back", callback_data="admin_back")])
    await safe_edit_message(callback.message.bot, callback.message.chat.id, callback.message.message_id, "Select target queue for the new <b>Flezen Uploader</b>:", reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))


@router.callback_query(F.data.startswith("sel_q_uploader:"))
async def admin_select_queue_for_uploader(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    if callback.from_user.id != get_admin_id():
        return
    queue_id = int(callback.data.split(":")[1])
    await state.update_data(new_uploader_queue_id=queue_id)
    await state.set_state(AdminStates.waiting_for_uploader_id)
    await safe_edit_message(callback.message.bot, callback.message.chat.id, callback.message.message_id, "👤 Send numeric <b>Telegram User ID</b> of the uploader:")


@router.callback_query(F.data == "admin_view_uploaders")
async def admin_view_uploaders(callback: CallbackQuery):
    await callback.answer()
    if callback.from_user.id != get_admin_id():
        return

    now = datetime.utcnow()
    day_ago = now - timedelta(days=1)
    week_ago = now - timedelta(days=7)
    month_ago = now - timedelta(days=30)

    pool = await get_pool()
    async with pool.acquire() as conn:
        uploaders = await conn.fetch("""
            SELECT u.user_id, u.name, q.name AS q_name
            FROM uploaders u
            LEFT JOIN queues q ON u.queue_id = q.id
            ORDER BY u.created_at DESC
        """)

        if not uploaders:
            kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Back", callback_data="admin_uploaders_menu")]])
            await safe_edit_message(callback.message.bot, callback.message.chat.id, callback.message.message_id, "👥 No Flezen uploaders configured.", reply_markup=kb)
            return

        report = ["📊 <b>Flezen Uploaders & Performance:</b>\n"]
        buttons = []
        for row in uploaders:
            uid = row["user_id"]
            uname = row["name"]
            qname = row["q_name"]

            d_c = await conn.fetchval(
                "SELECT COUNT(*) FROM user_logs WHERE user_id = $1 AND is_duplicate = FALSE AND created_at >= $2",
                uid, day_ago
            )
            d_dup = await conn.fetchval(
                "SELECT COUNT(*) FROM user_logs WHERE user_id = $1 AND is_duplicate = TRUE AND created_at >= $2",
                uid, day_ago
            )
            w_c = await conn.fetchval(
                "SELECT COUNT(*) FROM user_logs WHERE user_id = $1 AND is_duplicate = FALSE AND created_at >= $2",
                uid, week_ago
            )
            m_c = await conn.fetchval(
                "SELECT COUNT(*) FROM user_logs WHERE user_id = $1 AND is_duplicate = FALSE AND created_at >= $2",
                uid, month_ago
            )

            dup_info = f" (⚠️ {d_dup} dups today)" if d_dup > 0 else ""
            report.append(
                f"👤 <b>{uname}</b> (<code>{uid}</code>)\n"
                f"• Queue: <code>{qname or 'None'}</code>\n"
                f"• Day: <code>{d_c}</code>{dup_info} | Week: <code>{w_c}</code> | Month: <code>{m_c}</code>\n"
            )
            buttons.append([InlineKeyboardButton(text=f"🗑 Delete {uname}", callback_data=f"admin_del_uploader:{uid}")])

    text = "\n".join(report)
    buttons.append([InlineKeyboardButton(text="🔙 Back", callback_data="admin_uploaders_menu")])
    kb = InlineKeyboardMarkup(inline_keyboard=buttons)
    await safe_edit_message(callback.message.bot, callback.message.chat.id, callback.message.message_id, text, reply_markup=kb)


@router.callback_query(F.data.startswith("admin_del_uploader:"))
async def admin_del_uploader(callback: CallbackQuery, bot: Bot):
    await callback.answer("Deleting uploader...")
    if callback.from_user.id != get_admin_id():
        return
    
    uid = int(callback.data.split(":")[1])
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM uploaders WHERE user_id = $1", uid)
    
    await dispatch_notification(bot, f"🗑 <b>Uploader Removed:</b> User ID <code>{uid}</code> has been deleted by admin.")
    await callback.answer("Uploader deleted successfully!", show_alert=True)
    await admin_view_uploaders(callback)
