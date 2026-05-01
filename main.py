import asyncio
import aiohttp
import random
import os
import sys
import uuid
import sqlite3
import logging
from datetime import datetime
from pathlib import Path
from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.types import (
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    CallbackQuery,
    BotCommand,
)
from aiogram.filters import Command
from aiogram.fsm.storage.memory import MemoryStorage
import aiosqlite
from dotenv import load_dotenv

# optional: read-only access to the second bot's MongoDB
from pymongo import MongoClient
from bson import Int64

load_dotenv()

# basic logging to stdout for easier debugging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

BOT_TOKEN = os.environ.get("BOT_TOKEN")
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN environment variable is required")

SQLITE_PATH = os.environ.get("SQLITE_PATH", "mquick.db")

# MongoDB configuration - used only to read tokens from the second bot's database
MONGO_URI = os.environ.get("MONGO_URI")
MONGO_DB_NAME = os.environ.get("MONGO_DB_NAME", "meeff_tokens")

# globals
user_tokens = {}
matching_tasks = {}
user_stats = {}
task_meta = {}

# sync meta for /sync flow
sync_meta = {}

sql_db = None
mongo_client = None
mongo_db = None

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher(storage=MemoryStorage())

HEADERS_TEMPLATE = {
    "User-Agent": "okhttp/5.1.0 (Linux; Android 13; Pixel 6 Build/TQ3A.230901.001)",
    "Accept-Encoding": "gzip",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
    "Host": "api.meeff.com",
}

ANSWER_URL = "https://api.meeff.com/user/undoableAnswer/v5/?userId={user_id}&isOkay=1"


async def init_db():
    global sql_db
    # initialize a new aiosqlite connection and ensure tables exist
    sql_db = await aiosqlite.connect(SQLITE_PATH, timeout=30)
    await sql_db.execute("PRAGMA journal_mode=WAL;")
    await sql_db.execute("PRAGMA synchronous=NORMAL;")
    await sql_db.execute(
        """
        CREATE TABLE IF NOT EXISTS config (
            key TEXT PRIMARY KEY,
            value TEXT
        );
        """
    )
    await sql_db.execute(
        """
        CREATE TABLE IF NOT EXISTS exclude (
            chat_id INTEGER,
            country TEXT,
            PRIMARY KEY(chat_id, country)
        );
        """
    )
    await sql_db.execute(
        """
        CREATE TABLE IF NOT EXISTS history (
            user_id TEXT,
            chat_id INTEGER,
            first_added_at TEXT,
            reserved INTEGER DEFAULT 0,
            PRIMARY KEY(user_id, chat_id)
        );
        """
    )
    await sql_db.commit()


async def ensure_db():
    """
    Ensure the global sql_db is initialized and active.
    If the existing connection is closed or invalid, reinitialize it.
    """
    global sql_db
    if sql_db is None:
        await init_db()
        return
    try:
        # probe the connection to ensure it's active
        async with sql_db.execute("SELECT 1") as cur:
            await cur.fetchone()
    except (ValueError, sqlite3.ProgrammingError):
        # no active connection or connection invalid -> re-init
        await init_db()
    except Exception:
        # on any unexpected error, try to re-init as a recovery step
        await init_db()


async def get_config_value(key):
    await ensure_db()
    async with sql_db.execute("SELECT value FROM config WHERE key = ?", (key,)) as cur:
        row = await cur.fetchone()
        return row[0] if row else None


async def set_config_value(key, value):
    await ensure_db()
    await sql_db.execute(
        "INSERT INTO config(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        (key, value),
    )
    await sql_db.commit()


async def get_config_bool(key, default=False):
    await ensure_db()
    v = await get_config_value(key)
    if v is None:
        return default
    return v == "1"


async def set_config_bool(key, val):
    await ensure_db()
    await set_config_value(key, "1" if val else "0")


async def list_excluded_countries(chat_id):
    await ensure_db()
    async with sql_db.execute("SELECT country FROM exclude WHERE chat_id = ?", (chat_id,)) as cur:
        rows = await cur.fetchall()
        return [r[0] for r in rows]


async def add_excluded_countries(chat_id, countries):
    await ensure_db()
    async with sql_db.execute("BEGIN"):
        for c in countries:
            await sql_db.execute(
                "INSERT OR IGNORE INTO exclude(chat_id, country) VALUES(?, ?)",
                (chat_id, c),
            )
    await sql_db.commit()


async def clear_excluded_countries(chat_id):
    await ensure_db()
    await sql_db.execute("DELETE FROM exclude WHERE chat_id = ?", (chat_id,))
    await sql_db.commit()


async def reserve_user(user_id, chat_id):
    await ensure_db()
    now = datetime.utcnow().isoformat()
    try:
        await sql_db.execute(
            "INSERT INTO history(user_id, chat_id, first_added_at, reserved) VALUES(?, ?, ?, 1)",
            (user_id, chat_id, now),
        )
        await sql_db.commit()
        return True
    except sqlite3.IntegrityError:
        return False


async def mark_user_added(user_id, chat_id):
    await ensure_db()
    async with sql_db.execute("SELECT reserved FROM history WHERE user_id = ? AND chat_id = ?", (user_id, chat_id)) as cur:
        row = await cur.fetchone()
        if not row:
            now = datetime.utcnow().isoformat()
            await sql_db.execute(
                "INSERT OR REPLACE INTO history(user_id, chat_id, first_added_at, reserved) VALUES(?, ?, ?, 0)",
                (user_id, chat_id, now),
            )
            await sql_db.commit()
            return
    await sql_db.execute("UPDATE history SET reserved = 0 WHERE user_id = ? AND chat_id = ?", (user_id, chat_id))
    await sql_db.commit()


async def unreserve_user_on_failure(user_id, chat_id):
    await ensure_db()
    await sql_db.execute("DELETE FROM history WHERE user_id = ? AND chat_id = ? AND reserved = 1", (user_id, chat_id))
    await sql_db.commit()


async def history_for_chat(chat_id, limit=20):
    await ensure_db()
    async with sql_db.execute(
        "SELECT user_id, first_added_at FROM history WHERE chat_id = ? ORDER BY first_added_at DESC LIMIT ?",
        (chat_id, limit),
    ) as cur:
        rows = await cur.fetchall()
        return rows


async def history_count_for_chat(chat_id):
    await ensure_db()
    async with sql_db.execute("SELECT COUNT(*) FROM history WHERE chat_id = ?", (chat_id,)) as cur:
        row = await cur.fetchone()
        return row[0] if row else 0


async def history_total_count():
    await ensure_db()
    async with sql_db.execute("SELECT COUNT(*) FROM history") as cur:
        row = await cur.fetchone()
        return row[0] if row else 0


async def clear_history_for_chat(chat_id):
    await ensure_db()
    await sql_db.execute("DELETE FROM history WHERE chat_id = ?", (chat_id,))
    await sql_db.commit()


async def clear_all_history():
    await ensure_db()
    await sql_db.execute("DELETE FROM history")
    await sql_db.commit()


async def fetch_users(session, explore_url):
    async with session.get(explore_url) as res:
        status = res.status
        text = await res.text()
        if status != 200:
            return status, text, None
        try:
            data = await res.json(content_type=None)
        except:
            return status, text, None
        return status, text, data


async def start_matching(chat_id, token, explore_url, stat_msg, task_id, keyboard):
    key = f"{chat_id}:{token}"
    headers = HEADERS_TEMPLATE.copy()
    headers["meeff-access-token"] = token
    stats = {"requests": 0, "cycles": 0, "errors": 0}
    user_stats[key] = stats
    timeout = aiohttp.ClientTimeout(total=30)
    connector = aiohttp.TCPConnector(ssl=False, limit_per_host=10)
    empty_count = 0
    stop_reason = None
    try:
        async with aiohttp.ClientSession(timeout=timeout, connector=connector, headers=headers) as session:
            async def answer_user(user_id):
                nonlocal stop_reason
                try:
                    async with session.get(ANSWER_URL.format(user_id=user_id)) as res:
                        text = await res.text()
                        if res.status == 429 or "LikeExceeded" in text:
                            stop_reason = "LIMIT EXCEEDED"
                            await unreserve_user_on_failure(user_id, chat_id)
                            return False
                        if res.status == 401 or "AuthRequired" in text:
                            stop_reason = "TOKEN EXPIRED"
                            await unreserve_user_on_failure(user_id, chat_id)
                            return False
                        if res.status == 200:
                            await mark_user_added(user_id, chat_id)
                        else:
                            await unreserve_user_on_failure(user_id, chat_id)
                        return True
                except Exception:
                    stats["errors"] += 1
                    try:
                        await unreserve_user_on_failure(user_id, chat_id)
                    except:
                        pass
                    return True

            while task_meta.get(task_id) and task_meta[task_id].get("running", True):
                try:
                    countries_enabled = await get_config_bool(f"countries_enabled:{chat_id}", default=True)
                except Exception:
                    countries_enabled = True
                try:
                    countries_mode = (await get_config_value(f"countries_mode:{chat_id}")) or "exclude"
                except Exception:
                    countries_mode = "exclude"
                try:
                    countries_list = set([c.upper() for c in await list_excluded_countries(chat_id)])
                except Exception:
                    countries_list = set()
                status, raw_text, data = await fetch_users(session, explore_url)
                if status == 401 or "AuthRequired" in str(raw_text):
                    stop_reason = "TOKEN EXPIRED"
                    break
                if data is None or not data.get("users"):
                    empty_count += 1
                    if empty_count >= 6:
                        stop_reason = "NO USERS FOUND"
                        break
                    await asyncio.sleep(1)
                    continue
                empty_count = 0
                users = data.get("users", [])
                tasks = []
                results = []
                for user in users:
                    user_id = user.get("_id")
                    if not user_id:
                        continue
                    nat = user.get("nationalityCode") or user.get("locale")
                    if nat:
                        nat_code = nat.upper()
                        if "-" in nat_code:
                            nat_code = nat_code.split("-")[-1]
                    else:
                        nat_code = None
                    if countries_mode == "exclude":
                        if countries_enabled and nat_code and nat_code in countries_list:
                            continue
                    else:
                        if countries_enabled:
                            if not nat_code or nat_code not in countries_list:
                                continue
                    reserved = True
                    if await get_config_bool(f"history_enabled:{chat_id}", default=True):
                        reserved = await reserve_user(user_id, chat_id)
                    if not reserved:
                        continue
                    task = asyncio.create_task(answer_user(user_id))
                    tasks.append(task)
                    stats["requests"] += 1
                    await asyncio.sleep(random.uniform(0.05, 0.2))
                    if len(tasks) >= 10:
                        batch_results = await asyncio.gather(*tasks)
                        results.extend(batch_results)
                        tasks.clear()
                        if False in batch_results:
                            break
                if tasks:
                    batch_results = await asyncio.gather(*tasks)
                    results.extend(batch_results)
                if False in results:
                    break
                stats["cycles"] += 1
                final_text = (
                    f"Live Stats:\n"
                    f"Requests: {stats['requests']}\n"
                    f"Cycles: {stats['cycles']}\n"
                    f"Errors: {stats['errors']}"
                )
                if stop_reason:
                    final_text += f"\n\n⚠️ {stop_reason}"
                try:
                    await stat_msg.edit_text(final_text, reply_markup=keyboard)
                except:
                    pass
                await asyncio.sleep(random.uniform(1, 2))
    except asyncio.CancelledError:
        try:
            await stat_msg.edit_text(
                f"Stopped.\n\nRequests: {stats['requests']}\nCycles: {stats['cycles']}\nErrors: {stats['errors']}"
            )
        except:
            pass
        raise
    except Exception as e:
        try:
            await stat_msg.edit_text(f"Error: {e}", reply_markup=keyboard)
        except:
            pass
    if stop_reason:
        try:
            await stat_msg.edit_text(
                f"Live Stats:\n"
                f"Requests: {stats['requests']}\n"
                f"Cycles: {stats['cycles']}\n"
                f"Errors: {stats['errors']}\n\n"
                f"⚠️ {stop_reason}"
            )
        except:
            pass
    matching_tasks.pop(key, None)
    user_stats.pop(key, None)
    task_meta.pop(task_id, None)
    lst = user_tokens.get(chat_id, [])
    try:
        if token in lst:
            lst.remove(token)
            if lst:
                user_tokens[chat_id] = lst
            else:
                user_tokens.pop(chat_id, None)
    except Exception:
        pass


# helper: initialize mongo client lazily (read-only usage)
def get_mongo_db():
    global mongo_client, mongo_db
    if not MONGO_URI:
        logging.debug("get_mongo_db: MONGO_URI not set")
        return None
    # compare against None explicitly because pymongo Database objects don't support truth testing
    if mongo_db is not None:
        return mongo_db
    try:
        mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        # ping to surface connection issues early
        mongo_client.admin.command("ping")
        mongo_db = mongo_client[MONGO_DB_NAME]
        logging.info("Connected to MongoDB %s (db=%s)", MONGO_URI, MONGO_DB_NAME)
        return mongo_db
    except Exception as e:
        logging.exception("Failed to connect to MongoDB: %s", e)
        mongo_client = None
        mongo_db = None
        return None


async def fetch_tokens_from_mongo(user_id):
    """Return list of token dicts from the second bot's MongoDB for given user_id.
    This reads the same collections used by the provided db.py (tokens collection).
    The function attempts to match both integer and string representations of user_id.
    """
    db = get_mongo_db()
    if db is None:
        logging.debug("fetch_tokens_from_mongo: no db available")
        return []
    try:
        # try several likely representations (int, Int64, string)
        candidates = [user_id, str(user_id)]
        try:
            candidates.append(Int64(int(user_id)))
        except Exception:
            pass
        # use $in to match whichever type is stored
        docs = list(db.tokens.find({"user_id": {"$in": candidates}}, {"_id": 0, "token": 1, "name": 1, "active": 1, "email": 1}))
        logging.info("fetch_tokens_from_mongo: user_id=%s -> found %d docs", user_id, len(docs))
        return docs
    except Exception as e:
        logging.exception("Error fetching tokens for user_id=%s: %s", user_id, e)
        return []


@dp.message(Command("sync"))
async def sync_command(message):
    """Fetch Meeff tokens stored in the second bot's MongoDB for this Telegram user and ask for confirmation
    to start requests on all found tokens.
    """
    user_id = message.chat.id
    if not MONGO_URI:
        await message.reply("MongoDB is not configured. Set MONGO_URI environment variable to use /sync.")
        return

    db = get_mongo_db()
    if db is None:
        await message.reply("Failed to connect to MongoDB using MONGO_URI. Check configuration.")
        return

    docs = await fetch_tokens_from_mongo(user_id)
    if not docs:
        # fallback: try string user id explicitly
        docs = await fetch_tokens_from_mongo(str(user_id))
        if not docs:
            await message.reply(
                "No tokens found for your account in the MongoDB database.\n"
                "If your second bot stores user_id under a different field or type, let me know and I can try other matches."
            )
            return

    # prepare summary
    tokens = [d.get("token") for d in docs if d.get("token")]
    names = [d.get("name") or "(unnamed)" for d in docs]
    count = len(tokens)

    # mask tokens for display (show only start+end)
    def mask(t):
        if not t:
            return "(missing)"
        return t[:6] + "..." + t[-6:]

    display_lines = [f"{i+1}. {names[i]} - {mask(tokens[i])}" for i in range(len(tokens))]
    display_names = "\n".join(display_lines)
    sync_id = uuid.uuid4().hex
    sync_meta[sync_id] = {"user_id": user_id, "tokens": tokens}
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"Start ({count})", callback_data=f"sync_start:{sync_id}"),
         InlineKeyboardButton(text="Cancel", callback_data=f"sync_cancel:{sync_id}")]
    ])
    await message.reply(f"Found {count} account(s) in the shared DB:\n{display_names}\n\nStart sending requests on all these tokens?", reply_markup=kb)


@dp.callback_query(F.data.startswith("sync_cancel:"))
async def _sync_cancel(callback: CallbackQuery):
    sid = callback.data.split(":", 1)[1]
    meta = sync_meta.pop(sid, None)
    try:
        await callback.answer("Sync cancelled.", show_alert=False)
        await callback.message.edit_text("Sync cancelled by user.")
    except:
        pass


@dp.callback_query(F.data.startswith("sync_start:"))
async def _sync_start(callback: CallbackQuery):
    sid = callback.data.split(":", 1)[1]
    meta = sync_meta.pop(sid, None)
    if not meta:
        await callback.answer("Sync data expired or invalid.", show_alert=False)
        return
    chat_id = meta.get("user_id")
    tokens = meta.get("tokens", [])
    explore_url = await get_config_value("explore_url")
    if not explore_url:
        await callback.answer("Explore URL not configured. Send the explore URL first.", show_alert=False)
        return
    # start a matching task for each token (parallel)
    started = 0
    too_many = False
    for token in tokens:
        if not token:
            continue
        key = f"{chat_id}:{token}"
        if key in matching_tasks:
            continue
        # register token in user_tokens
        lst = user_tokens.get(chat_id, [])
        if token not in lst:
            lst.append(token)
            user_tokens[chat_id] = lst
        task_id = uuid.uuid4().hex
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Stop", callback_data=f"stop_task:{task_id}")]
        ])
        try:
            stat_msg = await bot.send_message(
                chat_id,
                "Live Stats:\nRequests: 0\nCycles: 0\nErrors: 0",
                reply_markup=keyboard,
            )
        except Exception:
            # unable to send status message (maybe user blocked bot); skip starting this token
            continue
        task = asyncio.create_task(start_matching(chat_id, token, explore_url, stat_msg, task_id, keyboard))
        matching_tasks[key] = task
        task_meta[task_id] = {"key": key, "stat_msg": stat_msg, "running": True, "token": token}
        started += 1
        # small delay to avoid burst creation
        await asyncio.sleep(0.05)
        if started >= 100:
            too_many = True
            break
    if started == 0:
        await callback.answer("No new tokens started (they may already be running).", show_alert=False)
        try:
            await callback.message.edit_text("No new tokens were started. They may already be running.")
        except:
            pass
        return
    await callback.answer(f"Started {started} token(s).", show_alert=False)
    try:
        txt = f"Sync started. Started {started} token(s)."
        if too_many:
            txt += "\nStopped starting more after 100 tokens to avoid overload."
        await callback.message.edit_text(txt)
    except:
        pass


@dp.message(F.text.startswith("https://api.meeff.com/user/explore"))
async def set_explore_url_direct(message):
    url = message.text.strip()
    await set_config_value("explore_url", url)
    await message.answer("Explore URL saved.")


@dp.message(Command("start"))
async def start(message):
    await message.answer("Send Meeff Token.")


@dp.message(Command("countries"))
async def countries_cmd(message):
    chat_id = message.chat.id
    parts = message.text.split(maxsplit=1)
    args = parts[1].strip() if len(parts) > 1 else ""
    if not args:
        countries = await list_excluded_countries(chat_id)
        mode = (await get_config_value(f"countries_mode:{chat_id}")) or "exclude"
        enabled = await get_config_bool(f"countries_enabled:{chat_id}", default=True)
        state = "ON" if enabled else "OFF"
        display = ", ".join(countries) if countries else "No countries set."
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=f"Mode: {mode.upper()}", callback_data=f"countries_mode_toggle:{chat_id}"),
             InlineKeyboardButton(text=f"{'ON' if enabled else 'OFF'}", callback_data=f"countries_enabled_toggle:{chat_id}")],
            [InlineKeyboardButton(text="Clear", callback_data=f"countries_clear:{chat_id}")]
        ])
        await message.answer(f"Countries ({mode.upper()}) ({state}):\n{display}", reply_markup=kb)
        return
    codes = [p.upper() for p in args.split() if p.strip()]
    if not codes:
        await message.answer("No country codes provided.")
        return
    await add_excluded_countries(chat_id, codes)
    await message.answer("Added to countries list: " + ", ".join(codes))


@dp.message(Command("history"))
async def history_cmd(message):
    chat_id = message.chat.id
    parts = message.text.split(maxsplit=1)
    args = parts[1].strip() if len(parts) > 1 else ""
    if not args:
        count = await history_count_for_chat(chat_id)
        enabled = await get_config_bool(f"history_enabled:{chat_id}", default=True)
        state = "ON" if enabled else "OFF"
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=f"{state}", callback_data=f"hist_toggle:{chat_id}"),
             InlineKeyboardButton(text="Clear", callback_data=f"hist_clear:{chat_id}")]
        ])
        await message.answer(f"History ({state}):\nYour saved ids: {count}", reply_markup=kb)
        return
    if args.lower() == "clear":
        try:
            await clear_history_for_chat(chat_id)
            await message.answer("History cleared for this chat.")
        except Exception as e:
            await message.answer(f"Error clearing history: {e}")
        return


@dp.message(Command("restart"))
async def restart_cmd(message):
    msg = await message.answer("Restarting...")

    for key, task in list(matching_tasks.items()):
        try:
            task.cancel()
        except:
            pass
    matching_tasks.clear()
    user_stats.clear()

    for tid, meta in list(task_meta.items()):
        try:
            meta["running"] = False
        except:
            pass
    task_meta.clear()
    user_tokens.clear()

    try:
        global sql_db
        if sql_db:
            await sql_db.close()
            # prevent reuse of a closed connection
            sql_db = None
    except:
        pass

    try:
        await bot.close()
    except:
        try:
            if getattr(bot, "session", None):
                await bot.session.close()
        except:
            pass

    await msg.edit_text("Restarted.")

    os.execv(sys.executable, [sys.executable] + sys.argv)


@dp.message(F.text)
async def receive_token(message):
    if not message.text:
        return
    if message.text.startswith("/"):
        return
    if message.text.startswith("https://api.meeff.com/user/explore"):
        return
    chat_id = message.chat.id
    token = message.text.strip()
    lst = user_tokens.get(chat_id, [])
    if token not in lst:
        lst.append(token)
        user_tokens[chat_id] = lst
    explore_url = await get_config_value("explore_url")
    if not explore_url:
        return await message.answer("Send explore URL.")
    key = f"{chat_id}:{token}"
    if key in matching_tasks:
        return
    task_id = uuid.uuid4().hex
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Stop", callback_data=f"stop_task:{task_id}")]
    ])
    stat_msg = await bot.send_message(
        chat_id,
        "Live Stats:\nRequests: 0\nCycles: 0\nErrors: 0",
        reply_markup=keyboard,
    )
    task = asyncio.create_task(start_matching(chat_id, token, explore_url, stat_msg, task_id, keyboard))
    matching_tasks[key] = task
    task_meta[task_id] = {"key": key, "stat_msg": stat_msg, "running": True, "token": token}


@dp.callback_query(F.data.startswith("stop_task:"))
async def _stop_task(callback: CallbackQuery):
    task_id = callback.data.split(":", 1)[1]
    meta = task_meta.get(task_id)
    if not meta:
        await callback.answer("Already stopped.", show_alert=False)
        return
    meta["running"] = False
    key = meta.get("key")
    t = matching_tasks.pop(key, None)
    if t:
        t.cancel()
    try:
        await meta["stat_msg"].edit_text("Stopping...")
    except:
        pass
    await callback.answer("Stopping task.", show_alert=False)


async def register_bot_commands():
    commands = [
        BotCommand(command="start", description="Start and send Meeff token"),
        BotCommand(command="countries", description="Manage countries filter"),
        BotCommand(command="history", description="Show history stats"),
        BotCommand(command="restart", description="Restart the bot"),
        BotCommand(command="sync", description="Sync tokens from shared MongoDB and start requests"),
    ]
    await bot.set_my_commands(commands)


async def main():
    await init_db()
    await register_bot_commands()
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
