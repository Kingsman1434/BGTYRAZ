import sqlite3
import threading
import time
import traceback
import os
import sys
import atexit
import html as html_module
from contextlib import contextmanager
from typing import Optional
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import telebot
from telebot.types import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardMarkup,
)

# =====================================================================
# 1. НАСТРОЙКИ И КОНСТАНТЫ
# =====================================================================

BOT_TOKEN = os.environ["BOT_TOKEN"]
ADMIN_IDS = {}
PROJECT_DIR = "/home/kingsman/Ocheredi"
DB_NAME = os.path.join(PROJECT_DIR, "lab_queue.db")
PID_FILE = os.path.join(PROJECT_DIR, "bot.pid")

DELETE_DELAY = 60
MAX_SEATS_PER_USER = 10
CYCLIC_SESSIONS_DAYS_AHEAD = 30
SESSION_KEEP_MIN = 3

TZ = ZoneInfo("Europe/Moscow")


def now_local() -> datetime:
    return datetime.now(TZ).replace(tzinfo=None)


DAYS_OF_WEEK = [
    "Понедельник", "Вторник", "Среда", "Четверг",
    "Пятница", "Суббота", "Воскресенье"
]

DAYS_SHORT = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]

BTN_JOIN = "\U0001F4DD Занять очередь"
BTN_VIEW = "\U0001F465 Просмотр очередей"
BTN_MY = "\U0001F4CB Мои очереди"
BTN_ADMIN = "\u2699\ufe0f Админ-панель"
MENU_TEXTS = {BTN_JOIN, BTN_VIEW, BTN_MY, BTN_ADMIN}

bot = telebot.TeleBot(BOT_TOKEN, parse_mode="HTML")
user_states = {}

_sync_lock = threading.Lock()


def log(msg: str) -> None:
    stamp = now_local().strftime("%H:%M:%S")
    print(f"[{stamp}] {msg}", flush=True)


def send_to_chat(chat_id: int, text: str, thread_id: Optional[int] = None, **kwargs):
    if thread_id:
        return bot.send_message(chat_id, text, message_thread_id=thread_id, **kwargs)
    return bot.send_message(chat_id, text, **kwargs)


# =====================================================================
# 2. ЗАЩИТА ОТ ДУБЛИКАТОВ
# =====================================================================

def remove_pid_file():
    if os.path.exists(PID_FILE):
        try:
            os.remove(PID_FILE)
        except Exception:
            pass


def check_already_running():
    if os.path.exists(PID_FILE):
        try:
            with open(PID_FILE, 'r') as f:
                pid = int(f.read().strip())
            os.kill(pid, 0)
            log(f"\u26a0 Бот уже запущен с PID {pid}. Новый экземпляр не создается.")
            sys.exit(0)
        except (ProcessLookupError, ValueError, FileNotFoundError, PermissionError):
            remove_pid_file()

    with open(PID_FILE, 'w') as f:
        f.write(str(os.getpid()))

    atexit.register(remove_pid_file)


# =====================================================================
# АВТОУДАЛЕНИЕ СООБЩЕНИЙ В ГРУППЕ
# =====================================================================

def delete_messages_after_delay(chat_id, user_message_id, bot_message_id, delay=DELETE_DELAY):
    def _delete():
        try:
            bot.delete_message(chat_id, user_message_id)
        except Exception as e:
            log(f"\u26a0 Не удалось удалить сообщение пользователя: {e}")
        try:
            bot.delete_message(chat_id, bot_message_id)
        except Exception as e:
            log(f"\u26a0 Не удалось удалить сообщение бота: {e}")

    timer = threading.Timer(delay, _delete)
    timer.daemon = True
    timer.start()


# =====================================================================
# 3. БАЗА ДАННЫХ
# =====================================================================

@contextmanager
def get_db():
    conn = sqlite3.connect(DB_NAME, timeout=15)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=15000")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def migrate_queue_table(db):
    try:
        indexes = db.execute("PRAGMA index_list('queue')").fetchall()
        has_unique = False
        for idx in indexes:
            if idx["unique"]:
                cols = db.execute(f"PRAGMA index_info('{idx['name']}')").fetchall()
                col_names = [c["name"] for c in cols]
                if set(col_names) == {"session_id", "user_id"}:
                    has_unique = True
                    break

        if has_unique:
            log("\U0001f504 Миграция таблицы queue: убираю UNIQUE constraint...")
            db.execute("""
                CREATE TABLE queue_new (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id INTEGER NOT NULL,
                    user_id INTEGER NOT NULL,
                    joined_at TEXT NOT NULL,
                    position INTEGER NOT NULL
                )
            """)
            db.execute("INSERT INTO queue_new SELECT * FROM queue")
            db.execute("DROP TABLE queue")
            db.execute("ALTER TABLE queue_new RENAME TO queue")
            db.execute("CREATE INDEX IF NOT EXISTS idx_queue_session ON queue(session_id, position)")
            log("\u2705 Миграция queue завершена")
    except Exception as e:
        log(f"\u26a0 Ошибка миграции queue: {e}")


def init_db():
    conn = sqlite3.connect(DB_NAME, timeout=15)
    try:
        conn.execute("PRAGMA journal_mode=DELETE")
    finally:
        conn.close()

    with get_db() as db:
        db.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                telegram_id INTEGER UNIQUE NOT NULL,
                full_name TEXT,
                username TEXT,
                is_blocked INTEGER DEFAULT 0,
                max_seats INTEGER DEFAULT 1
            )
        """)
        db.execute("""
            CREATE TABLE IF NOT EXISTS sessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL,
                datetime TEXT,
                is_cyclic INTEGER DEFAULT 0,
                day_of_week INTEGER,
                time_str TEXT,
                is_closed INTEGER DEFAULT 0,
                chat_id INTEGER,
                message_thread_id INTEGER,
                notified_12h INTEGER DEFAULT 0,
                queue_announced_12h INTEGER DEFAULT 0,
                queue_announced_1h INTEGER DEFAULT 0
            )
        """)
        db.execute("""
            CREATE TABLE IF NOT EXISTS topic_bindings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT UNIQUE NOT NULL,
                chat_id INTEGER NOT NULL,
                message_thread_id INTEGER
            )
        """)
        db.execute("""
            CREATE TABLE IF NOT EXISTS queue (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                joined_at TEXT NOT NULL,
                position INTEGER NOT NULL
            )
        """)

        db.execute("CREATE INDEX IF NOT EXISTS idx_sessions_dt ON sessions(datetime, is_cyclic, is_closed)")
        db.execute("CREATE INDEX IF NOT EXISTS idx_queue_session ON queue(session_id, position)")
        db.execute("CREATE INDEX IF NOT EXISTS idx_topic_chat ON topic_bindings(chat_id, message_thread_id)")

        columns_to_add = [
            ("sessions", "chat_id", "INTEGER"),
            ("sessions", "message_thread_id", "INTEGER"),
            ("sessions", "notified_12h", "INTEGER DEFAULT 0"),
            ("sessions", "queue_announced_12h", "INTEGER DEFAULT 0"),
            ("sessions", "queue_announced_1h", "INTEGER DEFAULT 0"),
            ("users", "is_blocked", "INTEGER DEFAULT 0"),
            ("users", "max_seats", "INTEGER DEFAULT 1"),
        ]
        for table, col, col_type in columns_to_add:
            try:
                db.execute(f"ALTER TABLE {table} ADD COLUMN {col} {col_type}")
            except sqlite3.OperationalError:
                pass

        migrate_queue_table(db)
    log(f"\u2705 База данных '{DB_NAME}' инициализирована.")


def register_user(tg_user) -> int:
    with get_db() as db:
        db.execute(
            "INSERT INTO users (telegram_id, username) VALUES (?, ?) "
            "ON CONFLICT(telegram_id) DO UPDATE SET username = excluded.username",
            (tg_user.id, tg_user.username),
        )
        row = db.execute("SELECT id FROM users WHERE telegram_id = ?", (tg_user.id,)).fetchone()
        return row["id"]


def get_user_surname(tg_user_id: int) -> Optional[str]:
    with get_db() as db:
        row = db.execute("SELECT full_name FROM users WHERE telegram_id = ?", (tg_user_id,)).fetchone()
        return row["full_name"] if row else None


def get_user_max_seats(tg_user_id: int) -> int:
    with get_db() as db:
        row = db.execute("SELECT max_seats FROM users WHERE telegram_id = ?", (tg_user_id,)).fetchone()
        return row["max_seats"] if row and row["max_seats"] else 1


def get_user_seats_count(session_id: int, user_id: int) -> int:
    with get_db() as db:
        row = db.execute(
            "SELECT COUNT(*) as cnt FROM queue WHERE session_id = ? AND user_id = ?",
            (session_id, user_id)
        ).fetchone()
        return row["cnt"] if row else 0


def is_user_blocked(tg_user_id: int) -> bool:
    with get_db() as db:
        row = db.execute("SELECT is_blocked FROM users WHERE telegram_id = ?", (tg_user_id,)).fetchone()
        return row["is_blocked"] == 1 if row else False


def apply_topic_bindings_to_session(db, title: str, session_id: int):
    binding = db.execute(
        "SELECT chat_id, message_thread_id FROM topic_bindings WHERE title = ?", (title,)
    ).fetchone()
    if binding:
        db.execute(
            "UPDATE sessions SET chat_id = ?, message_thread_id = ? WHERE id = ?",
            (binding["chat_id"], binding["message_thread_id"], session_id),
        )


def get_subject_by_topic(chat_id: int, message_thread_id: Optional[int]) -> Optional[str]:
    with get_db() as db:
        if message_thread_id:
            row = db.execute(
                "SELECT title FROM topic_bindings WHERE chat_id = ? AND message_thread_id = ?",
                (chat_id, message_thread_id),
            ).fetchone()
        else:
            row = db.execute(
                "SELECT title FROM topic_bindings WHERE chat_id = ? AND message_thread_id IS NULL",
                (chat_id,),
            ).fetchone()
        return row["title"] if row else None


def _sync_cyclic_sessions_impl():
    now = now_local()
    now_str = now.strftime("%Y-%m-%d %H:%M")

    with get_db() as db:
        templates = db.execute("SELECT * FROM sessions WHERE is_cyclic = 1").fetchall()
        if not templates:
            return

        for day_offset in range(CYCLIC_SESSIONS_DAYS_AHEAD):
            target_date = now + timedelta(days=day_offset)
            weekday = target_date.weekday()
            date_prefix = target_date.strftime("%Y-%m-%d")

            for tpl in templates:
                if tpl["day_of_week"] == weekday:
                    session_dt = f"{date_prefix} {tpl['time_str']}"
                    if session_dt <= now_str:
                        continue
                    exists = db.execute(
                        "SELECT id FROM sessions WHERE is_cyclic = 0 AND title = ? AND datetime = ?",
                        (tpl["title"], session_dt),
                    ).fetchone()
                    if not exists:
                        db.execute(
                            """
                            INSERT INTO sessions (title, datetime, is_cyclic, day_of_week, time_str,
                                                  is_closed, chat_id, message_thread_id)
                            VALUES (?, ?, 0, ?, ?, 0, ?, ?)
                            """,
                            (tpl["title"], session_dt, weekday, tpl["time_str"],
                             tpl["chat_id"], tpl["message_thread_id"]),
                        )
                        new_id = db.execute("SELECT last_insert_rowid()").fetchone()[0]
                        apply_topic_bindings_to_session(db, tpl["title"], new_id)


def sync_cyclic_sessions():
    if not _sync_lock.acquire(blocking=False):
        return
    try:
        _sync_cyclic_sessions_impl()
    except Exception as e:
        log(f"\u26a0 Ошибка в sync_cyclic_sessions: {e}")
    finally:
        _sync_lock.release()


def cleanup_past_sessions():
    try:
        cutoff = (now_local() - timedelta(minutes=SESSION_KEEP_MIN)).strftime("%Y-%m-%d %H:%M")
        with get_db() as db:
            past = db.execute(
                "SELECT id, title, datetime FROM sessions WHERE is_cyclic = 0 AND datetime < ?",
                (cutoff,),
            ).fetchall()
            for s in past:
                log(f"\U0001f5d1 Удаление прошедшей пары '{s['title']}' ({s['datetime']})")
                db.execute("DELETE FROM queue WHERE session_id = ?", (s["id"],))
                db.execute("DELETE FROM sessions WHERE id = ?", (s["id"],))
            if past:
                log(f"\U0001f9f9 Удалено {len(past)} прошедших пар и их очередей")
    except Exception as e:
        log(f"\u26a0 Ошибка в cleanup_past_sessions: {e}")


def get_upcoming_sessions(include_closed=False, subject: str = None, user_id: int = None):
    now_str = now_local().strftime("%Y-%m-%d %H:%M")
    two_weeks_later = (now_local() + timedelta(days=14)).strftime("%Y-%m-%d %H:%M")

    user_check = ""
    params = [now_str, two_weeks_later]

    if user_id:
        user_check = """,
            CASE WHEN EXISTS(
                SELECT 1 FROM queue q2
                JOIN users u2 ON q2.user_id = u2.id
                WHERE q2.session_id = s.id AND u2.telegram_id = ?
            ) THEN 1 ELSE 0 END as user_in_queue"""
        params.insert(0, user_id)

    query = f"""
        SELECT s.*, COUNT(q.id) as queue_count{user_check}
        FROM sessions s
        LEFT JOIN queue q ON s.id = q.session_id
        WHERE s.is_cyclic = 0
          AND s.datetime >= ?
          AND s.datetime <= ?
    """

    if subject:
        query += " AND s.title = ? "
        params.append(subject)

    if not include_closed:
        query += " AND s.is_closed = 0 "

    query += " GROUP BY s.id ORDER BY s.datetime ASC "

    with get_db() as db:
        sessions = db.execute(query, params).fetchall()

    result = []
    for s in sessions:
        session_dict = dict(s)
        if user_id:
            session_dict["user_in_queue"] = session_dict.get("user_in_queue", 0)
        result.append(session_dict)

    return result


def recalculate_positions(session_id: int):
    with get_db() as db:
        rows = db.execute(
            "SELECT id, position FROM queue WHERE session_id = ? ORDER BY position ASC, joined_at ASC, id ASC",
            (session_id,),
        ).fetchall()
        for idx, r in enumerate(rows, start=1):
            if r["position"] != idx:
                db.execute("UPDATE queue SET position = ? WHERE id = ?", (idx, r["id"]))


def format_dt_short(iso_dt: str) -> str:
    try:
        dt = datetime.strptime(iso_dt, "%Y-%m-%d %H:%M")
        return dt.strftime("%d.%m %H:%M")
    except (ValueError, TypeError):
        return iso_dt


def get_day_short(iso_dt: str) -> str:
    try:
        dt = datetime.strptime(iso_dt, "%Y-%m-%d %H:%M")
        return DAYS_SHORT[dt.weekday()]
    except (ValueError, TypeError):
        return ""


def format_dt_with_week(iso_dt: str) -> str:
    try:
        dt = datetime.strptime(iso_dt, "%Y-%m-%d %H:%M")
        diff_days = (dt.date() - now_local().date()).days
        week_tag = "\U0001f4cc Эта неделя" if diff_days < 7 else "\U0001f4c5 Следующая неделя"
        return f"{dt.strftime('%d.%m.%Y %H:%M')} ({week_tag})"
    except (ValueError, TypeError):
        return iso_dt


def generate_queue_text(session_id: int) -> str:
    with get_db() as db:
        s = db.execute("SELECT * FROM sessions WHERE id = ?", (session_id,)).fetchone()
        students = db.execute(
            """
            SELECT q.position, u.full_name
            FROM queue q
            JOIN users u ON q.user_id = u.id
            WHERE q.session_id = ?
            ORDER BY q.position ASC
            """,
            (session_id,),
        ).fetchall()

    if not s:
        return ""

    lines = [
        f"\U0001f4cb <b>Очередь: {s['title']}</b>",
        f"\U0001f552 {format_dt_with_week(s['datetime'])}",
        "",
    ]

    if not students:
        lines.append("<i>Очередь пуста.</i>")
    else:
        for st in students:
            lines.append(f"<b>#{st['position']}</b> — {st['full_name'] or 'Без фамилии'}")

    return "\n".join(lines)


def notify_queue_update(session_id: int, action_text: str = ""):
    try:
        with get_db() as db:
            s = db.execute(
                "SELECT title, chat_id, message_thread_id, datetime, is_closed FROM sessions WHERE id = ?",
                (session_id,),
            ).fetchone()
            if s:
                apply_topic_bindings_to_session(db, s["title"], session_id)
                s = db.execute(
                    "SELECT title, chat_id, message_thread_id, datetime, is_closed FROM sessions WHERE id = ?",
                    (session_id,),
                ).fetchone()

        if not s or not s["chat_id"] or s["is_closed"]:
            return

        session_dt = datetime.strptime(s["datetime"], "%Y-%m-%d %H:%M")
        hours_left = (session_dt - now_local()).total_seconds() / 3600

        if hours_left > 12:
            return

        header = f"\U0001f514 <b>{action_text}</b>\n\n" if action_text else "\U0001f514 <b>Обновление очереди:</b>\n\n"
        send_to_chat(s["chat_id"], header + generate_queue_text(session_id), thread_id=s["message_thread_id"])
    except Exception as e:
        log(f"\u26a0 Ошибка отправки уведомления в чат: {e}")


# =====================================================================
# 4. ИНТЕРФЕЙС И КНОПКИ
# =====================================================================

def main_menu_kb(user_id: int):
    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    kb.row(KeyboardButton(BTN_JOIN))
    kb.row(KeyboardButton(BTN_VIEW))
    kb.row(KeyboardButton(BTN_MY))
    if user_id in ADMIN_IDS:
        kb.row(KeyboardButton(BTN_ADMIN))
    return kb


def admin_menu_kb():
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(
        InlineKeyboardButton("\u2795 Добавить разовое занятие", callback_data="adm_add_once"),
        InlineKeyboardButton("\U0001f504 Добавить циклическую пару", callback_data="adm_add_cyclic"),
        InlineKeyboardButton("\U0001f4cb Управление очередями", callback_data="adm_sessions"),
        InlineKeyboardButton("\u2795 Добавить человека в очередь", callback_data="adm_add_user"),
        InlineKeyboardButton("\U0001f465 Пользователи", callback_data="adm_users"),
        InlineKeyboardButton("\U0001f4c5 Шаблоны", callback_data="adm_templates"),
    )
    return kb


def cancel_kb():
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("\u274c Отмена", callback_data="adm_cancel"))
    return kb


def build_sessions_kb(sessions, user_id: int = None):
    kb = InlineKeyboardMarkup(row_width=1)
    for s in sessions:
        is_closed = s["is_closed"]
        count = s["queue_count"]
        user_in = s.get("user_in_queue", 0) if isinstance(s, dict) else 0

        if is_closed:
            status = "\U0001f512 "
        elif user_in:
            status = "\U0001f7e2 "
        else:
            status = "\U0001f534 "

        count_text = f" ({count})" if count > 0 else ""
        check = "\u2705 " if user_in else ""

        kb.add(InlineKeyboardButton(
            f"{check}{status}{format_dt_short(s['datetime'])} | {s['title']}{count_text}",
            callback_data=f"view_q:{s['id']}",
        ))
    return kb


# =====================================================================
# 5. КОМАНДЫ БОТА
# =====================================================================

@bot.message_handler(commands=["start"])
def handle_start(message):
    if message.chat.type != "private":
        return
    uid = message.from_user.id

    if is_user_blocked(uid):
        bot.send_message(message.chat.id, "\U0001f6ab Вы заблокированы.")
        return

    user_states.pop(uid, None)
    register_user(message.from_user)

    if not get_user_surname(uid):
        user_states[uid] = {"step": "input_surname"}
        bot.send_message(message.chat.id, "\U0001f44b Привет!\nВведите вашу <b>Фамилию</b>:")
        return

    bot.send_message(message.chat.id, f"Рад видеть, <b>{get_user_surname(uid)}</b>!", reply_markup=main_menu_kb(uid))


@bot.message_handler(commands=["admin"])
@bot.message_handler(func=lambda m: m.from_user is not None and m.text == BTN_ADMIN)
def handle_admin(message):
    if message.chat.type != "private" or message.from_user.id not in ADMIN_IDS:
        return
    if is_user_blocked(message.from_user.id):
        return
    user_states.pop(message.from_user.id, None)
    bot.send_message(message.chat.id, "<b>\u2699\ufe0f Админ-панель:</b>", reply_markup=admin_menu_kb())


@bot.message_handler(commands=["bind"])
def handle_bind_command(message):
    if message.from_user.id not in ADMIN_IDS:
        return
    if is_user_blocked(message.from_user.id):
        return
    with get_db() as db:
        rows = db.execute("SELECT MIN(id) AS id, title FROM sessions GROUP BY title ORDER BY title ASC").fetchall()
    thread_id = getattr(message, "message_thread_id", None)
    if not rows:
        send_to_chat(message.chat.id, "\U0001f4ed Сначала создайте пару в ЛС.", thread_id=thread_id)
        return
    kb = InlineKeyboardMarkup(row_width=1)
    for r in rows:
        kb.add(InlineKeyboardButton(f"\U0001f4cc Привязать к: {r['title']}", callback_data=f"bind_subj:{r['id']}"))
    send_to_chat(message.chat.id, "\U0001f4cd Выберите предмет:", thread_id=thread_id, reply_markup=kb)


@bot.message_handler(commands=["join", "zanyat"])
@bot.message_handler(func=lambda m: m.from_user is not None and m.text == BTN_JOIN)
def handle_join_command(message):
    uid = message.from_user.id
    if is_user_blocked(uid):
        return
    surname = get_user_surname(uid)
    thread_id = getattr(message, "message_thread_id", None)
    is_group = message.chat.type != "private"

    if not surname:
        bot_msg = send_to_chat(message.chat.id, "\u26a0 Сначала зарегистрируйтесь у бота в ЛС (/start).", thread_id=thread_id)
        if is_group:
            delete_messages_after_delay(message.chat.id, message.message_id, bot_msg.message_id)
        return

    chat_id = message.chat.id
    if is_group:
        subject = get_subject_by_topic(chat_id, thread_id)
        if not subject:
            bot_msg = send_to_chat(chat_id, "\u26a0 Эта тема не привязана к предмету. Админ может привязать через /bind.", thread_id=thread_id)
            delete_messages_after_delay(chat_id, message.message_id, bot_msg.message_id)
            return
        sessions = get_upcoming_sessions(include_closed=False, subject=subject, user_id=uid)
    else:
        sessions = get_upcoming_sessions(include_closed=False, subject=None, user_id=uid)

    if not sessions:
        bot_msg = send_to_chat(chat_id, "\U0001f4ed Нет открытых пар для записи.", thread_id=thread_id)
        if is_group:
            delete_messages_after_delay(chat_id, message.message_id, bot_msg.message_id)
        return

    kb = InlineKeyboardMarkup(row_width=1)
    for s in sessions:
        is_closed = s["is_closed"]
        count = s["queue_count"]
        user_in = s.get("user_in_queue", 0) if isinstance(s, dict) else 0

        check = "\u2705 " if user_in else ""
        lock = "\U0001f512 " if is_closed else ""
        count_text = f" ({count})" if count > 0 else ""
        day = get_day_short(s["datetime"])

        kb.add(InlineKeyboardButton(
            f"{check}{lock}{day} {format_dt_short(s['datetime'])} | {s['title']}{count_text}",
            callback_data=f"join:{s['id']}:{uid}",
        ))

    bot_msg = send_to_chat(chat_id, f"<b>{surname}, выберите занятие:</b>", thread_id=thread_id, reply_markup=kb)

    if is_group:
        delete_messages_after_delay(chat_id, message.message_id, bot_msg.message_id)


@bot.message_handler(func=lambda m: m.from_user is not None and m.text == BTN_VIEW and m.chat.type == "private")
def handle_view_all_queues(message):
    if is_user_blocked(message.from_user.id):
        return
    uid = message.from_user.id
    sessions = get_upcoming_sessions(include_closed=False, user_id=uid)
    if not sessions:
        bot.send_message(message.chat.id, "\U0001f4ed Нет предстоящих занятий.")
        return

    lines = ["\U0001f465 <b>Все очереди:</b>\n"]
    for s in sessions:
        is_closed = s["is_closed"]
        user_in = s.get("user_in_queue", 0) if isinstance(s, dict) else 0

        if is_closed:
            status = "\U0001f512"
        elif user_in:
            status = "\U0001f7e2"
        else:
            status = "\U0001f534"

        day = get_day_short(s["datetime"])
        lines.append(f"{status} <b>{day} {format_dt_short(s['datetime'])} | {s['title']}</b>")

        with get_db() as db:
            students = db.execute(
                "SELECT q.position, u.full_name FROM queue q JOIN users u ON q.user_id = u.id WHERE q.session_id = ? ORDER BY q.position ASC",
                (s["id"],),
            ).fetchall()

        if students:
            for st in students:
                lines.append(f"  #{st['position']} — {st['full_name'] or 'Без фамилии'}")
        else:
            lines.append("  <i>Пусто</i>")
        lines.append("")

    bot.send_message(message.chat.id, "\n".join(lines))


@bot.message_handler(commands=["queue", "list"])
def handle_group_queue_command(message):
    if is_user_blocked(message.from_user.id):
        return
    chat_id = message.chat.id
    thread_id = getattr(message, "message_thread_id", None)
    is_group = message.chat.type != "private"

    subject = get_subject_by_topic(chat_id, thread_id)
    if not subject:
        bot_msg = send_to_chat(chat_id, "\u26a0 Эта тема не привязана к предмету (/bind).", thread_id=thread_id)
        if is_group:
            delete_messages_after_delay(chat_id, message.message_id, bot_msg.message_id)
        return

    sessions = get_upcoming_sessions(include_closed=False, subject=subject)
    if not sessions:
        bot_msg = send_to_chat(chat_id, f"\U0001f4ed По предмету <b>{subject}</b> нет активных пар.", thread_id=thread_id)
        if is_group:
            delete_messages_after_delay(chat_id, message.message_id, bot_msg.message_id)
        return

    send_to_chat(chat_id, generate_queue_text(sessions[0]["id"]), thread_id=thread_id)
    log(f"\U0001f4cb Очередь отправлена в группу: {subject}")


# =====================================================================
# 6. "МОИ ОЧЕРЕДИ" — ВЫШЕ FSM
# =====================================================================

@bot.message_handler(func=lambda m: m.from_user is not None and m.text == BTN_MY and m.chat.type == "private")
def handle_my_queues(message):
    if is_user_blocked(message.from_user.id):
        return
    db_uid = register_user(message.from_user)
    with get_db() as db:
        rows = db.execute(
            """
            SELECT s.id as sid, s.title, s.datetime,
                   MIN(q.position) as min_pos,
                   MAX(q.position) as max_pos,
                   COUNT(q.id) as seats_count
            FROM queue q
            JOIN sessions s ON q.session_id = s.id
            WHERE q.user_id = ?
              AND s.datetime >= ?
            GROUP BY s.id
            ORDER BY s.datetime ASC
            """,
            (db_uid, now_local().strftime("%Y-%m-%d %H:%M")),
        ).fetchall()

    if not rows:
        bot.send_message(message.chat.id, "\U0001f4ed Вы никуда не записаны.")
        return

    lines = ["<b>Ваши записи:</b>\n"]
    for r in rows:
        day = get_day_short(r["datetime"])
        lines.append(f"\U0001f4da <b>{r['title']}</b>")
        lines.append(f"\U0001f552 {day} {format_dt_short(r['datetime'])}")

        if r["seats_count"] == 1:
            lines.append(f"\U0001f4cd Место: <b>#{r['min_pos']}</b>")
        else:
            lines.append(f"\U0001f4cd Места: <b>#{r['min_pos']}-#{r['max_pos']}</b> ({r['seats_count']} шт.)")
        lines.append("")

    kb = InlineKeyboardMarkup(row_width=1)
    for r in rows:
        if r["seats_count"] == 1:
            btn_text = f"\u274c Выйти: {r['title']} ({format_dt_short(r['datetime'])})"
        else:
            btn_text = f"\u274c Выйти: {r['title']} ({format_dt_short(r['datetime'])}, {r['seats_count']} мест)"
        kb.add(InlineKeyboardButton(btn_text, callback_data=f"leave:{r['sid']}"))

    bot.send_message(message.chat.id, "\n".join(lines), reply_markup=kb)


# =====================================================================
# 7. FSM
# =====================================================================

@bot.message_handler(func=lambda m: m.from_user is not None and m.from_user.id in user_states)
def handle_fsm_steps(message):
    if message.chat.type != "private":
        return
    if is_user_blocked(message.from_user.id):
        return
    uid = message.from_user.id
    state = user_states.get(uid)
    if not state:
        return
    text = (message.text or "").strip()

    if text in MENU_TEXTS or text.startswith("/"):
        user_states.pop(uid, None)
        return

    text = html_module.escape(text)

    if state["step"] == "input_surname":
        if not text:
            return
        with get_db() as db:
            db.execute("UPDATE users SET full_name = ? WHERE telegram_id = ?", (text, uid))
        user_states.pop(uid, None)
        bot.send_message(message.chat.id, f"\u2705 Сохранено: <b>{text}</b>", reply_markup=main_menu_kb(uid))
        return

    if state["step"] == "once_title":
        state["title"] = text
        state["step"] = "once_dt"
        bot.send_message(message.chat.id, "Введите дату/время (<code>ДД.ММ.ГГГГ ЧЧ:ММ</code>):", reply_markup=cancel_kb())

    elif state["step"] == "once_dt":
        try:
            iso_dt = datetime.strptime(text, "%d.%m.%Y %H:%M").strftime("%Y-%m-%d %H:%M")
        except ValueError:
            bot.send_message(message.chat.id, "\u26a0 Ошибка формата!", reply_markup=cancel_kb())
            return
        with get_db() as db:
            cur = db.execute("INSERT INTO sessions (title, datetime, is_cyclic) VALUES (?, ?, 0)", (state["title"], iso_dt))
            apply_topic_bindings_to_session(db, state["title"], cur.lastrowid)
        user_states.pop(uid, None)
        bot.send_message(message.chat.id, "\u2705 Занятие создано!", reply_markup=admin_menu_kb())

    elif state["step"] == "cyclic_title":
        state["title"] = text
        state["step"] = "cyclic_day"
        kb = InlineKeyboardMarkup(row_width=2)
        for i in range(0, len(DAYS_OF_WEEK), 2):
            row = [InlineKeyboardButton(DAYS_OF_WEEK[i], callback_data=f"cday:{i}")]
            if i + 1 < len(DAYS_OF_WEEK):
                row.append(InlineKeyboardButton(DAYS_OF_WEEK[i + 1], callback_data=f"cday:{i + 1}"))
            kb.row(*row)
        bot.send_message(message.chat.id, "Выберите день недели:", reply_markup=kb)

    elif state["step"] == "cyclic_time":
        try:
            t_fmt = datetime.strptime(text, "%H:%M").strftime("%H:%M")
        except ValueError:
            bot.send_message(message.chat.id, "\u26a0 Неверно! Введите время (<code>ЧЧ:ММ</code>):", reply_markup=cancel_kb())
            return
        with get_db() as db:
            cur = db.execute("INSERT INTO sessions (title, is_cyclic, day_of_week, time_str) VALUES (?, 1, ?, ?)", (state["title"], state["day"], t_fmt))
            apply_topic_bindings_to_session(db, state["title"], cur.lastrowid)
        user_states.pop(uid, None)
        sync_cyclic_sessions()
        bot.send_message(message.chat.id, "\u2705 Циклическая пара создана!", reply_markup=admin_menu_kb())

    elif state["step"] == "add_user_name":
        if not text:
            return
        state["user_name"] = text
        state["step"] = "add_user_select_session"
        sessions = get_upcoming_sessions(include_closed=False)
        if not sessions:
            bot.send_message(message.chat.id, "\U0001f4ed Нет доступных занятий.", reply_markup=admin_menu_kb())
            user_states.pop(uid, None)
            return
        kb = InlineKeyboardMarkup(row_width=1)
        for s in sessions:
            kb.add(InlineKeyboardButton(
                f"{format_dt_short(s['datetime'])} | {s['title']}",
                callback_data=f"add_user_sess:{s['id']}"
            ))
        bot.send_message(message.chat.id, f"Выберите занятие для <b>{text}</b>:", reply_markup=kb)


# =====================================================================
# 8. ФОНОВЫЙ ПОТОК
# =====================================================================

def background_notification_worker():
    last_cleanup = 0.0
    while True:
        targets = []
        try:
            sync_cyclic_sessions()

            now_ts = time.time()
            if now_ts - last_cleanup >= 300:
                cleanup_past_sessions()
                last_cleanup = now_ts

            now = now_local()
            min_str = now.strftime("%Y-%m-%d %H:%M")
            max_str = (now + timedelta(minutes=60)).strftime("%Y-%m-%d %H:%M")

            with get_db() as db:
                rows = db.execute(
                    """
                    SELECT id, title FROM sessions
                    WHERE is_cyclic = 0 AND is_closed = 0 AND queue_announced_1h = 0
                      AND datetime > ? AND datetime <= ?
                    """,
                    (min_str, max_str),
                ).fetchall()
                for r in rows:
                    apply_topic_bindings_to_session(db, r["title"], r["id"])

                rows = db.execute(
                    """
                    SELECT id, title, chat_id, message_thread_id FROM sessions
                    WHERE is_cyclic = 0 AND is_closed = 0 AND queue_announced_1h = 0
                      AND datetime > ? AND datetime <= ?
                    """,
                    (min_str, max_str),
                ).fetchall()
                targets = [
                    (r["id"], r["title"], r["chat_id"], r["message_thread_id"])
                    for r in rows
                ]
        except Exception as e:
            log(f"\u26a0 Ошибка в фоновом потоке: {e}")

        for sid, title, chat_id, thread_id in targets:
            if not chat_id:
                continue
            text = "\u23f0 <b>Скоро пара! Актуальная очередь:</b>\n\n" + generate_queue_text(sid)
            try:
                send_to_chat(chat_id, text, thread_id=thread_id)
            except Exception as e:
                log(f"\u26a0 Ошибка отправки уведомления для '{title}': {e}")
                continue
            try:
                with get_db() as db:
                    db.execute("UPDATE sessions SET queue_announced_1h = 1 WHERE id = ?", (sid,))
                log(f"\U0001f4e2 Отправлено уведомление для '{title}'")
            except Exception as e:
                log(f"\u26a0 Ошибка отметки queue_announced_1h для '{title}': {e}")

        time.sleep(60)


# =====================================================================
# 9. CALLBACK
# =====================================================================

@bot.callback_query_handler(func=lambda call: True)
def handle_callbacks(call):
    uid = call.from_user.id
    data = call.data or ""

    if is_user_blocked(uid):
        bot.answer_callback_query(call.id, "Вы заблокированы", show_alert=True)
        return

    surname = get_user_surname(uid) or "Студент"

    if data.startswith("bind_subj:"):
        if uid not in ADMIN_IDS:
            bot.answer_callback_query(call.id, "Нет доступа", show_alert=True)
            return
        try:
            sess_id = int(data.split(":", 1)[1])
        except ValueError:
            bot.answer_callback_query(call.id, "Некорректный идентификатор", show_alert=True)
            return
        with get_db() as db:
            row = db.execute("SELECT title FROM sessions WHERE id = ?", (sess_id,)).fetchone()
            if not row:
                bot.answer_callback_query(call.id, "Занятие не найдено", show_alert=True)
                return
            subject = row["title"]
            chat_id = call.message.chat.id
            thread_id = call.message.message_thread_id
            db.execute(
                """
                INSERT INTO topic_bindings (title, chat_id, message_thread_id)
                VALUES (?, ?, ?) ON CONFLICT(title) DO
                UPDATE SET chat_id = excluded.chat_id, message_thread_id = excluded.message_thread_id
                """,
                (subject, chat_id, thread_id),
            )
            db.execute("UPDATE sessions SET chat_id = ?, message_thread_id = ? WHERE title = ?", (chat_id, thread_id, subject))
        bot.answer_callback_query(call.id, "Успешно привязано!")
        bot.edit_message_text(f"\u2705 Тема привязана к <b>{subject}</b>", call.message.chat.id, call.message.message_id)
        return

    if data.startswith("join:"):
        parts = data.split(":")
        sid = int(parts[1])
        if len(parts) > 2 and uid != int(parts[2]):
            bot.answer_callback_query(call.id, "\u26a0 Это меню не для вас.", show_alert=True)
            return

        db_uid = register_user(call.from_user)
        max_seats = get_user_max_seats(uid)

        with get_db() as db:
            s = db.execute("SELECT * FROM sessions WHERE id = ?", (sid,)).fetchone()
        if not s or s["is_closed"]:
            bot.answer_callback_query(call.id, "\U0001f512 Запись закрыта.", show_alert=True)
            return

        current_count = get_user_seats_count(sid, db_uid)

        if current_count >= max_seats:
            bot.answer_callback_query(call.id, f"\u2139 Вы уже заняли {current_count} из {max_seats} мест!", show_alert=True)
            return

        if max_seats == 1:
            with get_db() as db:
                pos = db.execute("SELECT COALESCE(MAX(position), 0) + 1 FROM queue WHERE session_id = ?", (sid,)).fetchone()[0]
                db.execute(
                    "INSERT INTO queue (session_id, user_id, joined_at, position) VALUES (?, ?, ?, ?)",
                    (sid, db_uid, now_local().strftime("%Y-%m-%d %H:%M:%S"), pos),
                )
            bot.answer_callback_query(call.id, f"\u2705 Успешно! Место #{pos}")
            try:
                bot.edit_message_text(f"\u2705 <b>{surname}</b> записан! Место: #{pos}", call.message.chat.id, call.message.message_id)
            except Exception:
                pass
            notify_queue_update(sid, f"\u2795 {surname} занял(а) место #{pos}")
            return

        available = max_seats - current_count
        kb = InlineKeyboardMarkup(row_width=1)
        for seats in range(1, available + 1):
            word = "место" if seats == 1 else ("места" if seats in [2, 3, 4] else "мест")
            kb.add(InlineKeyboardButton(f"\U0001f4cd {seats} {word}", callback_data=f"join_confirm:{sid}:{uid}:{seats}"))
        kb.add(InlineKeyboardButton("\u274c Отмена", callback_data="adm_cancel"))

        bot.edit_message_text(f"<b>{surname}</b>, сколько мест занять? (доступно: {available})", call.message.chat.id, call.message.message_id, reply_markup=kb)
        bot.answer_callback_query(call.id)
        return

    if data.startswith("join_confirm:"):
        parts = data.split(":")
        sid = int(parts[1])
        confirm_uid = int(parts[2])
        seats = int(parts[3])

        if uid != confirm_uid:
            bot.answer_callback_query(call.id, "\u26a0 Это меню не для вас.", show_alert=True)
            return

        db_uid = register_user(call.from_user)
        max_seats = get_user_max_seats(uid)

        with get_db() as db:
            s = db.execute("SELECT * FROM sessions WHERE id = ?", (sid,)).fetchone()
        if not s or s["is_closed"]:
            bot.answer_callback_query(call.id, "\U0001f512 Запись закрыта.", show_alert=True)
            return

        current_count = get_user_seats_count(sid, db_uid)
        if current_count + seats > max_seats:
            bot.answer_callback_query(call.id, f"\u26a0 Нельзя! У вас лимит {max_seats} мест, уже занято {current_count}.", show_alert=True)
            return

        first_pos = 0
        try:
            with get_db() as db:
                max_pos = db.execute("SELECT COALESCE(MAX(position), 0) FROM queue WHERE session_id = ?", (sid,)).fetchone()[0]
                now_str = now_local().strftime("%Y-%m-%d %H:%M:%S")
                first_pos = max_pos + 1
                for i in range(seats):
                    db.execute(
                        "INSERT INTO queue (session_id, user_id, joined_at, position) VALUES (?, ?, ?, ?)",
                        (sid, db_uid, now_str, max_pos + i + 1),
                    )
        except Exception as e:
            bot.answer_callback_query(call.id, f"\u26a0 Ошибка: {e}", show_alert=True)
            return

        word = "место" if seats == 1 else ("места" if seats in [2, 3, 4] else "мест")
        bot.answer_callback_query(call.id, f"\u2705 Успешно! Занято {seats} {word} (с #{first_pos})")
        try:
            bot.edit_message_text(f"\u2705 <b>{surname}</b> записан! Занято мест: {seats} (с #{first_pos})", call.message.chat.id, call.message.message_id)
        except Exception:
            pass
        notify_queue_update(sid, f"\u2795 {surname} занял(а) {seats} {word}")
        return

    if data.startswith("view_q:"):
        sid = int(data.split(":")[1])
        with get_db() as db:
            s = db.execute("SELECT * FROM sessions WHERE id = ?", (sid,)).fetchone()
            students = db.execute(
                "SELECT q.position, u.full_name FROM queue q JOIN users u ON q.user_id = u.id WHERE q.session_id = ? ORDER BY q.position ASC",
                (sid,),
            ).fetchall()
        if not s:
            bot.answer_callback_query(call.id, "Занятие не найдено", show_alert=True)
            return

        status = "\U0001f512 ЗАНЯТО" if s["is_closed"] else "\U0001f7e2 ОТКРЫТО"
        lines = [f"{status}\n", f"<b>{format_dt_short(s['datetime'])} | {s['title']}</b>\n"]

        if students:
            for st in students:
                lines.append(f"#{st['position']} — {st['full_name'] or 'Без фамилии'}")
        else:
            lines.append("<i>Очередь пуста</i>")

        kb = InlineKeyboardMarkup(row_width=1)
        if not s["is_closed"]:
            kb.add(InlineKeyboardButton("\U0001f4dd Занять", callback_data=f"join:{sid}:{uid}"))
        kb.add(InlineKeyboardButton("\u2b05 Назад", callback_data="back_to_view_list"))

        try:
            bot.edit_message_text("\n".join(lines), call.message.chat.id, call.message.message_id, reply_markup=kb)
        except Exception:
            pass
        bot.answer_callback_query(call.id)
        return

    if data == "back_to_view_list":
        sessions = get_upcoming_sessions(include_closed=False, user_id=uid)
        try:
            if sessions:
                bot.edit_message_text("\U0001f465 <b>Просмотр очередей:</b>", call.message.chat.id, call.message.message_id, reply_markup=build_sessions_kb(sessions, user_id=uid))
            else:
                bot.edit_message_text("\U0001f4ed Нет предстоящих занятий.", call.message.chat.id, call.message.message_id)
        except Exception:
            pass
        bot.answer_callback_query(call.id)
        return

    if data.startswith("leave:"):
        sid = int(data.split(":")[1])
        db_uid = register_user(call.from_user)

        seats_count = get_user_seats_count(sid, db_uid)

        if seats_count == 0:
            bot.answer_callback_query(call.id, "Вы не записаны на эту пару.", show_alert=True)
            return

        if seats_count == 1:
            with get_db() as db:
                db.execute("DELETE FROM queue WHERE session_id = ? AND user_id = ?", (sid, db_uid))
            recalculate_positions(sid)
            bot.answer_callback_query(call.id, "Вы вышли из очереди")
            try:
                bot.edit_message_text("\u2705 Вы вышли из очереди.", call.message.chat.id, call.message.message_id)
            except Exception:
                pass
            notify_queue_update(sid, f"\u274c {surname} вышел(-ла) из очереди")
            return

        kb = InlineKeyboardMarkup(row_width=1)
        for seats in range(1, seats_count + 1):
            word = "место" if seats == 1 else ("места" if seats in [2, 3, 4] else "мест")
            kb.add(InlineKeyboardButton(f"Отменить {seats} {word}", callback_data=f"leave_confirm:{sid}:{uid}:{seats}"))
        kb.add(InlineKeyboardButton("\u274c Отмена", callback_data="adm_cancel"))

        bot.edit_message_text(f"<b>{surname}</b>, сколько мест отменить? (у вас занято: {seats_count})", call.message.chat.id, call.message.message_id, reply_markup=kb)
        bot.answer_callback_query(call.id)
        return

    if data.startswith("leave_confirm:"):
        parts = data.split(":")
        sid = int(parts[1])
        confirm_uid = int(parts[2])
        seats_to_remove = int(parts[3])

        if uid != confirm_uid:
            bot.answer_callback_query(call.id, "\u26a0 Это меню не для вас.", show_alert=True)
            return

        db_uid = register_user(call.from_user)

        with get_db() as db:
            db.execute(
                """
                DELETE FROM queue
                WHERE id IN (
                    SELECT id FROM queue
                    WHERE session_id = ? AND user_id = ?
                    ORDER BY position DESC
                    LIMIT ?
                )
                """,
                (sid, db_uid, seats_to_remove),
            )
        recalculate_positions(sid)

        word = "место" if seats_to_remove == 1 else ("места" if seats_to_remove in [2, 3, 4] else "мест")
        bot.answer_callback_query(call.id, f"Отменено {seats_to_remove} {word}")
        try:
            bot.edit_message_text(f"\u2705 Отменено {seats_to_remove} {word}.", call.message.chat.id, call.message.message_id)
        except Exception:
            pass
        notify_queue_update(sid, f"\u274c {surname} отменил(а) {seats_to_remove} {word}")
        return

    if data.startswith("add_user_sess:"):
        if uid not in ADMIN_IDS:
            bot.answer_callback_query(call.id, "Нет доступа", show_alert=True)
            return
        sid = int(data.split(":")[1])
        user_name = user_states.get(uid, {}).get("user_name")
        if not user_name:
            bot.answer_callback_query(call.id, "Ошибка: имя не указано", show_alert=True)
            return

        with get_db() as db:
            user_row = db.execute("SELECT id FROM users WHERE full_name = ? AND telegram_id < 0", (user_name,)).fetchone()

            if not user_row:
                max_neg_id = db.execute("SELECT COALESCE(MIN(telegram_id), 0) FROM users WHERE telegram_id < 0").fetchone()[0]
                new_telegram_id = max_neg_id - 1 if max_neg_id < 0 else -1
                db.execute("INSERT INTO users (telegram_id, full_name) VALUES (?, ?)", (new_telegram_id, user_name))
                user_row = db.execute("SELECT id FROM users WHERE full_name = ?", (user_name,)).fetchone()

            if user_row:
                user_db_id = user_row["id"]
                pos = db.execute("SELECT COALESCE(MAX(position), 0) + 1 FROM queue WHERE session_id = ?", (sid,)).fetchone()[0]
                db.execute(
                    "INSERT INTO queue (session_id, user_id, joined_at, position) VALUES (?, ?, ?, ?)",
                    (sid, user_db_id, now_local().strftime("%Y-%m-%d %H:%M:%S"), pos),
                )
                bot.answer_callback_query(call.id, f"\u2705 {user_name} добавлен на место #{pos}")
            else:
                bot.answer_callback_query(call.id, "\u26a0 Ошибка создания пользователя", show_alert=True)

        user_states.pop(uid, None)
        bot.edit_message_text("\u2705 Пользователь добавлен в очередь.", call.message.chat.id, call.message.message_id, reply_markup=admin_menu_kb())
        return

    if uid not in ADMIN_IDS:
        bot.answer_callback_query(call.id, "Нет доступа", show_alert=True)
        return

    if data in ("adm_cancel", "adm_main"):
        user_states.pop(uid, None)
        try:
            bot.edit_message_text("<b>\u2699\ufe0f Админ-панель:</b>", call.message.chat.id, call.message.message_id, reply_markup=admin_menu_kb())
        except Exception:
            pass
        bot.answer_callback_query(call.id)
        return

    if data == "adm_add_once":
        user_states[uid] = {"step": "once_title"}
        bot.edit_message_text("Введите название занятия:", call.message.chat.id, call.message.message_id, reply_markup=cancel_kb())
        bot.answer_callback_query(call.id)

    elif data == "adm_add_cyclic":
        user_states[uid] = {"step": "cyclic_title"}
        bot.edit_message_text("Введите название регулярной пары:", call.message.chat.id, call.message.message_id, reply_markup=cancel_kb())
        bot.answer_callback_query(call.id)

    elif data == "adm_add_user":
        user_states[uid] = {"step": "add_user_name"}
        bot.edit_message_text("Введите имя пользователя для добавления в очередь:", call.message.chat.id, call.message.message_id, reply_markup=cancel_kb())
        bot.answer_callback_query(call.id)

    elif data.startswith("cday:") and uid in user_states:
        day = int(data.split(":")[1])
        if not 0 <= day <= 6:
            bot.answer_callback_query(call.id, "Неверный день", show_alert=True)
            return
        user_states[uid]["day"] = day
        user_states[uid]["step"] = "cyclic_time"
        bot.edit_message_text("Введите время начала (<code>ЧЧ:ММ</code>):", call.message.chat.id, call.message.message_id, reply_markup=cancel_kb())
        bot.answer_callback_query(call.id)

    elif data == "adm_users":
        _show_users_list(call)
        bot.answer_callback_query(call.id)
        return

    elif data.startswith("user_mgmt:"):
        user_id = int(data.split(":")[1])
        _show_user_mgmt(call, user_id)
        bot.answer_callback_query(call.id)
        return

    elif data.startswith("seat_plus:"):
        user_id = int(data.split(":")[1])
        with get_db() as db:
            u = db.execute("SELECT max_seats FROM users WHERE id = ?", (user_id,)).fetchone()
            current = u["max_seats"] if u else 1
            new_val = min(current + 1, MAX_SEATS_PER_USER)
            db.execute("UPDATE users SET max_seats = ? WHERE id = ?", (new_val, user_id))
        bot.answer_callback_query(call.id, f"Увеличено до {new_val} мест")
        _show_user_mgmt(call, user_id)
        return

    elif data.startswith("seat_minus:"):
        user_id = int(data.split(":")[1])
        with get_db() as db:
            u = db.execute("SELECT max_seats FROM users WHERE id = ?", (user_id,)).fetchone()
            current = u["max_seats"] if u else 1
            new_val = max(current - 1, 1)
            db.execute("UPDATE users SET max_seats = ? WHERE id = ?", (new_val, user_id))
        bot.answer_callback_query(call.id, f"Уменьшено до {new_val}")
        _show_user_mgmt(call, user_id)
        return

    elif data == "noop":
        bot.answer_callback_query(call.id)
        return

    elif data.startswith("user_block:"):
        user_id = int(data.split(":")[1])
        with get_db() as db:
            db.execute("UPDATE users SET is_blocked = 1 WHERE id = ?", (user_id,))
        bot.answer_callback_query(call.id, "Пользователь заблокирован")
        _show_user_mgmt(call, user_id)
        return

    elif data.startswith("user_unblock:"):
        user_id = int(data.split(":")[1])
        with get_db() as db:
            db.execute("UPDATE users SET is_blocked = 0 WHERE id = ?", (user_id,))
        bot.answer_callback_query(call.id, "Пользователь разблокирован")
        _show_user_mgmt(call, user_id)
        return

    elif data.startswith("user_delete:"):
        user_id = int(data.split(":")[1])
        with get_db() as db:
            db.execute("DELETE FROM queue WHERE user_id = ?", (user_id,))
            db.execute("DELETE FROM users WHERE id = ?", (user_id,))
        bot.answer_callback_query(call.id, "Пользователь удалён")
        _show_users_list(call)
        return

    elif data == "adm_templates":
        _show_templates(call)
        bot.answer_callback_query(call.id)
        return

    elif data.startswith("del_tpl:"):
        with get_db() as db:
            db.execute("DELETE FROM sessions WHERE id = ? AND is_cyclic = 1", (int(data.split(":")[1]),))
        bot.answer_callback_query(call.id, "Удалено")
        _show_templates(call)
        return

    elif data == "adm_sessions":
        _show_sessions_list(call)
        bot.answer_callback_query(call.id)
        return

    elif data.startswith("mng:"):
        sid = int(data.split(":")[1])
        _show_session_mgmt(call, sid)
        bot.answer_callback_query(call.id)
        return

    elif data.startswith("kick:"):
        parts = data.split(":")
        sid = int(parts[1])
        target_uid = int(parts[2])
        with get_db() as db:
            tu = db.execute("SELECT full_name FROM users WHERE id = ?", (target_uid,)).fetchone()
            t_surname = tu["full_name"] if tu else "Студент"
            db.execute(
                "DELETE FROM queue WHERE id = (SELECT id FROM queue WHERE session_id = ? AND user_id = ? ORDER BY position DESC LIMIT 1)",
                (sid, target_uid),
            )
        recalculate_positions(sid)
        bot.answer_callback_query(call.id, "Удалён")
        notify_queue_update(sid, f"\U0001f5d1 Админ удалил из очереди: {t_surname}")
        _show_session_mgmt(call, sid)
        return

    elif data.startswith("clear:"):
        sid = int(data.split(":")[1])
        with get_db() as db:
            db.execute("DELETE FROM queue WHERE session_id = ?", (sid,))
        bot.answer_callback_query(call.id, "Очередь очищена")
        notify_queue_update(sid, "\U0001f9f9 Очередь очищена администратором")
        _show_session_mgmt(call, sid)
        return

    elif data.startswith("toggle:"):
        sid = int(data.split(":")[1])
        with get_db() as db:
            db.execute("UPDATE sessions SET is_closed = CASE WHEN is_closed = 1 THEN 0 ELSE 1 END WHERE id = ?", (sid,))
        bot.answer_callback_query(call.id, "Статус изменён")
        _show_session_mgmt(call, sid)
        return

    elif data.startswith("delsess:"):
        sid = int(data.split(":")[1])
        with get_db() as db:
            db.execute("DELETE FROM queue WHERE session_id = ?", (sid,))
            db.execute("DELETE FROM sessions WHERE id = ?", (sid,))
        bot.answer_callback_query(call.id, "Удалено, очередь очищена")
        _show_sessions_list(call)
        return


# =====================================================================
# 10. ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ АДМИНКИ
# =====================================================================

def _show_user_mgmt(call, user_id):
    with get_db() as db:
        u = db.execute("SELECT * FROM users WHERE id = ?", (user_id,)).fetchone()
    if not u:
        bot.answer_callback_query(call.id, "Пользователь не найден", show_alert=True)
        return

    status = "\U0001f6ab Заблокирован" if u["is_blocked"] else "\u2705 Активен"
    name = u["full_name"] or u["username"] or f"ID:{u['telegram_id']}"
    max_seats = u["max_seats"] or 1

    kb = InlineKeyboardMarkup(row_width=1)
    if u["is_blocked"]:
        kb.add(InlineKeyboardButton("\u2705 Разблокировать", callback_data=f"user_unblock:{user_id}"))
    else:
        kb.add(InlineKeyboardButton("\U0001f6ab Заблокировать", callback_data=f"user_block:{user_id}"))

    kb.row(
        InlineKeyboardButton("\u2796 Мест", callback_data=f"seat_minus:{user_id}"),
        InlineKeyboardButton(f"Мест: {max_seats}", callback_data="noop"),
        InlineKeyboardButton("\u2795 Мест", callback_data=f"seat_plus:{user_id}")
    )

    kb.add(InlineKeyboardButton("\U0001f5d1 Удалить", callback_data=f"user_delete:{user_id}"))
    kb.add(InlineKeyboardButton("\u2b05 Назад к списку", callback_data="adm_users"))

    text = f"<b>{name}</b>\nTelegram ID: {u['telegram_id']}\nСтатус: {status}\nМакс. мест: {max_seats}"
    try:
        bot.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=kb)
    except Exception:
        pass


def _show_users_list(call):
    with get_db() as db:
        users = db.execute("SELECT id, telegram_id, full_name, username, is_blocked, max_seats FROM users ORDER BY id DESC LIMIT 50").fetchall()
    kb = InlineKeyboardMarkup(row_width=1)
    for u in users:
        status = "\U0001f6ab" if u["is_blocked"] else "\u2705"
        name = u["full_name"] or u["username"] or f"ID:{u['telegram_id']}"
        seats = u["max_seats"] or 1
        kb.add(InlineKeyboardButton(f"{status} {name} ({seats} мест)", callback_data=f"user_mgmt:{u['id']}"))
    kb.add(InlineKeyboardButton("\u2b05 Назад", callback_data="adm_main"))
    try:
        bot.edit_message_text("<b>\U0001f465 Пользователи:</b>", call.message.chat.id, call.message.message_id, reply_markup=kb)
    except Exception:
        pass


def _show_templates(call):
    with get_db() as db:
        tpls = db.execute("SELECT * FROM sessions WHERE is_cyclic = 1").fetchall()
    kb = InlineKeyboardMarkup(row_width=1)
    for t in tpls:
        kb.add(InlineKeyboardButton(f"\U0001f5d1 {t['title']} ({DAYS_OF_WEEK[t['day_of_week']]} {t['time_str']})", callback_data=f"del_tpl:{t['id']}"))
    kb.add(InlineKeyboardButton("\u2b05 Назад", callback_data="adm_main"))
    try:
        bot.edit_message_text("<b>\U0001f4c5 Шаблоны:</b>", call.message.chat.id, call.message.message_id, reply_markup=kb)
    except Exception:
        pass


def _show_session_mgmt(call, sid):
    with get_db() as db:
        s = db.execute("SELECT * FROM sessions WHERE id = ?", (sid,)).fetchone()
        students = db.execute(
            "SELECT q.position, u.id as uid, u.full_name FROM queue q JOIN users u ON q.user_id = u.id WHERE q.session_id = ? ORDER BY q.position ASC",
            (sid,),
        ).fetchall()
    if not s:
        bot.answer_callback_query(call.id, "Занятие не найдено", show_alert=True)
        return
    lines = [f"\U0001f4da <b>{s['title']}</b> ({format_dt_short(s['datetime'])})\n"]
    kb = InlineKeyboardMarkup(row_width=1)
    for st in students:
        lines.append(f"#{st['position']} | {st['full_name']}")
        kb.add(InlineKeyboardButton(f"\u2705 Удалить #{st['position']}: {st['full_name']}", callback_data=f"kick:{sid}:{st['uid']}"))
    kb.row(InlineKeyboardButton("\U0001f513 Открыть/Закрыть", callback_data=f"toggle:{sid}"), InlineKeyboardButton("\U0001f9f9 Очистить", callback_data=f"clear:{sid}"))
    kb.add(InlineKeyboardButton("\U0001f5d1 Удалить", callback_data=f"delsess:{sid}"), InlineKeyboardButton("\u2b05 Назад", callback_data="adm_sessions"))
    try:
        bot.edit_message_text("\n".join(lines), call.message.chat.id, call.message.message_id, reply_markup=kb)
    except Exception:
        pass


def _show_sessions_list(call):
    sessions = get_upcoming_sessions(include_closed=True)
    kb = InlineKeyboardMarkup(row_width=1)
    for s in sessions:
        status = "\U0001f512" if s["is_closed"] else "\U0001f7e2"
        kb.add(InlineKeyboardButton(f"{status} {format_dt_short(s['datetime'])} | {s['title']} ({s['queue_count']})", callback_data=f"mng:{s['id']}"))
    kb.add(InlineKeyboardButton("\u2b05 Назад", callback_data="adm_main"))
    try:
        bot.edit_message_text("<b>Управление парами:</b>", call.message.chat.id, call.message.message_id, reply_markup=kb)
    except Exception:
        pass


# =====================================================================
# 11. ЗАПУСК
# =====================================================================

if __name__ == "__main__":
    check_already_running()

    try:
        log("\U0001f680 Запуск бота...")
        init_db()
        sync_cyclic_sessions()
        cleanup_past_sessions()

        threading.Thread(target=background_notification_worker, daemon=True).start()
        log("\u23f0 Фоновый сервис уведомлений запущен.")

        me = bot.get_me()
        log(f"\U0001f916 Бот подключен: @{me.username}")

        while True:
            try:
                bot.infinity_polling(skip_pending=True, timeout=60, long_polling_timeout=60)
            except Exception as e:
                error_msg = str(e)
                if "409" in error_msg or "Conflict" in error_msg:
                    log("\u26a0 Конфликт polling! Жду 15 секунд...")
                    time.sleep(15)
                elif "message is not modified" in error_msg:
                    pass
                else:
                    log(f"\u26a0 Ошибка polling: {e}")
                    log("Перезапуск через 5 секунд...")
                    time.sleep(5)
    except Exception as e:
        log(f"\U0001f4a5 ОШИБКА: {e}")
        traceback.print_exc()