from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime
from functools import wraps
from threading import RLock
from typing import Any
from urllib.parse import quote_plus
from urllib.request import Request, urlopen
import asyncio
import html
import json
import logging
import math
import os
import re
import sqlite3
import time

from cachetools import TTLCache, cached
import feedparser
from openai import AsyncOpenAI
import pandas as pd
import pytz
from telegram import Update
from telegram.constants import ParseMode
from telegram.error import BadRequest, Forbidden, NetworkError, RetryAfter, TimedOut
from telegram.ext import Application, CommandHandler, ContextTypes
import yfinance as yf


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
)
logger = logging.getLogger(__name__)


# ============================================================
# CONFIGURATION
# ============================================================

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o").strip()
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
DATABASE_PATH = os.getenv("DATABASE_PATH", "portfolio.db").strip()

raw_allowed_ids = os.getenv("ALLOWED_CHAT_IDS", "")
ALLOWED_CHAT_IDS = {
    item.strip()
    for item in raw_allowed_ids.split(",")
    if item.strip()
}

if not OPENAI_API_KEY:
    raise RuntimeError("OPENAI_API_KEY is missing from environment variables.")
if not TELEGRAM_BOT_TOKEN:
    raise RuntimeError("TELEGRAM_BOT_TOKEN is missing from environment variables.")
if not ALLOWED_CHAT_IDS:
    raise RuntimeError(
        "ALLOWED_CHAT_IDS is missing or empty. "
        "For security, the bot refuses to start in public mode."
    )
if not OPENAI_MODEL:
    raise RuntimeError("OPENAI_MODEL cannot be empty.")


# ============================================================
# CONSTANTS
# ============================================================

MARKET_TIMEZONE = pytz.timezone("America/New_York")
UTC = pytz.UTC

COOLDOWN_TIME = 4
MAX_ALERTS_PER_CHAT = 50
MAX_MESSAGE_LENGTH = 3900

STOCK_CACHE_TTL = 300
ALERT_CACHE_TTL = 30
NEWS_CACHE_TTL = 180
YAHOO_MAX_CONCURRENT_REQUESTS = 4

ALERT_CHECK_INTERVAL_SECONDS = 300
ALERT_LOOKBACK_PERIOD = "1d"
ALERT_INTERVAL = "1m"


# ============================================================
# CACHES, LOCKS, CLIENTS
# ============================================================

stock_cache: TTLCache = TTLCache(maxsize=200, ttl=STOCK_CACHE_TTL)
alert_intraday_cache: TTLCache = TTLCache(maxsize=200, ttl=ALERT_CACHE_TTL)
news_cache: TTLCache = TTLCache(maxsize=100, ttl=NEWS_CACHE_TTL)
cooldowns: TTLCache = TTLCache(maxsize=2000, ttl=10)

stock_cache_lock = RLock()
alert_cache_lock = RLock()
news_cache_lock = RLock()
cooldown_lock = RLock()

yahoo_semaphore = asyncio.Semaphore(YAHOO_MAX_CONCURRENT_REQUESTS)
_ticker_async_locks: dict[str, asyncio.Lock] = {}
_alert_ticker_async_locks: dict[str, asyncio.Lock] = {}

openai_client = AsyncOpenAI(
    api_key=OPENAI_API_KEY,
    timeout=30.0,
    max_retries=2,
)


# ============================================================
# DATABASE
# ============================================================

@contextmanager
def get_db_connection():
    conn = sqlite3.connect(
        DATABASE_PATH,
        timeout=15,
        check_same_thread=False,
    )
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA busy_timeout=15000;")
    conn.execute("PRAGMA foreign_keys=ON;")

    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def table_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return {str(row["name"]) for row in rows}


def init_db() -> None:
    with get_db_connection() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS positions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id TEXT NOT NULL,
                ticker TEXT NOT NULL,
                shares REAL NOT NULL CHECK(shares > 0),
                buy_price REAL NOT NULL CHECK(buy_price > 0),
                total_fees REAL NOT NULL DEFAULT 0 CHECK(total_fees >= 0),
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(chat_id, ticker)
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS alerts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id TEXT NOT NULL,
                ticker TEXT NOT NULL,
                target_price REAL NOT NULL CHECK(target_price > 0),
                condition TEXT NOT NULL CHECK(condition IN ('above', 'below')),
                status TEXT NOT NULL DEFAULT 'active'
                    CHECK(status IN ('active', 'processing')),
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                last_checked_at TEXT,
                UNIQUE(chat_id, ticker, target_price, condition)
            )
            """
        )

        # Safe migrations for databases created by older versions.
        position_columns = table_columns(conn, "positions")
        if "total_fees" not in position_columns:
            conn.execute(
                "ALTER TABLE positions ADD COLUMN total_fees REAL NOT NULL DEFAULT 0"
            )
        if "updated_at" not in position_columns:
            conn.execute("ALTER TABLE positions ADD COLUMN updated_at TEXT")
            conn.execute(
                "UPDATE positions SET updated_at = CURRENT_TIMESTAMP "
                "WHERE updated_at IS NULL"
            )

        alert_columns = table_columns(conn, "alerts")
        if "status" not in alert_columns:
            conn.execute(
                "ALTER TABLE alerts ADD COLUMN status TEXT NOT NULL DEFAULT 'active'"
            )
        if "created_at" not in alert_columns:
            conn.execute("ALTER TABLE alerts ADD COLUMN created_at TEXT")
            conn.execute(
                "UPDATE alerts SET created_at = CURRENT_TIMESTAMP "
                "WHERE created_at IS NULL"
            )
        if "last_checked_at" not in alert_columns:
            conn.execute("ALTER TABLE alerts ADD COLUMN last_checked_at TEXT")

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_positions_chat_id "
            "ON positions(chat_id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_alerts_chat_status "
            "ON alerts(chat_id, status)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_alerts_ticker_status "
            "ON alerts(ticker, status)"
        )


def db_get_positions(chat_id: str) -> list[sqlite3.Row]:
    with get_db_connection() as conn:
        return conn.execute(
            """
            SELECT ticker, shares, buy_price, total_fees
            FROM positions
            WHERE chat_id = ?
            ORDER BY ticker ASC
            """,
            (chat_id,),
        ).fetchall()


def db_get_position(chat_id: str, ticker: str) -> sqlite3.Row | None:
    with get_db_connection() as conn:
        return conn.execute(
            """
            SELECT shares, buy_price, total_fees
            FROM positions
            WHERE chat_id = ? AND ticker = ?
            """,
            (chat_id, ticker),
        ).fetchone()


def db_set_position(
    chat_id: str,
    ticker: str,
    shares: float,
    effective_average_price: float,
    total_fees: float,
) -> None:
    with get_db_connection() as conn:
        conn.execute(
            """
            INSERT INTO positions (
                chat_id, ticker, shares, buy_price, total_fees, updated_at
            )
            VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(chat_id, ticker)
            DO UPDATE SET
                shares = excluded.shares,
                buy_price = excluded.buy_price,
                total_fees = excluded.total_fees,
                updated_at = CURRENT_TIMESTAMP
            """,
            (chat_id, ticker, shares, effective_average_price, total_fees),
        )


def db_add_position(
    chat_id: str,
    ticker: str,
    added_shares: float,
    added_execution_price: float,
    added_fee: float,
) -> tuple[float, float, float]:
    with get_db_connection() as conn:
        conn.execute("BEGIN IMMEDIATE")
        existing = conn.execute(
            """
            SELECT shares, buy_price, total_fees
            FROM positions
            WHERE chat_id = ? AND ticker = ?
            """,
            (chat_id, ticker),
        ).fetchone()

        if existing:
            old_shares = float(existing["shares"])
            old_effective_average = float(existing["buy_price"])
            old_fees = float(existing["total_fees"] or 0)
            old_cost_basis = old_shares * old_effective_average

            new_shares = old_shares + added_shares
            new_cost_basis = (
                old_cost_basis
                + added_shares * added_execution_price
                + added_fee
            )
            new_average = new_cost_basis / new_shares
            new_total_fees = old_fees + added_fee

            conn.execute(
                """
                UPDATE positions
                SET shares = ?, buy_price = ?, total_fees = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE chat_id = ? AND ticker = ?
                """,
                (
                    new_shares,
                    new_average,
                    new_total_fees,
                    chat_id,
                    ticker,
                ),
            )
        else:
            new_shares = added_shares
            new_total_fees = added_fee
            new_average = (
                added_shares * added_execution_price + added_fee
            ) / added_shares
            conn.execute(
                """
                INSERT INTO positions (
                    chat_id, ticker, shares, buy_price, total_fees, updated_at
                )
                VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """,
                (
                    chat_id,
                    ticker,
                    new_shares,
                    new_average,
                    new_total_fees,
                ),
            )

    return new_shares, new_average, new_total_fees


def db_remove_position(chat_id: str, ticker: str) -> int:
    with get_db_connection() as conn:
        cursor = conn.execute(
            "DELETE FROM positions WHERE chat_id = ? AND ticker = ?",
            (chat_id, ticker),
        )
        return cursor.rowcount


def db_create_alert(
    chat_id: str,
    ticker: str,
    target_price: float,
    condition: str,
) -> tuple[bool, str]:
    with get_db_connection() as conn:
        count = conn.execute(
            "SELECT COUNT(*) FROM alerts WHERE chat_id = ?",
            (chat_id,),
        ).fetchone()[0]
        if count >= MAX_ALERTS_PER_CHAT:
            return False, "limit"

        try:
            conn.execute(
                """
                INSERT INTO alerts (
                    chat_id, ticker, target_price, condition,
                    status, created_at, last_checked_at
                )
                VALUES (?, ?, ?, ?, 'active', CURRENT_TIMESTAMP, NULL)
                """,
                (chat_id, ticker, target_price, condition),
            )
        except sqlite3.IntegrityError:
            return False, "duplicate"

    return True, "created"


def db_list_alerts(chat_id: str) -> list[sqlite3.Row]:
    with get_db_connection() as conn:
        return conn.execute(
            """
            SELECT id, ticker, target_price, condition, status, created_at
            FROM alerts
            WHERE chat_id = ?
            ORDER BY id ASC
            """,
            (chat_id,),
        ).fetchall()


def db_remove_alert(chat_id: str, alert_id: int) -> int:
    with get_db_connection() as conn:
        cursor = conn.execute(
            "DELETE FROM alerts WHERE id = ? AND chat_id = ?",
            (alert_id, chat_id),
        )
        return cursor.rowcount


def db_get_active_alerts() -> list[sqlite3.Row]:
    with get_db_connection() as conn:
        return conn.execute(
            """
            SELECT id, chat_id, ticker, target_price, condition,
                   created_at, last_checked_at
            FROM alerts
            WHERE status = 'active'
            ORDER BY ticker, id
            """
        ).fetchall()


def db_update_alert_checked(alert_id: int, checked_at: str) -> None:
    with get_db_connection() as conn:
        conn.execute(
            """
            UPDATE alerts
            SET last_checked_at = ?
            WHERE id = ? AND status = 'active'
            """,
            (checked_at, alert_id),
        )


def db_claim_alert(alert_id: int) -> bool:
    with get_db_connection() as conn:
        cursor = conn.execute(
            """
            UPDATE alerts
            SET status = 'processing'
            WHERE id = ? AND status = 'active'
            """,
            (alert_id,),
        )
        return cursor.rowcount == 1


def db_delete_claimed_alert(alert_id: int) -> None:
    with get_db_connection() as conn:
        conn.execute(
            "DELETE FROM alerts WHERE id = ? AND status = 'processing'",
            (alert_id,),
        )


def db_release_claimed_alert(alert_id: int) -> None:
    with get_db_connection() as conn:
        conn.execute(
            """
            UPDATE alerts
            SET status = 'active'
            WHERE id = ? AND status = 'processing'
            """,
            (alert_id,),
        )


# ============================================================
# GENERAL UTILITIES
# ============================================================

def validate_ticker(ticker: str) -> bool:
    return bool(re.fullmatch(r"[A-Z0-9.\-^=]{1,15}", ticker))


def validate_supported_ticker(ticker: str) -> tuple[bool, str | None]:
    if not validate_ticker(ticker):
        return False, "❌ არასწორი ticker-ის ფორმატი."

    if ticker == "QNT":
        return (
            False,
            "⚠️ QNT ჩვეულებრივ Quant კრიპტოვალუტას აღნიშნავს და არა "
            "Quantinuum-ს.\n\nQuant კრიპტოვალუტისთვის გამოიყენე QNT-USD.\n"
            "Quantinuum ცალკე საჯარო ticker-ით ამჟამად არ ივაჭრება.",
        )

    return True, None


def finite_positive_number(value: str) -> float:
    number = float(value)
    if not math.isfinite(number) or number <= 0:
        raise ValueError
    return number


def finite_nonnegative_number(value: str) -> float:
    number = float(value)
    if not math.isfinite(number) or number < 0:
        raise ValueError
    return number


def check_cooldown(user_id: int, command: str) -> bool:
    now = time.monotonic()
    key = (user_id, command)

    with cooldown_lock:
        previous_time = cooldowns.get(key)
        if previous_time is not None and now - previous_time < COOLDOWN_TIME:
            return False
        cooldowns[key] = now

    return True


def split_plain_text(text: str, max_length: int = MAX_MESSAGE_LENGTH) -> list[str]:
    """Split plain text safely. Do not use this for raw HTML markup."""
    if not text:
        return []
    if len(text) <= max_length:
        return [text]

    chunks: list[str] = []
    remaining = text.strip()

    while len(remaining) > max_length:
        split_at = remaining.rfind("\n\n", 0, max_length)
        if split_at <= 0:
            split_at = remaining.rfind("\n", 0, max_length)
        if split_at <= 0:
            split_at = remaining.rfind(" ", 0, max_length)
        if split_at <= 0:
            split_at = max_length

        chunk = remaining[:split_at].strip()
        if chunk:
            chunks.append(chunk)
        remaining = remaining[split_at:].strip()

    if remaining:
        chunks.append(remaining)
    return chunks


async def send_plain_chunks(message, text: str) -> None:
    for chunk in split_plain_text(text):
        await message.reply_text(chunk, parse_mode=None)


def has_close_data(hist: pd.DataFrame | None) -> bool:
    return (
        hist is not None
        and not hist.empty
        and "Close" in hist.columns
        and not hist["Close"].dropna().empty
    )


def normalize_timestamp_to_market_timezone(timestamp: Any):
    if timestamp is None:
        return None
    try:
        ts = pd.Timestamp(timestamp)
        if ts.tzinfo is None:
            ts = ts.tz_localize(MARKET_TIMEZONE)
        else:
            ts = ts.tz_convert(MARKET_TIMEZONE)
        return ts
    except Exception:
        logger.warning("Could not normalize timestamp: %s", timestamp, exc_info=True)
        return None


def parse_db_timestamp(value: str | None) -> pd.Timestamp | None:
    if not value:
        return None
    try:
        ts = pd.Timestamp(value)
        if ts.tzinfo is None:
            ts = ts.tz_localize(UTC)
        else:
            ts = ts.tz_convert(UTC)
        return ts
    except Exception:
        logger.warning("Could not parse DB timestamp: %s", value, exc_info=True)
        return None


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def get_fallback_previous_close(hist: pd.DataFrame | None) -> float | None:
    if not has_close_data(hist):
        return None
    closes = hist["Close"].dropna()
    if len(closes) >= 2:
        return float(closes.iloc[-2])
    if len(closes) == 1:
        return float(closes.iloc[-1])
    return None


def get_async_lock(lock_map: dict[str, asyncio.Lock], ticker: str) -> asyncio.Lock:
    lock = lock_map.get(ticker)
    if lock is None:
        lock = asyncio.Lock()
        lock_map[ticker] = lock
    return lock


# ============================================================
# FINANCIAL DATA
# ============================================================

def fetch_stock_data_sync(ticker: str):
    try:
        stock = yf.Ticker(ticker)
        hist = stock.history(
            period="3mo",
            interval="1d",
            auto_adjust=False,
            timeout=12,
        )
        intraday = stock.history(
            period="1d",
            interval="1m",
            prepost=True,
            auto_adjust=False,
            timeout=12,
        )

        previous_close = None
        latest_price = None
        quote_timestamp = None
        quote_source = None

        try:
            fast_info = stock.fast_info
            previous_close = getattr(fast_info, "previous_close", None)
            latest_price = getattr(fast_info, "last_price", None)
            if latest_price is not None:
                quote_source = "Yahoo fast_info"
        except Exception:
            logger.warning("fast_info unavailable for %s", ticker, exc_info=True)

        if intraday is not None and not intraday.empty and "Close" in intraday.columns:
            clean_intraday = intraday["Close"].dropna()
            if not clean_intraday.empty:
                latest_price = float(clean_intraday.iloc[-1])
                quote_timestamp = normalize_timestamp_to_market_timezone(
                    clean_intraday.index[-1]
                )
                quote_source = "Yahoo intraday history"

        if previous_close is None:
            previous_close = get_fallback_previous_close(hist)

        if latest_price is None and has_close_data(hist):
            clean_daily = hist["Close"].dropna()
            latest_price = float(clean_daily.iloc[-1])
            quote_timestamp = normalize_timestamp_to_market_timezone(
                clean_daily.index[-1]
            )
            quote_source = "Yahoo daily history"

        previous_close = (
            float(previous_close) if previous_close is not None else None
        )
        latest_price = float(latest_price) if latest_price is not None else None

        change_pct = None
        if previous_close and latest_price is not None:
            change_pct = ((latest_price - previous_close) / previous_close) * 100

        return (
            {
                "regular_close": previous_close,
                "latest_price": latest_price,
                "change_pct_from_previous_close": change_pct,
                "quote_timestamp": quote_timestamp,
                "quote_source": quote_source,
            },
            hist,
        )
    except Exception:
        logger.exception("Error fetching data for %s", ticker)
        return None, None


def fetch_alert_intraday_sync(ticker: str) -> pd.DataFrame | None:
    try:
        stock = yf.Ticker(ticker)
        data = stock.history(
            period=ALERT_LOOKBACK_PERIOD,
            interval=ALERT_INTERVAL,
            prepost=True,
            auto_adjust=False,
            timeout=12,
        )
        if data is None or data.empty:
            return None
        required = {"High", "Low", "Close"}
        if not required.issubset(set(data.columns)):
            return None

        clean = data[["High", "Low", "Close"]].dropna(how="all").copy()
        if clean.empty:
            return None
        return clean
    except Exception:
        logger.exception("Error fetching alert intraday data for %s", ticker)
        return None


@cached(cache=stock_cache, lock=stock_cache_lock)
def get_cached_stock_data(ticker: str):
    return fetch_stock_data_sync(ticker)


@cached(cache=alert_intraday_cache, lock=alert_cache_lock)
def get_cached_alert_intraday(ticker: str):
    return fetch_alert_intraday_sync(ticker)


async def fetch_stock_data(ticker: str):
    lock = get_async_lock(_ticker_async_locks, ticker)
    async with lock:
        async with yahoo_semaphore:
            return await asyncio.to_thread(get_cached_stock_data, ticker)


async def fetch_alert_intraday(ticker: str):
    lock = get_async_lock(_alert_ticker_async_locks, ticker)
    async with lock:
        async with yahoo_semaphore:
            return await asyncio.to_thread(get_cached_alert_intraday, ticker)


# ============================================================
# TECHNICAL INDICATORS
# ============================================================

def calculate_rsi(prices: pd.Series, window: int = 14) -> float | None:
    if prices is None:
        return None
    try:
        prices = prices.astype(float).dropna()
        if len(prices) < window + 1:
            return None

        delta = prices.diff()
        gain = delta.clip(lower=0)
        loss = -delta.clip(upper=0)

        avg_gain = gain.ewm(
            alpha=1 / window,
            min_periods=window,
            adjust=False,
        ).mean()
        avg_loss = loss.ewm(
            alpha=1 / window,
            min_periods=window,
            adjust=False,
        ).mean()

        last_gain = float(avg_gain.iloc[-1])
        last_loss = float(avg_loss.iloc[-1])
        if math.isnan(last_gain) or math.isnan(last_loss):
            return None
        if last_loss == 0:
            return 50.0 if last_gain == 0 else 100.0

        rs = last_gain / last_loss
        return round(float(100 - (100 / (1 + rs))), 1)
    except Exception:
        logger.exception("Error calculating RSI")
        return None


def calculate_technical_indicators(hist: pd.DataFrame | None):
    if not has_close_data(hist):
        return None, None, None
    try:
        close = hist["Close"].astype(float).dropna()
        sma20 = float(close.rolling(20).mean().iloc[-1]) if len(close) >= 20 else None
        sma50 = float(close.rolling(50).mean().iloc[-1]) if len(close) >= 50 else None
        returns = close.pct_change().dropna()
        volatility = (
            float(returns.std() * math.sqrt(252) * 100)
            if not returns.empty
            else None
        )
        return sma20, sma50, volatility
    except Exception:
        logger.exception("Error calculating technical indicators")
        return None, None, None


# ============================================================
# ACCESS CONTROL
# ============================================================

def restricted_access(func):
    @wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        chat = update.effective_chat
        if not chat:
            return

        chat_id = str(chat.id)
        if chat_id not in ALLOWED_CHAT_IDS:
            if update.effective_message:
                await update.effective_message.reply_text(
                    "⛔ ამ ბოტზე წვდომა შეზღუდულია შენი ჩატისთვის."
                )
            logger.warning("Unauthorized access attempt from chat_id=%s", chat_id)
            return

        return await func(update, context)

    return wrapper


# ============================================================
# BACKGROUND ALERT CHECKER
# ============================================================

def normalize_intraday_index_to_utc(data: pd.DataFrame) -> pd.DataFrame:
    result = data.copy()
    index = pd.DatetimeIndex(result.index)
    if index.tz is None:
        index = index.tz_localize(MARKET_TIMEZONE)
    index = index.tz_convert(UTC)
    result.index = index
    return result


async def check_price_alerts_job(context: ContextTypes.DEFAULT_TYPE):
    logger.info("Running background price alert check")
    alerts = await asyncio.to_thread(db_get_active_alerts)
    if not alerts:
        return

    unique_tickers = sorted({str(row["ticker"]) for row in alerts})
    results = await asyncio.gather(
        *(fetch_alert_intraday(ticker) for ticker in unique_tickers),
        return_exceptions=True,
    )

    intraday_map: dict[str, pd.DataFrame | None] = {}
    for ticker, result in zip(unique_tickers, results):
        if isinstance(result, Exception):
            logger.error("Alert request failed for %s: %s", ticker, result)
            intraday_map[ticker] = None
        else:
            intraday_map[ticker] = result

    checked_at = utc_now_iso()
    checked_at_ts = pd.Timestamp(checked_at)

    for alert in alerts:
        alert_id = int(alert["id"])
        ticker = str(alert["ticker"])
        data = intraday_map.get(ticker)
        if data is None or data.empty:
            continue

        try:
            data_utc = normalize_intraday_index_to_utc(data)
            created_at = parse_db_timestamp(alert["created_at"])
            last_checked_at = parse_db_timestamp(alert["last_checked_at"])
            since = last_checked_at or created_at

            window = data_utc
            if since is not None:
                window = data_utc[data_utc.index > since]

            # If Yahoo returned no newer bars, mark the check and continue.
            if window.empty:
                await asyncio.to_thread(db_update_alert_checked, alert_id, checked_at)
                continue

            target = float(alert["target_price"])
            condition = str(alert["condition"])
            high = float(window["High"].dropna().max())
            low = float(window["Low"].dropna().min())
            latest = float(window["Close"].dropna().iloc[-1])
            last_bar_time = window.index[-1]

            triggered = (
                condition == "above" and high >= target
            ) or (
                condition == "below" and low <= target
            )

            if not triggered:
                await asyncio.to_thread(db_update_alert_checked, alert_id, checked_at)
                continue

            claimed = await asyncio.to_thread(db_claim_alert, alert_id)
            if not claimed:
                continue

            observed = high if condition == "above" else low
            market_time = last_bar_time.tz_convert(MARKET_TIMEZONE)
            text = (
                "🚨 ფასის ალერტი\n\n"
                f"{ticker}\n"
                f"პირობა: {condition} ${target:,.2f}\n"
                f"ინტერვალში დაფიქსირებული {'მაქსიმუმი' if condition == 'above' else 'მინიმუმი'}: "
                f"${observed:,.2f}\n"
                f"ბოლო მიღებული ფასი: ${latest:,.2f}\n"
                f"მონაცემის დრო: {market_time.strftime('%Y-%m-%d %H:%M:%S %Z')}\n\n"
                "⚠️ Yahoo/yfinance მონაცემი შეიძლება დაგვიანებული ან არასრული იყოს "
                "და არ წარმოადგენს ოფიციალურ real-time საბირჟო feed-ს."
            )

            try:
                await context.bot.send_message(
                    chat_id=alert["chat_id"],
                    text=text,
                    parse_mode=None,
                )
                await asyncio.to_thread(db_delete_claimed_alert, alert_id)
            except Forbidden:
                logger.warning(
                    "Bot cannot message chat_id=%s; deleting alert id=%s",
                    alert["chat_id"],
                    alert_id,
                )
                await asyncio.to_thread(db_delete_claimed_alert, alert_id)
            except (RetryAfter, TimedOut, NetworkError):
                logger.exception("Temporary Telegram failure for alert id=%s", alert_id)
                await asyncio.to_thread(db_release_claimed_alert, alert_id)
            except Exception:
                logger.exception("Failed to send alert id=%s", alert_id)
                await asyncio.to_thread(db_release_claimed_alert, alert_id)
        except Exception:
            logger.exception("Failed to process alert id=%s", alert_id)


# ============================================================
# COMMANDS
# ============================================================

@restricted_access
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    message = update.effective_message
    if not user or not message or not check_cooldown(user.id, "start"):
        return
    await message.reply_text(
        "👋 გამარჯობა!\n\n"
        "მე ვარ შენი ფინანსური ასისტენტი ბოტი.\n"
        "გამოიყენე /help ბრძანება შესაძლებლობების სანახავად."
    )


@restricted_access
async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    message = update.effective_message
    if not user or not message or not check_cooldown(user.id, "help"):
        return

    text = (
        "🤖 ბოტის ბრძანებები\n\n"
        "/portfolio — პორტფელი და P/L\n\n"
        "/position TICKER shares price [fees]\n"
        "პოზიციის სრული დამატება ან ჩანაცვლება. price არის შესრულების ფასი; "
        "fees სურვილისამებრ.\n\n"
        "/addposition TICKER shares price [fees]\n"
        "აქციების დამატება საშუალო თვითღირებულების ავტომატური გამოთვლით.\n\n"
        "/remove TICKER — პოზიციის წაშლა\n\n"
        "/rsi TICKER — RSI, SMA და ვოლატილობა\n\n"
        "/ai TICKER — AI ტექნიკური და რისკის ანალიზი\n\n"
        "/alert TICKER above/below price — ფასის ალერტი\n\n"
        "/alerts — აქტიური ალერტები\n\n"
        "/removealert ID — ალერტის წაშლა\n\n"
        "/concentration — პორტფელის კონცენტრაცია\n\n"
        "/news TICKER — Yahoo Finance RSS სათაურები\n\n"
        "⚠️ Yahoo/yfinance მონაცემები შეიძლება დაგვიანებული ან არასრული იყოს "
        "და არ წარმოადგენს ოფიციალურ real-time საბირჟო feed-ს."
    )
    await send_plain_chunks(message, text)


@restricted_access
async def position_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    message = update.effective_message
    if not user or not message or not check_cooldown(user.id, "position"):
        return

    if len(context.args) not in {3, 4}:
        await message.reply_text(
            "გამოყენება:\n/position TICKER ოდენობა ფასი [საკომისიო]\n\n"
            "მაგალითი:\n/position NVDA 10 120.5 4.95\n\n"
            "⚠️ ეს ბრძანება არსებულ პოზიციას მთლიანად ჩაანაცვლებს."
        )
        return

    ticker = context.args[0].upper().strip()
    valid, error = validate_supported_ticker(ticker)
    if not valid:
        await message.reply_text(error or "❌ არასწორი ticker.")
        return

    try:
        shares = finite_positive_number(context.args[1])
        execution_price = finite_positive_number(context.args[2])
        fees = finite_nonnegative_number(context.args[3]) if len(context.args) == 4 else 0.0
    except (TypeError, ValueError):
        await message.reply_text(
            "❌ ოდენობა და ფასი უნდა იყოს ნულზე მეტი, ხოლო საკომისიო — "
            "ნულზე მეტი ან ტოლი რიცხვი."
        )
        return

    effective_average = (shares * execution_price + fees) / shares
    chat_id = str(update.effective_chat.id)
    await asyncio.to_thread(
        db_set_position,
        chat_id,
        ticker,
        shares,
        effective_average,
        fees,
    )

    await message.reply_text(
        "✅ პოზიცია შენახულია\n\n"
        f"{ticker}\n"
        f"რაოდენობა: {shares:g}\n"
        f"შესრულების ფასი: ${execution_price:,.2f}\n"
        f"საკომისიო: ${fees:,.2f}\n"
        f"ეფექტური საშუალო თვითღირებულება: ${effective_average:,.4f}"
    )


@restricted_access
async def add_position_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    message = update.effective_message
    if not user or not message or not check_cooldown(user.id, "addposition"):
        return

    if len(context.args) not in {3, 4}:
        await message.reply_text(
            "გამოყენება:\n/addposition TICKER ოდენობა ფასი [საკომისიო]\n\n"
            "მაგალითი:\n/addposition NVDA 20 180 4.95"
        )
        return

    ticker = context.args[0].upper().strip()
    valid, error = validate_supported_ticker(ticker)
    if not valid:
        await message.reply_text(error or "❌ არასწორი ticker.")
        return

    try:
        added_shares = finite_positive_number(context.args[1])
        added_price = finite_positive_number(context.args[2])
        added_fee = finite_nonnegative_number(context.args[3]) if len(context.args) == 4 else 0.0
    except (TypeError, ValueError):
        await message.reply_text("❌ არგუმენტები სწორ დადებით რიცხვებად მიუთითე.")
        return

    chat_id = str(update.effective_chat.id)
    new_shares, new_average, total_fees = await asyncio.to_thread(
        db_add_position,
        chat_id,
        ticker,
        added_shares,
        added_price,
        added_fee,
    )

    await message.reply_text(
        "✅ პოზიციას დაემატა ახალი აქციები\n\n"
        f"{ticker}\n"
        f"დამატებული რაოდენობა: {added_shares:g}\n"
        f"შესრულების ფასი: ${added_price:,.2f}\n"
        f"ამ ოპერაციის საკომისიო: ${added_fee:,.2f}\n\n"
        f"სრული რაოდენობა: {new_shares:g}\n"
        f"ახალი ეფექტური საშუალო: ${new_average:,.4f}\n"
        f"ჯამური აღრიცხული საკომისიო: ${total_fees:,.2f}"
    )


@restricted_access
async def portfolio_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    message = update.effective_message
    if not user or not message or not check_cooldown(user.id, "portfolio"):
        return

    chat_id = str(update.effective_chat.id)
    positions = await asyncio.to_thread(db_get_positions, chat_id)
    if not positions:
        await message.reply_text("📭 შენი პორტფელი ცარიელია.")
        return

    tickers = [str(row["ticker"]) for row in positions]
    results = await asyncio.gather(
        *(fetch_stock_data(ticker) for ticker in tickers),
        return_exceptions=True,
    )

    data_map = {}
    for ticker, result in zip(tickers, results):
        data_map[ticker] = (None, None) if isinstance(result, Exception) else result

    lines = ["📋 შენი პორტფელი", ""]
    total_cost_basis = 0.0
    priced_cost_basis = 0.0
    total_value = 0.0
    total_fees = 0.0
    missing: list[str] = []
    timestamps: list[pd.Timestamp] = []

    for row in positions:
        ticker = str(row["ticker"])
        shares = float(row["shares"])
        average = float(row["buy_price"])
        fees = float(row["total_fees"] or 0)
        cost_basis = shares * average

        total_cost_basis += cost_basis
        total_fees += fees
        price_data, _ = data_map.get(ticker, (None, None))
        current_price = price_data.get("latest_price") if price_data else None

        lines.extend([
            f"• {ticker}",
            f"  რაოდენობა: {shares:g}",
            f"  ეფექტური საშუალო: ${average:,.4f}",
            f"  აღრიცხული საკომისიო: ${fees:,.2f}",
        ])

        if current_price is None:
            missing.append(ticker)
            lines.extend(["  მიმდინარე ფასი: მიუწვდომელია", ""])
            continue

        current_price = float(current_price)
        value = shares * current_price
        pnl = value - cost_basis
        pnl_pct = pnl / cost_basis * 100 if cost_basis > 0 else 0.0

        priced_cost_basis += cost_basis
        total_value += value

        timestamp = normalize_timestamp_to_market_timezone(
            price_data.get("quote_timestamp")
        )
        if timestamp is not None:
            timestamps.append(timestamp)

        lines.extend([
            f"  მიმდინარე ფასი: ${current_price:,.2f}",
            f"  მიმდინარე ღირებულება: ${value:,.2f}",
            f"  P/L: ${pnl:,.2f} ({pnl_pct:+.2f}%)",
            "",
        ])

    total_pnl = total_value - priced_cost_basis
    total_pnl_pct = (
        total_pnl / priced_cost_basis * 100 if priced_cost_basis > 0 else 0.0
    )

    lines.extend([
        "━━━━━━━━━━━━━━━━━━",
        f"სრული თვითღირებულება: ${total_cost_basis:,.2f}",
        f"ჯამური აღრიცხული საკომისიო: ${total_fees:,.2f}",
        f"ფასდადგენილი პოზიციების ღირებულება: ${total_value:,.2f}",
        f"ფასდადგენილი პოზიციების P/L: ${total_pnl:,.2f} ({total_pnl_pct:+.2f}%)",
    ])

    if missing:
        lines.extend(["", "⚠️ ფასი ვერ მოიძებნა: " + ", ".join(missing)])
    if timestamps:
        latest_ts = max(timestamps)
        lines.extend([
            "",
            "🕒 უახლესი ხელმისაწვდომი timestamp: "
            + latest_ts.strftime("%Y-%m-%d %H:%M:%S %Z"),
        ])

    lines.extend([
        "",
        "⚠️ Yahoo/yfinance მონაცემები შეიძლება დაგვიანებული ან არასრული იყოს "
        "და არ წარმოადგენს ოფიციალურ real-time feed-ს.",
    ])
    await send_plain_chunks(message, "\n".join(lines))


@restricted_access
async def remove_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    message = update.effective_message
    if not user or not message or not check_cooldown(user.id, "remove"):
        return

    if len(context.args) != 1:
        await message.reply_text("გამოყენება:\n/remove TICKER")
        return

    ticker = context.args[0].upper().strip()
    if not validate_ticker(ticker):
        await message.reply_text("❌ არასწორი ticker-ის ფორმატი.")
        return

    rowcount = await asyncio.to_thread(
        db_remove_position,
        str(update.effective_chat.id),
        ticker,
    )
    await message.reply_text(
        f"✅ პოზიცია {ticker} წარმატებით წაიშალა."
        if rowcount
        else f"❌ პოზიცია {ticker} ვერ მოიძებნა."
    )


@restricted_access
async def alert_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    message = update.effective_message
    if not user or not message or not check_cooldown(user.id, "alert"):
        return

    if len(context.args) != 3:
        await message.reply_text(
            "გამოყენება:\n/alert TICKER above/below ფასი\n\n"
            "მაგალითები:\n/alert NVDA above 200\n/alert NVDA below 180"
        )
        return

    ticker = context.args[0].upper().strip()
    valid, error = validate_supported_ticker(ticker)
    if not valid:
        await message.reply_text(error or "❌ არასწორი ticker.")
        return

    condition = context.args[1].lower().strip()
    if condition not in {"above", "below"}:
        await message.reply_text("❌ მეორე არგუმენტი უნდა იყოს above ან below.")
        return

    try:
        target = finite_positive_number(context.args[2])
    except (TypeError, ValueError):
        await message.reply_text("❌ სამიზნე ფასი დადებითი რიცხვი უნდა იყოს.")
        return

    created, reason = await asyncio.to_thread(
        db_create_alert,
        str(update.effective_chat.id),
        ticker,
        target,
        condition,
    )

    if not created and reason == "limit":
        await message.reply_text(
            f"⚠️ მაქსიმუმ {MAX_ALERTS_PER_CHAT} აქტიური ალერტი შეიძლება გქონდეს."
        )
        return
    if not created and reason == "duplicate":
        await message.reply_text("⚠️ ზუსტად ასეთი ალერტი უკვე არსებობს.")
        return

    await message.reply_text(
        "✅ ფასის ალერტი შეიქმნა\n\n"
        f"{ticker}\nპირობა: {condition}\nსამიზნე: ${target:,.2f}\n\n"
        f"ალერტები დაახლოებით ყოველ {ALERT_CHECK_INTERVAL_SECONDS // 60} წუთში მოწმდება. "
        "ბოტი ამოწმებს ბოლო შემოწმების შემდეგ მიღებულ 1-წუთიან High/Low მონაცემებს."
    )


@restricted_access
async def list_alerts_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    message = update.effective_message
    if not user or not message or not check_cooldown(user.id, "alerts"):
        return

    alerts = await asyncio.to_thread(
        db_list_alerts,
        str(update.effective_chat.id),
    )
    if not alerts:
        await message.reply_text("📭 აქტიური ალერტები არ გაქვს.")
        return

    lines = ["📋 აქტიური ალერტები", ""]
    for row in alerts:
        lines.extend([
            f"• ID: {row['id']}",
            f"  ticker: {row['ticker']}",
            f"  პირობა: {row['condition']}",
            f"  ფასი: ${float(row['target_price']):,.2f}",
            f"  სტატუსი: {row['status']}",
            "",
        ])
    await send_plain_chunks(message, "\n".join(lines))


@restricted_access
async def remove_alert_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    message = update.effective_message
    if not user or not message or not check_cooldown(user.id, "removealert"):
        return

    if len(context.args) != 1:
        await message.reply_text("გამოყენება:\n/removealert ID")
        return

    try:
        alert_id = int(context.args[0])
        if alert_id <= 0:
            raise ValueError
    except ValueError:
        await message.reply_text("❌ ID დადებითი მთელი რიცხვი უნდა იყოს.")
        return

    rowcount = await asyncio.to_thread(
        db_remove_alert,
        str(update.effective_chat.id),
        alert_id,
    )
    await message.reply_text(
        f"✅ ალერტი #{alert_id} წარმატებით წაიშალა."
        if rowcount
        else f"❌ ალერტი #{alert_id} ვერ მოიძებნა."
    )


@restricted_access
async def concentration_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    message = update.effective_message
    if not user or not message or not check_cooldown(user.id, "concentration"):
        return

    positions = await asyncio.to_thread(
        db_get_positions,
        str(update.effective_chat.id),
    )
    if not positions:
        await message.reply_text("📭 შენი პორტფელი ცარიელია.")
        return

    tickers = [str(row["ticker"]) for row in positions]
    results = await asyncio.gather(
        *(fetch_stock_data(ticker) for ticker in tickers),
        return_exceptions=True,
    )
    data_map = {
        ticker: ((None, None) if isinstance(result, Exception) else result)
        for ticker, result in zip(tickers, results)
    }

    valid_positions: list[tuple[str, float]] = []
    missing: list[str] = []
    total_value = 0.0

    for row in positions:
        ticker = str(row["ticker"])
        price_data, _ = data_map.get(ticker, (None, None))
        price = price_data.get("latest_price") if price_data else None
        if price is None:
            missing.append(ticker)
            continue
        value = float(row["shares"]) * float(price)
        total_value += value
        valid_positions.append((ticker, value))

    if total_value <= 0:
        await message.reply_text("⚠️ კონცენტრაცია ვერ დაითვალა, რადგან ფასები მიუწვდომელია.")
        return

    valid_positions.sort(key=lambda x: x[1], reverse=True)
    lines = ["📊 პორტფელის კონცენტრაცია", ""]
    for ticker, value in valid_positions:
        pct = value / total_value * 100
        marker = " 🔴" if pct >= 50 else " 🟠" if pct >= 30 else " 🟡" if pct >= 15 else ""
        lines.append(f"• {ticker}: ${value:,.2f} ({pct:.2f}%){marker}")

    if missing:
        lines.extend(["", "⚠️ ფასი ვერ მოიძებნა: " + ", ".join(missing)])
    lines.extend(["", "🔴 50%+   🟠 30%–49.99%   🟡 15%–29.99%"])
    await send_plain_chunks(message, "\n".join(lines))


def fetch_news_sync(ticker: str) -> list[dict[str, str]]:
    rss_url = f"https://finance.yahoo.com/rss/headline?s={quote_plus(ticker)}"
    request = Request(
        rss_url,
        headers={"User-Agent": "Mozilla/5.0 FinancialTelegramBot/1.0"},
    )
    with urlopen(request, timeout=12) as response:
        raw = response.read()
    feed = feedparser.parse(raw)

    result: list[dict[str, str]] = []
    for entry in getattr(feed, "entries", [])[:10]:
        link = str(entry.get("link", "")).strip()
        if not link.startswith(("https://", "http://")):
            continue
        source = "წყარო უცნობია"
        source_data = entry.get("source")
        if isinstance(source_data, dict):
            source = str(source_data.get("title", source))
        result.append({
            "title": str(entry.get("title", "უსათაურო")),
            "link": link,
            "published": str(entry.get("published", "დრო უცნობია")),
            "source": source,
        })
        if len(result) >= 3:
            break
    return result


@cached(cache=news_cache, lock=news_cache_lock)
def get_cached_news(ticker: str):
    return fetch_news_sync(ticker)


@restricted_access
async def news_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    message = update.effective_message
    if not user or not message or not check_cooldown(user.id, "news"):
        return

    if len(context.args) != 1:
        await message.reply_text("გამოყენება:\n/news TICKER\n\nმაგალითი:\n/news NVDA")
        return

    ticker = context.args[0].upper().strip()
    valid, error = validate_supported_ticker(ticker)
    if not valid:
        await message.reply_text(error or "❌ არასწორი ticker.")
        return

    try:
        entries = await asyncio.to_thread(get_cached_news, ticker)
        if not entries:
            await message.reply_text(f"📭 სიახლეები ვერ მოიძებნა {ticker}-ისთვის.")
            return

        lines = [f"📰 უახლესი სათაურები — {ticker}", ""]
        for entry in entries:
            lines.extend([
                f"• {entry['title']}",
                f"  {entry['source']} | {entry['published']}",
                f"  {entry['link']}",
                "",
            ])
        lines.append(
            "⚠️ ეს არის RSS-დან ავტომატურად მიღებული სათაურები; "
            "მათი შინაარსი დამოუკიდებლად გადამოწმებული არ არის."
        )
        await send_plain_chunks(message, "\n".join(lines))
    except Exception:
        logger.exception("News fetch error for %s", ticker)
        await message.reply_text("⚠️ სიახლეების მიღება ვერ მოხერხდა. სცადე მოგვიანებით.")


@restricted_access
async def rsi_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    message = update.effective_message
    if not user or not message or not check_cooldown(user.id, "rsi"):
        return

    if len(context.args) != 1:
        await message.reply_text("გამოყენება:\n/rsi TICKER\n\nმაგალითი:\n/rsi NVDA")
        return

    ticker = context.args[0].upper().strip()
    valid, error = validate_supported_ticker(ticker)
    if not valid:
        await message.reply_text(error or "❌ არასწორი ticker.")
        return

    price_data, hist = await fetch_stock_data(ticker)
    if not price_data or price_data.get("latest_price") is None:
        await message.reply_text(f"❌ მონაცემები ვერ მოიძებნა {ticker}-ისთვის.")
        return

    rsi = calculate_rsi(hist["Close"]) if has_close_data(hist) else None
    sma20, sma50, volatility = calculate_technical_indicators(hist)
    status = "მონაცემი არასაკმარისია"
    if rsi is not None:
        status = (
            "შესაძლო overbought მდგომარეობა"
            if rsi >= 70
            else "შესაძლო oversold მდგომარეობა"
            if rsi <= 30
            else "ნეიტრალური ზონა"
        )

    lines = [
        f"📈 ტექნიკური ინდიკატორები — {ticker}",
        "",
        f"• მიმდინარე ფასი: ${float(price_data['latest_price']):,.2f}",
    ]
    if price_data.get("regular_close") is not None:
        lines.append(f"• წინა დახურვა: ${float(price_data['regular_close']):,.2f}")
    if price_data.get("change_pct_from_previous_close") is not None:
        lines.append(
            "• ცვლილება წინა დახურვიდან: "
            f"{float(price_data['change_pct_from_previous_close']):+.2f}%"
        )
    lines.extend([
        f"• RSI (14): {rsi if rsi is not None else 'N/A'}",
        f"• RSI შეფასება: {status}",
        f"• SMA 20: {f'${sma20:,.2f}' if sma20 is not None else 'N/A'}",
        f"• SMA 50: {f'${sma50:,.2f}' if sma50 is not None else 'N/A'}",
        f"• წლიური ვოლატილობა: {f'{volatility:.2f}%' if volatility is not None else 'N/A'}",
    ])
    timestamp = normalize_timestamp_to_market_timezone(price_data.get("quote_timestamp"))
    if timestamp is not None:
        lines.append("• მონაცემის დრო: " + timestamp.strftime("%Y-%m-%d %H:%M:%S %Z"))
    lines.extend([
        "",
        "⚠️ ტექნიკური ინდიკატორები არ წარმოადგენს გარანტირებულ ყიდვა/გაყიდვის სიგნალს.",
    ])
    await send_plain_chunks(message, "\n".join(lines))


@restricted_access
async def ai_analysis_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    message = update.effective_message
    if not user or not message or not check_cooldown(user.id, "ai"):
        return

    if len(context.args) != 1:
        await message.reply_text("გამოყენება:\n/ai TICKER\n\nმაგალითი:\n/ai NVDA")
        return

    ticker = context.args[0].upper().strip()
    valid, error = validate_supported_ticker(ticker)
    if not valid:
        await message.reply_text(error or "❌ არასწორი ticker.")
        return

    price_data, hist = await fetch_stock_data(ticker)
    if not price_data or price_data.get("latest_price") is None:
        await message.reply_text(f"❌ მონაცემები ვერ მოიძებნა {ticker}-ისთვის.")
        return

    rsi = calculate_rsi(hist["Close"]) if has_close_data(hist) else None
    sma20, sma50, volatility = calculate_technical_indicators(hist)
    position = await asyncio.to_thread(
        db_get_position,
        str(update.effective_chat.id),
        ticker,
    )

    user_position = None
    if position:
        shares = float(position["shares"])
        average = float(position["buy_price"])
        fees = float(position["total_fees"] or 0)
        cost_basis = shares * average
        current_value = shares * float(price_data["latest_price"])
        pnl = current_value - cost_basis
        user_position = {
            "shares": shares,
            "effective_average_cost": average,
            "recorded_total_fees": fees,
            "cost_basis": cost_basis,
            "current_value": current_value,
            "profit_loss": pnl,
            "profit_loss_pct": pnl / cost_basis * 100 if cost_basis else 0,
            "breakeven_price": average,
        }

    package = {
        "ticker": ticker,
        "previous_close": price_data.get("regular_close"),
        "latest_price": price_data.get("latest_price"),
        "change_pct_from_previous_close": price_data.get(
            "change_pct_from_previous_close"
        ),
        "quote_timestamp": str(price_data.get("quote_timestamp")),
        "quote_source": price_data.get("quote_source"),
        "rsi14": rsi,
        "sma20": sma20,
        "sma50": sma50,
        "annualized_volatility_pct": volatility,
        "user_position": user_position,
        "data_source": "Yahoo Finance through yfinance",
        "data_warning": (
            "Quote may be delayed, incomplete, or inconsistent, especially "
            "during pre-market and after-hours trading."
        ),
    }

    system_prompt = (
        "შენ ხარ კონსერვატიული ფინანსური ანალიტიკოსი და რისკ-მენეჯერი.\n"
        "გააანალიზე მხოლოდ მომხმარებლის JSON მონაცემები.\n"
        "არ გამოიყენო მიმდინარე ამბები, ანგარიშები, სამიზნე ფასები ან სხვა ფაქტები, "
        "რომლებიც JSON-ში არ არის. არ გამოიგონო დაკარგული ინფორმაცია.\n"
        "პასუხი დაწერე ქართულად, უბრალო ტექსტით, HTML-ისა და Markdown-ის გარეშე.\n\n"
        "სტრუქტურა:\n"
        "1. მიმდინარე მდგომარეობა\n"
        "2. ტექნიკური სურათი\n"
        "3. მომხმარებლის პოზიცია და რისკი\n"
        "4. bullish სცენარი\n"
        "5. base სცენარი\n"
        "6. bearish სცენარი\n"
        "7. თითოეული სცენარის გაუქმების პირობები\n"
        "8. მთავარი რისკები\n\n"
        "არ გასცე კატეგორიული ბრძანება და არ წარმოაჩინო ვარაუდი ფაქტად. "
        "თუ მონაცემი აკლია, პირდაპირ დაწერე, რომ მონაცემი არასაკმარისია."
    )

    try:
        response = await openai_client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[
                {"role": "system", "content": system_prompt},
                {
                    "role": "user",
                    "content": json.dumps(
                        package,
                        ensure_ascii=False,
                        indent=2,
                        default=str,
                    ),
                },
            ],
            temperature=0.1,
            max_tokens=1200,
        )
        ai_text = response.choices[0].message.content or "AI-მ ცარიელი პასუხი დააბრუნა."
        full_text = (
            f"🤖 AI ანალიზი — {ticker}\n\n"
            f"{ai_text}\n\n"
            "⚠️ ეს ანალიზი დაფუძნებულია შეზღუდულ ტექნიკურ მონაცემებზე და "
            "არ წარმოადგენს პერსონალურ საინვესტიციო რეკომენდაციას."
        )
        await send_plain_chunks(message, full_text)
    except Exception:
        logger.exception("OpenAI analysis failed for %s", ticker)
        await message.reply_text("⚠️ AI ანალიზი დროებით მიუწვდომელია. სცადე მოგვიანებით.")


# ============================================================
# ERROR HANDLER
# ============================================================

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    logger.error("Exception while handling update", exc_info=context.error)

    if isinstance(context.error, BadRequest) and "Message is not modified" in str(context.error):
        return

    if isinstance(update, Update) and update.effective_message:
        try:
            await update.effective_message.reply_text(
                "⚠️ დაფიქსირდა ტექნიკური შეცდომა. შეცდომა ჩაიწერა სისტემის ჟურნალში."
            )
        except Exception:
            logger.exception("Could not send error message to user")


# ============================================================
# MAIN
# ============================================================

def main() -> None:
    init_db()

    application = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

    handlers = {
        "start": start_command,
        "help": help_command,
        "position": position_command,
        "addposition": add_position_command,
        "portfolio": portfolio_command,
        "remove": remove_command,
        "alert": alert_command,
        "alerts": list_alerts_command,
        "removealert": remove_alert_command,
        "concentration": concentration_command,
        "news": news_command,
        "rsi": rsi_command,
        "ai": ai_analysis_command,
    }
    for command, callback in handlers.items():
        application.add_handler(CommandHandler(command, callback))

    application.add_error_handler(error_handler)

    job_queue = application.job_queue
    if job_queue is None:
        raise RuntimeError(
            "JobQueue unavailable. Install dependencies with:\n"
            "pip install 'python-telegram-bot[job-queue]'"
        )

    job_queue.run_repeating(
        check_price_alerts_job,
        interval=ALERT_CHECK_INTERVAL_SECONDS,
        first=10,
        name="price-alert-checker",
        job_kwargs={"max_instances": 1, "coalesce": True, "misfire_grace_time": 60},
    )

    logger.info("Financial Telegram bot is starting polling")
    application.run_polling(
        allowed_updates=Update.ALL_TYPES,
        drop_pending_updates=False,
    )


if __name__ == "__main__":
    main()
