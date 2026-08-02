from contextlib import contextmanager
from datetime import datetime, time as dt_time
from functools import wraps
from threading import RLock
import asyncio
import html
import json
import logging
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
from telegram.ext import Application, CommandHandler, ContextTypes
import yfinance as yf


# ============================================================
# LOGGING SETUP
# ============================================================

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)

logger = logging.getLogger(__name__)


# ============================================================
# CONFIGURATION
# ============================================================

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o").strip()
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")

if not OPENAI_API_KEY:
  raise RuntimeError(
      "OPENAI_API_KEY is missing from environment variables."
  )

if not TELEGRAM_BOT_TOKEN:
  raise RuntimeError(
      "TELEGRAM_BOT_TOKEN is missing from environment variables."
  )


raw_allowed_ids = os.getenv("ALLOWED_CHAT_IDS", "")

ALLOWED_CHAT_IDS = [
    chat_id.strip()
    for chat_id in raw_allowed_ids.split(",")
    if chat_id.strip()
]


# ============================================================
# CONSTANTS
# ============================================================

DATABASE_PATH = os.getenv("DATABASE_PATH", "portfolio.db")

MARKET_TIMEZONE = pytz.timezone("America/New_York")

COOLDOWN_TIME = 4
MAX_ALERTS_PER_CHAT = 50

STOCK_CACHE_TTL = 300
ALERT_CACHE_TTL = 30

YAHOO_MAX_CONCURRENT_REQUESTS = 4


# ============================================================
# CACHES AND LOCKS
# ============================================================

stock_cache = TTLCache(
    maxsize=100,
    ttl=STOCK_CACHE_TTL,
)

alert_price_cache = TTLCache(
    maxsize=200,
    ttl=ALERT_CACHE_TTL,
)

cooldowns = TTLCache(
    maxsize=1000,
    ttl=10,
)

stock_cache_lock = RLock()
alert_cache_lock = RLock()
cooldown_lock = RLock()

yahoo_semaphore = asyncio.Semaphore(
    YAHOO_MAX_CONCURRENT_REQUESTS
)


# ============================================================
# OPENAI CLIENT
# ============================================================

openai_client = AsyncOpenAI(
    api_key=OPENAI_API_KEY,
    timeout=30.0,
    max_retries=2,
)


# ============================================================
# DATABASE HELPERS
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


def init_db():
  with get_db_connection() as conn:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS positions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id TEXT NOT NULL,
            ticker TEXT NOT NULL,
            shares REAL NOT NULL CHECK(shares > 0),
            buy_price REAL NOT NULL CHECK(buy_price > 0),
            UNIQUE(chat_id, ticker)
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id TEXT NOT NULL,
            ticker TEXT NOT NULL,
            target_price REAL NOT NULL CHECK(target_price > 0),
            condition TEXT NOT NULL
                CHECK(condition IN ('above', 'below'))
        )
    """)

    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_positions_chat_id
        ON positions(chat_id)
    """)

    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_alerts_chat_id
        ON alerts(chat_id)
    """)

    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_alerts_ticker
        ON alerts(ticker)
    """)


# ============================================================
# GENERAL UTILITIES
# ============================================================

def validate_ticker(ticker: str) -> bool:
  return bool(
      re.fullmatch(
          r"[A-Z0-9.\-^=]{1,15}",
          ticker,
      )
  )


def validate_supported_ticker(
    ticker: str,
) -> tuple[bool, str | None]:

  if not validate_ticker(ticker):
    return (
        False,
        "❌ არასწორი ტიქერის ფორმატი.",
    )

  if ticker == "QNT":
    return (
        False,
        "⚠️ <b>QNT</b> ჩვეულებრივ Quant კრიპტოვალუტას აღნიშნავს "
        "და არა Quantinuum-ს.\n\n"
        "Quant კრიპტოვალუტისთვის გამოიყენე "
        "<code>QNT-USD</code>.\n"
        "Quantinuum ამჟამად ცალკე საჯარო ticker-ით არ ივაჭრება.",
    )

  return True, None


def check_cooldown(
    user_id: int,
    command: str,
) -> bool:

  now = time.monotonic()
  key = (user_id, command)

  with cooldown_lock:
    previous_time = cooldowns.get(key)

    if (
        previous_time is not None
        and now - previous_time < COOLDOWN_TIME
    ):
      return False

    cooldowns[key] = now

  return True


def safe_send_message(
    text: str,
    max_length: int = 4000,
) -> list[str]:

  if not text:
    return []

  if len(text) <= max_length:
    return [text]

  chunks: list[str] = []
  remaining = text.strip()

  while len(remaining) > max_length:
    split_at = remaining.rfind(
        "\n",
        0,
        max_length,
    )

    if split_at <= 0:
      split_at = remaining.rfind(
          " ",
          0,
          max_length,
      )

    if split_at <= 0:
      split_at = max_length

    chunk = remaining[:split_at].strip()

    if chunk:
      chunks.append(chunk)

    remaining = remaining[split_at:].strip()

  if remaining:
    chunks.append(remaining)

  return chunks


def has_close_data(
    hist: pd.DataFrame | None,
) -> bool:

  return (
      hist is not None
      and not hist.empty
      and "Close" in hist.columns
      and not hist["Close"].dropna().empty
  )


def normalize_timestamp_to_market_timezone(
    timestamp,
):
  if timestamp is None:
    return None

  try:
    if getattr(timestamp, "tzinfo", None) is not None:
      return timestamp.tz_convert(MARKET_TIMEZONE)

    return MARKET_TIMEZONE.localize(
        timestamp.to_pydatetime()
        if hasattr(timestamp, "to_pydatetime")
        else timestamp
    )

  except Exception:
    logger.warning(
        "Could not normalize timestamp: %s",
        timestamp,
        exc_info=True,
    )
    return timestamp


def get_fallback_previous_close(
    hist: pd.DataFrame | None,
) -> float | None:

  if not has_close_data(hist):
    return None

  closes = hist["Close"].dropna()

  if closes.empty:
    return None

  now_et = datetime.now(MARKET_TIMEZONE)
  today_et = now_et.date()

  last_timestamp = closes.index[-1]

  try:
    if getattr(last_timestamp, "tzinfo", None) is not None:
      last_date = last_timestamp.tz_convert(
          MARKET_TIMEZONE
      ).date()
    else:
      last_date = last_timestamp.date()

  except Exception:
    logger.warning(
        "Could not interpret daily timestamp: %s",
        last_timestamp,
        exc_info=True,
    )

    return float(closes.iloc[-1])

  # თუ ბოლო daily row წინა სავაჭრო დღისაა,
  # სწორედ ის წარმოადგენს ბოლო ოფიციალურ დახურვას.
  if last_date != today_et:
    return float(closes.iloc[-1])

  # თუ დღევანდელი row ბაზრის დახურვამდე მიიღება,
  # ის შეიძლება ჯერ არასრული იყოს.
  if (
      now_et.time() < dt_time(16, 5)
      and len(closes) >= 2
  ):
    return float(closes.iloc[-2])

  # ბაზრის დახურვის შემდეგ დღევანდელი row
  # სავარაუდოდ ოფიციალურ დახურვას წარმოადგენს.
  return float(closes.iloc[-1])


# ============================================================
# FINANCIAL DATA FUNCTIONS
# ============================================================

def fetch_stock_data_sync(
    ticker: str,
):
  try:
    stock = yf.Ticker(ticker)

    hist = stock.history(
        period="3mo",
        interval="1d",
        auto_adjust=False,
        timeout=10,
    )

    intraday = stock.history(
        period="1d",
        interval="1m",
        prepost=True,
        auto_adjust=False,
        timeout=10,
    )

    regular_close = None
    latest_price = None
    quote_timestamp = datetime.now(
        MARKET_TIMEZONE
    )

    # FastInfo ზოგიერთ ticker-ზე შეიძლება დროებით ჩავარდეს.
    try:
      fast_info = stock.fast_info

      regular_close = getattr(
          fast_info,
          "previous_close",
          None,
      )

      latest_price = getattr(
          fast_info,
          "last_price",
          None,
      )

    except Exception:
      logger.warning(
          "fast_info unavailable for %s",
          ticker,
          exc_info=True,
      )

    # Intraday მონაცემს ვანიჭებთ უპირატესობას,
    # რადგან pre-market და after-hours ინფორმაციაც შეიძლება ჰქონდეს.
    if (
        intraday is not None
        and not intraday.empty
        and "Close" in intraday.columns
    ):
      clean_intraday = intraday["Close"].dropna()

      if not clean_intraday.empty:
        latest_price = float(
            clean_intraday.iloc[-1]
        )

        quote_timestamp = normalize_timestamp_to_market_timezone(
            clean_intraday.index[-1]
        )

    # Previous close fallback.
    if regular_close is None:
      regular_close = get_fallback_previous_close(
          hist
      )

    # Latest price fallback daily history-დან.
    if latest_price is None and has_close_data(hist):
      daily_close = hist["Close"].dropna()

      if not daily_close.empty:
        latest_price = float(
            daily_close.iloc[-1]
        )

        quote_timestamp = normalize_timestamp_to_market_timezone(
            daily_close.index[-1]
        )

    if regular_close is not None:
      regular_close = float(regular_close)

    if latest_price is not None:
      latest_price = float(latest_price)

    change_pct_from_previous_close = None

    if (
        regular_close is not None
        and latest_price is not None
        and regular_close > 0
    ):
      change_pct_from_previous_close = (
          (latest_price - regular_close)
          / regular_close
      ) * 100

    price_data = {
        "regular_close": regular_close,
        "latest_price": latest_price,
        "change_pct_from_previous_close":
            change_pct_from_previous_close,
        "quote_timestamp": quote_timestamp,
    }

    return price_data, hist

  except Exception:
    logger.exception(
        "Error fetching data for %s",
        ticker,
    )

    return None, None


def fetch_alert_price_sync(
    ticker: str,
) -> float | None:

  try:
    stock = yf.Ticker(ticker)

    intraday = stock.history(
        period="1d",
        interval="1m",
        prepost=True,
        auto_adjust=False,
        timeout=10,
    )

    if (
        intraday is not None
        and not intraday.empty
        and "Close" in intraday.columns
    ):
      clean_intraday = intraday["Close"].dropna()

      if not clean_intraday.empty:
        return float(
            clean_intraday.iloc[-1]
        )

    hist = stock.history(
        period="5d",
        interval="1d",
        auto_adjust=False,
        timeout=10,
    )

    if has_close_data(hist):
      clean_hist = hist["Close"].dropna()

      if not clean_hist.empty:
        return float(
            clean_hist.iloc[-1]
        )

    return None

  except Exception:
    logger.exception(
        "Error fetching alert price for %s",
        ticker,
    )

    return None


@cached(
    cache=stock_cache,
    lock=stock_cache_lock,
)
def get_cached_stock_data(
    ticker: str,
):
  return fetch_stock_data_sync(ticker)


@cached(
    cache=alert_price_cache,
    lock=alert_cache_lock,
)
def get_cached_alert_stock_data(
    ticker: str,
):
  return fetch_alert_price_sync(ticker)


async def fetch_stock_data(
    ticker: str,
):
  async with yahoo_semaphore:
    return await asyncio.to_thread(
        get_cached_stock_data,
        ticker,
    )


async def fetch_stock_price_for_alert(
    ticker: str,
):
  async with yahoo_semaphore:
    return await asyncio.to_thread(
        get_cached_alert_stock_data,
        ticker,
    )


# ============================================================
# TECHNICAL INDICATORS
# ============================================================

def calculate_rsi(
    prices: pd.Series,
    window: int = 14,
) -> float | None:

  if (
      prices is None
      or len(prices) < window + 1
  ):
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

    clean_gain = avg_gain.dropna()
    clean_loss = avg_loss.dropna()

    if clean_gain.empty or clean_loss.empty:
      return None

    last_gain = float(clean_gain.iloc[-1])
    last_loss = float(clean_loss.iloc[-1])

    if last_loss == 0:
      if last_gain == 0:
        return 50.0

      return 100.0

    rs = last_gain / last_loss

    rsi = 100 - (
        100 / (1 + rs)
    )

    return round(
        float(rsi),
        1,
    )

  except Exception:
    logger.exception(
        "Error calculating RSI"
    )

    return None


def calculate_technical_indicators(
    hist: pd.DataFrame | None,
):
  if not has_close_data(hist):
    return None, None, None

  try:
    close = (
        hist["Close"]
        .astype(float)
        .dropna()
    )

    sma20 = None
    sma50 = None

    if len(close) >= 20:
      sma20 = float(
          close
          .rolling(window=20)
          .mean()
          .iloc[-1]
      )

    if len(close) >= 50:
      sma50 = float(
          close
          .rolling(window=50)
          .mean()
          .iloc[-1]
      )

    returns = close.pct_change().dropna()

    volatility = None

    if not returns.empty:
      volatility = float(
          returns.std()
          * (252 ** 0.5)
          * 100
      )

    return sma20, sma50, volatility

  except Exception:
    logger.exception(
        "Error calculating technical indicators"
    )

    return None, None, None


async def get_rsi_analysis_data(
    ticker: str,
):
  price_data, hist = await fetch_stock_data(
      ticker
  )

  if (
      not price_data
      or price_data.get("latest_price") is None
  ):
    return None

  rsi = None

  if has_close_data(hist):
    rsi = calculate_rsi(
        hist["Close"]
    )

  sma20, sma50, volatility = (
      calculate_technical_indicators(
          hist
      )
  )

  return {
      "current_price":
          price_data.get("latest_price"),
      "regular_close":
          price_data.get("regular_close"),
      "change_pct_from_previous_close":
          price_data.get(
              "change_pct_from_previous_close"
          ),
      "quote_timestamp":
          price_data.get("quote_timestamp"),
      "rsi": rsi,
      "sma20": sma20,
      "sma50": sma50,
      "volatility": volatility,
      "hist": hist,
  }


# ============================================================
# ACCESS CONTROL
# ============================================================

def restricted_access(func):

  @wraps(func)
  async def wrapper(
      update: Update,
      context: ContextTypes.DEFAULT_TYPE,
  ):
    chat = update.effective_chat

    if not chat:
      return

    chat_id = str(chat.id)

    if (
        ALLOWED_CHAT_IDS
        and chat_id not in ALLOWED_CHAT_IDS
    ):
      if update.effective_message:
        await update.effective_message.reply_text(
            "⛔ ამ ბოტზე წვდომა შეზღუდულია "
            "შენი ჩატისთვის."
        )

      logger.warning(
          "Unauthorized access attempt from chat_id: %s",
          chat_id,
      )

      return

    return await func(
        update,
        context,
    )

  return wrapper


# ============================================================
# BACKGROUND PRICE ALERT JOB
# ============================================================

async def check_price_alerts_job(
    context: ContextTypes.DEFAULT_TYPE,
):
  logger.info(
      "Running background price alert check..."
  )

  with get_db_connection() as conn:
    alerts = conn.execute(
        """
        SELECT
            id,
            chat_id,
            ticker,
            target_price,
            condition
        FROM alerts
        """
    ).fetchall()

  if not alerts:
    return

  unique_tickers = sorted({
      alert["ticker"]
      for alert in alerts
  })

  results = await asyncio.gather(
      *(
          fetch_stock_price_for_alert(
              ticker
          )
          for ticker in unique_tickers
      ),
      return_exceptions=True,
  )

  prices: dict[str, float | None] = {}

  for ticker, result in zip(
      unique_tickers,
      results,
  ):
    if isinstance(result, Exception):
      logger.error(
          "Alert price request failed for %s: %s",
          ticker,
          result,
      )

      prices[ticker] = None

    else:
      prices[ticker] = result

  for alert in alerts:
    ticker = alert["ticker"]

    current_price = prices.get(
        ticker
    )

    if current_price is None:
      continue

    target_price = float(
        alert["target_price"]
    )

    condition = alert["condition"]

    triggered = (
        condition == "above"
        and current_price >= target_price
    ) or (
        condition == "below"
        and current_price <= target_price
    )

    if not triggered:
      continue

    try:
      await context.bot.send_message(
          chat_id=alert["chat_id"],
          text=(
              "🚨 <b>ფასის ალერტი</b>\n\n"
              f"<b>{html.escape(ticker)}</b>\n"
              f"პირობა: <code>{condition}</code> "
              f"${target_price:,.2f}\n"
              f"მიღებული ფასი: "
              f"<b>${current_price:,.2f}</b>\n\n"
              "⚠️ <i>ფასი შეიძლება დაგვიანებული იყოს. "
              "მონაცემი არ წარმოადგენს ოფიციალურ "
              "real-time საბირჟო feed-ს.</i>"
          ),
          parse_mode=ParseMode.HTML,
      )

      with get_db_connection() as conn:
        conn.execute(
            """
            DELETE FROM alerts
            WHERE id = ?
            """,
            (alert["id"],),
        )

    except Exception:
      logger.exception(
          "Failed to process alert ID %s",
          alert["id"],
      )


# ============================================================
# /START
# ============================================================

@restricted_access
async def start_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
  user = update.effective_user
  message = update.effective_message

  if not user or not message:
    return

  if not check_cooldown(
      user.id,
      "start",
  ):
    return

  await message.reply_text(
      "👋 გამარჯობა!\n\n"
      "მე ვარ შენი ფინანსური ასისტენტი ბოტი.\n"
      "გამოიყენე /help ბრძანება შესაძლებლობების სანახავად."
  )


# ============================================================
# /HELP
# ============================================================

@restricted_access
async def help_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
  user = update.effective_user
  message = update.effective_message

  if not user or not message:
    return

  if not check_cooldown(
      user.id,
      "help",
  ):
    return

  help_text = (
      "🤖 <b>ბოტის ბრძანებების სახელმძღვანელო</b>\n\n"

      "📊 <code>/portfolio</code>\n"
      "პორტფელის მდგომარეობა და P/L\n\n"

      "📝 <code>/position TICKER shares price</code>\n"
      "პოზიციის სრული დამატება ან ჩანაცვლება\n\n"

      "➕ <code>/addposition TICKER shares price</code>\n"
      "აქციების დამატება საშუალო ფასის ავტომატური გამოთვლით\n\n"

      "❌ <code>/remove TICKER</code>\n"
      "პოზიციის წაშლა\n\n"

      "📈 <code>/rsi TICKER</code>\n"
      "RSI, SMA და ვოლატილობა\n\n"

      "🤖 <code>/ai TICKER</code>\n"
      "AI ტექნიკური და რისკის ანალიზი\n\n"

      "🚨 <code>/alert TICKER above/below price</code>\n"
      "ფასის ალერტის შექმნა\n\n"

      "📋 <code>/alerts</code>\n"
      "აქტიური ალერტების სია\n\n"

      "🗑 <code>/removealert ID</code>\n"
      "ალერტის წაშლა\n\n"

      "📊 <code>/concentration</code>\n"
      "პორტფელის კონცენტრაციის ანალიზი\n\n"

      "📰 <code>/news TICKER</code>\n"
      "Yahoo Finance RSS-ის უახლესი სათაურები\n\n"

      "⚠️ ფასები შეიძლება დაგვიანებული იყოს და არ წარმოადგენს "
      "ოფიციალურ real-time საბირჟო მონაცემს."
  )

  for chunk in safe_send_message(
      help_text
  ):
    await message.reply_text(
        chunk,
        parse_mode=ParseMode.HTML,
    )


# ============================================================
# /POSITION
# ============================================================

@restricted_access
async def position_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
  user = update.effective_user
  message = update.effective_message

  if not user or not message:
    return

  if not check_cooldown(
      user.id,
      "position",
  ):
    return

  if len(context.args) != 3:
    await message.reply_text(
        "გამოყენება:\n"
        "<code>/position TICKER ოდენობა ფასი</code>\n\n"
        "მაგალითი:\n"
        "<code>/position NVDA 10 120.5</code>\n\n"
        "⚠️ ეს ბრძანება არსებულ პოზიციას მთლიანად ჩაანაცვლებს.",
        parse_mode=ParseMode.HTML,
    )
    return

  ticker = context.args[0].upper().strip()

  valid, error_message = validate_supported_ticker(
      ticker
  )

  if not valid:
    await message.reply_text(
        error_message or "❌ არასწორი ticker.",
        parse_mode=ParseMode.HTML,
    )
    return

  try:
    shares = float(context.args[1])
    buy_price = float(context.args[2])

    if shares <= 0 or buy_price <= 0:
      raise ValueError

  except (TypeError, ValueError):
    await message.reply_text(
        "❌ ოდენობა და ფასი უნდა იყოს "
        "ნულზე მეტი რიცხვი."
    )
    return

  chat_id = str(
      update.effective_chat.id
  )

  with get_db_connection() as conn:
    conn.execute(
        """
        INSERT INTO positions (
            chat_id,
            ticker,
            shares,
            buy_price
        )
        VALUES (?, ?, ?, ?)
        ON CONFLICT(chat_id, ticker)
        DO UPDATE SET
            shares = excluded.shares,
            buy_price = excluded.buy_price
        """,
        (
            chat_id,
            ticker,
            shares,
            buy_price,
        ),
    )

  await message.reply_text(
      f"✅ პოზიცია წარმატებით შეინახა\n\n"
      f"<b>{html.escape(ticker)}</b>\n"
      f"აქციების რაოდენობა: <b>{shares:g}</b>\n"
      f"საშუალო ფასი: <b>${buy_price:,.2f}</b>",
      parse_mode=ParseMode.HTML,
  )


# ============================================================
# /ADDPOSITION
# ============================================================

@restricted_access
async def add_position_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
  user = update.effective_user
  message = update.effective_message

  if not user or not message:
    return

  if not check_cooldown(
      user.id,
      "addposition",
  ):
    return

  if len(context.args) != 3:
    await message.reply_text(
        "გამოყენება:\n"
        "<code>/addposition TICKER ოდენობა ფასი</code>\n\n"
        "მაგალითი:\n"
        "<code>/addposition NVDA 20 180</code>",
        parse_mode=ParseMode.HTML,
    )
    return

  ticker = context.args[0].upper().strip()

  valid, error_message = validate_supported_ticker(
      ticker
  )

  if not valid:
    await message.reply_text(
        error_message or "❌ არასწორი ticker.",
        parse_mode=ParseMode.HTML,
    )
    return

  try:
    added_shares = float(
        context.args[1]
    )

    added_price = float(
        context.args[2]
    )

    if (
        added_shares <= 0
        or added_price <= 0
    ):
      raise ValueError

  except (TypeError, ValueError):
    await message.reply_text(
        "❌ ოდენობა და ფასი დადებითი "
        "რიცხვები უნდა იყოს."
    )
    return

  chat_id = str(
      update.effective_chat.id
  )

  with get_db_connection() as conn:
    # თავიდანვე write lock,
    # რათა ორი ერთდროული დამატება არ დაიკარგოს.
    conn.execute(
        "BEGIN IMMEDIATE"
    )

    existing = conn.execute(
        """
        SELECT
            shares,
            buy_price
        FROM positions
        WHERE chat_id = ?
          AND ticker = ?
        """,
        (
            chat_id,
            ticker,
        ),
    ).fetchone()

    if existing:
      old_shares = float(
          existing["shares"]
      )

      old_price = float(
          existing["buy_price"]
      )

      new_shares = (
          old_shares
          + added_shares
      )

      new_average = (
          (
              old_shares * old_price
          )
          + (
              added_shares * added_price
          )
      ) / new_shares

      conn.execute(
          """
          UPDATE positions
          SET
              shares = ?,
              buy_price = ?
          WHERE chat_id = ?
            AND ticker = ?
          """,
          (
              new_shares,
              new_average,
              chat_id,
              ticker,
          ),
      )

    else:
      new_shares = added_shares
      new_average = added_price

      conn.execute(
          """
          INSERT INTO positions (
              chat_id,
              ticker,
              shares,
              buy_price
          )
          VALUES (?, ?, ?, ?)
          """,
          (
              chat_id,
              ticker,
              new_shares,
              new_average,
          ),
      )

  await message.reply_text(
      f"✅ პოზიციას დაემატა ახალი აქციები\n\n"
      f"<b>{html.escape(ticker)}</b>\n"
      f"დამატებული რაოდენობა: <b>{added_shares:g}</b>\n"
      f"დამატების ფასი: <b>${added_price:,.2f}</b>\n\n"
      f"სრული რაოდენობა: <b>{new_shares:g}</b>\n"
      f"ახალი საშუალო ფასი: <b>${new_average:,.2f}</b>",
      parse_mode=ParseMode.HTML,
  )


# ============================================================
# /PORTFOLIO
# ============================================================

@restricted_access
async def portfolio_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
  user = update.effective_user
  message = update.effective_message

  if not user or not message:
    return

  if not check_cooldown(
      user.id,
      "portfolio",
  ):
    return

  chat_id = str(
      update.effective_chat.id
  )

  with get_db_connection() as conn:
    positions = conn.execute(
        """
        SELECT
            ticker,
            shares,
            buy_price
        FROM positions
        WHERE chat_id = ?
        ORDER BY ticker ASC
        """,
        (chat_id,),
    ).fetchall()

  if not positions:
    await message.reply_text(
        "📭 შენი პორტფელი ცარიელია."
    )
    return

  tickers = [
      position["ticker"]
      for position in positions
  ]

  results = await asyncio.gather(
      *(
          fetch_stock_data(ticker)
          for ticker in tickers
      ),
      return_exceptions=True,
  )

  price_data_map = {}

  for ticker, result in zip(
      tickers,
      results,
  ):
    if (
        isinstance(result, Exception)
        or result is None
    ):
      price_data_map[ticker] = (
          None,
          None,
      )

    else:
      price_data_map[ticker] = result

  report = (
      "📋 <b>შენი პორტფელი</b>\n\n"
  )

  total_invested = 0.0
  priced_invested = 0.0
  total_value = 0.0

  missing_tickers: list[str] = []
  timestamps = []

  for position in positions:
    ticker = position["ticker"]
    shares = float(
        position["shares"]
    )
    buy_price = float(
        position["buy_price"]
    )

    invested = (
        shares * buy_price
    )

    total_invested += invested

    price_data, _ = price_data_map.get(
        ticker,
        (None, None),
    )

    current_price = None

    if price_data:
      current_price = price_data.get(
          "latest_price"
      )

      timestamp = price_data.get(
          "quote_timestamp"
      )

      if timestamp is not None:
        normalized_timestamp = (
            normalize_timestamp_to_market_timezone(
                timestamp
            )
        )

        if normalized_timestamp is not None:
          timestamps.append(
              normalized_timestamp
          )

    safe_ticker = html.escape(
        ticker
    )

    if current_price is None:
      missing_tickers.append(
          ticker
      )

      report += (
          f"• <b>{safe_ticker}</b>\n"
          f"  რაოდენობა: {shares:g}\n"
          f"  საშუალო ფასი: ${buy_price:,.2f}\n"
          f"  მიმდინარე ფასი: <i>მიუწვდომელია</i>\n\n"
      )

      continue

    current_price = float(
        current_price
    )

    priced_invested += invested

    current_value = (
        shares * current_price
    )

    total_value += current_value

    profit_loss = (
        current_value - invested
    )

    profit_loss_pct = (
        profit_loss / invested * 100
        if invested > 0
        else 0.0
    )

    report += (
        f"• <b>{safe_ticker}</b>\n"
        f"  რაოდენობა: {shares:g}\n"
        f"  საშუალო ფასი: ${buy_price:,.2f}\n"
        f"  მიმდინარე ფასი: ${current_price:,.2f}\n"
        f"  მიმდინარე ღირებულება: ${current_value:,.2f}\n"
        f"  P/L: ${profit_loss:,.2f} "
        f"({profit_loss_pct:+.2f}%)\n\n"
    )

  total_profit_loss = (
      total_value
      - priced_invested
  )

  total_profit_loss_pct = (
      total_profit_loss
      / priced_invested
      * 100
      if priced_invested > 0
      else 0.0
  )

  report += (
      "━━━━━━━━━━━━━━━━━━\n"
      f"💵 <b>სრული ჩადებული თანხა:</b> "
      f"${total_invested:,.2f}\n"
      f"💰 <b>ფასდადგენილი პოზიციების ღირებულება:</b> "
      f"${total_value:,.2f}\n"
      f"📈 <b>ფასდადგენილი პოზიციების P/L:</b> "
      f"${total_profit_loss:,.2f} "
      f"({total_profit_loss_pct:+.2f}%)"
  )

  if missing_tickers:
    report += (
        "\n\n⚠️ <b>ფასი ვერ მოიძებნა:</b> "
        + ", ".join(
            html.escape(ticker)
            for ticker in missing_tickers
        )
    )

  if timestamps:
    latest_timestamp = max(
        timestamps
    )

    formatted_time = (
        latest_timestamp.strftime(
            "%Y-%m-%d %H:%M:%S %Z"
        )
    )

    report += (
        f"\n\n🕒 <i>ფასების უახლესი timestamp: "
        f"{formatted_time}</i>"
    )

  report += (
      "\n\n⚠️ <i>ფასები შეიძლება დაგვიანებული იყოს "
      "და არ წარმოადგენს ოფიციალურ real-time feed-ს.</i>"
  )

  for chunk in safe_send_message(
      report
  ):
    await message.reply_text(
        chunk,
        parse_mode=ParseMode.HTML,
    )


# ============================================================
# /REMOVE
# ============================================================

@restricted_access
async def remove_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
  user = update.effective_user
  message = update.effective_message

  if not user or not message:
    return

  if not check_cooldown(
      user.id,
      "remove",
  ):
    return

  if len(context.args) != 1:
    await message.reply_text(
        "გამოყენება:\n"
        "<code>/remove TICKER</code>",
        parse_mode=ParseMode.HTML,
    )
    return

  ticker = context.args[0].upper().strip()

  if not validate_ticker(ticker):
    await message.reply_text(
        "❌ არასწორი ტიქერის ფორმატი."
    )
    return

  chat_id = str(
      update.effective_chat.id
  )

  with get_db_connection() as conn:
    cursor = conn.execute(
        """
        DELETE FROM positions
        WHERE chat_id = ?
          AND ticker = ?
        """,
        (
            chat_id,
            ticker,
        ),
    )

    rowcount = cursor.rowcount

  safe_ticker = html.escape(
      ticker
  )

  if rowcount == 0:
    await message.reply_text(
        f"❌ პოზიცია <b>{safe_ticker}</b> ვერ მოიძებნა.",
        parse_mode=ParseMode.HTML,
    )

  else:
    await message.reply_text(
        f"✅ პოზიცია <b>{safe_ticker}</b> წარმატებით წაიშალა.",
        parse_mode=ParseMode.HTML,
    )


# ============================================================
# /ALERT
# ============================================================

@restricted_access
async def alert_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
  user = update.effective_user
  message = update.effective_message

  if not user or not message:
    return

  if not check_cooldown(
      user.id,
      "alert",
  ):
    return

  if len(context.args) != 3:
    await message.reply_text(
        "გამოყენება:\n"
        "<code>/alert TICKER above/below ფასი</code>\n\n"
        "მაგალითები:\n"
        "<code>/alert NVDA above 200</code>\n"
        "<code>/alert NVDA below 180</code>",
        parse_mode=ParseMode.HTML,
    )
    return

  ticker = context.args[0].upper().strip()

  valid, error_message = validate_supported_ticker(
      ticker
  )

  if not valid:
    await message.reply_text(
        error_message or "❌ არასწორი ticker.",
        parse_mode=ParseMode.HTML,
    )
    return

  condition = (
      context.args[1]
      .lower()
      .strip()
  )

  if condition not in {
      "above",
      "below",
  }:
    await message.reply_text(
        "❌ მეორე არგუმენტი უნდა იყოს "
        "<code>above</code> ან <code>below</code>.",
        parse_mode=ParseMode.HTML,
    )
    return

  try:
    target_price = float(
        context.args[2]
    )

    if target_price <= 0:
      raise ValueError

  except (TypeError, ValueError):
    await message.reply_text(
        "❌ სამიზნე ფასი დადებითი რიცხვი უნდა იყოს."
    )
    return

  chat_id = str(
      update.effective_chat.id
  )

  with get_db_connection() as conn:
    alert_count = conn.execute(
        """
        SELECT COUNT(*)
        FROM alerts
        WHERE chat_id = ?
        """,
        (chat_id,),
    ).fetchone()[0]

    if alert_count >= MAX_ALERTS_PER_CHAT:
      await message.reply_text(
          f"⚠️ მაქსიმუმ {MAX_ALERTS_PER_CHAT} "
          "აქტიური ალერტი შეიძლება გქონდეს."
      )
      return

    conn.execute(
        """
        INSERT INTO alerts (
            chat_id,
            ticker,
            target_price,
            condition
        )
        VALUES (?, ?, ?, ?)
        """,
        (
            chat_id,
            ticker,
            target_price,
            condition,
        ),
    )

  await message.reply_text(
      "✅ ფასის ალერტი შეიქმნა\n\n"
      f"<b>{html.escape(ticker)}</b>\n"
      f"პირობა: <code>{condition}</code>\n"
      f"სამიზნე ფასი: <b>${target_price:,.2f}</b>\n\n"
      "⚠️ ალერტები დაახლოებით ყოველ 5 წუთში მოწმდება.",
      parse_mode=ParseMode.HTML,
  )


# ============================================================
# /ALERTS
# ============================================================

@restricted_access
async def list_alerts_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
  user = update.effective_user
  message = update.effective_message

  if not user or not message:
    return

  if not check_cooldown(
      user.id,
      "alerts",
  ):
    return

  chat_id = str(
      update.effective_chat.id
  )

  with get_db_connection() as conn:
    alerts = conn.execute(
        """
        SELECT
            id,
            ticker,
            target_price,
            condition
        FROM alerts
        WHERE chat_id = ?
        ORDER BY id ASC
        """,
        (chat_id,),
    ).fetchall()

  if not alerts:
    await message.reply_text(
        "📭 აქტიური ალერტები არ გაქვს."
    )
    return

  report = (
      "📋 <b>აქტიური ალერტები</b>\n\n"
  )

  for alert in alerts:
    report += (
        f"• <b>ID: {alert['id']}</b>\n"
        f"  ticker: <b>{html.escape(alert['ticker'])}</b>\n"
        f"  პირობა: <code>{alert['condition']}</code>\n"
        f"  ფასი: ${float(alert['target_price']):,.2f}\n\n"
    )

  for chunk in safe_send_message(
      report
  ):
    await message.reply_text(
        chunk,
        parse_mode=ParseMode.HTML,
    )


# ============================================================
# /REMOVEALERT
# ============================================================

@restricted_access
async def remove_alert_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
  user = update.effective_user
  message = update.effective_message

  if not user or not message:
    return

  if not check_cooldown(
      user.id,
      "removealert",
  ):
    return

  if len(context.args) != 1:
    await message.reply_text(
        "გამოყენება:\n"
        "<code>/removealert ID</code>",
        parse_mode=ParseMode.HTML,
    )
    return

  try:
    alert_id = int(
        context.args[0]
    )

    if alert_id <= 0:
      raise ValueError

  except (TypeError, ValueError):
    await message.reply_text(
        "❌ ალერტის ID დადებითი მთელი რიცხვი უნდა იყოს."
    )
    return

  chat_id = str(
      update.effective_chat.id
  )

  with get_db_connection() as conn:
    cursor = conn.execute(
        """
        DELETE FROM alerts
        WHERE id = ?
          AND chat_id = ?
        """,
        (
            alert_id,
            chat_id,
        ),
    )

    rowcount = cursor.rowcount

  if rowcount == 0:
    await message.reply_text(
        f"❌ ალერტი ID-ით <b>{alert_id}</b> ვერ მოიძებნა.",
        parse_mode=ParseMode.HTML,
    )

  else:
    await message.reply_text(
        f"✅ ალერტი #{alert_id} წარმატებით წაიშალა."
    )


# ============================================================
# /CONCENTRATION
# ============================================================

@restricted_access
async def concentration_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
  user = update.effective_user
  message = update.effective_message

  if not user or not message:
    return

  if not check_cooldown(
      user.id,
      "concentration",
  ):
    return

  chat_id = str(
      update.effective_chat.id
  )

  with get_db_connection() as conn:
    positions = conn.execute(
        """
        SELECT
            ticker,
            shares
        FROM positions
        WHERE chat_id = ?
        ORDER BY ticker ASC
        """,
        (chat_id,),
    ).fetchall()

  if not positions:
    await message.reply_text(
        "📭 შენი პორტფელი ცარიელია."
    )
    return

  tickers = [
      position["ticker"]
      for position in positions
  ]

  results = await asyncio.gather(
      *(
          fetch_stock_data(ticker)
          for ticker in tickers
      ),
      return_exceptions=True,
  )

  price_data_map = {}

  for ticker, result in zip(
      tickers,
      results,
  ):
    if (
        isinstance(result, Exception)
        or result is None
    ):
      price_data_map[ticker] = (
          None,
          None,
      )

    else:
      price_data_map[ticker] = result

  total_portfolio_value = 0.0
  valid_positions = []
  missing_tickers = []

  for position in positions:
    ticker = position["ticker"]
    shares = float(
        position["shares"]
    )

    price_data, _ = price_data_map.get(
        ticker,
        (None, None),
    )

    current_price = (
        price_data.get("latest_price")
        if price_data
        else None
    )

    if current_price is None:
      missing_tickers.append(
          ticker
      )
      continue

    position_value = (
        shares * float(current_price)
    )

    total_portfolio_value += (
        position_value
    )

    valid_positions.append(
        (
            ticker,
            position_value,
        )
    )

  if total_portfolio_value <= 0:
    await message.reply_text(
        "⚠️ პორტფელის კონცენტრაცია ვერ დაითვალა, "
        "რადგან ფასები მიუწვდომელია."
    )
    return

  valid_positions.sort(
      key=lambda item: item[1],
      reverse=True,
  )

  report = (
      "📊 <b>პორტფელის კონცენტრაცია</b>\n\n"
  )

  for ticker, value in valid_positions:
    percentage = (
        value
        / total_portfolio_value
        * 100
    )

    risk_marker = ""

    if percentage >= 50:
      risk_marker = " 🔴"
    elif percentage >= 30:
      risk_marker = " 🟠"
    elif percentage >= 15:
      risk_marker = " 🟡"

    report += (
        f"• <b>{html.escape(ticker)}</b>: "
        f"${value:,.2f} "
        f"({percentage:.2f}%){risk_marker}\n"
    )

  if missing_tickers:
    report += (
        "\n⚠️ <b>ფასი ვერ მოიძებნა:</b> "
        + ", ".join(
            html.escape(ticker)
            for ticker in missing_tickers
        )
    )

  report += (
      "\n\n🔴 50%+\n"
      "🟠 30%–49.99%\n"
      "🟡 15%–29.99%"
  )

  for chunk in safe_send_message(
      report
  ):
    await message.reply_text(
        chunk,
        parse_mode=ParseMode.HTML,
    )


# ============================================================
# /NEWS
# ============================================================

@restricted_access
async def news_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
  user = update.effective_user
  message = update.effective_message

  if not user or not message:
    return

  if not check_cooldown(
      user.id,
      "news",
  ):
    return

  if len(context.args) != 1:
    await message.reply_text(
        "გამოყენება:\n"
        "<code>/news TICKER</code>\n\n"
        "მაგალითი:\n"
        "<code>/news NVDA</code>",
        parse_mode=ParseMode.HTML,
    )
    return

  ticker = context.args[0].upper().strip()

  valid, error_message = validate_supported_ticker(
      ticker
  )

  if not valid:
    await message.reply_text(
        error_message or "❌ არასწორი ticker.",
        parse_mode=ParseMode.HTML,
    )
    return

  try:
    rss_url = (
        "https://finance.yahoo.com/rss/headline"
        f"?s={ticker}"
    )

    feed = await asyncio.to_thread(
        feedparser.parse,
        rss_url,
    )

    if getattr(feed, "bozo", False):
      logger.warning(
          "RSS parsing warning for %s: %s",
          ticker,
          getattr(
              feed,
              "bozo_exception",
              "Unknown RSS error",
          ),
      )

    entries = getattr(
        feed,
        "entries",
        [],
    )

    if not entries:
      await message.reply_text(
          f"📭 სიახლეები ვერ მოიძებნა "
          f"<b>{html.escape(ticker)}</b>-ისთვის.",
          parse_mode=ParseMode.HTML,
      )
      return

    text = (
        f"📰 <b>უახლესი სათაურები — "
        f"{html.escape(ticker)}</b>\n\n"
    )

    valid_entries = 0

    for entry in entries[:10]:
      title = html.escape(
          str(
              entry.get(
                  "title",
                  "უსათაურო",
              )
          )
      )

      raw_link = str(
          entry.get(
              "link",
              "",
          )
      ).strip()

      if not raw_link.startswith(
          (
              "https://",
              "http://",
          )
      ):
        continue

      safe_link = html.escape(
          raw_link,
          quote=True,
      )

      published = html.escape(
          str(
              entry.get(
                  "published",
                  "გამოქვეყნების დრო უცნობია",
              )
          )
      )

      source_name = "წყარო უცნობია"

      source_data = entry.get(
          "source"
      )

      if isinstance(
          source_data,
          dict,
      ):
        source_name = str(
            source_data.get(
                "title",
                source_name,
            )
        )

      safe_source = html.escape(
          source_name
      )

      text += (
          f'• <a href="{safe_link}">{title}</a>\n'
          f"<i>{safe_source} | {published}</i>\n\n"
      )

      valid_entries += 1

      if valid_entries >= 3:
        break

    if valid_entries == 0:
      await message.reply_text(
          f"📭 მოქმედი სიახლეების ბმულები ვერ მოიძებნა "
          f"<b>{html.escape(ticker)}</b>-ისთვის.",
          parse_mode=ParseMode.HTML,
      )
      return

    text += (
        "⚠️ <i>ეს არის RSS-დან ავტომატურად მიღებული სათაურები. "
        "მათი შინაარსი დამოუკიდებლად გადამოწმებული არ არის.</i>"
    )

    for chunk in safe_send_message(
        text
    ):
      await message.reply_text(
          chunk,
          parse_mode=ParseMode.HTML,
          disable_web_page_preview=True,
      )

  except Exception:
    logger.exception(
        "News fetch error for %s",
        ticker,
    )

    await message.reply_text(
        "⚠️ სიახლეების მიღება ვერ მოხერხდა. "
        "სცადე მოგვიანებით."
    )


# ============================================================
# /RSI
# ============================================================

@restricted_access
async def rsi_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
  user = update.effective_user
  message = update.effective_message

  if not user or not message:
    return

  if not check_cooldown(
      user.id,
      "rsi",
  ):
    return

  if len(context.args) != 1:
    await message.reply_text(
        "გამოყენება:\n"
        "<code>/rsi TICKER</code>\n\n"
        "მაგალითი:\n"
        "<code>/rsi NVDA</code>",
        parse_mode=ParseMode.HTML,
    )
    return

  ticker = context.args[0].upper().strip()

  valid, error_message = validate_supported_ticker(
      ticker
  )

  if not valid:
    await message.reply_text(
        error_message or "❌ არასწორი ticker.",
        parse_mode=ParseMode.HTML,
    )
    return

  data = await get_rsi_analysis_data(
      ticker
  )

  if not data:
    await message.reply_text(
        f"❌ მონაცემები ვერ მოიძებნა "
        f"<b>{html.escape(ticker)}</b>-ისთვის.",
        parse_mode=ParseMode.HTML,
    )
    return

  current_price = data["current_price"]
  regular_close = data["regular_close"]
  change_pct = data[
      "change_pct_from_previous_close"
  ]

  rsi = data["rsi"]
  sma20 = data["sma20"]
  sma50 = data["sma50"]
  volatility = data["volatility"]

  rsi_status = "მონაცემი არასაკმარისია"

  if rsi is not None:
    if rsi >= 70:
      rsi_status = "შესაძლო overbought მდგომარეობა"
    elif rsi <= 30:
      rsi_status = "შესაძლო oversold მდგომარეობა"
    else:
      rsi_status = "ნეიტრალური ზონა"

  text = (
      f"📈 <b>ტექნიკური ინდიკატორები — "
      f"{html.escape(ticker)}</b>\n\n"
      f"• მიმდინარე ფასი: "
      f"<b>${float(current_price):,.2f}</b>\n"
  )

  if regular_close is not None:
    text += (
        f"• წინა დახურვა: "
        f"<b>${float(regular_close):,.2f}</b>\n"
    )

  if change_pct is not None:
    text += (
        f"• ცვლილება წინა დახურვიდან: "
        f"<b>{float(change_pct):+.2f}%</b>\n"
    )

  text += (
      f"• RSI (14): "
      f"<b>{rsi if rsi is not None else 'N/A'}</b>\n"
      f"• RSI შეფასება: <b>{rsi_status}</b>\n"
      f"• SMA 20: "
      f"<b>{f'${sma20:,.2f}' if sma20 is not None else 'N/A'}</b>\n"
      f"• SMA 50: "
      f"<b>{f'${sma50:,.2f}' if sma50 is not None else 'N/A'}</b>\n"
      f"• წლიური ვოლატილობა: "
      f"<b>{f'{volatility:.2f}%' if volatility is not None else 'N/A'}</b>\n\n"
      "⚠️ <i>ტექნიკური ინდიკატორები არ წარმოადგენს "
      "ყიდვის ან გაყიდვის გარანტირებულ სიგნალს.</i>"
  )

  await message.reply_text(
      text,
      parse_mode=ParseMode.HTML,
  )


# ============================================================
# /AI
# ============================================================

@restricted_access
async def ai_analysis_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
  user = update.effective_user
  message = update.effective_message

  if not user or not message:
    return

  if not check_cooldown(
      user.id,
      "ai",
  ):
    return

  if len(context.args) != 1:
    await message.reply_text(
        "გამოყენება:\n"
        "<code>/ai TICKER</code>\n\n"
        "მაგალითი:\n"
        "<code>/ai NVDA</code>",
        parse_mode=ParseMode.HTML,
    )
    return

  ticker = context.args[0].upper().strip()

  valid, error_message = validate_supported_ticker(
      ticker
  )

  if not valid:
    await message.reply_text(
        error_message or "❌ არასწორი ticker.",
        parse_mode=ParseMode.HTML,
    )
    return

  chat_id = str(
      update.effective_chat.id
  )

  price_data, hist = await fetch_stock_data(
      ticker
  )

  if (
      not price_data
      or price_data.get("latest_price") is None
  ):
    await message.reply_text(
        f"❌ მონაცემები ვერ მოიძებნა "
        f"<b>{html.escape(ticker)}</b>-ისთვის.",
        parse_mode=ParseMode.HTML,
    )
    return

  rsi = None

  if has_close_data(hist):
    rsi = calculate_rsi(
        hist["Close"]
    )

  sma20, sma50, volatility = (
      calculate_technical_indicators(
          hist
      )
  )

  with get_db_connection() as conn:
    position = conn.execute(
        """
        SELECT
            shares,
            buy_price
        FROM positions
        WHERE chat_id = ?
          AND ticker = ?
        """,
        (
            chat_id,
            ticker,
        ),
    ).fetchone()

  user_position = None

  if position:
    shares = float(
        position["shares"]
    )

    buy_price = float(
        position["buy_price"]
    )

    total_invested = (
        shares * buy_price
    )

    current_price = float(
        price_data["latest_price"]
    )

    current_value = (
        shares * current_price
    )

    profit_loss = (
        current_value
        - total_invested
    )

    profit_loss_pct = (
        profit_loss
        / total_invested
        * 100
        if total_invested > 0
        else 0.0
    )

    user_position = {
        "shares": shares,
        "average_buy_price": buy_price,
        "total_invested": total_invested,
        "current_value": current_value,
        "profit_loss": profit_loss,
        "profit_loss_pct": profit_loss_pct,
        "breakeven_price": buy_price,
    }

  market_package = {
      "ticker": ticker,
      "regular_close":
          price_data.get("regular_close"),
      "latest_price":
          price_data.get("latest_price"),
      "change_pct_from_previous_close":
          price_data.get(
              "change_pct_from_previous_close"
          ),
      "quote_timestamp":
          str(
              price_data.get(
                  "quote_timestamp"
              )
          ),
      "rsi14": rsi,
      "sma20": sma20,
      "sma50": sma50,
      "annualized_volatility_pct":
          volatility,
      "user_position":
          user_position,
      "data_source":
          "Yahoo Finance data accessed through yfinance",
      "data_warning": (
          "The quote may be delayed, incomplete, "
          "or inconsistent, especially during "
          "pre-market and after-hours trading."
      ),
  }

  system_prompt = (
      "შენ ხარ პროფესიონალი ფინანსური ანალიტიკოსი და რისკ-მენეჯერი.\n\n"

      "გააანალიზე მხოლოდ მომხმარებლის მიერ მოწოდებული JSON მონაცემები.\n"
      "არ გამოიყენო საკუთარი ცოდნიდან მიმდინარე ფასები, ახალი ამბები, "
      "ფინანსური შედეგები, ანალიტიკოსების სამიზნე ფასები ან სხვა ფაქტები.\n"
      "არ გამოიგონო დაკარგული ინფორმაცია.\n\n"

      "პასუხი დაწერე ქართულად, უბრალო ტექსტით, "
      "HTML-ისა და Markdown-ის გარეშე.\n\n"

      "პასუხის სტრუქტურა:\n"
      "1. მიმდინარე მდგომარეობა\n"
      "2. ტექნიკური სურათი\n"
      "3. მომხმარებლის პოზიციის მდგომარეობა და რისკი\n"
      "4. bullish სცენარი\n"
      "5. base სცენარი\n"
      "6. bearish სცენარი\n"
      "7. რა გააუქმებდა თითოეულ სცენარს\n"
      "8. მთავარი რისკები\n\n"

      "არ გასცე კატეგორიული ბრძანებები, როგორიცაა "
      "'აუცილებლად იყიდე', 'აუცილებლად გაყიდე' ან "
      "'ფასი აუცილებლად გაიზრდება'.\n\n"

      "თუ მონაცემი აკლია, პირდაპირ დაწერე, რომ "
      "დასკვნისთვის მონაცემი არასაკმარისია."
  )

  prompt = (
      "გააანალიზე მხოლოდ ქვემოთ მოცემული JSON მონაცემები.\n\n"
      + json.dumps(
          market_package,
          ensure_ascii=False,
          indent=2,
          default=str,
      )
  )

  try:
    response = await openai_client.chat.completions.create(
        model=OPENAI_MODEL,
        messages=[
            {
                "role": "system",
                "content": system_prompt,
            },
            {
                "role": "user",
                "content": prompt,
            },
        ],
        temperature=0.2,
    )

    ai_text = (
        response
        .choices[0]
        .message
        .content
    )

    safe_ai_text = html.escape(
        ai_text
        or "AI-მ ცარიელი პასუხი დააბრუნა."
    )

    safe_ticker = html.escape(
        ticker
    )

    full_response = (
        f"<b>🤖 AI ანალიზი — {safe_ticker}</b>\n\n"
        f"{safe_ai_text}\n\n"
        "⚠️ <i>ეს ანალიზი დაფუძნებულია შეზღუდულ ტექნიკურ "
        "მონაცემებზე და არ წარმოადგენს პერსონალურ "
        "საინვესტიციო რეკომენდაციას.</i>"
    )

    for chunk in safe_send_message(
        full_response
    ):
      await message.reply_text(
          chunk,
          parse_mode=ParseMode.HTML,
      )

  except Exception:
    logger.exception(
        "OpenAI analysis failed for %s",
        ticker,
    )

    await message.reply_text(
        "⚠️ AI ანალიზი დროებით მიუწვდომელია. "
        "სცადე მოგვიანებით."
    )


# ============================================================
# GLOBAL ERROR HANDLER
# ============================================================

async def error_handler(
    update: object,
    context: ContextTypes.DEFAULT_TYPE,
):
  logger.error(
      "Exception while handling an update:",
      exc_info=context.error,
  )

  if (
      isinstance(update, Update)
      and update.effective_message
  ):
    try:
      await update.effective_message.reply_text(
          "⚠️ <b>დაფიქსირდა ტექნიკური შეცდომა.</b>\n"
          "შეცდომა ჩაიწერა სისტემის ჟურნალში.",
          parse_mode=ParseMode.HTML,
      )

    except Exception:
      logger.exception(
          "Could not send error message to user"
      )


# ============================================================
# MAIN INITIALIZATION
# ============================================================

def main():
  init_db()

  application = (
      Application
      .builder()
      .token(TELEGRAM_BOT_TOKEN)
      .build()
  )

  application.add_handler(
      CommandHandler(
          "start",
          start_command,
      )
  )

  application.add_handler(
      CommandHandler(
          "help",
          help_command,
      )
  )

  application.add_handler(
      CommandHandler(
          "position",
          position_command,
      )
  )

  application.add_handler(
      CommandHandler(
          "addposition",
          add_position_command,
      )
  )

  application.add_handler(
      CommandHandler(
          "portfolio",
          portfolio_command,
      )
  )

  application.add_handler(
      CommandHandler(
          "remove",
          remove_command,
      )
  )

  application.add_handler(
      CommandHandler(
          "alert",
          alert_command,
      )
  )

  application.add_handler(
      CommandHandler(
          "alerts",
          list_alerts_command,
      )
  )

  application.add_handler(
      CommandHandler(
          "removealert",
          remove_alert_command,
      )
  )

  application.add_handler(
      CommandHandler(
          "concentration",
          concentration_command,
      )
  )

  application.add_handler(
      CommandHandler(
          "news",
          news_command,
      )
  )

  application.add_handler(
      CommandHandler(
          "rsi",
          rsi_command,
      )
  )

  application.add_handler(
      CommandHandler(
          "ai",
          ai_analysis_command,
      )
  )

  application.add_error_handler(
      error_handler
  )

  job_queue = application.job_queue

  if job_queue is None:
    raise RuntimeError(
        "JobQueue unavailable. Install dependencies with:\n"
        "pip install 'python-telegram-bot[job-queue]'"
    )

  job_queue.run_repeating(
      check_price_alerts_job,
      interval=300,
      first=10,
      name="price-alert-checker",
  )

  logger.info(
      "Financial Telegram bot is starting polling..."
  )

  application.run_polling(
      allowed_updates=Update.ALL_TYPES,
  )


if __name__ == "__main__":
  main()
