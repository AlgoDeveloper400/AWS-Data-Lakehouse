"""
mt5_to_s3.py
------------
Streams live tick data from MetaTrader 5 directly to S3.

Collection windows (SAST = UTC+2, no DST):
    - 07:50:00 – 08:00:00 SAST  (600 seconds → max 600 ticks per symbol)
    - 13:50:00 – 14:00:00 SAST  (600 seconds → max 600 ticks per symbol)

Rules:
    - Only the FIRST tick seen within each UTC second is recorded (1-second bars).
    - Collection is skipped on weekends and when MT5 reports the symbol as closed.
    - Each window produces one CSV per symbol, uploaded to S3 on window close.
    - File naming: BTCUSD_20260306_0750.csv  (symbol_date_windowstart in SAST)
    - Tick DateTime column is stored in UTC for financial data best practice.

Requirements (Windows only):
    pip install MetaTrader5 boto3 python-dotenv

Usage:
    python mt5_to_s3.py
    python mt5_to_s3.py --config symbols.json
    python mt5_to_s3.py --loglevel DEBUG
"""

import argparse
import csv
import io
import json
import logging
import os
import signal
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import boto3
import MetaTrader5 as mt5
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Load AWS credentials from .env
# ---------------------------------------------------------------------------

ENV_PATH = Path(
    r"your\base\path\AWS Data Lakehouse\.env"
)

if not ENV_PATH.exists():
    print(f"[ERROR] .env file not found at: {ENV_PATH}")
    print("        Please create it with AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION")
    sys.exit(1)

load_dotenv(dotenv_path=ENV_PATH)

_required_env_vars = ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_REGION", "BUCKET"]
_missing = [v for v in _required_env_vars if not os.getenv(v)]
if _missing:
    print(f"[ERROR] Missing required variables in .env: {', '.join(_missing)}")
    sys.exit(1)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

log.info("Loaded AWS credentials from: %s", ENV_PATH)

# ---------------------------------------------------------------------------
# Timezone — SAST (South Africa Standard Time = UTC+2, no DST)
# ---------------------------------------------------------------------------

SAST = ZoneInfo("Africa/Johannesburg")

# ---------------------------------------------------------------------------
# Collection windows (SAST).
# Each entry: (start_hour, start_minute, end_hour, end_minute)
# ---------------------------------------------------------------------------

COLLECTION_WINDOWS = [
    (7,  50, 8,  0),   # 07:50 – 08:00 SAST
    (13, 50, 14, 0),   # 13:50 – 14:00 SAST
]

MAX_TICKS_PER_WINDOW = 600   # 600 seconds × 1 tick/second

# ---------------------------------------------------------------------------
# Crypto symbols — the only instruments collected on weekends.
# Add or remove symbols here to match your broker's exact naming.
# Matching is case-insensitive.
# ---------------------------------------------------------------------------

CRYPTO_SYMBOLS = {
    "BTCUSD", "ETHUSD", "LTCUSD", "XRPUSD", "BCHUSD",
    "ADAUSD", "DOTUSD", "SOLUSD", "DOGEUSD", "BNBUSD",
}

# ---------------------------------------------------------------------------
# CSV columns
# ---------------------------------------------------------------------------

CSV_COLUMNS = ["DateTime", "Bid", "Ask"]

# ---------------------------------------------------------------------------
# Helpers: window scheduling
# ---------------------------------------------------------------------------

def current_sast() -> datetime:
    """Return the current time in SAST (Africa/Johannesburg, UTC+2)."""
    return datetime.now(SAST)


def window_for(now: datetime):
    """
    Return the (start_dt, end_dt) of the collection window that `now` falls
    inside, or None if we are between windows. `now` must be SAST-aware.
    """
    for sh, sm, eh, em in COLLECTION_WINDOWS:
        start = now.replace(hour=sh, minute=sm, second=0, microsecond=0)
        end   = now.replace(hour=eh, minute=em, second=0, microsecond=0)
        if start <= now < end:
            return start, end
    return None


def _window_key(dt: datetime, start_hour: int, start_minute: int) -> str:
    """Unique string key for a (date, window) pair — used to track completion."""
    return dt.strftime("%Y%m%d") + f"_{start_hour:02d}{start_minute:02d}"


def next_uncompleted_window(now: datetime, completed: set[str]) -> datetime:
    """
    Return the SAST datetime of the next collection window that has NOT yet
    been completed. Checks remaining windows today first, then future days.
    Scans up to 7 days ahead.
    """
    # Check windows still ahead today (not in progress, not past)
    for sh, sm, eh, em in COLLECTION_WINDOWS:
        win_start = now.replace(hour=sh, minute=sm, second=0, microsecond=0)
        win_end   = now.replace(hour=eh, minute=em, second=0, microsecond=0)
        wkey      = _window_key(now, sh, sm)
        if now < win_start and wkey not in completed:
            return win_start

    # Nothing left today — scan forward day by day
    candidate_day = now + timedelta(days=1)
    for _ in range(7):
        day_start = candidate_day.replace(hour=0, minute=0, second=0, microsecond=0)
        for sh, sm, _, _ in COLLECTION_WINDOWS:
            win_start = day_start.replace(hour=sh, minute=sm, second=0, microsecond=0)
            wkey      = _window_key(win_start, sh, sm)
            if wkey not in completed:
                return win_start
        candidate_day += timedelta(days=1)

    raise RuntimeError("Could not find an uncompleted window within 7 days.")


def is_symbol_market_open(symbol: str) -> bool:
    """
    Ask MT5 whether the symbol currently has an active market.
    trade_mode == 0  →  SYMBOL_TRADE_MODE_DISABLED (market closed for this broker).
    Note: some symbols (e.g. crypto) trade on weekends — we rely solely on MT5
    trade_mode rather than the calendar day.
    """
    info = mt5.symbol_info(symbol)
    if info is None:
        log.warning("symbol_info('%s') returned None — treating as closed.", symbol)
        return False
    if info.trade_mode == 0:
        log.debug("%s  trade_mode=DISABLED — market closed.", symbol)
        return False
    return True

def is_crypto(symbol: str) -> bool:
    """Return True if the symbol is in the crypto set (case-insensitive)."""
    return symbol.upper() in CRYPTO_SYMBOLS

def load_config(path: str) -> dict:
    p = Path(path)
    if not p.exists():
        log.error("Config file not found: %s", p.resolve())
        sys.exit(1)

    with p.open() as fh:
        cfg = json.load(fh)

    if "symbols" not in cfg or not isinstance(cfg["symbols"], list) or not cfg["symbols"]:
        log.error("Config must contain a non-empty 'symbols' list.")
        sys.exit(1)

    cfg.setdefault("settings", {})
    s = cfg["settings"]
    s.setdefault("s3_prefix",        "raw")
    s.setdefault("poll_interval_ms", 200)   # tighter poll inside windows

    log.info("Loaded %d symbol(s): %s", len(cfg["symbols"]), cfg["symbols"])
    return cfg

# ---------------------------------------------------------------------------
# MT5 connection
# ---------------------------------------------------------------------------

def mt5_connect():
    log.info("Connecting to MetaTrader 5 terminal...")
    if not mt5.initialize():
        log.error("mt5.initialize() failed: %s", mt5.last_error())
        log.error("Make sure MetaTrader 5 is open and logged into an account.")
        sys.exit(1)

    info = mt5.terminal_info()
    log.info(
        "Connected  |  terminal=%s  build=%s  connected=%s",
        info.name, info.build, info.connected,
    )


def validate_symbols(symbols: list[str]) -> list[str]:
    valid = []
    for sym in symbols:
        info = mt5.symbol_info(sym)
        if info is None:
            log.warning(
                "Symbol '%s' not found in MT5 — skipping. "
                "Check the exact name in Market Watch.",
                sym,
            )
            continue
        if not info.visible:
            log.info("Enabling market data for %s...", sym)
            mt5.symbol_select(sym, True)
            time.sleep(0.2)
        valid.append(sym)
        log.info("  %-14s  digits=%d  spread=%d", sym, info.digits, info.spread)
    return valid

# ---------------------------------------------------------------------------
# Tick collection
# ---------------------------------------------------------------------------

_last_tick_ms: dict[str, int] = {}


def get_new_ticks(symbol: str) -> list[dict]:
    """
    Pull ticks that arrived since the last poll and return only the FIRST tick
    seen in each UTC second (1-second bar logic).
    """
    now_ms  = int(current_sast().astimezone(timezone.utc).timestamp() * 1000)
    from_ms = _last_tick_ms.get(symbol, now_ms - 2_000)
    from_dt = datetime.fromtimestamp(from_ms / 1000, tz=timezone.utc)
    raw     = mt5.copy_ticks_from(symbol, from_dt, 5_000, mt5.COPY_TICKS_ALL)

    if raw is None or len(raw) == 0:
        return []

    new_ticks: list[dict] = []
    seen_seconds: set[str] = set()   # tracks which UTC seconds already have a tick
    max_ms = from_ms

    for t in raw:
        tick_ms = int(t["time_msc"])
        if tick_ms <= from_ms:
            continue

        flags = int(t["flags"])
        # Only bid/ask change ticks (flags 2=BID, 4=ASK)
        if not (flags & 2 or flags & 4):
            continue

        ts_utc  = datetime.fromtimestamp(tick_ms / 1000, tz=timezone.utc)
        sec_key = ts_utc.strftime("%Y-%m-%d %H:%M:%S")   # truncated to second

        # Keep only the first tick in each second
        if sec_key in seen_seconds:
            if tick_ms > max_ms:
                max_ms = tick_ms
            continue

        seen_seconds.add(sec_key)
        new_ticks.append({
            "DateTime": ts_utc.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
            "Bid":      float(t["bid"]),
            "Ask":      float(t["ask"]),
        })

        if tick_ms > max_ms:
            max_ms = tick_ms

    if new_ticks:
        _last_tick_ms[symbol] = max_ms

    return new_ticks

# ---------------------------------------------------------------------------
# Per-symbol window buffer
# ---------------------------------------------------------------------------

class WindowBuffer:
    """
    Accumulates ticks for one symbol during a single collection window.
    Enforces MAX_TICKS_PER_WINDOW.
    """

    def __init__(self, symbol: str, window_start: datetime):
        self.symbol       = symbol.upper()
        self.window_start = window_start
        self.ticks: list[dict] = []

    def add(self, ticks: list[dict]):
        remaining = MAX_TICKS_PER_WINDOW - len(self.ticks)
        if remaining <= 0:
            return
        self.ticks.extend(ticks[:remaining])

    def is_full(self) -> bool:
        return len(self.ticks) >= MAX_TICKS_PER_WINDOW

    def flush(self) -> list[dict]:
        ticks      = self.ticks
        self.ticks = []
        return ticks

# ---------------------------------------------------------------------------
# S3 helpers
# ---------------------------------------------------------------------------

def build_s3_key(prefix: str, symbol: str, window_start: datetime) -> str:
    """raw/BTCUSD/BTCUSD_20260306_0750.csv"""
    sym      = symbol.upper()
    date_str = window_start.strftime("%Y%m%d")
    time_str = window_start.strftime("%H%M")
    return f"{prefix}/{sym}/{sym}_{date_str}_{time_str}.csv"


def ticks_to_csv_bytes(ticks: list[dict]) -> bytes:
    buf    = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=CSV_COLUMNS, extrasaction="ignore",
                            lineterminator="\n")
    writer.writeheader()
    writer.writerows(ticks)
    return buf.getvalue().encode("utf-8")


def upload_to_s3(s3_client, bucket: str, key: str, ticks: list[dict]):
    data = ticks_to_csv_bytes(ticks)
    try:
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=data,
            ContentType="text/csv",
        )
        log.info(
            "Uploaded  s3://%s/%s  (%d ticks, %.1f KB)",
            bucket, key, len(ticks), len(data) / 1024,
        )
    except Exception as exc:
        log.error("S3 upload failed for %s: %s", key, exc)

# ---------------------------------------------------------------------------
# Graceful shutdown
# ---------------------------------------------------------------------------

_running = True


def _handle_signal(signum, frame):
    global _running
    log.info("Shutdown signal received — flushing buffers and stopping...")
    _running = False


signal.signal(signal.SIGINT,  _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run(config_path: str):
    cfg      = load_config(config_path)
    settings = cfg["settings"]

    bucket  = os.environ["BUCKET"]
    prefix  = settings["s3_prefix"].rstrip("/")
    poll_ms = int(settings["poll_interval_ms"])

    s3_client = boto3.client(
        "s3",
        region_name=os.environ["AWS_REGION"],
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    )

    mt5_connect()
    symbols = validate_symbols(cfg["symbols"])

    if not symbols:
        log.error("No valid symbols found in MT5. Exiting.")
        mt5.shutdown()
        sys.exit(1)

    log.info(
        "Monitoring %d symbol(s) | poll=%dms | max %d ticks/window",
        len(symbols), poll_ms, MAX_TICKS_PER_WINDOW,
    )
    log.info("Collection windows (SAST): 07:50-08:00 and 13:50-14:00 | Press Ctrl+C to stop.")

    poll_secs = poll_ms / 1000

    # active WindowBuffer per symbol (None when outside a window)
    buffers: dict[str, WindowBuffer | None] = {sym: None for sym in symbols}

    # Track which (date, window_start_hhmm) combos have been collected or skipped,
    # so we never re-enter a window we already processed or deliberately skipped.
    completed_windows: set[str] = set()

    # ------------------------------------------------------------------
    # Startup: mark already-past or in-progress windows as done.
    # Each window is independent — missing one never affects the other.
    # ------------------------------------------------------------------
    now = current_sast()
    skipped, ahead = [], []
    for sh, sm, eh, em in COLLECTION_WINDOWS:
        win_start = now.replace(hour=sh, minute=sm, second=0, microsecond=0)
        win_end   = now.replace(hour=eh, minute=em, second=0, microsecond=0)
        wkey      = _window_key(now, sh, sm)
        if now >= win_end:
            # Window has fully passed — skip it today, collect tomorrow
            completed_windows.add(wkey)
            skipped.append(f"{sh:02d}:{sm:02d}")
        elif now >= win_start:
            # Window is currently in progress — skip it (never join mid-window)
            completed_windows.add(wkey)
            skipped.append(f"{sh:02d}:{sm:02d} (in progress)")
        else:
            ahead.append(f"{sh:02d}:{sm:02d}")

    if skipped:
        log.info(
            "Startup (%s SAST): skipping windows already past/in-progress today: %s",
            now.strftime("%H:%M"), ", ".join(skipped),
        )
    if ahead:
        log.info("Startup: windows still ahead today: %s SAST", ", ".join(ahead))
    else:
        nxt = next_uncompleted_window(now, completed_windows)
        log.info(
            "Startup: all windows past for today. Next window: %s SAST",
            nxt.strftime("%Y-%m-%d %H:%M"),
        )

    total_ticks = 0
    total_files = 0

    while _running:
        now    = current_sast()
        window = window_for(now)

        # ----------------------------------------------------------------
        # Outside any window
        # ----------------------------------------------------------------
        if window is None:
            nxt  = next_uncompleted_window(now, completed_windows)
            wait = (nxt - now).total_seconds()
            log.info(
                "Outside collection windows. Next window: %s SAST  (%.0f s away)",
                nxt.strftime("%Y-%m-%d %H:%M:%S"), wait,
            )
            _sleep_chunked(min(wait, 3600))
            continue

        win_start, win_end = window
        wkey = _window_key(now, win_start.hour, win_start.minute)

        # ----------------------------------------------------------------
        # Skip windows we've already completed or deliberately skipped
        # ----------------------------------------------------------------
        if wkey in completed_windows:
            nxt  = next_uncompleted_window(now, completed_windows)
            wait = (nxt - now).total_seconds()
            log.debug(
                "Window %s already done. Next uncompleted window: %s SAST  (%.0f s)",
                wkey, nxt.strftime("%Y-%m-%d %H:%M"), wait,
            )
            _sleep_chunked(min(wait, 3600))
            continue

        # ----------------------------------------------------------------
        # Inside an active window — initialise per-symbol buffers
        # ----------------------------------------------------------------
        for sym in symbols:
            if buffers[sym] is None:
                buffers[sym] = WindowBuffer(sym, win_start)
                log.info(
                    "Window open: %s–%s SAST  |  collecting %s",
                    win_start.strftime("%H:%M"), win_end.strftime("%H:%M"), sym,
                )

        # ----------------------------------------------------------------
        # Poll each symbol.
        # On weekends, only crypto symbols are collected — all others are
        # skipped regardless of what MT5 reports for trade_mode.
        # ----------------------------------------------------------------
        cycle_start  = time.monotonic()
        is_wknd      = current_sast().weekday() >= 5   # Saturday=5, Sunday=6

        for sym in symbols:
            buf = buffers[sym]
            if buf is None:
                continue

            # Weekend guard — non-crypto symbols sit out entirely
            if is_wknd and not is_crypto(sym):
                log.debug("%s  weekend — non-crypto, skipping.", sym)
                continue

            if not is_symbol_market_open(sym):
                log.debug("%s  market closed by broker — skipping tick fetch.", sym)
                continue

            if buf.is_full():
                log.debug("%s  buffer full (%d ticks).", sym, MAX_TICKS_PER_WINDOW)
                continue

            ticks = get_new_ticks(sym)
            if ticks:
                buf.add(ticks)
                total_ticks += len(ticks)
                log.debug(
                    "%s  +%d tick(s)  buffer=%d/%d",
                    sym, len(ticks), len(buf.ticks), MAX_TICKS_PER_WINDOW,
                )

        # ----------------------------------------------------------------
        # Window just closed — flush and upload, then mark complete
        # ----------------------------------------------------------------
        if now >= win_end:
            for sym in symbols:
                buf = buffers[sym]
                if buf is not None:
                    collected = buf.flush()
                    if collected:
                        key = build_s3_key(prefix, sym, buf.window_start)
                        upload_to_s3(s3_client, bucket, key, collected)
                        total_files += 1
                    elif is_wknd and not is_crypto(sym):
                        log.debug("%s  weekend non-crypto — no ticks collected, nothing uploaded.", sym)
                    else:
                        log.warning("%s  window ended with 0 ticks — nothing uploaded.", sym)
                    buffers[sym] = None

            completed_windows.add(wkey)
            log.info("Window %s complete and marked done.", wkey)

        # ----------------------------------------------------------------
        # Sleep for the remainder of the poll cycle
        # ----------------------------------------------------------------
        elapsed   = time.monotonic() - cycle_start
        sleep_for = max(0.0, poll_secs - elapsed)
        if sleep_for > 0 and _running:
            time.sleep(sleep_for)

    # ----------------------------------------------------------------
    # Shutdown: flush any open window buffers
    # ----------------------------------------------------------------
    log.info("Flushing remaining window buffers...")
    for sym, buf in buffers.items():
        if buf is not None and buf.ticks:
            collected = buf.flush()
            key       = build_s3_key(prefix, sym, buf.window_start)
            upload_to_s3(s3_client, bucket, key, collected)
            total_files += 1

    mt5.shutdown()
    log.info("Stopped. Total ticks: %d | Files uploaded: %d", total_ticks, total_files)




# ---------------------------------------------------------------------------
# Helper: interruptible sleep
# ---------------------------------------------------------------------------

def _sleep_chunked(seconds: float, chunk: float = 5.0):
    """Sleep for `seconds` total, waking every `chunk` seconds to check _running."""
    remaining = seconds
    while _running and remaining > 0:
        time.sleep(min(chunk, remaining))
        remaining -= chunk


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=(
            "Stream MT5 1-second ticks to S3 during two daily windows "
            "(07:50-08:00 and 13:50-14:00 SAST)."
        ),
    )
    parser.add_argument(
        "--config", default="symbols.json",
        help="Path to symbols JSON config (default: symbols.json)",
    )
    parser.add_argument(
        "--loglevel", default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Log verbosity (default: INFO)",
    )
    args = parser.parse_args()
    logging.getLogger().setLevel(getattr(logging, args.loglevel))
    run(args.config)