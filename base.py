# Nihar Mahesh Jani — niharmaheshjani@gmail.com
#
# This is the downloader that feeds everything else. It pulls daily OHLCV data
# for each exchange and writes CSVs directly to stock_exchange/{EXCHANGE}/{ticker}.csv.
# No intermediate staging — the C++ engine reads from those paths directly.
#
# The thread pool runs 12 workers per exchange. That number is deliberate:
# yfinance gets rate-limited if you push too many concurrent requests, and
# 12 gives good throughput without triggering the backoff logic constantly.
#
# One thing I'm proud of here is the atomic write. Every file goes through
# a .tmp name first, then gets renamed atomically. That means a partial
# download never leaves a corrupt CSV sitting there for the engine to choke on.

import os, sys, time, json, mmap, logging, threading
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

def _ensure(*pkgs):
    # Install anything that's missing quietly. The user shouldn't have to
    # worry about environment setup before running.
    for p in pkgs:
        mod = "yaml" if p == "pyyaml" else p
        try:
            __import__(mod)
        except ImportError:
            import subprocess
            subprocess.check_call([sys.executable, "-m", "pip",
                                   "install", "--quiet", p])

_ensure("yfinance", "pandas", "numpy", "pyyaml")

import yfinance as yf
import pandas as pd
import numpy as np
import yaml

# Load config once at import time. Every exchange script imports this file
# so they all get the same settings from one place.
_CFG_PATH = Path(__file__).parent / "config.yaml"
with open(_CFG_PATH) as _f:
    _CFG = yaml.safe_load(_f)

G          = _CFG["global"]
EXCHANGES  = _CFG["exchanges"]

START_DATE  = G["start_date"]
END_DATE    = G["end_date"]
STOCK_ROOT  = Path(G.get("stock_root", "stock_exchange"))
LOG_DIR     = Path(G.get("log_dir",   "logs"))
MAX_RETRIES = int(G.get("retry_count",  3))
RETRY_DELAY = float(G.get("retry_delay", 1.0))
MIN_ROWS    = int(G.get("min_rows",    20))

# 12 threads is the sweet spot for yfinance without hitting rate limits.
DL_THREADS = 12


def _parse_tickers(raw: list) -> list:
    # Config uses semicolons to pack multiple tickers on one YAML line.
    # This unpacks them into a flat list.
    out = []
    for item in raw:
        for t in str(item).split(";"):
            t = t.strip()
            if t:
                out.append(t)
    return out


def _fetch_ohlcv(ticker: str) -> "pd.DataFrame | None":
    # yfinance with auto_adjust=True gives clean adjusted prices without
    # needing a separate corporate action feed. actions=False skips the
    # dividend/split table which we don't use and slows the request.
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            t   = yf.Ticker(ticker)
            raw = t.history(
                start=START_DATE,
                end=END_DATE,
                auto_adjust=True,
                actions=False,
            )
            if raw is None or raw.empty or len(raw) < MIN_ROWS:
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_DELAY * attempt)
                    continue
                return None

            raw = raw.reset_index()

            # yfinance returns "Datetime" for intraday and "Date" for daily.
            dc  = "Datetime" if "Datetime" in raw.columns else "Date"
            raw["Date"] = pd.to_datetime(raw[dc]).dt.strftime("%Y-%m-%d")

            # Column names vary slightly across yfinance versions.
            # This normalises them without failing on minor differences.
            for need in ["Open", "High", "Low", "Close", "Volume"]:
                if need not in raw.columns:
                    match = [c for c in raw.columns if c.lower() == need.lower()]
                    if match:
                        raw = raw.rename(columns={match[0]: need})

            out = pd.DataFrame({
                "Date":   raw["Date"],
                "Open":   raw["Open"].astype("float32"),
                "High":   raw["High"].astype("float32"),
                "Low":    raw["Low"].astype("float32"),
                "Close":  raw["Close"].astype("float32"),
                # float32 halves memory for volume — the values are large but
                # we don't need decimal precision here.
                "Volume": raw["Volume"].astype("float32"),
            })
            out = out.dropna(subset=["Open", "Close"])
            out = out[out["Close"] > 0]

            if len(out) < MIN_ROWS:
                return None
            return out

        except Exception:
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY * attempt)
    return None


def _save_csv(df: pd.DataFrame, out_path: Path):
    # Write to a .tmp file first, then rename atomically.
    # This way the engine never sees a half-written CSV even if we crash mid-write.
    tmp  = out_path.with_suffix(".tmp")
    data = df.to_csv(index=False, float_format="%.4f").encode()
    size = len(data)

    if size > 512 * 1024:
        # For large files, mmap lets the kernel handle page-cache flushing
        # more efficiently than a regular write() call.
        with open(tmp, "wb") as fh:
            fh.write(b"\x00" * size)
        with open(tmp, "r+b") as fh:
            mm = mmap.mmap(fh.fileno(), size)
            mm.write(data)
            mm.flush()
            mm.close()
    else:
        # Small files: one write syscall, done.
        tmp.write_bytes(data)

    os.replace(tmp, out_path)


def _validate(path: Path) -> bool:
    # Quick sanity check after downloading. If a file is under 100 bytes
    # or missing the expected columns, something went wrong and we retry.
    try:
        if path.stat().st_size < 100:
            return False
        df = pd.read_csv(path, nrows=3)
        return "Date" in df.columns and "Close" in df.columns
    except Exception:
        return False


def _stats(df: pd.DataFrame) -> dict:
    c = df["Close"].values.astype(np.float64)
    if len(c) < 2:
        return {"days": int(len(df)), "vol_pct": 0.0, "ret_pct": 0.0}
    r = np.diff(np.log(c + 1e-10))
    return {
        "days":    int(len(df)),
        "vol_pct": float(np.std(r) * np.sqrt(252) * 100),
        "ret_pct": float(np.mean(r) * 252 * 100),
    }


_counter_lock = threading.Lock()


def _worker(task: tuple) -> tuple:
    # Each thread runs this. Exceptions are caught so one bad ticker
    # can't kill the whole pool — it just gets logged as FAILED.
    ticker, out_path, total, counter = task

    # Skip if we already have a valid file. Useful when rerunning after a
    # partial download — we don't refetch what we already have.
    if out_path.exists():
        try:
            if out_path.stat().st_size > 200 and _validate(out_path):
                df_c = pd.read_csv(out_path)
                if len(df_c) >= MIN_ROWS:
                    with _counter_lock:
                        counter[0] += 1
                        n = counter[0]
                    return (ticker, "CACHED", _stats(df_c), n, total)
        except Exception:
            pass

    df = _fetch_ohlcv(ticker)
    with _counter_lock:
        counter[0] += 1
        n = counter[0]

    if df is not None:
        _save_csv(df, out_path)
        return (ticker, "OK", _stats(df), n, total)
    return (ticker, "FAILED", {}, n, total)


def _make_logger(code: str) -> logging.Logger:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    fmt    = "%(asctime)s [%(levelname)s] %(message)s"
    logger = logging.getLogger(f"dl.{code}")
    if not logger.handlers:
        logger.setLevel(logging.DEBUG)
        fh = logging.FileHandler(LOG_DIR / f"download_{code.lower()}.log")
        fh.setFormatter(logging.Formatter(fmt))
        ch = logging.StreamHandler(sys.stdout)
        ch.setFormatter(logging.Formatter(fmt))
        logger.addHandler(fh)
        logger.addHandler(ch)
    return logger


def download_exchange(exchange_code: str):
    """
    Main entry point for each exchange downloader.
    Downloads all tickers and saves to stock_exchange/{EXCHANGE}/{ticker}.csv
    """
    code   = exchange_code.upper()
    cfg    = EXCHANGES.get(code, {})
    logger = _make_logger(code)

    if not cfg:
        logger.error(f"Exchange {code} not found in config.yaml")
        return 0, 0

    # Create output directory once before launching threads.
    # No point doing mkdir inside every worker.
    out_dir = STOCK_ROOT / code
    out_dir.mkdir(parents=True, exist_ok=True)

    tickers = _parse_tickers(cfg.get("tickers", []))

    logger.info("=" * 60)
    logger.info(f"Exchange : {code}  — {cfg.get('full_name', '')}")
    logger.info(f"Tickers  : {len(tickers)}")
    logger.info(f"Period   : {START_DATE} → {END_DATE}")
    logger.info(f"Threads  : {DL_THREADS}")
    logger.info(f"Output   : {out_dir}/")
    logger.info("=" * 60)

    counter = [0]
    tasks   = [(t, out_dir / f"{t.lower()}.csv", len(tickers), counter)
               for t in tickers]

    downloaded, failed, stats_map = [], [], {}
    t0 = time.time()

    with ThreadPoolExecutor(max_workers=DL_THREADS) as pool:
        futures = {pool.submit(_worker, task): task[0] for task in tasks}
        for fut in as_completed(futures):
            try:
                ticker, status, st, n, total = fut.result()
                stats_map[ticker.lower()] = st
                if status in ("OK", "CACHED"):
                    downloaded.append(ticker)
                    logger.info(
                        f"  [{n:3d}/{total}] {ticker:<18s} {status:<7s}"
                        f"  {st.get('days', 0):4d}d"
                        f"  vol={st.get('vol_pct', 0):5.1f}%"
                        f"  ret={st.get('ret_pct', 0):+6.1f}%"
                    )
                else:
                    failed.append(ticker)
                    logger.warning(f"  [{n:3d}/{total}] {ticker:<18s} FAILED")
            except Exception as e:
                t = futures[fut]
                failed.append(t)
                logger.error(f"  Worker exception [{t}]: {e}")

    # Parallel validation after all downloads complete.
    # Occasionally a response looks complete but the CSV is malformed.
    bad = []
    with ThreadPoolExecutor(max_workers=8) as pool:
        vfuts = {pool.submit(_validate, out_dir / f"{t.lower()}.csv"): t
                 for t in downloaded}
        for vf in as_completed(vfuts):
            t = vfuts[vf]
            if not vf.result():
                bad.append(t)
                logger.warning(f"  Validation failed (removing): {t}")
    for t in bad:
        if t in downloaded:
            downloaded.remove(t)
        failed.append(t)
        try:
            (out_dir / f"{t.lower()}.csv").unlink()
        except Exception:
            pass

    # Write a manifest so it's easy to see what was downloaded
    # without listing the directory.
    manifest = out_dir / "tickers_downloaded.txt"
    manifest.write_text("\n".join(t.lower() for t in downloaded) + "\n")

    elapsed = time.time() - t0
    logger.info("=" * 60)
    logger.info(f"Done — {len(downloaded)}/{len(tickers)} OK  "
                f"failed={len(failed)}  time={elapsed:.0f}s")
    if failed:
        logger.warning(f"Failed tickers: {failed}")

    return len(downloaded), len(failed)


def main(exchange_code: str):
    ok, _ = download_exchange(exchange_code)
    sys.exit(0 if ok > 0 else 1)
