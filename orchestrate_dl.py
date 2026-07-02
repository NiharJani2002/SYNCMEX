# Nihar Mahesh Jani — niharmaheshjani@gmail.com
#
# Launches all 11 exchange downloaders simultaneously as separate processes.
# Each one runs independently — if B3 is slow, it doesn't hold up NYSE.
# Stdout and stderr from each downloader go into logs/download_{exchange}.log.

import os, sys, subprocess, signal, time
from pathlib import Path

EXCHANGES = ["NYSE", "NASDAQ", "LSE", "EURONEXT", "JPX",
             "SSE", "NSE", "ASX", "B3", "JSE", "TSX"]

LOG_DIR = Path(os.environ.get("LOG_DIR", "logs"))
PYTHON  = sys.executable
LOG_DIR.mkdir(parents=True, exist_ok=True)

procs     = []
_shutdown = False


def stop_all(sig, _):
    global _shutdown
    _shutdown = True
    for p, _ in procs:
        try:
            p.terminate()
        except Exception:
            pass


signal.signal(signal.SIGINT,  stop_all)
signal.signal(signal.SIGTERM, stop_all)

print(f"  Launching {len(EXCHANGES)} downloaders in parallel...")

for ex in EXCHANGES:
    script = f"{ex.lower()}.py"
    if not Path(script).exists():
        print(f"  [!] {script} not found — skipping {ex}")
        continue
    fh = open(LOG_DIR / f"download_{ex.lower()}.log", "a", buffering=1)
    p  = subprocess.Popen([PYTHON, "-u", script], stdout=fh, stderr=fh)
    procs.append((p, ex))
    print(f"  [+] {ex:<10s} PID={p.pid}")

while procs and not _shutdown:
    time.sleep(3)
    still = []
    for p, ex in procs:
        ret = p.poll()
        if ret is None:
            still.append((p, ex))
        else:
            print(f"  [{'OK' if ret == 0 else 'ERR'}] {ex} done (exit={ret})")
    procs[:] = still

print("  All downloaders finished.")
