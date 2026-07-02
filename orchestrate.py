# Nihar Mahesh Jani — niharmaheshjani@gmail.com
#
# This script launches all the C++ engine processes and waits for them to finish.
# The number of processes per exchange comes from run.sh via PROCS_PER_EXCHANGE.
# For n=2 and 11 exchanges, that's 22 processes running simultaneously.
#
# Each process writes its own individual exchange result JSON. Once they've
# all finished, this script aggregates everything into brief_exchange_result.json.
# That aggregation step is important — without it, whichever process finishes
# last overwrites the brief file and you only see one exchange in the output.

import os, sys, time, json, signal, subprocess
from pathlib import Path

EXCHANGES = ["NYSE", "NASDAQ", "LSE", "EURONEXT", "JPX",
             "SSE", "NSE", "ASX", "B3", "JSE", "TSX"]

# Resolve to absolute path before launching subprocesses.
# Path("./engine") strips the "./" and subprocess won't find it in cwd.
_engine_raw = os.environ.get("ENGINE_BIN", "./engine")
ENGINE      = os.path.abspath(_engine_raw)
LOG_DIR     = Path(os.environ.get("LOG_DIR", "logs"))
N_PROCS     = int(os.environ.get("PROCS_PER_EXCHANGE", "1"))

LOG_DIR.mkdir(parents=True, exist_ok=True)

procs     = []  # (Popen, label, log_file_handle)
_shutdown = False


def handle_signal(sig, _):
    global _shutdown
    _shutdown = True
    print(f"\n  Shutting down — stopping {len(procs)} workers...")
    for p, _, _ in procs:
        try:
            p.terminate()
        except Exception:
            pass


signal.signal(signal.SIGINT,  handle_signal)
signal.signal(signal.SIGTERM, handle_signal)

print(f"\n  [orchestrate] Launching {N_PROCS} × {len(EXCHANGES)} = "
      f"{N_PROCS * len(EXCHANGES)} engine processes\n")

for exchange in EXCHANGES:
    for proc_id in range(N_PROCS):
        label    = f"{exchange}_p{proc_id}"
        log_path = LOG_DIR / f"engine_{label}.log"
        cmd      = [ENGINE,
                    "--exchange", exchange,
                    "--proc-id",  str(proc_id),
                    "--n-procs",  str(N_PROCS)]
        log_fh   = open(log_path, "a", buffering=1)
        p        = subprocess.Popen(cmd, stdout=log_fh, stderr=log_fh)
        procs.append((p, label, log_fh))
        print(f"  [+] {label:<16s} PID={p.pid:6d}  log={log_path.name}")

print(f"\n  All {len(procs)} workers running. Waiting for completion...\n")

done = 0
while procs and not _shutdown:
    time.sleep(2)
    still = []
    for p, label, fh in procs:
        ret = p.poll()
        if ret is None:
            still.append((p, label, fh))
        else:
            fh.close()
            status = "OK" if ret == 0 else f"EXIT={ret}"
            print(f"  [{'OK' if ret == 0 else 'ERR'}] {label:<16s} {status}")
            done += 1
    procs[:] = still

for p, label, fh in procs:
    try:
        p.wait(timeout=5)
    except Exception:
        p.kill()
    fh.close()

print(f"\n  [orchestrate] All {done} workers done.\n")

# Aggregate brief_exchange_result.json from all individual exchange files.
# Each C++ process writes only its own exchange to the brief file, so the
# last writer wins and you lose the rest. Reading all individual files here
# and merging them after all processes finish is the clean solution.

_indiv_dir  = Path("result/json/individual_exchange_result")
_brief_dir  = Path("result/json/all_exchange_result")
_brief_path = _brief_dir / "brief_exchange_result.json"
_brief_dir.mkdir(parents=True, exist_ok=True)

all_briefs = []
for exch in EXCHANGES:
    fpath = _indiv_dir / f"{exch}_result.json"
    if not fpath.exists():
        continue
    try:
        stocks = json.loads(fpath.read_text())
        if not stocks:
            continue
        brief = {"exchange_name": exch}
        for ext_i in range(1, 16):
            key  = f"ext{ext_i:02d}_result"
            vals = [s[key]["p50_ns"] for s in stocks
                    if key in s and isinstance(s[key], dict)]
            brief[f"ext{ext_i:02d}_average"] = (
                round(sum(vals) / len(vals), 4) if vals else 0.0
            )
        all_briefs.append(brief)
        print(f"  [+] {exch:<12s} aggregated  ({len(stocks)} stocks)")
    except Exception as e:
        print(f"  [!] Could not read {fpath.name}: {e}")

_brief_path.write_text(json.dumps(all_briefs, indent=2))
print(f"\n  [OK] brief_exchange_result.json — {len(all_briefs)}/11 exchanges\n")
