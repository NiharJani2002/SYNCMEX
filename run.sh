# Nihar Mahesh Jani — niharmaheshjani@gmail.com
#
# One script to run the entire pipeline from scratch.
# It asks how many processes you want per exchange, downloads the market data,
# compiles the C++ engine, and runs everything in parallel.
#
# Written in POSIX sh (not bash) so it works on macOS which ships bash 3.2,
# and on Linux without modification. No arrays, no associative maps, no bashisms.
#
# Usage:
#   chmod +x run.sh && ./run.sh          — interactive, prompts for process count
#   ./run.sh --procs=2                   — skip the prompt, use n=2
#   ./run.sh --no-download               — skip downloading, run engine only
#   ./run.sh --compile-only              — just (re)compile the C++ engine

set -e

ok()   { printf "  [OK] %s\n" "$*"; }
info() { printf "  [->] %s\n" "$*"; }
warn() { printf "  [!]  %s\n" "$*"; }
die()  { printf "  [ERR] %s\n" "$*" >&2; exit 1; }

RUN_DOWNLOAD=true
COMPILE_ONLY=false
PROCS_FLAG=""

for arg in "$@"; do
    case "$arg" in
        --no-download)  RUN_DOWNLOAD=false  ;;
        --compile-only) COMPILE_ONLY=true   ;;
        --procs=*)      PROCS_FLAG="${arg#*=}" ;;
        -h|--help)
            echo "Usage: ./run.sh [--no-download] [--compile-only] [--procs=N]"
            echo "  --procs=N  set process count per exchange, skips the prompt"
            exit 0 ;;
    esac
done

echo ""
echo "================================================================="
echo "  Synchronized Messaging Research Pipeline"
echo "  Nihar Mahesh Jani — niharmaheshjani@gmail.com"
echo "  11 exchanges  |  17 threads per stock  |  15 extensions"
echo "  macOS (M-chip and Intel)  |  Linux"
echo "================================================================="
echo ""

for f in config.yaml base.py engine.cpp orchestrate.py orchestrate_dl.py; do
    test -f "$f" || die "Missing required file: $f"
done
ok "Working directory: $(pwd)"
echo ""

# ── Step 1 — Directories ──────────────────────────────────────────────────────
# Create the folder layout before anything runs. Doing this once here means
# neither the downloaders nor the engine need to worry about it.
echo "[ STEP 1/5 ]  Create Directories"
EXCHANGES="NYSE NASDAQ LSE EURONEXT JPX SSE NSE ASX B3 JSE TSX"
for exch in $EXCHANGES; do
    mkdir -p "stock_exchange/${exch}"
done
mkdir -p logs state \
         result/json/individual_exchange_result \
         result/json/all_exchange_result
ok "stock_exchange/ and result/json/ directories ready"
echo ""

# ── Step 2 — Python ───────────────────────────────────────────────────────────
echo "[ STEP 2/5 ]  Locate Python 3"
PYTHON=""
for cand in python3 python python3.12 python3.11 python3.10 python3.9; do
    if command -v "$cand" >/dev/null 2>&1; then
        ver=$("$cand" --version 2>&1 | awk '{print $2}')
        maj=$(echo "$ver" | cut -d. -f1)
        if test "$maj" -ge 3 2>/dev/null; then
            PYTHON="$cand"
            ok "Python: $PYTHON ($ver)"
            break
        fi
    fi
done
test -n "$PYTHON" || die "Python 3 not found."

info "Checking Python packages..."
"$PYTHON" -m pip install --quiet yfinance pandas numpy pyyaml 2>/dev/null || true
ok "Packages ready"
echo ""

# ── Step 3 — Compile ─────────────────────────────────────────────────────────
# -march=native tells the compiler to use every CPU instruction available
# on the machine it's running on. On M-chip Macs we use -mcpu=apple-m1 instead,
# which is the equivalent for ARM.
echo "[ STEP 3/5 ]  Compile C++ Engine"
CXX=""
for cand in g++ g++-13 g++-12 g++-11 clang++; do
    command -v "$cand" >/dev/null 2>&1 && { CXX="$cand"; break; }
done
test -n "$CXX" || die "No C++ compiler found. Install Xcode Command Line Tools on macOS or build-essential on Linux."
ok "Compiler: $($CXX --version 2>&1 | head -1)"

FLAGS="-O3 -std=c++17 -pthread"
OS_NAME=$(uname -s 2>/dev/null || echo "Linux")
CPU_ARCH=$(uname -m 2>/dev/null || echo "x86_64")
if test "$OS_NAME" = "Darwin" && test "$CPU_ARCH" = "arm64"; then
    FLAGS="$FLAGS -mcpu=apple-m1"
    info "Apple Silicon detected"
else
    FLAGS="$FLAGS -march=native"
fi

info "Compiling engine.cpp..."
$CXX $FLAGS -o engine engine.cpp 2>&1 || die "Compilation failed — check the error above."
ok "Compiled → ./engine"

# Absolute path so Python's subprocess.Popen can find it.
# Path("./engine") strips the "./" and the binary becomes unfindable.
export ENGINE_BIN="$(pwd)/engine"

test "$COMPILE_ONLY" = "true" && { ok "Done."; exit 0; }
echo ""

# ── Step 0 — Process count ────────────────────────────────────────────────────
# I put this after compilation intentionally — if compilation fails there's
# no point asking for a process count. Now that we know the engine works,
# we can ask.
echo "================================================================="
echo "  STEP 0 — How many processes per exchange?"
echo "================================================================="
echo ""

if test -n "$PROCS_FLAG"; then
    N_PROCS_INPUT="$PROCS_FLAG"
    echo "  Using --procs flag: $N_PROCS_INPUT"
elif test -t 0; then
    echo "  Each process handles a shard of stocks with 17 threads."
    echo "  n=1 means 11 total processes. n=2 means 22, and so on."
    echo "  Anything above 4 is only useful if you have a lot of cores."
    echo ""
    printf "  Enter n [default=1]: "
    read N_PROCS_INPUT
else
    # Piped input — echo "2" | ./run.sh
    read N_PROCS_INPUT
    echo "  Got n=$N_PROCS_INPUT from stdin"
fi

N_PROCS=$(echo "$N_PROCS_INPUT" | tr -d '[:space:]')
case "$N_PROCS" in ''|*[!0-9]*) N_PROCS=1 ;; esac
test "$N_PROCS" -lt 1 2>/dev/null && N_PROCS=1
if test "$N_PROCS" -gt 32 2>/dev/null; then
    warn "n > 32 will likely saturate memory. Capping at 32."
    N_PROCS=32
fi

export PROCS_PER_EXCHANGE="$N_PROCS"
echo ""
ok "Processes per exchange : $N_PROCS"
ok "Total C++ processes    : $((N_PROCS * 11))  (${N_PROCS} × 11 exchanges)"
ok "Threads per stock      : 17  (1 reader + 15 extensions + 1 reserved)"
ok "Max threads            : $((N_PROCS * 11 * 17))"
echo ""

# ── Step 4 — Download ─────────────────────────────────────────────────────────
if test "$RUN_DOWNLOAD" = "true"; then
    echo "[ STEP 4/5 ]  Download Market Data"
    info "All 11 downloaders run in parallel via orchestrate_dl.py"
    info "Logs: logs/download_{exchange}.log"
    echo ""
    export LOG_DIR="logs"
    "$PYTHON" orchestrate_dl.py
    ok "Downloads complete"
    echo ""
else
    echo "[ STEP 4/5 ]  Skipping download (--no-download)"
    echo ""
fi

# ── Step 5 — Engine ───────────────────────────────────────────────────────────
echo "[ STEP 5/5 ]  C++ Engine  (${N_PROCS} processes per exchange)"
info "Launching $((N_PROCS * 11)) processes via orchestrate.py"
info "Each process: 17 threads per stock"
info "Results → result/json/"
echo ""
"$PYTHON" orchestrate.py

# Graceful shutdown handler. Ctrl+C kills the orchestrator via signal,
# which in turn kills all child processes before this script exits.
cleanup() {
    echo ""
    warn "Interrupted."
    exit 0
}
trap cleanup INT TERM

# ── Summary ───────────────────────────────────────────────────────────────────
echo ""
echo "================================================================="
echo "  Done"
echo "================================================================="
echo ""
echo "  Results:"
for exch in $EXCHANGES; do
    f="result/json/individual_exchange_result/${exch}_result.json"
    if test -f "$f"; then
        stocks=$(grep -c '"stock_name"' "$f" 2>/dev/null || echo "?")
        printf "    [OK] %-12s  %s stocks\n" "$exch" "$stocks"
    else
        printf "    [--] %-12s  no data\n" "$exch"
    fi
done
echo ""
test -f "result/json/all_exchange_result/brief_exchange_result.json" \
    && ok "brief_exchange_result.json written"
echo ""
info "Engine logs : logs/engine_*.log"
info "Download logs: logs/download_*.log"
echo ""
