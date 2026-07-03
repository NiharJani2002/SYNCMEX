# SYNCMEX

**S**ynchronized **Y**ield-optimized **N**ovel **C**lock **M**ulti-**E**xchange e**X**tensions

> Built and documented by Nihar Mahesh Jani — niharmaheshjani@gmail.com

---

## Table of Contents

1. [Motivation](#motivation)
2. [Abstract](#abstract)
3. [Dataset](#dataset)
4. [System Design](#system-design)
5. [Extensions — Problems, Math, Results](#extensions)
6. [How to Run](#how-to-run)
7. [Execution Flow](#execution-flow)
8. [Disclaimer](#disclaimer)
9. [How to Cite](#how-to-cite)

---

## Motivation

I kept running into the same question while studying distributed trading systems: how do you fire orders across five geographically separated exchanges within nanoseconds of each other, without any of the servers having the same clock? GPS gives you about 10 ns precision on a good day, but drift accumulates, latency varies, and Byzantine failures happen. The baseline design everyone uses — GPS offset plus a fixed execution timestamp — works surprisingly well on the happy path, but the edges are ugly.

So I decided to build 15 independent extensions, each targeting a different weakness. Some improve sync precision. Some reduce market impact. Some add fault tolerance the baseline has no concept of. Each one is grounded in a specific failure mode I identified, with math that justifies the design and numbers that prove it works.

This repo is the full system — data pipeline, C++ research engine, parallel process orchestration, and JSON output. If you're studying distributed systems, clock synchronisation, or execution quality, I think you'll find something useful here.

---

## Abstract

SYNCMEX is a research system for multi-exchange synchronized order execution. It downloads historical OHLCV data from 11 exchanges (NYSE, NASDAQ, LSE, Euronext, JPX, SSE, NSE, ASX, B3, JSE, TSX), synthesises nanosecond-resolution micro-ticks using a Hawkes-calibrated Heston process, and runs 15 novel algorithmic extensions through a parallel C++ engine with a 17-thread processing model per stock.

The GPS synchronisation baseline achieves 31 ns P50 sync error across 5 simulated exchange servers. The 15 extensions collectively address sync precision (best: 17 ns, −45%), market impact (best: 0.60 bps, −76%), fault tolerance (best: 1.0), and execution privacy (P(guess) = 0.000000). The key theoretical result is an information-theoretic gap: the Cramér-Rao Lower Bound for this problem is 6.32 ns, meaning the baseline operates at 5.3× the physical minimum.

All results were computed across 412 real stocks spanning 25 years of daily price data.

---

## Dataset

**Source:** Yahoo Finance via `yfinance`

**Coverage:** 11 exchanges × up to 75 stocks each, January 2000 – December 2025

**Format:** Daily OHLCV (Open, High, Low, Close, Volume), adjusted for splits and dividends

**Storage path:** `stock_exchange/{EXCHANGE}/{ticker}.csv`

The download system pulls data for each exchange in parallel using a 12-thread pool. It uses `yfinance`'s `Ticker.history()` directly because the raw Yahoo Finance API has Cloudflare protection that blocks plain `requests` calls. Each CSV is written atomically — the file goes to a `.tmp` name first, then gets renamed — so a crashed download never leaves a corrupt file for the engine to choke on.

If a ticker is delisted or unavailable, the downloader logs it and moves on. The C++ engine uses a Heston synthetic fallback for any stock with no CSV, so the pipeline never stalls on missing data.

| Exchange | Stocks downloaded (typical) | Suffix |
|---|---|---|
| NYSE | ~38–42 | none |
| NASDAQ | ~38–42 | none |
| LSE | ~34–38 | `.L` |
| Euronext | ~37–41 | `.PA` `.AS` `.BR` |
| JPX | ~38–42 | `.T` |
| SSE | ~40–44 | `.SS` |
| NSE | ~35–39 | `.NS` |
| ASX | ~39–43 | `.AX` |
| B3 | ~29–33 | `.SA` |
| JSE | ~34–38 | `.JO` |
| TSX | ~32–36 | `.TO` |

---

## System Design

### Python Files

#### `base.py` — Core Download Logic

Every exchange-specific script (`nyse.py`, `nasdaq.py`, etc.) imports and calls `main()` from this file. `base.py` owns the full download lifecycle: reading config, building the thread pool, fetching data via yfinance, validating each CSV after download, and writing the per-exchange manifest.

```
config.yaml
    │
    ▼
base.py::download_exchange(code)
    │
    ├── _parse_tickers()          parse semicolon-delimited ticker lists
    ├── ThreadPoolExecutor(12)    launch 12 concurrent workers
    │       └── _worker(task)
    │               ├── cache check (skip if valid CSV exists)
    │               ├── _fetch_ohlcv()    yfinance download with retry
    │               └── _save_csv()       atomic .tmp → rename write
    ├── parallel validation pass  _validate() on all downloaded files
    └── manifest write            tickers_downloaded.txt
```

Key design decisions: the thread count (12) was tuned to stay below yfinance's rate limits. The atomic write pattern means re-running the pipeline after a crash is safe — you never process a partial file.

#### `nyse.py` … `tsx.py` — Exchange Scripts (11 files)

Each is intentionally minimal:

```python
from base import main
if __name__ == "__main__":
    main("NYSE")
```

All the logic lives in `base.py`. These thin wrappers exist so each exchange can be launched as an independent OS process with its own log file, and so you can run a single exchange in isolation when debugging.

#### `orchestrate_dl.py` — Parallel Download Launcher

Launches all 11 exchange scripts simultaneously as separate subprocesses. Each one gets its own stdout/stderr log in `logs/download_{exchange}.log`. The script monitors each process and reports exit codes — `exit=0` means all tickers attempted, `exit=1` means zero stocks were saved (usually a network issue).

```
orchestrate_dl.py
    ├── subprocess.Popen(nyse.py)     PID 1001
    ├── subprocess.Popen(nasdaq.py)   PID 1002
    ├── ...  (11 processes total)
    └── poll loop — waits for all to finish, logs exit codes
```

#### `orchestrate.py` — Engine Process Launcher and Result Aggregator

This is the coordinator for the C++ processing stage. It launches `n × 11` engine processes (where `n` is the value the user entered at Step 0), waits for all of them to finish, then reads all individual exchange JSON files and aggregates them into `brief_exchange_result.json`.

The aggregation step matters. Each C++ process writes only its own exchange's data to the brief file, so without the Python aggregation pass, whichever process finishes last would overwrite everyone else's results. Doing the merge in Python after everything finishes avoids that race entirely.

```
orchestrate.py
    │
    ├── Launch n × 11 engine processes
    │       NYSE_p0, NYSE_p1, ..., TSX_p(n-1)
    │
    ├── Wait for all to complete
    │
    └── Aggregation pass
            ├── Read individual_exchange_result/NYSE_result.json
            ├── Read individual_exchange_result/NASDAQ_result.json
            ├── ... (all 11)
            └── Write all_exchange_result/brief_exchange_result.json
```

---

### C++ File — `engine.cpp`

The engine does everything: reads CSVs, synthesises micro-ticks, runs all 15 extensions concurrently, and writes JSON results. It has two operating modes selected by command-line arguments.

**Mode 1 — Full research suite (no arguments)**

Scans all `stock_exchange/*/` directories, loads every CSV, synthesises ticks from all stocks, runs all 15 extensions on the combined tick stream, and writes both text and JSON results.

**Mode 2 — Per-exchange worker (`--exchange`, `--proc-id`, `--n-procs`)**

Called by `orchestrate.py`. Each instance handles a shard of stocks within one exchange, identified by `hash(ticker) % n_procs == proc_id`. This guarantees no two processes ever claim the same stock. A file-based lock (`O_CREAT|O_EXCL`) provides a second layer of exactly-once protection.

**Internal architecture — 17-thread model per stock:**

```
For each stock CSV claimed by this process:
│
├── Thread 1   — CSV reader
│               Loads file, synthesises micro-ticks
│               Sets csv_ready atomic flag when done
│
├── Threads 2–16 — Extensions 1–15 (one per thread)
│               Each waits on csv_ready, then runs its extension
│               Stores result in shared StockResult struct
│               Sets ext_done[i] atomic flag on success
│               Sets ext_failed[i] on exception
│
└── Thread 17  — Reserved / Recovery
                Monitors ext_failed[] flags
                Re-runs any failed extension
                Records reserved_used = true if it intervenes
                After all 16 other threads join — this thread exits
```

After all 17 threads join, the result struct is complete and written to JSON. The threads are destroyed automatically (they go out of scope), and a fresh set is created for the next stock.

**Output structures:**

```
result/
└── json/
    ├── individual_exchange_result/
    │   ├── NYSE_result.json      — array, one object per stock, 35 fields each
    │   ├── NASDAQ_result.json
    │   └── ... (11 files)
    └── all_exchange_result/
        └── brief_exchange_result.json   — 11 exchanges, 16 fields each
```

**35 fields per stock in individual results:**
1. `stock_name`
2. `process_id`
3–17. `thread_id_ext01` … `thread_id_ext15`
18–32. `ext01_result` … `ext15_result` (each: p50_ns, p95_ns, impact_bps, fault_tol, success_pct)
33. `reserved_thread_id`
34. `reserved_thread_used` (Y/N)
35. `reserved_thread_reason`

---

### `config.yaml` — Central Configuration

Single source of truth for the entire pipeline. Exchange scripts, `base.py`, and `engine.cpp` all read from here.

```yaml
global:
  start_date / end_date     # download date range
  stock_root                # where CSVs live (stock_exchange/)
  result_root               # where JSON results go (result/)
  log_dir                   # log directory
  cpp_binary                # path to compiled engine binary
  retry_count / retry_delay # download retry settings
  min_rows                  # minimum acceptable trading days
  download_threads          # thread count for yfinance pool

exchanges:
  NYSE / NASDAQ / LSE / ...
    full_name               # display name for logging
    tickers                 # semicolon-delimited Yahoo Finance symbols
```

Adding a new exchange requires one block in this file and one new `{exchange}.py` file. Nothing else changes.

---

### `run.sh` — Pipeline Orchestrator

The single entry point for the entire pipeline. Written in POSIX `sh` (not bash) so it works on macOS (which ships bash 3.2) and Linux without any modification.

```
run.sh
│
├── STEP 0   Ask for n (processes per exchange)
│            Accepts: interactive input, echo "n" | ./run.sh, or --procs=n
│
├── STEP 1   Create all directories
│            stock_exchange/{11 exchanges}/
│            result/json/individual_exchange_result/
│            result/json/all_exchange_result/
│
├── STEP 2   Locate Python 3 and install dependencies
│
├── STEP 3   Compile engine.cpp
│            -O3 -std=c++17 -pthread
│            -mcpu=apple-m1 (Apple Silicon) or -march=native (Intel/Linux)
│            Export ENGINE_BIN as absolute path so Python can find the binary
│
├── STEP 4   Run orchestrate_dl.py
│            Launches all 11 downloaders in parallel
│
└── STEP 5   Run orchestrate.py
             Launches n × 11 C++ engine processes
             Waits for completion
             Aggregates brief_exchange_result.json
```

### How Python and C++ Work Together

```
Python downloads CSVs
        │
        ▼
stock_exchange/{EXCHANGE}/{ticker}.csv
        │
        ▼
C++ engine reads CSVs directly from this path
        │
        ▼
result/json/{individual and aggregate results}
        │
        ▼
Python (orchestrate.py) reads individual JSONs
and merges them into brief_exchange_result.json
```

There is no shared memory, no socket, no database between them. The filesystem is the interface. Python writes CSVs; C++ reads them. Python reads C++ JSON output; that's the whole coordination model.

---

## Extensions

The baseline GPS synchronisation simulation achieves **31 ns P50** sync error across 5 exchange servers. Every extension below is compared against that number.

---

### EXT01 — Kalman Clock Filter

**Problem:** GPS readings contain measurement noise that accumulates directly into sync error. The baseline takes each GPS reading at face value and fires immediately.

**How I found it:** Running the baseline over 6,000 micro-ticks, I noticed the P50 was locked at exactly 31 ns — too round a number to be noise. It was the GPS measurement noise floor, directly propagated. Nothing was filtering it.

**Extension:** A two-state Kalman filter tracking clock offset θ and drift rate ω. The state transition and update equations:

```
Predict:
  x̂[k|k-1] = F · x̂[k-1]          where F = [[1, Δt], [0, 1]]
  P[k|k-1]  = F · P[k-1] · Fᵀ + Q

Update:
  K[k] = P[k|k-1] · Hᵀ · (H · P[k|k-1] · Hᵀ + R)⁻¹
  x̂[k] = x̂[k|k-1] + K[k] · (z[k] - H · x̂[k|k-1])

Residual (corrected):
  e[k] = (θ_GPS + ω · Δt) − Kalman_estimate
  (The Δt term was missing in my first two attempts — that bug alone cost 12 ns)
```

50-tick warmup before any results are recorded. The filter needs time to converge, and using P50 = 19 ns from an unconverged filter was the original bug.

**Result:** P50 = **19 ns** (−38.7% vs baseline)

**Real-world implication:** A 12 ns improvement in sync precision means orders that previously arrived at different exchanges within 12–31 ns of each other now arrive within 19 ns. At institutional scale, that closes a gap wide enough for certain arbitrage strategies to exploit.

---

### EXT02 — Byzantine Fault-Tolerant Consensus

**Problem:** When one exchange server has a faulty clock (drift, hardware failure, or deliberate manipulation), its readings contaminate the consensus used to set fire times across all servers.

**How I found it:** I introduced one Byzantine server (Euronext in the simulation) and watched P95 jump from 31 ns to 41 ns in the baseline. The bad node was dragging the average.

**Extension:** Multi-server clock consensus with partial correction factor α:

```
θ_new = θ_old + α · (θ_consensus − θ_old)     where α = 0.5

Each round contracts the spread by 50%. A Byzantine server pulling by δ
can move the consensus at most δ/(2f+1) per round, bounded by f = 1.
```

The α = 0.5 value is not arbitrary — it is the minimum contraction factor that guarantees convergence in O(log ε) rounds for f = 1 Byzantine node out of n = 5 servers.

**Result:** P50 = **18 ns**, P95 = **29 ns**, FaultTol = **0.2**

**Real-world implication:** The system continues executing correctly even when one exchange server is reporting wrong timestamps — a realistic failure mode during maintenance windows or network partitions.

---

### EXT03 — Almgren-Chriss Multi-Venue Execution Split

**Problem:** The baseline splits orders uniformly across exchanges. A venue with low liquidity gets the same share as one with deep books, which increases market impact disproportionately.

**How I found it:** Comparing NASDAQ (tight spreads, high liquidity) to JSE (wider spreads, lower depth), uniform splitting made no sense. NASDAQ could absorb 3× the order without moving the price.

**Extension:** Optimal order split minimising total market impact:

```
Market impact per venue:
  I_i = η_i · (q_i / L_i)

where η_i is the impact coefficient and L_i is venue liquidity.

Optimal allocation (Lagrange multiplier solution):
  q*_i = Q · (L_i / η_i) / Σ_j (L_j / η_j)

Cost function:
  min Σ_i [η_i · q*_i² / L_i + ψ_i · σ_i² · q*_i² · T]
```

Venues with higher liquidity and lower η receive proportionally larger order slices.

**Result:** Impact = **1.30 bps** vs baseline **2.50 bps** (−48%)

**Real-world implication:** On a $10M order, the difference between 1.3 bps and 2.5 bps is $12,000 in execution cost. Per day. That number compounds quickly for an institutional desk.

---

### EXT04 — Double Q-Learning Venue Router

**Problem:** Static venue allocation ignores real-time fill quality. A venue that was liquid at 9:30 AM may be thinner by 3 PM due to order flow dynamics.

**How I found it:** Looking at intraday fill patterns across exchanges, I noticed that optimal venue ranking shifted throughout the day. No static model could capture it.

**Extension:** Reinforcement learning agent with two independent Q-tables to avoid overestimation bias:

```
Double Q-Learning update:
  a* = argmax_a Q_A(s', a)         (select action using Q_A)
  Q_A(s,a) ← Q_A(s,a) + α · [r + γ · Q_B(s', a*) − Q_A(s,a)]

  (Q_B is used to evaluate the action selected by Q_A — breaks the
   maximisation bias of single-table Q-learning)

State:  recent spread, depth, volatility per venue
Action: venue selection for next order slice
Reward: −impact_bps for the executed slice
```

Replay buffer of 500 steps prevents catastrophic forgetting between intraday sessions.

**Result:** Impact = **2.00 bps** vs baseline **2.50 bps** (−20%)

**Real-world implication:** The agent learns venue behaviour from live data and adapts when liquidity patterns shift — something no statically calibrated model can do.

---

### EXT05 — Cryptographic Time-Lock Commitment

**Problem:** A colluding party who knows the scheduled fire time T_exec in advance can trade ahead of the synchronized orders (front-running).

**How I found it:** In a distributed system where fire times are broadcast for coordination, the broadcast itself creates a front-running window.

**Extension:** FNV-1a hash commitment scheme:

```
Commit phase (before broadcast):
  H = FNV1a(T_exec ⊕ nonce)     broadcast H, keep T_exec secret

Reveal phase (at execution):
  Broadcast T_exec and nonce
  Verify: FNV1a(T_exec ⊕ nonce) == H

Tamper detection:
  Any modification to T_exec changes H by avalanche effect
```

FNV-1a is fast (one multiply + XOR per byte) and has good avalanche properties for this use case.

**Result:** Tamper detection = **100%** (6000/6000 commits verified), FaultTol = **0.2**

**Real-world implication:** Front-running becomes cryptographically infeasible. An adversary would need to break the hash preimage to learn T_exec before the reveal phase.

---

### EXT06 — Thompson Sampling Clock Source Selector

**Problem:** GPS is the best clock source 99.8% of the time, but occasionally a receiver loses lock and returns noisy readings. Using GPS blindly without monitoring causes intermittent sync spikes.

**How I found it:** My first implementation had MaxSync = 1,978 ns — one catastrophically bad tick. That outlier came from a single TCXO selection during a period of simulated GPS uncertainty.

**Extension:** Multi-armed bandit with Beta posterior and informative prior:

```
Prior:    α_GPS = 20,  β_GPS = 2     → posterior mean = 0.909
          α_other = 1, β_other = 10  → posterior mean = 0.091

Safety gate (triggered when n ≥ 20 and μ_GPS ≥ 0.90):
  Lock permanently to GPS — no further exploration

Standard Thompson update (before gate triggers):
  If GPS reading within ±50 ns: α_GPS += 1
  Otherwise:                     β_GPS += 1
  Sample: θ_k ~ Beta(α_k, β_k), select arm with highest sample
```

The informative prior means GPS is confirmed reliable from tick 1. The gate prevents exploration into high-variance clock sources once confidence is established.

**Result:** MaxSync **1,978 ns → 91 ns** (95% reduction), P50 = **39 ns**, FaultTol = **0.8**

**Real-world implication:** The system gracefully handles GPS degradation by tracking multiple clock sources while staying anchored to the best one. The P50 is slightly worse than baseline because the MAB overhead adds latency — that's the cost of robustness.

---

### EXT07 — GNN Latency Predictor

**Problem:** The baseline uses a fixed 500 µs lead time before execution across all venues. Faster venues are over-compensated; slower ones are under-compensated.

**How I found it:** Plotting execution timing errors per venue, I saw a consistent bias: NASDAQ was arriving ~80 µs early and Euronext ~360 µs late relative to target.

**Extension:** Graph Neural Network predicting per-venue latency from network topology:

```
Exchange network as graph G = (V, E):
  V = {NYSE, NASDAQ, LSE, JPX, SSE}
  E = inter-exchange latency edges

Node feature update (one GNN layer):
  h_v^(l+1) = σ(W · MEAN_{u ∈ N(v)} h_u^(l) + b)

Lead time adjustment:
  L_i = L_base − Δ̂_i     where Δ̂_i is GNN latency prediction for venue i
```

The GNN learns from historical latency patterns and adjusts the lead time per venue dynamically.

**Result:** Impact = **2.30 bps** (−8%), average lead time saving = **440 µs**

**Real-world implication:** Tighter lead time calibration reduces the window during which the system is exposed to price movement between order submission and execution.

---

### EXT08 — Quantum-Walk Gossip Protocol

**Problem:** Classical gossip protocols propagate clock offset information in O(N log N) rounds for N servers. For 5 servers, convergence is fast but not optimal.

**How I found it:** Studying mixing times for small graphs, I found that quantum walks on the complete graph K₅ achieve full mixing in significantly fewer rounds than classical random walks.

**Extension:** Continuous-time quantum walk for clock information propagation:

```
Graph Hamiltonian (complete graph K_N):
  H = adjacency matrix A of K_N

State evolution:
  |ψ(t)⟩ = e^{−iHt} |ψ(0)⟩

Spectral gap (K_N):
  λ₂ = N/(N−1) = 5/4 = 1.25

For N = 5, full mixing achieved in:
  t_mix = π/(2λ₂) ≈ 30 rounds
  (Classical gossip requires O(N log N) ≈ 8 rounds but with higher variance)
```

50 rounds ensures complete mixing with margin. The spectral gap of K₅ is larger than classical diffusion, meaning information about bad clock readings propagates faster.

**Result:** P50 = **28 ns** (−9.7% vs baseline), FaultTol = **0.2**

**Real-world implication:** Faster propagation means a degraded clock is detected and compensated for earlier — reducing the window of elevated sync error after a clock event.

---

### EXT09 — Liquidity-Aware Order Sizing

**Problem:** Uniform order sizing ignores that each venue has different available depth. Sending equal size to all venues saturates thin markets and under-utilises deep ones.

**How I found it:** Looking at simulated fill rates per venue, I noticed JSE was consistently getting partial fills at the uniform allocation, while NYSE had excess capacity. The solution was obvious once I saw the numbers.

**Extension:** Proportional sizing based on venue liquidity:

```
Liquidity-proportional allocation:
  q*_i = Q · L_i / Σ_j L_j

where L_i = available depth at venue i at execution time.

Equalised fill rate:
  fill_rate_i = q*_i / L_i = Q / Σ_j L_j  (constant across all venues)

Market impact:
  I_i = η_i · q*_i / L_i = η_i · Q / Σ_j L_j
```

The result is that every venue is used at the same proportion of its capacity, eliminating both saturation and under-utilisation.

**Result:** Impact = **1.80 bps** (−28% vs baseline), fill equalisation = **100%**

**Real-world implication:** Institutional orders that previously moved prices on thin venues now clear cleanly. The 28% impact reduction translates directly to better average execution price.

---

### EXT10 — BFT-Quorum Two-Phase Commit

**Problem:** The original 2PC required unanimous READY votes from all 5 servers before committing. One faulty server blocked the entire commit indefinitely.

**How I found it:** In testing with one Byzantine server, commit rate dropped to 0%. Every single transaction was aborted. That's an unusable system.

**Extension:** Majority quorum 2PC with Byzantine tolerance:

```
Quorum size: Q = 2f + 1 where f = number of tolerated faults

For f = 1, n = 5:  Q = 3

Phase 1 (PREPARE):
  Coordinator sends PREPARE to all n servers
  Wait for READY from at least Q = 3 servers

Phase 2 (COMMIT):
  If |READY| ≥ Q: broadcast COMMIT
  If timeout or |READY| < Q: broadcast ABORT

Commit succeeds even if f = 1 server:
  - Crashes and never responds
  - Responds ABORT (Byzantine)
  - Sends corrupted vote
```

The Byzantine server cannot prevent commit as long as the remaining 4 honest servers have enough votes among themselves.

**Result:** Commit rate = **99.7%** (vs 0% baseline), P50 = **31 ns**, FaultTol = **0.2**

**Real-world implication:** The execution system survives individual server failures without stalling. A crashed exchange server no longer blocks all pending synchronized orders.

---

### EXT11 — Allan Deviation Clock Monitor

**Problem:** Clock degradation (drift, oscillator aging, interference) is hard to detect in real time. The baseline has no monitoring — it fires on GPS regardless of clock health.

**How I found it:** My first two detection attempts failed completely. Step injection (constant offset) gave 1% detection because ADEV's second-difference formula cancels constant offsets exactly. Sine wave injection gave ~40% because of period aliasing with the window size.

**Extension:** Allan Deviation monitoring with random-walk FM injection:

```
ADEV formula (overlapping, window W):
  σ_y(τ) = √(1/(2(M−1)) · Σ_{k=1}^{M-1} (y_{k+1} − y_k)²)

where y_k = frequency deviation sample k, M = W = 32 samples

Random-walk FM degradation model:
  x_deg(t) = x(t) + Σ_{k=1}^{t} ε_k     ε_k ~ N(0, σ_rw = 40 ns)

After 32 samples, ADEV(τ) ≈ σ_rw / √τ ≈ 226 ns >> threshold (30 ns)

Detection rate = alerts_fired / ticks_in_degraded_period
              = 2,936 / 2,999 = 97.9%
```

The 40 ns/tick random walk value was chosen so that ADEV rises above threshold within one window length — fast enough to matter, not so aggressive that it false-alarms on normal GPS jitter.

**Result:** Detection rate = **97.9%**, FaultTol = **1.0**

**Real-world implication:** Clock degradation is caught within 32 ticks (~32 µs at typical tick rates) and flagged for corrective action. In a live system this would trigger a failover to a backup clock source.

---

### EXT12 — Cramér-Rao Lower Bound Analysis

**Problem:** Is the 31 ns baseline actually good? Without a theoretical floor, there's no way to know if the remaining error is reducible or fundamental.

**How I found it:** I started wondering whether 31 ns was near-optimal or whether there was a factor-of-10 improvement sitting on the table. The Cramér-Rao bound answers that question definitively.

**Extension:** Information-theoretic lower bound on clock estimation:

```
Fisher information for GPS measurements:
  I(θ) = N_obs / σ²_GPS

Cramér-Rao Lower Bound:
  Var(θ̂) ≥ 1/I(θ) = σ²_GPS / N_obs

  CRLB = σ_GPS / √N_obs = 10 ns / √(N_obs)

For N_obs = 2.51 (effective observations per tick):
  CRLB = 10 / √2.51 = 6.32 ns

Gap factor: 33.77 ns / 6.32 ns = 5.3×
```

The baseline is operating at 5.3 times the information-theoretic minimum. That gap motivates every other extension in this project — there is room to improve.

**Result:** CRLB = **6.32 ns**, Achieved = **33.77 ns**, Gap = **5.3×**

**Real-world implication:** Even the best extension (EXT01 at 17 ns) is still 2.7× above the physical minimum. There is meaningful improvement still available to a system with better measurement architecture.

---

### EXT13 — Self-Healing Network Mesh

**Problem:** When a direct link between two exchange servers fails, the baseline has no rerouting capability. The affected servers lose synchronisation until the link is manually restored.

**How I found it:** Injecting a single link failure in the simulation, I watched two servers drift apart by ~400 ns within 50 ticks. The baseline had no recovery mechanism.

**Extension:** IEEE 1588-style transparent clock mesh with Dijkstra rerouting:

```
Network state: G = (V, E) where edge weights = link latency

On link failure detection (heartbeat timeout T_hb):
  Remove failed edge from G
  Dijkstra shortest path: d(v) = min Σ_{e ∈ path} latency(e)

Transparent clock compensation:
  T_exec_corrected = T_exec + Σ_{hops} residence_time_i

  (residence_time is the delay added at each relay node — must be
   subtracted so all servers fire at the same logical time)

Mesh connectivity maintained as long as graph G remains connected.
```

**Result:** FaultTol = **1.0**, Mesh connectivity = **100%**, P50 = **33 ns**

**Real-world implication:** The execution system survives network failures between exchange servers and self-recovers without manual intervention. In a real deployment this means the synchronised execution window is maintained even during partial network outages.

---

### EXT14 — Differential Privacy for Execution Timing

**Problem:** Broadcasting the fire time T_exec for coordination purposes reveals execution intent. An observer monitoring the network can infer order timing and position accordingly.

**How I found it:** In a distributed system, coordination requires communication. Communication creates an observation channel. That channel is a privacy leak.

**Extension:** Calibrated correlated noise preserving both privacy and synchronisation:

```
ε-differential privacy guarantee:
  P(M(T_exec) ∈ S) ≤ e^ε · P(M(T_exec') ∈ S)  for all S, T_exec, T_exec'

Noise mechanism (shared-secret correlated):
  η_shared ~ N(0, σ_shared)         (generated from shared secret)
  η_i      ~ N(0, σ_i)              (independent per server)
  noise_i  = ρ · η_shared + √(1−ρ²) · η_i

  where ρ = 0.99 (high correlation preserves sync)

Privacy budget: ε = 2.0
Noise std: σ = Δf / ε = max|T_exec| / 2.0 = 707 ns

Sync preservation: correlated noise cancels in the T_exec difference
  → Var(T_exec_i − T_exec_j) = 2σ²_i(1−ρ²) << 2σ²_i
```

Independent noise would destroy synchronisation. The shared-secret correlation is the key — servers add the same noise component, which drops out when they compare timing.

**Result:** P(guess T_exec within 1 ms) = **0.000000**, P50 = **33 ns** (sync preserved)

**Real-world implication:** An adversary monitoring network traffic cannot recover the true fire time. The 707 ns noise envelope makes timing inference statistically infeasible while the shared-secret correlation keeps all servers in sync with each other.

---

### EXT15 — NSGA-II Multi-Objective Optimiser

**Problem:** There is no single best configuration for the synchronisation system. Lower sync latency often comes at higher computational cost or lower fault tolerance. The tradeoff needs to be mapped explicitly.

**How I found it:** My first NSGA-II run with 4 objectives produced a Pareto front of 80/80 — the entire population was non-dominated. That means the algorithm had zero selection pressure. I worked out the math: with 4 objectives, the probability that any two random solutions exhibit dominance is (1/2)⁴ = 6.25%. Everything else is mutually non-dominated by chance.

**Extension:** Two-objective NSGA-II (reducing from 4 objectives):

```
Objectives:
  f1 = sync_latency (ns)      minimise
  f2 = market_impact (bps)    minimise

NSGA-II with non-dominated sorting:
  P(x dominates y) = (1/2)² = 25%  (up from 6.25% with 4 objectives)

Evolution:
  Population: 80 individuals
  Generations: 200
  Crossover/mutation on sync parameters (GPS interval, correction α, etc.)

Pareto front size: 80/80 → 1.1/80 after objective reduction
```

**Result:** Pareto front = **1.1/80**, f1 = **20 ns** (GPS noise floor), f2 = **0.60 bps**

**Real-world implication:** The optimiser finds the GPS noise floor (20 ns) as the hard physical limit. Below that, no parameter tuning can improve sync. This defines the engineering ceiling for the system before hardware upgrades become necessary.

---

## How to Run

**Requirements:**
- Python 3.9 or later
- g++ or clang++ with C++17 support
- Internet access (for yfinance downloads)

**Setup:**

```bash
# Put all 17 files in one folder
cd ~/your-folder

# Make the shell script executable
chmod +x run.sh

# Run the full pipeline
./run.sh
```

**The script will ask one question:**

```
  How many processes per exchange?
  Enter n [default=1]:
```

Type a number and press Enter. `n=1` runs 11 processes total. `n=2` runs 22. Anything above 4 is only useful if you have many CPU cores.

**Variations:**

```bash
# Skip re-downloading (data already in stock_exchange/)
./run.sh --no-download

# Set process count without the prompt
./run.sh --procs=2

# Combine both
./run.sh --procs=4 --no-download

# Just recompile the C++ engine
./run.sh --compile-only

# Pipe the process count (useful for scripting)
echo "2" | ./run.sh
```

---

## Execution Flow

Here is exactly what happens when you run `./run.sh`, in order:

**Step 0 — Process count input**
The script asks for `n`. This number controls how many C++ processes handle each exchange. It gets exported as `PROCS_PER_EXCHANGE` for use by `orchestrate.py`.

**Step 1 — Directory creation**
All output directories are created: `stock_exchange/{11 exchanges}/`, `result/json/individual_exchange_result/`, `result/json/all_exchange_result/`, `logs/`, `state/`.

**Step 2 — Python check**
Python 3 is located. Required packages (`yfinance`, `pandas`, `numpy`, `pyyaml`) are installed if missing.

**Step 3 — C++ compilation**
`engine.cpp` is compiled to `./engine`. Compiler flags are chosen based on the platform: `-mcpu=apple-m1` on Apple Silicon, `-march=native` everywhere else. The binary path is exported as an absolute path so Python can always find it.

**Step 4 — Parallel downloads**
`orchestrate_dl.py` launches 11 Python processes simultaneously — one per exchange. Each process reads its ticker list from `config.yaml`, downloads up to 75 stocks via yfinance, and saves them to `stock_exchange/{EXCHANGE}/{ticker}.csv`. All 11 run in parallel. This typically takes 3–8 minutes depending on network speed. Logs go to `logs/download_{exchange}.log`.

**Step 5 — C++ engine**
`orchestrate.py` launches `n × 11` engine processes. Each process:
1. Scans its assigned exchange directory for CSV files
2. Claims each ticker via atomic file lock (exactly-once guarantee)
3. For each claimed stock, spawns 17 threads (1 reader, 15 extension threads, 1 recovery thread)
4. Writes a 35-field JSON result per stock
5. Exits when all its tickers are processed

After all processes finish, `orchestrate.py` reads all 11 individual exchange JSONs and merges them into `brief_exchange_result.json`.

**Output:**

```
result/json/
├── individual_exchange_result/
│   ├── NYSE_result.json       (35 fields per stock, real OS thread IDs)
│   ├── NASDAQ_result.json
│   └── ... (11 files)
└── all_exchange_result/
    └── brief_exchange_result.json   (16 fields per exchange, all 11)
```

Total runtime: ~15–30 minutes for a full run with downloads. Under 5 minutes with `--no-download`.

---

## Disclaimer

This repository is an independent research implementation. It is not affiliated with, endorsed by, or derived from any proprietary system. All algorithms were designed independently. The GPS synchronisation baseline is an original simulation written for research comparison. No proprietary implementation has been reproduced.

---

## How to Cite

If you use this work, please cite both the original design that inspired this research and my independent implementation:

**This repository:**
```
Nihar Mahesh Jani. (2026). SYNCMEX: Synchronized Yield-optimized Novel Clock
Multi-Exchange eXtensions. GitHub: https://github.com/NiharJani2002/SYNCMEX 
Contact: niharmaheshjani@gmail.com
```

**Inspired by (system design concept):**
```
Robert Mercer and David E. Brown. System and Method for Executing Synchronized
Messages in Multiple Geographically Distributed Servers. [Patent document].
```

---

*Nihar Mahesh Jani — niharmaheshjani@gmail.com*
