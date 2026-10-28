# ConcurEx

A concurrent stock exchange backend in Java, built phase by phase to learn concurrency by implementing it, breaking it, and fixing it.

---

## What is this?

ConcurEx is a mini stock exchange. It does what the core of NSE or Nasdaq does:

- Traders submit **buy** and **sell** orders for a stock (limit or market).
- Each stock has an **order book**: waiting buy orders and waiting sell orders.
- When a buy price meets or exceeds a sell price, the engine creates a **trade**. The best price wins; ties go to the earliest order (price-time priority).
- **Settlement** moves cash and shares between the buyer's and seller's accounts.
- Clients see live prices (best bid/ask, depth, last trade) through a WebSocket feed.

The exchange is the vehicle. **Concurrency is the actual subject.** Thousands of orders arrive at once, and the system must stay correct while they do.

### The correctness rules that make it hard

| Rule | Failure if broken | Concept it exercises |
|---|---|---|
| No two orders fill the same shares | Double fill | Race conditions, single-writer design |
| Balances never go negative or lose money | Lost update | Locks, atomics, transactions |
| A retried request creates exactly one order | Duplicate order | Idempotency |
| Overload is rejected cleanly | OutOfMemory, crash | Bounded queues, backpressure |
| Readers see consistent prices without slowing the matcher | Stale or torn reads, contention | Read-write locks, immutability |
| Transfers between accounts never freeze | Deadlock | Lock ordering |

---

## Final architecture

```
REST API
   |
   v
Intake thread pool (bounded queue, rejection policy)
   |
   v
Idempotency check (clientOrderId)
   |
   v
Shard router (hash by symbol)
   |
   v
Per-symbol bounded queue  -->  single matcher thread per symbol
   |
   v
Trade events  -->  Settlement (ordered locks / transactions)  -->  PostgreSQL
   |
   v
Immutable market-data snapshot  -->  WebSocket feed
```

Key design decision: each symbol has **one matcher thread** that owns its order book. The book itself needs no locks. Concurrency is handled at the edges (intake, settlement, market-data reads), which is how real low-latency engines are designed.

---

## Tech stack

- Java 21 (virtual threads), Spring Boot
- PostgreSQL (orders, trades, balances), Redis (idempotency keys)
- JUnit 5, JMH (benchmarks), Docker
- The matching core is **plain Java with no framework**, so it stays fast and testable.

---

## Test data

No real market data is needed. A **seeded synthetic order generator** produces random limit, market, and cancel orders around a moving mid price. The same seed gives the same input every run, which makes tests and benchmarks reproducible. A real-data replay (Binance or LOBSTER sample) is added at the end as a demo.

---

## The 20 concurrency concepts and where each is covered

Source list: 20 must-know concepts for concurrency in system design.

| # | Concept | Where it is covered | Depth |
|---|---|---|---|
| 1 | Concurrency | Phase 1: concurrent order submission | Build |
| 2 | Concurrency vs parallelism | Phase 1 and Phase 5: matchers on multiple cores, Amdahl's law, benchmarks | Understand + measure |
| 3 | Processes vs threads | Phase 1 (study only), Phase 8: platform vs virtual threads | Understand |
| 4 | Thread lifecycle | Phase 1, Phase 3: states, reading states in thread dumps | Understand + observe |
| 5 | Race condition | Phase 1: reproduce lost orders and double fills with a failing test | Build + break |
| 6 | Mutex | Phase 2: `synchronized` and `ReentrantLock` on the book | Build |
| 7 | Semaphore | Phase 6: per-client rate cap, connection pool drill | Build |
| 8 | Condition variable | Phase 2: `wait/notify` and `Condition` in producer-consumer drill | Build |
| 9 | Coarse vs fine-grained locking | Phase 2 (one big lock), Phase 3 (per-account locks), Phase 5 (sharding by symbol) | Build + benchmark |
| 10 | Reentrant lock | Phase 2: `ReentrantLock` in the engine | Build |
| 11 | Deadlock | Phase 3: two-account transfer deadlock, `jstack`, lock-ordering fix | Build + break + fix |
| 12 | Livelock | Phase 3: small `tryLock` retry demo | Understand + demo |
| 13 | Starvation | Phase 7: writer starvation with read-write locks, fair vs unfair locks | Understand + demo |
| 14 | Atomic operations | Phase 4: lock-free order and trade IDs, `LongAdder` metrics, CAS | Build |
| 15 | Thread pool | Phase 6: `ThreadPoolExecutor` with bounded queue and rejection policy | Build |
| 16 | Producer-consumer | Phase 5: bounded `BlockingQueue` per symbol feeding the matcher | Build |
| 17 | Blocking vs non-blocking | Phase 5 (blocking queue), Phase 4 (CAS), Phase 8 (`CompletableFuture`, virtual threads) | Build + compare |
| 18 | Backpressure | Phase 6: reject with HTTP 429 vs caller-runs under overload test | Build |
| 19 | Immutability | Phase 0 (immutable `Order` and `Trade`), Phase 7 (immutable market-data snapshots) | Build + benchmark |
| 20 | Idempotency under concurrency | Phase 9: `clientOrderId`, Redis and DB unique constraints, retry-storm test | Build |

### Beyond the 20 (important for real backend work)

| Topic | Phase |
|---|---|
| Java Memory Model: `volatile`, visibility, happens-before | Phase 2 |
| `ConcurrentHashMap` internals, `CopyOnWriteArrayList` | Phase 7 |
| `CountDownLatch`, `CyclicBarrier` | Phase 6 |
| `CompletableFuture` chaining and error handling | Phase 8 |
| Virtual threads (Java 21) | Phase 8 |
| DB transactions, isolation levels, lost update, write skew | Phase 9 |
| Optimistic (`@Version`) vs pessimistic (`SELECT FOR UPDATE`) locking | Phase 9 |

### What is only conceptual

Processes vs threads and thread lifecycle are theory, observed through tools but not "built". Livelock and starvation get short demos, not full features. Distributed locks (Redis/Redlock) are studied for caveats but not a core deliverable.

---

## Warm-up drills

Before the main phases, five small standalone programs (`drills/` module):

1. A race condition, fixed three ways
2. A producer-consumer queue (`wait/notify`, then `BlockingQueue`)
3. A connection pool using a `Semaphore`
4. A deadlock on purpose, then fixed
5. A thread pool with a bounded queue, written from scratch

---

## Roadmap: study, then implement

Every phase has the same shape: **study** the concept, **implement** it in ConcurEx, then pass a **done-check**.

| Phase | Focus | Implement | Done when |
|---|---|---|---|
| 0 | Single-threaded baseline | Order, Trade, OrderBook, `match()`, cancel, tests | Fixed input gives identical trades every run |
| 1 | Threads and race conditions | Naive concurrent `submit()` | A failing test reproduces the race |
| 2 | Locks and memory model | Coarse lock, then `ReentrantLock`, benchmark both | Race test passes, throughput recorded |
| 3 | Accounts and deadlock | Settlement with transfers; cause, capture, and fix a deadlock | No deadlock, no negative balances under stress |
| 4 | Atomics | Lock-free ID sequencer and metrics | IDs unique and gap-free under 1000 threads |
| 5 | Producer-consumer and sharding | Bounded queue per symbol, single matcher thread | Throughput beats Phase 2, book needs no locks |
| 6 | Thread pools and backpressure | Intake pool, rejection policy, semaphore rate cap | Overload is rejected cleanly |
| 7 | Read-heavy market data | `ReadWriteLock` vs immutable snapshot, JMH comparison | Benchmark table produced |
| 8 | Async pipeline | `CompletableFuture` order lifecycle, virtual threads | p50 and p99 latency measured |
| 9 | Database concurrency and idempotency | PostgreSQL persistence, optimistic locking, idempotent submit | Duplicate submits yield exactly one order |
| 10 | Stress and polish | Invariant-checking stress harness, README with benchmarks | Invariants hold over N runs |

Phases 1 to 5 plus Phase 9 are the interview-critical core.

---

## Invariants verified by the stress harness

- No account balance ever goes negative
- No order is filled for more than its quantity (no double fill)
- Total cash and total shares across all accounts are conserved
- Every accepted `clientOrderId` produces exactly one order
- Output for a given seed is identical across runs (matcher determinism)

---

## Planned repository layout

```
concurex/
  README.md
  docs/
    STUDY.md          index of study notes
    topics/           one file per concept studied
  drills/             five warm-up programs
  engine-core/        Order, Trade, OrderBook, Matcher (plain Java)
  concurrency/        sequencer, shard router, bounded queues, executors
  accounts/           balances, settlement, locking
  api/                Spring Boot REST + WebSocket
  persistence/        JPA repositories, idempotency
  bench/              JMH benchmarks, stress harness, order generator
```

---

## Resources

- *Java Concurrency in Practice* by Brian Goetz (chapters 1-8 and 15 at minimum)
- Baeldung concurrency tutorials for syntax reference
- JMH documentation for benchmarking

---

## Resume summary

> Built a concurrent exchange backend in Java with per-symbol sharded matching, idempotent order intake, and settlement verified under stress tests. Reproduced and fixed race conditions and deadlocks, and benchmarked lock strategies (X orders/sec, p99 Y ms).

*(Fill in X and Y with real measurements after Phase 10.)*
