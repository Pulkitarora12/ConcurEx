# ConcurEx: Build a Concurrent Stock Exchange in Java

Learn concurrency by building a mini stock exchange, breaking it on purpose, and fixing it.

**Budget: 80 hours** (about 27 days x 3 hours). Every step below lists its hours. Learning time is included.

---

## What you are building

A mini exchange, like the core of NSE or Nasdaq:

- People send **buy** and **sell** orders for a stock.
- Each stock has an **order book**: a waiting list of buys and a waiting list of sells.
- When a buy price is at or above a sell price, they **match** and a **trade** happens. Best price goes first. At the same price, the earlier order goes first.
- **Settlement** moves money and shares between the two accounts.

The exchange is only the vehicle. The real subject is: **what breaks when thousands of orders arrive at the same time, and how do you fix it?**

**Stack:** Java 21, Maven, JUnit 5. Steps 1 to 14 are pure Java (no framework, no database). Steps 15 to 21 add CompletableFuture, PostgreSQL, Redis, Spring Boot and WebSocket, JMH.

**Data:** none needed. You generate random orders with a seeded generator. Same seed = same orders every run.

---

## How every step works

1. **Learn:** read the plain-words explanation (20 to 30 minutes).
2. **Build:** write the code listed.
3. **Break it:** cause the bug on purpose and watch it fail. This is where you actually learn.
4. **Check:** the test that proves it works. Do not move on until it passes.
5. **Note:** write 5 lines in `docs/topics/<step>.md` about what you learned. Link it from `docs/STUDY.md`.

Order matters. Do not skip steps.

---

## Hour budget

| Step | Topic | Hours |
|---|---|---|
| 0 | Setup | 1 |
| 1 | Domain model | 1.5 |
| 2 | Order book | 3 |
| 3 | Matcher | 3.5 |
| 4 | Order generator and golden file | 3 |
| 5 | Threads and race conditions | 4 |
| 6 | Locks and the memory model | 6 |
| 7 | Accounts and settlement | 4 |
| 8 | Deadlock and livelock | 5 |
| 9 | Atomics | 3 |
| 10 | Producer-consumer and sharding | 6 |
| 11 | Thread pools, semaphores, backpressure | 6 |
| 12 | Market data for many readers | 4 |
| 13 | Idempotency | 3 |
| 14 | Stress test | 4 |
| 15 | CompletableFuture and virtual threads | 4 |
| 16 | PostgreSQL concurrency | 5 |
| 17 | Redis idempotency | 3 |
| 18 | REST API and WebSocket feed | 5 |
| 19 | JMH benchmarks and StampedLock | 3 |
| 20 | Real market data replay | 1 |
| 21 | Final stress run and README | 2 |
| | **Total** | **80** |

---

## Step 0: Setup (1 hour)

- Install JDK 21 and Maven. Create one Maven project (single module, simple).
- Add JUnit 5.
- Packages inside `src/main/java/concurex/`:

```
engine/        Order, Trade, OrderBook, Matcher
account/       Account, AccountService
concurrent/    Sequencer, Shard, Engine variants
sim/           OrderGenerator, StressRunner
drills/        small standalone demos
api/           Spring Boot controllers (Step 18)
```

- Create `docs/STUDY.md` (an empty list of links) and a `docs/topics/` folder.

---

# Part 1: A correct single-threaded exchange (11 hours)

You cannot debug concurrency bugs if you do not know what the correct answer is. Build the correct version first.

## Step 1: Domain model (1.5 hours)

**Learn**
- **Limit order:** "buy up to this price" or "sell at least this price". If not fully matched, the leftover waits in the book.
- **Market order:** "buy or sell now". The leftover is thrown away, never waits. To keep things safe, a market order here carries a **worst acceptable price** (like a limit), but it never rests in the book.
- **Money is a `long` in ticks**, never `double`. Rs 100.50 is stored as `10050`. Reason: `0.1 + 0.2 != 0.3` in floating point, and money must add up exactly.
- **Immutable** means the object cannot change after creation. Java `record` gives you that. Immutable objects are always safe to share between threads.
- **No clock timestamps.** Time priority comes from the order id (a counter). Clocks make runs non-repeatable.

**Build**
```java
public enum Side { BUY, SELL }
public enum OrderType { LIMIT, MARKET }

public record Order(long id, String clientOrderId, long accountId, String symbol,
                    Side side, OrderType type, long priceTicks, long quantity) {
    public Order {
        Objects.requireNonNull(symbol);
        if (quantity <= 0) throw new IllegalArgumentException("quantity");
        if (priceTicks <= 0) throw new IllegalArgumentException("price");
    }
}

public record Trade(long tradeId, String symbol, long buyOrderId, long sellOrderId,
                    long buyAccountId, long sellAccountId,
                    long priceTicks, long quantity, long buyLimitTicks) {}
```
(`buyLimitTicks` is the buyer's own order price. Settlement needs it in Step 7.)

A resting order needs a changing "how much is left" value. Keep that outside the immutable order:
```java
class BookEntry { final Order order; long remaining; BookEntry(Order o, long r){ order=o; remaining=r; } }
```

**Break it**
- Temporarily make `Order` a mutable class. After it is resting in the book, change its price. The book still has it at the old price level but it now reports a new price. Trades will happen at the wrong price. Revert to `record`.

**Check:** tests that reject quantity <= 0, price <= 0, null symbol.

---

## Step 2: Order book (3 hours)

**Learn**
- Two sides: **bids** (buys, best = highest price first) and **asks** (sells, best = lowest price first).
- At each price there is a queue. First in, first out. That is time priority.
- Data structure: `TreeMap<price, ArrayDeque<BookEntry>>`. A `TreeMap` keeps prices sorted, so `firstKey()` is the best price. `ArrayDeque` is a fast FIFO queue.
- To cancel by order id quickly, keep a `HashMap<orderId, BookEntry>`.

**Build**
```java
public class OrderBook {
    final TreeMap<Long, ArrayDeque<BookEntry>> bids = new TreeMap<>(Comparator.reverseOrder());
    final TreeMap<Long, ArrayDeque<BookEntry>> asks = new TreeMap<>();
    final HashMap<Long, BookEntry> byId = new HashMap<>();
    // add(BookEntry), cancel(long orderId) -> Optional<BookEntry>, bestBid(), bestAsk()
}
```
- `add`: put in the right side and price level, create the level if missing, add to `byId`.
- `cancel`: look up in `byId`, remove from its level's deque, remove the level if now empty, remove from `byId`. Unknown id returns empty (decide and write down this rule).

**Break it:** cancel the same order twice. Cancel an order that already fully traded. Make sure nothing crashes and empty price levels are removed.

**Check:** tests for: two orders at same price come out in arrival order; better price comes out first; empty level disappears after cancel.

---

## Step 3: Matcher (3.5 hours)

**Learn**
- A new order **crosses** the book when: BUY price >= best ask, or SELL price <= best bid.
- The **trade price is the resting order's price** (the one already waiting).
- One order can match many resting orders (sweeping several price levels).
- **Self-trade rule:** if the best resting order belongs to the same account, stop matching and drop the remainder of the incoming order. (Simple and consistent.)

**Build** `Matcher.match(Order o)` returning `MatchResult(List<Trade> trades, long unrestedQty)`:

```
remaining = o.quantity
opposite = (o is BUY) ? asks : bids
while remaining > 0 and opposite is not empty:
    price = best price on opposite
    if price does not cross o.price: break
    queue = opposite.get(price)
    while remaining > 0 and queue is not empty:
        resting = queue.peekFirst()
        if resting.accountId == o.accountId: stop everything (self-trade rule)
        qty = min(remaining, resting.remaining)
        record Trade(price = resting price, qty)
        remaining -= qty; resting.remaining -= qty
        if resting.remaining == 0: queue.pollFirst(); byId.remove(...)
    if queue is empty: remove that price level
if remaining > 0 and o is LIMIT and no self-trade stop: add BookEntry(o, remaining) to the book; unrestedQty = 0
else: unrestedQty = remaining
```
Trade ids: a plain `long` counter for now (you fix this in Step 9).

**Break it:** feed orders that exactly cross, barely do not cross, sweep 5 levels, and self-trade.

**Check:** tests: full fill, partial fill, price priority, time priority, market order sweep and discard, limit order rests, cancel. Property check: total quantity bought equals total quantity sold across all trades.

---

## Step 4: Order generator and golden file (3 hours)

**Learn**
- A **seeded** `new Random(42)` gives the same numbers every time. This makes bugs reproducible.
- Model price as a **random walk**: the mid price moves a little each order. Orders land near the mid.
- Hidden non-repeatability comes from: iterating a `HashSet`/`HashMap` of objects without stable hashing, `System.nanoTime()` in logic, `Math.random()`, thread timing.

**Build**
- `OrderGenerator(seed, symbols, numAccounts)` producing commands: ~85% new limit orders, ~10% market orders, ~5 to 15% cancels of a random earlier order id (a cancel of something already gone is fine; it just returns "not found").
- Order ids come from a counter in the generator.
- `Replay`: run N commands through the matcher and write all trades to `golden.txt`, one per line.

**Break it:** run twice. If the files differ, find the hidden non-repeatability and remove it.

**Check:** two runs with the same seed produce **byte-identical** output. Commit that as your **golden file**. In every later step, the single-threaded engine must still reproduce it.

---

# Part 2: Threads, locks, and the memory model (22 hours)

## Step 5: Threads and race conditions (4 hours)

**Learn**
- **Thread:** one independent line of execution inside your program. Many threads share the same memory.
- **Concurrency** = many tasks making progress by taking turns. **Parallelism** = tasks literally running at the same instant on different CPU cores.
- **Process** = separate program with separate memory. **Thread** = lives inside a process and shares memory. Sharing is fast but dangerous.
- **Thread states in Java:** NEW, RUNNABLE, BLOCKED (waiting for a lock), WAITING / TIMED_WAITING (waiting for a signal or time), TERMINATED.
- **Race condition:** two threads touch the same data, and the result depends on who is faster.
- `count++` is really three steps: read, add, write. Two threads can both read 5 and both write 6. One increment is lost.
- Tools: `new Thread(runnable)`, `start()`, `join()` (wait for it to finish), `interrupt()`.

**Drill (do this first, 30 minutes)**
```java
public class RaceDemo {
    static int count = 0;
    public static void main(String[] a) throws Exception {
        Thread[] ts = new Thread[10];
        for (int i = 0; i < 10; i++) {
            ts[i] = new Thread(() -> { for (int j = 0; j < 100_000; j++) count++; });
            ts[i].start();
        }
        for (Thread t : ts) t.join();
        System.out.println(count); // you expect 1,000,000. You get less.
    }
}
```

**Build**
- `NaiveEngine`: one shared `Matcher` and a `submit(Command)` method with **no protection**.
- Test harness: 8 threads, each submitting a slice of generated commands.

**Break it:** run it in a loop of 200 runs. You should see exceptions from the `TreeMap`, duplicate trade ids, or an order filled more than its quantity.

**Check:** write a test that fails reliably (within the 200 runs). Assert: no exceptions, trade ids unique, no order's total filled quantity is more than its quantity. **This failing test is your regression test for the next steps.**

---

## Step 6: Locks and the memory model (6 hours)

**Learn**
- **`synchronized`**: only one thread at a time can be inside blocks guarded by the same object. Others wait (BLOCKED).
- **`ReentrantLock`**: same idea, as an object. Extra powers: `tryLock()` (do not wait forever), fairness, and `Condition`. Always unlock in `finally`.
- **Reentrant** means the same thread can take the same lock again without blocking itself.
- **`volatile`**: guarantees that a write by one thread is **visible** to other threads. It does **not** make `count++` safe.
- **Why visibility matters:** each CPU core and the JIT compiler may cache a variable. Without `volatile` or a lock, a reader thread can keep seeing an old value forever.
- **Happens-before (the Java Memory Model rule):** everything a thread did before releasing a lock is visible to the next thread that takes the same lock. Same for a volatile write followed by a volatile read. This is why locks fix visibility too.
- **`wait/notify`** and **`Condition`**: "sleep until someone says the condition is true". Always check the condition in a `while` loop, never `if`.
- **Coarse lock** = one big lock for everything (simple, slow). **Fine-grained** = many small locks (faster, easier to get wrong).

**Drill (1.5 hours)**
- Fix the Step 5 counter three ways: `synchronized`, `ReentrantLock`, `AtomicInteger`.
- Volatile demo:
```java
static boolean running = true;   // try with and without volatile
Thread t = new Thread(() -> { while (running) { } });
t.start(); Thread.sleep(1000); running = false; t.join();  // often hangs without volatile
```

**Build**
- `SyncEngine`: `public synchronized Result submit(Command c)`.
- `LockEngine`: same with `ReentrantLock`:
```java
lock.lock();
try { /* match */ } finally { lock.unlock(); }
```
- Timing: run 1 million generated orders on 1 thread, then on 8 threads with each engine. Print orders per second. (A simple `System.nanoTime()` timer is enough.) Save numbers in `docs/benchmarks.md`.

**Break it**
- Remove `volatile` from the stop flag. Watch the hang.
- Put a `System.out.println` inside the locked section. Watch throughput collapse (never do slow work while holding a lock).

**Check:** the Step 5 failing test now passes 200 times in a row. The single-threaded golden file still matches.

---

## Step 7: Accounts and settlement (4 hours)

**Learn**
- Each account has `cash` and `shares` per symbol.
- **Reservation:** when you place an order, the money or shares you would need are set aside so you cannot spend them twice. Fields: `reservedCash`, `reservedShares`.
- Rules:
  1. **Before** an order enters the book: BUY needs `cash - reservedCash >= price x qty`, then `reservedCash += price x qty`. SELL needs `shares - reservedShares >= qty`, then `reservedShares += qty`. Otherwise reject the order.
  2. **On each trade** at price `p`, quantity `q`:
     - Buyer: `cash -= p x q`, `shares += q`, `reservedCash -= buyLimitTicks x q`
     - Seller: `cash += p x q`, `shares -= q`, `reservedShares -= q`
  3. **When an order ends with leftover** (cancelled, market remainder thrown away, self-trade stop): release it. BUY: `reservedCash -= price x leftover`. SELL: `reservedShares -= leftover`.
- The buyer's price improvement (limit 100, trade at 98) is handled by rule 2 automatically: you remove the full `buyLimitTicks x q` from the reservation but only charge `p x q` in cash.

**Build**
- `Account` and `AccountService` (a map of accounts; seed 1000 accounts with fixed cash and shares).
- `reserve(order)`, `settle(trade)`, `release(order, leftoverQty)`.
- Engine flow: reserve, match, settle each trade, release `unrestedQty` (and on cancel, release the cancelled entry's `remaining`).
- Extend the generator to create accounts with known starting totals.

**Break it:** in `SyncEngine`, move `settle` **outside** the lock. Run 8 threads. Total cash stops adding up (lost updates). Move it back.

**Check (single-threaded replay and 8-thread run):**
- Total cash across all accounts never changes.
- Total shares per symbol never changes.
- No account has negative cash or negative shares.
- After cancelling every resting order, `reservedCash` and `reservedShares` are 0 everywhere.

---

## Step 8: Deadlock and livelock (5 hours)

**Learn**
- **Deadlock:** thread A holds lock 1 and waits for lock 2. Thread B holds lock 2 and waits for lock 1. Nobody moves, forever.
- Needs all four: mutual exclusion, hold-and-wait, no preemption, circular wait. **Break circular wait** and deadlock cannot happen.
- **Fix = lock ordering:** always take locks in the same global order (for example, lowest account id first).
- **Livelock:** threads keep reacting to each other and stay busy but make no progress (two people stepping aside in a corridor, forever). Fix: random wait before retrying.
- `tryLock(timeout)` avoids waiting forever, but you must handle the "failed" case.
- **Reading a thread dump:** run `jps` to get the process id, then `jstack <pid>`. Look for the text `Found one Java-level deadlock`. It shows each thread, the lock it holds and the lock it wants.

**Drill (1.5 hours)**
```java
Object a = new Object(), b = new Object();
new Thread(() -> { synchronized (a) { sleep(100); synchronized (b) { } } }).start();
new Thread(() -> { synchronized (b) { sleep(100); synchronized (a) { } } }).start();
// run it, then jstack it
```
Then write a livelock: two threads each `tryLock` both locks, and on failure release and retry immediately. Fix with a random sleep.

**Build**
- Give every `Account` its own `ReentrantLock`.
- `settle(trade)` locks **both** accounts. Lock the lower account id first, then the higher.
```java
Account first = byId(Math.min(t.buyAccountId(), t.sellAccountId()));
Account second = byId(Math.max(t.buyAccountId(), t.sellAccountId()));
first.lock.lock();
try { second.lock.lock(); try { apply(t); } finally { second.lock.unlock(); } }
finally { first.lock.unlock(); }
```
(The matcher never makes a trade between the same account, so first and second differ.)
- Reservation changes lock only the one account involved.

**Break it:** lock in (buyer, seller) order instead. Then run two threads settling trades "A buys from B" and "B buys from A" many times. It hangs. Capture the `jstack` output, find the deadlock text, and paste it in your notes.

**Check:** 1000 threads each settling opposite-direction trades finish with no hang and conserved totals.

---

## Step 9: Atomics (3 hours)

**Learn**
- An **atomic** operation happens completely or not at all; no thread sees it half done.
- **CAS (compare-and-set):** "set the value to 8 only if it is still 7". Returns true or false. No lock needed. If it fails, you retry.
- `AtomicLong.incrementAndGet()` does this for you.
- `LongAdder` is faster than `AtomicLong` when many threads only **add** (counters), because it spreads the work. Reading it is slower.
- **Non-blocking** means threads never wait on a lock. They retry instead. **Blocking** means they wait.
- **ABA problem** (concept only): a value changes 7 to 9 to 7 and CAS cannot tell. Not an issue for counters.

**Drill (30 minutes):** write your own CAS counter and compare with `AtomicLong`:
```java
AtomicLong v = new AtomicLong();
long next() {
    while (true) {
        long cur = v.get();
        if (v.compareAndSet(cur, cur + 1)) return cur + 1;
    }
}
```

**Build**
- `Sequencer` with an `AtomicLong`: used for order ids and trade ids (replace the plain counters).
- `Metrics`: `LongAdder` counters for orders received, trades made, orders rejected.

**Break it:** replace the sequencer with plain `long++`. Run 1000 threads. Duplicate ids appear.

**Check:** 1000 threads take 100 ids each: exactly 100,000 unique ids, with no gaps.

---

# Part 3: Making it scale (19 hours)

## Step 10: Producer-consumer and per-symbol sharding (6 hours)

**Learn**
- **Producer-consumer:** some threads produce work, others consume it, and a **queue** sits between them. The queue lets both sides run at their own speed.
- **`BlockingQueue`:**
  - `put()` waits if the queue is full. `take()` waits if it is empty.
  - `offer()` returns false immediately if full. `poll()` returns null immediately if empty.
  - `ArrayBlockingQueue` has a fixed size (bounded). `LinkedBlockingQueue` is unbounded by default (dangerous).
- **Single-writer principle:** if exactly **one** thread ever touches a piece of data, you need **no lock**. So give each symbol's order book to one matcher thread.
- **Sharding:** pick the thread by `symbol` so the same symbol always goes to the same thread: `Math.floorMod(symbol.hashCode(), numShards)`.
- Time priority now equals the order in which commands reach that shard's queue.

**Drill (1.5 hours)**
- Write a bounded buffer using `synchronized`, `wait()` and `notifyAll()`:
```java
class BoundedBuffer<T> {
    private final Queue<T> q = new ArrayDeque<>(); private final int cap;
    BoundedBuffer(int cap) { this.cap = cap; }
    synchronized void put(T x) throws InterruptedException { while (q.size() == cap) wait(); q.add(x); notifyAll(); }
    synchronized T take() throws InterruptedException { while (q.isEmpty()) wait(); T x = q.remove(); notifyAll(); return x; }
}
```
- Rewrite it with `ArrayBlockingQueue`. Compare how much shorter and safer it is.

**Build**
```java
class Shard implements Runnable {
    final BlockingQueue<Command> queue = new ArrayBlockingQueue<>(10_000);
    final Map<String, OrderBook> books = new HashMap<>();   // only THIS thread touches it
    public void run() {
        try {
            while (true) {
                Command c = queue.take();
                if (c == POISON) break;
                handle(c);                       // match, settle, complete c.result
            }
        } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }
}
```
- `ShardedEngine.submit(cmd)`: assign an id from the `Sequencer`, reserve funds, pick a shard, `offer()` the command. Each command carries a `CompletableFuture<Result>` (think of it as a box the caller can wait on; the shard fills it in with `complete(result)`).
- Shutdown: put `POISON` into each queue, then `join` the shard threads.
- Settlement uses the per-account locks from Step 8.

**Break it**
- Let a second thread read an order book "just to look". Run the stress test. It races.
- Make the queue unbounded and flood it faster than the shard can consume. Memory grows (this sets up Step 11).

**Check:**
- The Step 5 stress test passes with **no lock on the order book**.
- For each symbol, the trades match a single-threaded replay of the commands that shard received, in the order it received them.
- 8 symbols on 4 shards is faster than the Step 6 coarse-lock engine. Record both numbers.

---

## Step 11: Thread pools, semaphores, backpressure (6 hours)

**Learn**
- **Thread pool:** a fixed set of worker threads reused for many tasks. Creating a thread per request is slow and can crash you.
- **`ThreadPoolExecutor(core, max, keepAlive, unit, queue, threadFactory, rejectionPolicy)`**
  - Extra threads beyond `core` are created **only when the queue is full**.
  - With an **unbounded** queue, `max` is never used and overload is hidden until you run out of memory.
- **Rejection policies when queue and threads are full:**
  - `AbortPolicy`: throws `RejectedExecutionException` (use this, return "overloaded", later HTTP 429).
  - `CallerRunsPolicy`: the submitting thread does the work itself, which slows the producer down. That is natural backpressure.
  - `DiscardPolicy`: silently drops (almost always wrong).
- **Backpressure:** when the system is overloaded, push back on the sender (reject or slow down) instead of buffering without limit.
- **`Semaphore(n)`:** n permits. `acquire()` takes one (waits if none), `release()` returns one. `tryAcquire()` does not wait. Use it to limit how many things happen at once.
- **`CountDownLatch(n)`:** wait until a count reaches zero. You use it to make 1000 threads start at exactly the same moment in tests.
- Shutdown: `shutdown()` (finish queued work) vs `shutdownNow()` (interrupt), then `awaitTermination`.

**Drill (2 hours)**
- Connection pool using a `Semaphore` (permits = pool size) and a `BlockingQueue<Connection>`:
```java
Conn acquire() throws InterruptedException { permits.acquire(); return conns.take(); }
void release(Conn c) { conns.add(c); permits.release(); }
```
- A tiny thread pool from scratch: N worker threads looping on `queue.take()`, an `execute(Runnable)` using `queue.offer()` that rejects if full.

**Build**
- Intake pool in front of the engine:
```java
AtomicInteger n = new AtomicInteger();
var pool = new ThreadPoolExecutor(4, 4, 0L, TimeUnit.SECONDS,
        new ArrayBlockingQueue<>(1000),
        r -> new Thread(r, "intake-" + n.incrementAndGet()),   // named threads make thread dumps readable
        new ThreadPoolExecutor.AbortPolicy());
try { pool.execute(task); } catch (RejectedExecutionException e) { return Result.OVERLOADED; }
```
- If a shard queue `offer()` returns false: return `OVERLOADED` and **release the reservation** you took.
- Per-account cap using `Semaphore`: max 50 orders in flight per account (`tryAcquire`, release when the result is complete).

**Break it**
- Switch the intake queue to unbounded, run with `-Xmx64m`, and flood it. Watch heap climb and an `OutOfMemoryError`.
- Switch back to bounded + `AbortPolicy`. It stays alive and sheds load.
- Try `CallerRunsPolicy`. Compare: producers slow down instead of getting errors.

**Check:** send 10x the capacity. Memory stays flat, rejected count is greater than 0, and every **accepted** order still reaches a final result.

---

## Step 12: Market data for many readers (4 hours)

**Learn**
- Clients constantly read "best bid, best ask, last price, top 5 levels". Only the matcher writes. Many readers, one writer.
- **Approach A: `ReentrantReadWriteLock`.** Many readers at once, but a writer needs exclusive access. Simple, but readers and the writer block each other.
- **Approach B: immutable snapshot.** After each command the matcher builds a brand new immutable `Snapshot` object and stores it in a `volatile` field. Readers just read the field. No lock, and they always see one consistent picture.
```java
record Snapshot(long bestBid, long bestAsk, long lastTrade, List<Level> bids, List<Level> asks) {}
volatile Snapshot snapshot;                       // matcher: snapshot = build();  readers: snapshot
```
Use `List.copyOf(...)` so the lists inside are immutable.
- **Why the snapshot is safe:** the object is immutable, and the `volatile` write makes it fully visible to readers (this is "safe publication").
- **Starvation** (concept): a thread never gets its turn. Locks can be fair (`new ReentrantReadWriteLock(true)`) at a speed cost.
- **Concurrent collections (concept):** `ConcurrentHashMap` is a thread-safe map with fine-grained internal locking; use it for shared maps (accounts, Step 13). `CopyOnWriteArrayList` copies the whole array on every write; good only for rarely-changed lists.

**Drill (1 hour):** a cache with 16 reader threads and 1 writer, once with a read-write lock, once with a snapshot swap. Time both.

**Build**
- `MarketData` per symbol with Approach A and Approach B behind one interface.
- Matcher thread updates it after every command.
- A reader test: 16 reader threads loop reading.

**Break it:** store `bestBid` and `bestAsk` in two separate fields instead of one snapshot. Readers will sometimes see a new bid with an old ask (an impossible book). Run the reader test until it catches it.

**Check:** readers never see `bestBid >= bestAsk` or inconsistent depth. Timing table: lock vs snapshot at 1, 4 and 16 readers.

---

## Step 13: Idempotency under concurrency (3 hours)

**Learn**
- Clients retry: a timeout, a double-click, a network glitch. The same order may arrive twice or 100 times at once. Only **one** order must be created.
- **Idempotent** = repeating the request has the same effect as doing it once. The client sends a unique `clientOrderId`. The key is `accountId + ":" + clientOrderId`.
- **The trap:** check-then-act is a race.
```java
if (!seen.containsKey(key)) { seen.put(key, ...); process(); }   // two threads can both pass the check
```
- **The fix:** one atomic operation, `putIfAbsent`:
```java
CompletableFuture<Result> mine = new CompletableFuture<>();
CompletableFuture<Result> existing = seen.putIfAbsent(key, mine);   // seen is a ConcurrentHashMap
if (existing != null) return existing;      // duplicate: hand back the ORIGINAL result, not an error
// I am the first: process the order, then mine.complete(result)
```
- **In production** the same idea uses Redis `SET key NX` (fast check) plus a database unique constraint on `(accountId, clientOrderId)` (the truth that survives restarts). The concept is identical; you build the in-memory version.

**Build:** add the dedupe to `submit()` before reservation and routing.

**Break it:** use the `containsKey` version. Fire 100 threads with the same key using a `CountDownLatch`. You get several orders.

**Check:** 1000 threads submit the **same** `clientOrderId`: exactly one order exists, exactly one reservation was made, all 1000 callers receive the same result.

---

# Part 4: Prove it and ship it (7 hours)

## Step 14: Stress test (4 hours)

**Learn**
- A concurrent test that passes once proves almost nothing. Bugs depend on timing.
- Method: run lots of threads, lots of times, with random timing nudges, and check **invariants** (things that must always be true) at the end.
- Nudging timing: sprinkle `if (rnd.nextInt(100) == 0) Thread.yield();` at critical points to shuffle the thread schedule.
- Use a `CountDownLatch` so all threads start together.

**Build** `StressRunner`:
- N threads, each submitting M generated orders (random symbols and accounts; some duplicates, some cancels).
- Wait for everything to finish, drain, then check **all invariants**:
  - No negative cash or shares anywhere.
  - Total cash and total shares conserved.
  - Order ids and trade ids unique.
  - No order filled more than its quantity.
  - Every order ended in exactly one final state: filled, partly filled and resting, cancelled, rejected, or overloaded.
  - Duplicate `clientOrderId`s created exactly one order.
  - After cancelling all resting orders, all reservations are 0.
- Loop 500 times with different seeds.

**Break it:** re-introduce each old bug one at a time (unsynchronized submit, settle outside lock, lock in wrong order, plain `long++`, `containsKey` check). Confirm the stress test catches **every one**. A test that can never fail is worthless.

**Check:** 500 consecutive clean runs on the correct engine.

---

# Part 5: Real-service features (23 hours)

Steps 15 to 21 need Docker (for PostgreSQL and Redis). Add a `docker-compose.yml` with both before Step 16.

## Step 15: CompletableFuture and virtual threads (4 hours)

**Learn**
- A `CompletableFuture<T>` is a box for a result that arrives later. You attach steps to it instead of waiting.
  - `thenApply(f)`: transform the result.
  - `thenCompose(f)`: the next step itself returns a future (use this to chain async steps).
  - `thenCombine(other, f)`: wait for two futures, then merge.
  - `exceptionally(f)` / `handle(f)`: deal with failure. `handle` sees both result and error.
  - `orTimeout(2, TimeUnit.SECONDS)`: fail if it takes too long.
- By default `supplyAsync(...)` runs on a shared common pool. Always pass your own executor so you control which threads run your steps.
- **Virtual threads (Java 21):** very cheap threads managed by the JVM. You can have 100,000 of them. They are great when threads mostly **wait** (database, Redis, network). They do not help CPU-heavy work.
- Create them with `Executors.newVirtualThreadPerTaskExecutor()`.
- **Pinning:** in Java 21, blocking while inside a `synchronized` block holds the underlying OS thread. Prefer `ReentrantLock` around blocking code.

**Drill (1 hour):** chain three stages with a failure injected in the middle:
```java
CompletableFuture.supplyAsync(() -> validate(o), pool)
    .thenCompose(v -> reserveAsync(v))
    .thenApply(r -> route(r))
    .handle((res, err) -> err != null ? Result.failed(err.getMessage()) : res);
```

**Build**
- Turn `submit` into a pipeline: validate, dedupe, reserve, route to shard, wait for the shard's result, with `.orTimeout(...)`.
- Run intake once on the platform thread pool and once on virtual threads. Compare throughput when each order does a fake 5 ms blocking call (`Thread.sleep(5)`).
- Latency: record each order's `System.nanoTime()` duration into an array, sort it, print p50 and p99.

**Break it:** let a stage throw an exception with no `handle`. The order silently never completes. Fix with explicit `handle`.

**Check:** every order ends in exactly one final state (including "failed" and "timed out"). Latency numbers saved in `docs/benchmarks.md`.

---

## Step 16: PostgreSQL concurrency (5 hours)

**Learn**
- **Transaction:** a group of changes that all happen or none happen (`BEGIN` ... `COMMIT`).
- **Isolation level:** how much concurrent transactions can see of each other. PostgreSQL default is `READ COMMITTED`: you only see committed data, but a row can change between two of your reads.
- **Lost update:** two transactions read balance 100, both add 10, both write 110. One update vanishes. Same bug as `count++`, but in the database.
- **Write skew (concept):** two transactions each check a rule using different rows, both pass, together they break the rule. Fixed only by `SERIALIZABLE` or explicit locks.
- Three fixes for lost update:
  1. **Atomic update in SQL:** `UPDATE accounts SET cash = cash - ? WHERE id = ? AND cash >= ?` (check rows updated = 1).
  2. **Optimistic locking:** a `version` column. `UPDATE ... SET cash=?, version=version+1 WHERE id=? AND version=?`. If 0 rows updated, someone else won: re-read and retry. Good when conflicts are rare.
  3. **Pessimistic locking:** `SELECT ... FOR UPDATE` locks the row until you commit. Good when conflicts are frequent. Lock rows in id order to avoid deadlock (same rule as Step 8).

**Drill (1.5 hours):** plain JDBC, two threads on one row: read, sleep 50 ms, write. Show the lost update. Fix it all three ways.

**Build**
- Tables: `accounts(id, cash, version)`, `positions(account_id, symbol, shares)`, `orders(...)`, `trades(...)`.
- **The matcher thread must never wait on the database.** It puts trade events on a bounded queue. One writer thread drains the queue and writes trades plus balance changes in **one transaction** (batch several at a time).
- Use plain JDBC with a small connection pool (HikariCP, or your Step 11 semaphore pool).

**Break it:** run two concurrent writers updating the same account with read-then-write at `READ COMMITTED`. Watch cash go wrong.

**Check:** after a stress run, database totals (cash and shares) equal the in-memory totals, and totals match the starting amounts.

---

## Step 17: Redis idempotency (3 hours)

**Learn**
- Redis is a very fast in-memory key-value store shared by all your app instances.
- `SET key value NX EX 60` means: set only if the key does not exist, expire after 60 seconds. It is atomic, so exactly one caller gets "OK". This is `putIfAbsent` that works across many servers.
- Two layers:
  - **Redis NX:** fast check that stops most duplicates cheaply.
  - **Database unique constraint** on `(account_id, client_order_id)`: the final truth. Redis can lose data or the key can expire; the database cannot.
- A duplicate request returns the **original order's result** (read it from the DB), not an error.
- **Distributed lock caveat (concept):** a Redis lock with an expiry can expire while the holder is still working, and then two holders run at once. That is why the DB constraint is the real guard.

**Build**
- Add Redis to `docker-compose.yml`. Use Jedis or Lettuce.
- Flow: Redis `SET NX` fails: fetch and return the existing order. Succeeds: insert the order row. If the insert hits the unique constraint: return the existing order.

**Break it:** send 100 concurrent duplicates. Remove Redis (DB alone should still allow only one). Remove the DB constraint (Redis alone lets some through after key expiry). Seeing both fail alone shows why you need both.

**Check:** 1000 duplicate submits across many threads produce exactly one order row and one set of fills.

---

## Step 18: REST API and WebSocket feed (5 hours)

**Learn**
- A controller turns HTTP into an engine call. Map results to status codes: 200 accepted, 400 rejected, 429 overloaded.
- **WebSocket:** one long-lived connection where the server pushes data (trades, price updates) to the client.
- **The slow-client problem:** if the matcher thread writes to a slow client, the matcher blocks and the whole exchange stalls. **Never write to a socket from the matcher thread.**
- Fix: each client session gets its own small bounded queue and a sender. If the queue fills, drop old updates or disconnect the client.

**Build (REST, 2 hours)**
- Spring Boot app wrapping the engine behind an interface:
  - `POST /orders`, `DELETE /orders/{id}`, `GET /book/{symbol}` (reads the Step 12 snapshot).
- A load script that posts orders from many threads.

**Build (WebSocket, 3 hours)**
- Endpoint `/feed/{symbol}` pushing each new snapshot and each trade.
- The matcher puts events on an outbound bounded queue. A broadcaster thread copies them into each client's own bounded queue.

**Break it:** write to a deliberately slow client directly from the matcher thread. Watch matching throughput collapse. Then switch to per-client queues.

**Check:** one stalled client does not reduce matching throughput. The feed shows trades consistent with the book.

---

## Step 19: JMH benchmarks and StampedLock (3 hours)

**Learn**
- Hand-made `System.nanoTime()` timing lies: the JVM warms up, and the compiler can delete code whose result is unused. **JMH** is the standard tool that handles this: warmup, multiple forks, `Blackhole`.
- Key annotations: `@Benchmark`, `@State(Scope.Benchmark)`, `@Threads(n)`, `@BenchmarkMode(Mode.Throughput)`, `@Warmup`, `@Measurement`, `@Fork(2)`.
- **`StampedLock`** has an **optimistic read**: read without any lock, then check nothing changed.
```java
long stamp = sl.tryOptimisticRead();
Snapshot s = copyFields();
if (!sl.validate(stamp)) {          // a writer slipped in, fall back to a real read lock
    stamp = sl.readLock();
    try { s = copyFields(); } finally { sl.unlockRead(stamp); }
}
```
- It is **not reentrant**. Do not take it twice in one thread.

**Build:** a JMH module that benchmarks:
- `synchronized` vs `ReentrantLock` vs sharded engine
- `AtomicLong` vs `LongAdder` vs `synchronized` counter at 1, 4, 16 threads
- `ReadWriteLock` vs `StampedLock` optimistic vs volatile snapshot at 1, 4, 16 readers

**Check:** `docs/benchmarks.md` has one table per comparison with your machine specs. Every performance claim in your README points to a row.

---

## Step 20: Real market data replay (1 hour)

**Learn**
- Real prices make the demo convincing. Binance exposes public trade streams with no API key, for example the `btcusdt@trade` stream. Alternatively use a free LOBSTER sample file for Nasdaq stocks (check its terms).

**Build**
- Record a few minutes of trade prices to a file (one price per line).
- Feed that price series to `OrderGenerator` as the mid price instead of the random walk. Everything else stays the same.

**Check:** the feed from Step 18 shows prices that follow the real series.

---

## Step 21: Final stress run and README (2 hours)

**Build**
- Re-run the Step 14 stress test against the full system (including PostgreSQL totals). Add the invariant: database totals equal in-memory totals.
- Final project README: architecture diagram, benchmark tables, and a **"Bugs I reproduced and fixed"** section with the race, the lost update (in memory and in the database), the deadlock (paste the real `jstack` text), the memory blow-up, the duplicate order, and the slow WebSocket client.

**Check:** a fresh clone runs `docker-compose up -d`, `mvn test` is green, and `mvn spring-boot:run` serves the API and feed.

---

# Reference

## Where each of the 20 concepts is covered

| # | Concept | Step |
|---|---|---|
| 1 | Concurrency | 5 |
| 2 | Concurrency vs parallelism | 5, 10 |
| 3 | Processes vs threads | 5 |
| 4 | Thread lifecycle | 5, 8 |
| 5 | Race condition | 5, 6 |
| 6 | Mutex | 6 |
| 7 | Semaphore | 11 |
| 8 | Condition variable | 6, 10 |
| 9 | Coarse vs fine-grained locking | 6, 8, 10 |
| 10 | Reentrant lock | 6 |
| 11 | Deadlock | 8 |
| 12 | Livelock | 8 |
| 13 | Starvation | 12 (concept) |
| 14 | Atomic operations | 9 |
| 15 | Thread pool | 11 |
| 16 | Producer-consumer | 10 |
| 17 | Blocking vs non-blocking | 9, 10 |
| 18 | Backpressure | 11 |
| 19 | Immutability | 1, 12 |
| 20 | Idempotency under concurrency | 13 |

Also covered beyond the 20: Java Memory Model and `volatile` (6), `ConcurrentHashMap` (12, 13), `CountDownLatch` (11, 14), single-writer design (10), `CompletableFuture` and virtual threads (15), database isolation, optimistic and pessimistic locking (16), Redis `SET NX` (17), `StampedLock` and JMH (19).

## Final architecture

```
Client (REST)
   |
   v
Intake thread pool (bounded queue, rejects when full)
   |
   v
Idempotency check (Redis SET NX, then DB unique constraint)
   |
   v
Reserve funds (per-account lock)
   |
   v
Shard router: hash(symbol) -> shard
   |
   v
Shard bounded queue --> ONE matcher thread owns the books (no locks on books)
                              |-> settle trades (account locks, lowest id first)
                              |-> publish immutable market-data snapshot (volatile) --> WebSocket feed (per-client bounded queues)
                              |-> trade events on bounded queue --> single DB writer thread --> PostgreSQL
```

## Invariants the stress test enforces

- No negative cash or shares
- Total cash and shares conserved
- Unique order ids and trade ids
- No order filled more than its quantity
- One final state per order
- One order per `clientOrderId`
- Reservations return to 0 when everything is cancelled
- Single-threaded replay equals the golden file
- Database totals equal in-memory totals

## Reading (read only the chapter for the step you are on)

*Java Concurrency in Practice* (Goetz): ch. 1 to 3 (Steps 5, 6), ch. 5.3 (Step 10), ch. 6 and 8 (Step 11), ch. 10 (Step 8), ch. 12 (Step 14), ch. 15 (Step 9), ch. 16 (Step 6). Read it **after** you build, to fill gaps, not before. For Step 16 also read *Designing Data-Intensive Applications* (Kleppmann) ch. 7 and the PostgreSQL docs page on transaction isolation.