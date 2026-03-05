# Stream Processing: Advisory Lock Solution

**Date**: 2026-03-04 | **Branch**: `dev/PEG-2910-advisory-lock-stream`

## Problem

Pull-based stream processing (`EVENT_LISTENER` / `EVENT_INDEXER`) has a race condition in the error path:

```
1. BEGIN transaction         → row lock acquired (FOR NO KEY UPDATE SKIP LOCKED)
2. Process event             → FAILS
3. ROLLBACK                  → row lock RELEASED
4.   ← GAP: stream unprotected, another worker grabs it →
5. BEGIN new transaction     → record error in stream_error / stream_error_retry
```

Between steps 3 and 5, a concurrent worker can select the same stream via `SKIP LOCKED`, causing:
- `StreamErrorPersistenceException` — concurrent INSERT/UPDATE on `stream_error_retry`
- "Stream Status Position is changed" warnings — position advances between rollback and error recording

**Scope**: Only pull-based processing is affected. Push-based (`EVENT_PROCESSOR` via JMS) serializes per stream and is not vulnerable.

## Solution: Session-Level Advisory Locks

PostgreSQL session-level advisory locks (`pg_try_advisory_lock`) survive COMMIT and ROLLBACK — unlike transaction-level locks or row locks. A dedicated non-JTA JDBC connection holds the lock across the transaction boundary gap.

```
1. Open advisory lock connection (non-JTA, outside transaction)
2. BEGIN transaction → row lock acquired
3. pg_try_advisory_lock(streamId) on advisory connection
4. Process event → FAILS
5. ROLLBACK → row lock released, advisory lock SURVIVES
6. Record error (new transaction) → safe, advisory lock blocks other workers
7. FINALLY: pg_advisory_unlock + close advisory connection
```

### Key design decisions

| Decision | Rationale |
|----------|-----------|
| Session-level (not transaction-level) | Must survive ROLLBACK — transaction-level locks are released on rollback |
| Separate non-JTA connection | JTA-enlisted connections are returned to pool on rollback; advisory lock would be lost |
| `pg_try_advisory_lock` (non-blocking) | If another worker holds the lock, skip the stream — same behaviour as `SKIP LOCKED` |
| `streamId.getLeastSignificantBits()` as lock key | UUID → 64-bit long; collision risk negligible with UUID v4 |

### New class: `StreamSessionLockManager`

| Method | Purpose |
|--------|---------|
| `openLockConnection()` | Opens dedicated JDBC connection (`autoCommit=true`) |
| `tryLockStream(conn, streamId)` | `SELECT pg_try_advisory_lock(?)` — returns boolean |
| `unlockStream(conn, streamId)` | `SELECT pg_advisory_unlock(?)` |
| `closeQuietly(conn)` | Fault-tolerant close |

## Connection Overhead

Each worker uses one extra viewstore connection for advisory locks:

| Resource | Without | With advisory locks |
|----------|---------|---------------------|
| viewstore connections/worker | 1 | 2 |
| eventstore connections/worker | 1 | 1 |
| **Total for 10 workers** | **20** | **30** |

Pool capacity: viewstore max=50, eventstore max=50. Overhead is within limits.

## Load Test Results (120K events, 10 workers)

### Baseline (no errors)

| Metric | Value |
|--------|-------|
| viewstore peak / avg | 23 / 23.0 |
| eventstore peak / avg | 28 / 26.2 |
| Duration | 213s |

### Mixed error injection (80% DB trigger + 10% code failure + 5% constraint violation)

| Metric | Value |
|--------|-------|
| viewstore peak / avg | 25 / 23.7 |
| eventstore peak / avg | 30 / 28.7 |
| Duration | 754s |
| Streams errored | 8,166 / 10,000 (81.7%) |
| Race condition errors | **0** |

**Connections remain stable under 81.7% error rate.** No pool exhaustion, no spikes. The advisory lock successfully prevents the race condition — zero `StreamErrorPersistenceException` or "position changed" warnings.

## Files Changed

| File | Change |
|------|--------|
| `StreamSessionLockManager.java` | NEW — advisory lock lifecycle |
| `StreamSessionLockException.java` | NEW — RuntimeException wrapper |
| `StreamEventProcessor.java` | Uses `StreamSessionLockManager`; opens/closes advisory connection in `finally` |
| `StreamSessionLockManagerTest.java` | NEW — 10 unit tests |
| `StreamEventProcessorTest.java` | Updated — verifies lock lifecycle |