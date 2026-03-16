# Event Linking Batch Optimisation

## Problem

The `EventNumberLinker` assigns sequential `event_number` values to events in `event_log` and inserts them into `publish_queue`. The original implementation processes one event per JTA transaction, each requiring 6 SQL round-trips:

```
BEGIN → SET LOCAL timeout → advisory lock → find next → SELECT MAX → UPDATE → INSERT → COMMIT
```

If each per-event transaction takes ~5ms, the theoretical ceiling is ~200 events/second. Under high-throughput command processing (e.g. 500+ events/s), the linker cannot keep pace, creating a growing backlog that delays downstream publishing and stream processing.

## Solution

Batch N events (configurable, default 10) per JTA transaction using JDBC `executeBatch()`. One connection held for the entire batch.

### Transaction Flow

```
getConnection()
  UserTransaction.begin()
    SET LOCAL statement_timeout
    pg_try_advisory_xact_lock(42)        -- 1 round-trip
    SELECT id FROM event_log             -- 1 round-trip (LIMIT N)
      WHERE event_number IS NULL
      ORDER BY date_created
    SELECT MAX(event_number)             -- 1 round-trip
    UPDATE event_log SET event_number=?  -- 1 round-trip (executeBatch, N rows)
    INSERT INTO publish_queue            -- 1 round-trip (executeBatch, N rows)
    --- compatibility mode only (published_event migration) ---
    INSERT INTO published_event          -- 1 round-trip (executeBatch, N rows)
    setval(event_sequence_seq)           -- 1 round-trip
  UserTransaction.commit()
connection.close()
```

5 round-trips per batch (7 with compatibility mode) vs 6 per event. For batch size 20: **120 round-trips reduced to 5** (or 7). The per-event overhead (JTA begin/commit, advisory lock acquire/release, connection management) is amortised across the batch.

**Compatibility mode**: Controlled by JNDI `event.publishing.add.event.to.published.event.table.on.publish` (code default `true`). When enabled, the linker also batch-inserts into the `published_event` table and updates the `event_sequence_seq` sequence. This is a transitional path for contexts still relying on the `published_event` table — once all contexts migrate to the `publish_queue` pipeline, compatibility mode can be disabled.

### Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| Single connection per batch | All operations share one physical connection within JTA (IronJacamar logical handles). Eliminates per-call `getConnection()` overhead. |
| `executeBatch()` for UPDATE and INSERT | Biggest single win. Reduces N SQL round-trips to 1 per statement type. |
| Configurable batch size via JNDI | `event.linking.worker.batch.size`, default 10. Contexts can tune via their YAML config without code changes. |
| Compatibility mode preserved | Framework E's `published_event` table insert (`shouldAlsoInsertEventIntoPublishedEventTable`) batched via same pattern. |

### Changed Classes

| Class | Change |
|-------|--------|
| `EventNumberLinker` | Rewritten: `findAndLinkBatch()` returns `int` (count linked). Single connection, batch operations. |
| `EventLinkingWorker` | Loop calls `findAndLinkBatch()` until 0 returned or time exceeded. |
| `LinkEventsInEventLogDatabaseAccess` | New connection-accepting batch methods: `getEventStoreConnection()`, `findBatchOfNextEventIdsToLink(conn, size)`, `linkEventsBatch(conn, list)`, `insertBatchIntoPublishQueue(conn, list)`. Original self-connecting methods retained. |
| `AdvisoryLockDataAccess` | New overload: `tryNonBlockingTransactionLevelAdvisoryLock(Connection, Long)`. |
| `LinkedEventData` | New record: `(UUID eventId, long eventNumber, long previousEventNumber)`. |
| `EventLinkingWorkerConfig` | New field: `batchSize` (JNDI key `event.linking.worker.batch.size`, default `"10"`). |
| `CompatibilityModePublishedEventRepository` | New: `insertBatchIntoPublishedEvent(conn, list)`, `setEventNumberSequenceTo(conn, long)`. (Framework E only) |

## Results

Comparative results from CakeShopConcurrencyIT under identical conditions:

| Metric | Original (per-event) | Batch |
|--------|---------------------|-------|
| Linking throughput | ~3x slower than event creation rate | Keeps pace with event creation |
| Linking backlog at command completion | Large — majority of events still unlinked | Zero — linking completes with commands |
| End-to-end time to fully linked | ~3x the command execution time | ~1x the command execution time |
| Stream errors | 0 | 0 |
| Advisory lock leaks | 0 | 0 |

The original per-event linker accumulates a significant backlog under concurrent load because each event requires its own JTA transaction with 6 round-trips. The batch linker eliminates this backlog entirely by amortising the fixed transaction costs across N events, achieving ~3x higher throughput with a batch size of 10.

## Configuration

JNDI binding `java:global/event.linking.worker.batch.size`. In WildFly `standalone.xml`, under `<subsystem xmlns="urn:jboss:domain:naming:..."><bindings>`:

```xml
<simple name="java:global/event.linking.worker.batch.size" value="20" type="java.lang.String"/>
```

Default is 10 if not configured. Recommended range: 10-50.