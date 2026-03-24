# Notification-Based Event Pipeline — Framework F (17.105.x)

## Problem

The event pipeline uses timer-based polling at four stages: linking, publishing, discovery, and stream processing. With default intervals (100-500ms), this introduces up to **1300ms** of end-to-end latency from event append to handler execution. Reducing timer intervals lowers latency but increases CPU and database load during idle periods — a trade-off with no optimal point.

## Solution

CDI event notifications trigger each pipeline stage immediately when work arrives, eliminating timer-interval latency without the CPU cost of aggressive polling. Framework F extends this beyond linking and publishing (Phase 1, shared with D/E) to also notify stream discovery and processing (Phase 2, F only).

### Phase 1: Linking + Publishing (same concept as D/E)

Two notifier beans observe CDI events fired after an event is committed to `event_log`:

| Notifier | Fires on | Wakes | Effect |
|----------|----------|-------|--------|
| `EventLinkingNotifier` | New event committed | Linking background thread | Links events immediately via `EventNumberLinker.findAndLinkEventsInBatch()` |
| `EventPublishingNotifier` | Events linked | Publishing background thread | Publishes events immediately (publish_queue drain) |

Each notifier runs a single background thread managed by `ManagedExecutorService`. An `AtomicBoolean started` prevents concurrent executions. The existing timer continues as a safety net.

### Phase 2: Discovery + Stream Processing (F only)

Framework F's pull-based processing (EVENT_LISTENER/EVENT_INDEXER) enables a second notification stage. After linking, the notifier fires async CDI events that update `stream_status` and wake the `StreamProcessingCoordinator`:

| Notifier | Fires on | Effect |
|----------|----------|--------|
| `EventDiscoveryNotifier` | `EventsLinkedEvent` (async) | Conditional UPSERT on `stream_status.latest_known_position`; fires `StreamStatusAdvancedEvent` if position advanced |
| `StreamProcessingCoordinator` | `StreamStatusAdvancedEvent` (async) | Spawns 1 worker if none active for the source/component pair |

### Signaling: ArrayBlockingQueue(1)

Phase 1 notifiers use an `ArrayBlockingQueue<Object>(1)` as a coalescing signal:

- **Producer** (`wakeUp`): `workSignal.offer(SIGNAL)` — non-blocking. If the queue already has a signal (consumer is processing), the offer silently drops. The consumer drains all available work via an inner loop, so duplicate signals are unnecessary.
- **Consumer** (background thread): `workSignal.take()` — blocks until a signal arrives, then drains all accumulated work before blocking again.

Phase 1 observers are synchronous (`@Observes`) because `workSignal.offer()` is non-blocking — instant. Phase 2 observers are asynchronous (`@ObservesAsync`) because they perform DB operations and must not block the linking thread.

### Full CDI Event Chain

```
EventStreamManager.append()
  → EventAppendTriggerService.registerTransactionListener()
  → [TX commit]
  → fire(EventAppendedEvent)                                     ← marker, no payload
  → EventLinkingNotifier @Observes                               ← workSignal.offer(SIGNAL)
    → [background thread wakes from take()]
    → while (eventNumberLinker.findAndLinkEventsInBatch() > 0):
        → eventPublishingNotifier.wakeUp(false)                  ← Phase 1: wake publisher
        → fireAsync(EventsLinkedEvent(streamIds, positions))      ← Phase 2 trigger
          → EventDiscoveryNotifier @ObservesAsync                ← UPSERT stream_status
            → [if position advanced] fireAsync(StreamStatusAdvancedEvent(source, component))
              → StreamProcessingCoordinator @ObservesAsync       ← spawn worker if idle
```

### CDI Event Types

| Event | Fields | Fired By | Observed By | Type |
|-------|--------|----------|-------------|------|
| `EventAppendedEvent` | none (marker) | `EventAppendTriggerService` | `EventLinkingNotifier` | `@Observes` (sync) |
| `EventsLinkedEvent` | `List<StreamPosition>` (streamId, highest position per stream) | `EventLinkingNotifier` | `EventDiscoveryNotifier` | `@ObservesAsync` |
| `StreamStatusAdvancedEvent` | `source`, `component` | `EventDiscoveryNotifier` | `StreamProcessingCoordinator` | `@ObservesAsync` |

### EventDiscoveryNotifier — Conditional UPSERT

```sql
INSERT INTO stream_status (stream_id, position, source, component, discovered_at,
                           latest_known_position, is_up_to_date)
VALUES (?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (stream_id, source, component) DO UPDATE SET
    latest_known_position = EXCLUDED.latest_known_position,
    is_up_to_date = false
WHERE stream_status.latest_known_position < EXCLUDED.latest_known_position
```

`executeUpdate() > 0` means the position advanced — fire `StreamStatusAdvancedEvent`. This is idempotent: out-of-order async events or concurrent timer discovery both converge to the correct state.

## JNDI Configuration

### Phase 1

| Property | Default | Effect |
|----------|---------|--------|
| `event.linking.worker.notified` | `false` | Enables EventLinkingNotifier |
| `event.publishing.worker.notified` | `false` | Enables EventPublishingNotifier |

### Phase 2 (requires pull-based processing enabled)

| Property | Default | Effect |
|----------|---------|--------|
| `event.processing.by.pull.mechanism.enabled` | `false` | Master switch for pull-based processing (pre-existing) |
| `event.discovery.notified` | `false` | Enables EventDiscoveryNotifier |
| `stream.processing.discovery.notified` | `false` | Enables StreamProcessingCoordinator wake-up |

All flags default to OFF. Enabling them is purely additive — timers remain active as fallbacks. With notification enabled, consider relaxing safety-net timer intervals (e.g., 500ms–1000ms) to reduce idle polling.

## Expected Performance Characteristics

### Latency

Timer-based polling adds up to one timer interval of latency per pipeline stage. With four stages and default intervals of 100-500ms, end-to-end latency is dominated by timer wait times.

Notification eliminates this overhead — each stage is triggered immediately by CDI event, reducing latency to the actual processing time. Discovery notification has the largest impact on the pull-based read path: without it, each event waits for both the discovery timer and coordinator timer before the listener processes it.

### Throughput

Notification does not change the processing logic — the same linking, publishing, and stream processing code runs. During sustained burst load, discovery notification adds minor overhead due to concurrent `stream_status` UPSERTs. The trade-off is immediate stream discovery vs timer-based discovery which may lag behind under load.

### Idle CPU

Timer-based polling executes database queries on every tick, even when no work exists. Notification replaces this with `ArrayBlockingQueue.take()` — the thread blocks with near-zero idle cost.

### Crash Recovery

Notification does not change crash safety. Event linking is serialized by advisory lock 42, so event_number chain integrity is maintained regardless of replica crashes. With Framework F's pull-based listeners, viewstore recovery after crashes is expected to be complete, compared to push-based JMS listeners (D/E) where in-flight messages can be lost to Artemis DLQ during container failures.

## EventDiscoveryNotifier — Concurrency Design

`EventDiscoveryNotifier` requires `@ConcurrencyManagement(BEAN)` and `@TransactionAttribute(NOT_SUPPORTED)`:

- **`@ConcurrencyManagement(BEAN)`**: Default container-managed concurrency serializes all `@ObservesAsync` calls through a singleton lock. Under burst load, CDI async threads queue up and hit the `@AccessTimeout`, causing `ConcurrentAccessTimeoutException`. Bean-managed concurrency allows concurrent execution — the method only does independent per-stream UPSERTs, safe for concurrent access.

- **`@TransactionAttribute(NOT_SUPPORTED)`**: Without this, the EJB container wraps the entire method in a JTA transaction. All `getConnection()` calls inside the loop enlist in the same JTA transaction (IronJacamar auto-enlistment), accumulating row locks across multiple streams. When the timer-based `EventDiscoveryWorker` runs concurrently and UPSERTs the same streams in different order, PostgreSQL detects a deadlock. With `NOT_SUPPORTED`, each UPSERT auto-commits independently — single-statement transactions cannot deadlock.

## Safety Nets

Timers always run alongside notifications:

| Scenario | Recovery |
|----------|----------|
| Server restart | Timer `wakeUp(true)` starts notifier thread on first tick |
| Notification thread dies | `finally { started.set(false) }` allows timer to restart it |
| CDI event lost | Timer polls at configured interval (safety net) |
| Multi-node AKS | Advisory lock 42 serializes linking. Notification wakes local node only. Timer on other nodes picks up remaining work. |
| Out-of-order `@ObservesAsync` | Conditional UPSERT (`WHERE latest_known_position < ?`) is idempotent — converges regardless of delivery order |

## Bug Fixes Included

| Bug | Impact | Fix |
|-----|--------|-----|
| Dead notifier thread | `started` AtomicBoolean not reset on thread exit; notifier permanently stops | `finally { started.set(false); }` in `runWithInterruptable()` |
| submit() failure | `RejectedExecutionException` leaves `started=true`; same permanent stop | try-catch around `executorService.submit()` + reset |
| Logger injection | SLF4J logger via LoggerFactory; inconsistent with framework CDI style | Changed to `@Inject Logger` |

## Difference from D and E

| Aspect | D (17.103.x) | E (17.104.x) | F (17.105.x) |
|--------|-------------|-------------|-------------|
| Scope | Phase 1 only | Phase 1 only | **Phase 1 + Phase 2** |
| Linking class | `PrePublishNotifier` | `EventLinkingNotifier` | `EventLinkingNotifier` |
| JNDI prefix | `pre.publish.worker.*` | `event.linking.worker.*` | `event.linking.worker.*` |
| Linking method | `PrePublishProcessor` (single event, DB sequence) | `EventNumberLinker` (batch, advisory lock 42) | Same as E |
| Phase 2 | N/A | N/A | `EventsLinkedEvent` → `EventDiscoveryNotifier` → `StreamProcessingCoordinator` |
| Viewstore recovery after crash | 60-80% (push-based JMS) *† | 60-80% (push-based JMS) *† | **100% (pull-based)** *† |

*† Further testing required — these figures are from limited local replica failure tests (2 pods, 10–30 events per scenario). Production-scale validation with more replicas and higher event volumes is needed to confirm.*