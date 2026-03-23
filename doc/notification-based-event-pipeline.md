# Notification-Based Event Pipeline — Framework D (17.103.x)

## Problem

Event linking and publishing use timer-based polling (default 500ms interval). This introduces up to 1 second of pipeline latency per event (500ms linking + 500ms publishing). Reducing the timer interval lowers latency but proportionally increases CPU and database load during idle periods.

## Solution

CDI event notifications trigger the linking and publishing workers immediately when new events arrive, eliminating timer-interval latency without the CPU cost of aggressive polling.

### How It Works

Two notifier beans observe CDI events fired after an event is committed to `event_log`:

| Notifier | Fires on | Wakes | Effect |
|----------|----------|-------|--------|
| `PrePublishNotifier` | New event committed | Linking background thread | Links event immediately (pre_publish_queue drain) |
| `EventPublishingNotifier` | Event linked | Publishing background thread | Publishes event immediately (publish_queue drain) |

Each notifier runs a single background thread managed by `ManagedExecutorService`. An `AtomicBoolean started` prevents concurrent executions. The existing timer continues as a safety net.

### Signaling: ArrayBlockingQueue(1)

Each notifier uses an `ArrayBlockingQueue<Object>(1)` as a coalescing signal:

- **Producer** (`wakeUp`): `workSignal.offer(SIGNAL)` — non-blocking. If the queue already has a signal (consumer is processing), the offer silently drops. The consumer drains all available work via an inner loop, so duplicate signals are unnecessary.
- **Consumer** (background thread): `workSignal.take()` — blocks until a signal arrives, then drains all accumulated work before blocking again.

### CDI Event Flow

```
EventStreamManager.append()
  → eventAppendTriggerService.registerTransactionListener()
  → [TX commit]
  → EventAppendSynchronization.afterCompletion(STATUS_COMMITTED)
  → CDI fire(new EventAppendedEvent())
  → PrePublishNotifier.onEventAppendedEvent(@Observes)
  → wakeUp(false) → workSignal.offer(SIGNAL)
  → [background thread wakes from take()]
  → while (prePublishProcessor.prePublishNextEvent()):
      → eventPublishingNotifier.wakeUp(false) → workSignal.offer(SIGNAL)
      → [publishing thread wakes from take()]
      → while (publishedEventDeQueuerAndPublisher.deQueueAndPublish()) { }
```

### JNDI Configuration

| Property | Default | Effect |
|----------|---------|--------|
| `java:global/pre.publish.worker.notified` | `false` | Enables PrePublishNotifier |
| `java:global/event.publishing.worker.notified` | `false` | Enables EventPublishingNotifier |

Both flags default to OFF. Enabling them is purely additive — the timer remains active as a fallback. With notification enabled, consider relaxing safety-net timer intervals (e.g., 500ms–1000ms) to reduce idle polling.

> **Note**: Framework E/F uses `event.linking.worker.*` naming instead of `pre.publish.worker.*`, matching the `EventNumberLinker` class introduced in E.

## Bug Fixes Included

| Bug | Impact | Fix |
|-----|--------|-----|
| Dead notifier thread | `started` AtomicBoolean not reset on thread exit; notifier permanently stops | `finally { started.set(false); }` |
| submit() failure | `RejectedExecutionException` leaves `started=true`; same permanent stop | try-catch around submit + reset |
| Logger injection | SLF4J logger not injectable via CDI in notifier beans | Use `LoggerFactory.getLogger()` directly |