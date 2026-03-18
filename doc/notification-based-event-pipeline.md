# Notification-Based Event Pipeline — Framework D (17.103.x)

## Problem

Event linking and publishing use timer-based polling (default 500ms interval). This introduces up to 1 second of pipeline latency per event (500ms linking + 500ms publishing). Reducing the timer interval lowers latency but proportionally increases CPU and database load during idle periods.

## Solution

CDI event notifications trigger the linking and publishing workers immediately when new events arrive, eliminating timer-interval latency without the CPU cost of aggressive polling.

### How It Works

Two new notifier beans observe CDI events fired after an event is committed to `event_log`:

| Notifier | Fires on | Wakes | Effect |
|----------|----------|-------|--------|
| `PrePublishNotifier` | New event committed | `PrePublisherTimerBean` | Links event immediately (pre_publish_queue drain) |
| `EventPublishingNotifier` | Event linked | `PublisherTimerBean` | Publishes event immediately (publish_queue drain) |

Each notifier runs a single background thread that calls the same processing method as the timer. A `started` AtomicBoolean prevents concurrent executions. The existing timer continues as a safety net (unchanged interval).

### CDI Event Flow

```
EventStreamManager.append()
  → eventAppendTriggerService.registerTransactionListener()
  → [TX commit]
  → EventAppendSynchronization.afterCompletion(STATUS_COMMITTED)
  → CDI fire(new EventAppendedEvent())
  → PrePublishNotifier.onEventAppendedEvent(@Observes)
  → wakeUp(false) → monitor.notifyAll()
  → [background thread wakes]
  → prePublishProcessor.prePublishNextEvent()
  → [on success] eventPublishingNotifier.wakeUp(false)
  → publishedEventDeQueuerAndPublisher.deQueueAndPublish()
```

### JNDI Configuration

| Property | Default | Effect |
|----------|---------|--------|
| `java:global/pre.publish.worker.notified` | `false` | Enables PrePublishNotifier |
| `java:global/event.publishing.worker.notified` | `false` | Enables EventPublishingNotifier |
| `java:global/pre.publish.worker.backoff.min.milliseconds` | `5` | Min backoff when idle |
| `java:global/pre.publish.worker.backoff.max.milliseconds` | `100` | Max backoff cap |
| `java:global/pre.publish.worker.backoff.multiplier` | `1.5` | Backoff growth factor |
| `java:global/event.publishing.worker.backoff.*` | (same) | Same pattern for publishing |

Both notification flags default to OFF. Enabling them is purely additive — the timer remains active as a fallback.

> **Note**: Framework E/F uses `event.linking.worker.*` naming instead of `pre.publish.worker.*`, matching the `EventNumberLinker` class introduced in E.

## Bug Fixes Included

| Bug | Impact | Fix |
|-----|--------|-----|
| Dead notifier thread | `started` AtomicBoolean not reset on thread exit; notifier permanently stops | `finally { started.set(false); }` |
| submit() failure | `RejectedExecutionException` leaves `started=true`; same permanent stop | try-catch around submit + reset |
| Logger injection | SLF4J logger not injectable via CDI in notifier beans | Use `LoggerFactory.getLogger()` directly |