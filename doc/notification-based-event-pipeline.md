# Notification-Based Event Pipeline — Framework E (17.104.x)

## Problem

Event linking and publishing use timer-based polling (default 100ms interval). This introduces up to 200ms of pipeline latency per event (100ms linking + 100ms publishing). Reducing the timer interval further increases CPU and database load during idle periods.

## Solution

CDI event notifications trigger the linking and publishing workers immediately when new events arrive, eliminating timer-interval latency without the CPU cost of aggressive polling.

### How It Works

Two new notifier beans observe CDI events fired after an event is committed to `event_log`:

| Notifier | Fires on | Wakes | Effect |
|----------|----------|-------|--------|
| `EventLinkingNotifier` | New event committed | `EventLinkingTimerBean` | Links events immediately via `EventNumberLinker.findAndLinkEventsInBatch()` |
| `EventPublishingNotifier` | Events linked | `EventPublishingTimerBean` | Publishes events immediately (publish_queue drain) |

Each notifier runs a single background thread that calls the same processing method as the timer. A `started` AtomicBoolean prevents concurrent executions. The existing timer continues as a safety net (unchanged interval).

### CDI Event Flow

```
EventStreamManager.append()
  → eventAppendTriggerService.registerTransactionListener()
  → [TX commit]
  → EventAppendSynchronization.afterCompletion(STATUS_COMMITTED)
  → CDI fire(new EventAppendedEvent())
  → EventLinkingNotifier.onEventAppendedEvent(@Observes)
  → wakeUp(false) → monitor.notifyAll()
  → [background thread wakes]
  → eventNumberLinker.findAndLinkEventsInBatch()  ← advisory lock 42, batch mode
  → [on success] eventPublishingNotifier.wakeUp(false)
  → linkedEventPublisher.publishNextNewEvent()
```

### JNDI Configuration

| Property | Default | Effect |
|----------|---------|--------|
| `java:global/event.linking.worker.notified` | `false` | Enables EventLinkingNotifier |
| `java:global/event.publishing.worker.notified` | `false` | Enables EventPublishingNotifier |
| `java:global/event.linking.worker.backoff.min.milliseconds` | `5` | Min backoff when idle (linking) |
| `java:global/event.linking.worker.backoff.max.milliseconds` | `100` | Max backoff cap (linking) |
| `java:global/event.linking.worker.backoff.multiplier` | `1.5` | Backoff growth factor (linking) |
| `java:global/event.publishing.worker.backoff.min.milliseconds` | `5` | Min backoff when idle (publishing) |
| `java:global/event.publishing.worker.backoff.max.milliseconds` | `100` | Max backoff cap (publishing) |
| `java:global/event.publishing.worker.backoff.multiplier` | `1.5` | Backoff growth factor (publishing) |

Both notification flags default to OFF. Enabling them is purely additive — the timer remains active as a fallback.

## Difference from Framework D

| Aspect | D (17.103.x) | E (17.104.x) |
|--------|-------------|-------------|
| Linking class | `PrePublishNotifier` | `EventLinkingNotifier` |
| JNDI prefix | `pre.publish.worker.*` | `event.linking.worker.*` |
| Linking method | `PrePublishProcessor` (single event, DB sequence) | `EventNumberLinker` (batch, advisory lock 42) |

## Bug Fixes Included

| Bug | Impact | Fix |
|-----|--------|-----|
| Dead notifier thread | `started` AtomicBoolean not reset on thread exit; notifier permanently stops | `finally { started.set(false); }` |
| submit() failure | `RejectedExecutionException` leaves `started=true`; same permanent stop | try-catch around submit + reset |
| Logger injection | SLF4J logger via LoggerFactory; reviewer requested CDI injection | Changed to `@Inject Logger` |
