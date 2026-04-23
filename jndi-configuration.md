# JNDI Configuration Reference — cp-event-store

JNDI values are injected at application startup via CDI producers:

- `@GlobalValue` — resolved from `java:global/<key>`. Shared across all deployed applications in the WildFly instance.
- `@Value` — resolved first from `java:app/<service-context-name>/<key>`, falling back to `java:global/<key>`. Intended for per-service overrides.

If a key is not present in JNDI and no `defaultValue` is set, the application will fail to start with a `MissingPropertyException`.

---

## Event Store Writes

**Global** (`@GlobalValue`). Configured in `MaxRetryProvider`.

| JNDI Key | Scope | Default | Description |
|---|---|---|---|
| `internal.max.retry` | Global | `20` | Maximum optimistic-locking retry attempts when appending events to the event store. Retries handle concurrent write conflicts on the same stream. |

---

## Aggregate Snapshots

**App-specific** (`@Value`). Configured in `DefaultSnapshotStrategy`, `DefaultAggregateService`, and `AggregateSnapshotGenerationCommandConfiguration`.

| JNDI Key | Scope | Default | Description |
|---|---|---|---|
| `default.strategy.snapshot.threshold` | App | `25` | Number of events applied to an aggregate before a new snapshot is persisted. Lower values improve replay speed at the cost of more snapshots. |
| `snapshot.background.saving.threshold` | App | `50000` | Event count interval at which a background snapshot is triggered during full stream replay. |
| `jmx.aggregate.snapshot.generation.timout.seconds` | App | `72000` | JTA transaction timeout in seconds for JMX-triggered aggregate snapshot regeneration commands (default 20 hours). |

---

## Command Handler Retry

**App-specific** (`@Value`). Configured in `RetryInterceptor`.

| JNDI Key | Scope | Default | Description |
|---|---|---|---|
| `handler.retry.max` | App | `0` | Maximum optimistic-locking retries before the command handler gives up. `0` means retry indefinitely. |
| `handler.retry.immediate.retries` | App | `3` | Number of immediate retries (no sleep) before introducing a wait interval between attempts. |
| `handler.retry.wait.millis` | App | `1000` | Wait time in milliseconds between retry attempts once immediate retries are exhausted. |

---

## Event Discovery Timer

**App-specific** (`@Value`). Configured in `EventDiscoveryTimerConfig` and `EventDiscoveryConfig`.

| JNDI Key | Scope | Default | Description |
|---|---|---|---|
| `event.discovery.timer.start.wait.milliseconds` | App | `7250` | Initial delay (ms) before the event discovery timer fires for the first time after startup. |
| `event.discovery.timer.interval.milliseconds` | App | `1000` | Interval (ms) between event discovery timer ticks. |
| `event.discovery.batch.size` | App | `100` | Number of events fetched per batch during each event discovery run. |
| `event.discovery.notified` | App | `false` | When `true` (and pull mechanism is enabled), event discovery is triggered by CDI notification instead of timer polling. |

---

## Event Publishing Worker

**App-specific** (`@Value`). Configured in `EventPublishingWorkerConfig`.

| JNDI Key | Scope | Default | Description |
|---|---|---|---|
| `event.publishing.worker.start.wait.milliseconds` | App | `7250` | Initial delay (ms) before the event publishing worker timer starts after deployment. |
| `event.publishing.worker.timer.interval.milliseconds` | App | `100` | Interval (ms) between event publishing worker timer ticks. |
| `event.publishing.worker.time.between.runs.milliseconds` | App | `5` | Minimum pause (ms) between consecutive event publishing worker runs. |
| `event.publishing.worker.notified` | App | `false` | When `true`, the event publishing worker is driven by CDI event notification rather than timer polling. |

---

## Event Linking Worker

**App-specific** (`@Value`). Configured in `EventLinkingWorkerConfig` and `EventAppendTriggerService`.

| JNDI Key | Scope | Default | Description |
|---|---|---|---|
| `event.linking.worker.start.wait.milliseconds` | App | `7250` | Initial delay (ms) before the event linking worker timer starts after deployment. |
| `event.linking.worker.timer.interval.milliseconds` | App | `100` | Interval (ms) between event linking worker timer ticks. |
| `event.linking.worker.time.between.runs.milliseconds` | App | `5` | Minimum pause (ms) between consecutive event linking worker runs. |
| `event.linking.worker.batch.size` | App | `10` | Number of events processed per event-linking batch. |
| `event.linking.worker.transaction.timeout.seconds` | App | `5` | JTA transaction timeout in seconds for each event linking worker run. |
| `event.linking.worker.transaction.statement.timeout.seconds` | App | `5` | PostgreSQL statement timeout in seconds applied to event linking worker transactions. |
| `event.linking.worker.notified` | App | `false` | When `true`, a CDI event is fired after each successful transaction commit to immediately notify the linking worker, and a transaction synchronisation listener is registered by `EventAppendTriggerService`. |

---

## Stream Processing (Subscription Manager)

**App-specific** (`@Value`). Configured in `StreamProcessingConfig`.

| JNDI Key | Scope | Default | Description |
|---|---|---|---|
| `stream.processing.timer.start.wait.milliseconds` | App | `7250` | Initial delay (ms) before the stream processing timer starts after deployment. |
| `stream.processing.timer.interval.milliseconds` | App | `100` | Interval (ms) between stream processing timer ticks. |
| `stream.processing.discovery.notified` | App | `false` | When `true` (and pull mechanism is enabled), stream processing is triggered by CDI notification instead of timer polling. |
| `stream.processing.max.workers` | App | `15` | Maximum number of concurrent stream-processing worker threads. |
| `stream.processing.idle.threshold.milliseconds` | App | `1000` | Time (ms) a worker waits for work before being considered idle and releasing its stream lock. |
| `stream.processing.max.event.retries` | App | `7` | Maximum retry attempts for a failed event before the stream is marked in error and processing stops. |
| `stream.processing.retry.delay.milliseconds` | App | `1000` | Base delay (ms) between stream processing retry attempts. |
| `stream.processing.retry.delay.multiplier` | App | `1.0` | Multiplier applied to the retry delay on each successive attempt. `1.0` gives a fixed delay; values above `1.0` give exponential back-off. |
| `stream.processing.circuit.breaker.failure.threshold` | App | `50` | Percentage of failing events that trips the stream processing circuit breaker and halts processing. |
| `stream.processing.circuit.breaker.cooldown.milliseconds` | App | `30000` | Cool-down period (ms) after the circuit breaker trips before stream processing is retried. |

---

## Pull Mechanism

**App-specific** (`@Value`). Configured in `StreamProcessingConfig` and `EventDiscoveryConfig`.

| JNDI Key | Scope | Default | Description |
|---|---|---|---|
| `pull.mechanism.access.event.store.via.rest` | App | `false` | When `true`, the subscription manager and event discovery access the event store via its REST API rather than a direct database connection. Required in multi-event-store deployments where direct DB access is not available. |
