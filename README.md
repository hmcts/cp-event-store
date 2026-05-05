# cp-event-store

`uk.gov.justice.event-store:event-store`

The event sourcing persistence layer for the CPP microservice framework. It implements the event store that aggregates write to, manages event stream replay, aggregate snapshots, and the subscription machinery that drives downstream event processing.

## Position in the hierarchy

```
maven-framework-parent-pom
└── cp-event-store  ← this project
    └── event-store-bom  (imported by cp-cake-shop and all cpp-context-* services)
```

This project imports `framework-bom` from `cp-microservice-framework`.

## Modules

| Module | Artifact | Description |
|---|---|---|
| `event-sourcing` | `event-sourcing` | Core event store: `EventAppender` (optimistic-locking append with configurable retries), `EventStream` (replay), `EventRepository` (JDBC) |
| `aggregate-snapshot` | `aggregate-snapshot` | Snapshot persistence and retrieval; `DefaultSnapshotStrategy` determines when to snapshot (configurable event threshold); JMX-triggered bulk regeneration |
| `event-buffer` | `event-buffer` | Buffers incoming events to ensure ordered, gap-free delivery to stream processors |
| `event-tracking` | `event-tracking` | Tracks processed event positions; drives the event discovery timer that detects new events |
| `event-store-management` | `event-store-management` | JMX MBeans for operational management: replay streams, regenerate snapshots, inspect stream state |
| `interceptors` | `interceptors` | CDI interceptors: `RetryInterceptor` (optimistic-locking retry on command handlers), transaction management |
| `healthchecks` | `healthchecks` | WildFly MicroProfile health probes for event store connectivity and worker liveness |
| `event-store-metric-meters` | `event-store-metric-meters` | Micrometer metrics: event append rates, stream processing throughput, worker queue depths |
| `framework-rest-resources` | `framework-rest-resources` | REST API exposing event store data — used by the pull mechanism when direct DB access is not available |
| `test-utils-event-store` | `test-utils-event-store` | Test utilities: in-memory event store, stream builders, snapshot test helpers |
| `event-store-util` | `event-store-util` | Core interfaces and value types: `EventStreamId`, `PositionValue`, `PublishedEvent` |
| `event-store-bom` | `event-store-bom` | BOM that imports all `event-store` artifacts at a consistent version |

## Key concepts

### Appending events

```java
@Inject
private EventAppender eventAppender;

eventAppender.append(events, streamId, commandId, commandName);
```

Append uses optimistic locking on the stream position. Concurrent writes on the same stream are retried automatically (configurable via `internal.max.retry` JNDI key, default 20).

### Replaying a stream

```java
@Inject
private EventStream eventStream;

final Stream<JsonEnvelope> events = eventStream.read(streamId);
// apply events to rebuild aggregate state
```

### Aggregate snapshots

Snapshots are taken automatically by `DefaultSnapshotStrategy` every N events (default 25, configurable via `default.strategy.snapshot.threshold`). The snapshot stores the serialised aggregate state so stream replay only needs to load events since the last snapshot.

### Subscription manager (stream processing)

The subscription manager reads from the event stream and delivers events to registered `@EventListener` / `@EventProcessor` handlers. It uses a pool of worker threads (default 15, `stream.processing.max.workers`) with configurable retry, circuit breaker, and back-off.

### Pull mechanism

When `pull.mechanism.access.event.store.via.rest=true`, the subscription manager and event discovery access the event store via the REST API in `framework-rest-resources` rather than a direct JDBC connection. Required in multi-event-store deployments.

## JNDI configuration

All tunable parameters — worker thread counts, batch sizes, timer intervals, retry behaviour, circuit breaker thresholds — are JNDI-configurable. See [jndi-configuration.md](./jndi-configuration.md) for the complete reference.

## Build

```bash
# Build and install all modules
mvn clean install

# Skip integration tests
mvn clean install -Drun.it=false
```

Integration tests require a running PostgreSQL instance (via `cpp-developers-docker`).
