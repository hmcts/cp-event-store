package uk.gov.justice.services.eventsourcing.source.core;

import uk.gov.justice.services.eventsourcing.repository.jdbc.EventRepository;
import uk.gov.justice.services.eventsourcing.source.core.snapshot.SnapshotService;

import jakarta.inject.Inject;

public class SnapshotAwareEventSourceFactory {

    @Inject
    private EventStreamManager eventStreamManager;

    @Inject
    private EventRepository eventRepository;

    @Inject
    private SnapshotService snapshotService;

    public EventSource create(final String eventSourceName) {

        return new SnapshotAwareEventSource(
                eventStreamManager,
                eventRepository,
                snapshotService,
                eventSourceName);
    }
}
