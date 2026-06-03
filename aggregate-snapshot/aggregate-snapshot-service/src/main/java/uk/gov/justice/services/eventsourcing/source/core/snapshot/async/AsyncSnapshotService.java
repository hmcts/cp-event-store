package uk.gov.justice.services.eventsourcing.source.core.snapshot.async;

import uk.gov.justice.domain.aggregate.Aggregate;
import uk.gov.justice.services.eventsourcing.source.core.snapshot.async.event.SnapshotDeleterRequestEvent;
import uk.gov.justice.services.eventsourcing.source.core.snapshot.async.event.SnapshotSaverRequestEvent;

import java.time.ZonedDateTime;
import java.util.UUID;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Event;
import jakarta.inject.Inject;

import org.apache.commons.lang3.SerializationUtils;

@ApplicationScoped
public class AsyncSnapshotService {

    @Inject
    private Event<SnapshotSaverRequestEvent> aggregateSnapshotSaverRequestEventFirer;

    @Inject
    private Event<SnapshotDeleterRequestEvent> aggregateSnapshotDeletionRequestFirer;

    public <T extends Aggregate> void saveAggregateSnapshot(final UUID uuid, final Long positionInStream, final T aggregate) {
        final T clonedAggregate = SerializationUtils.clone(aggregate);// deep copy
        final SnapshotSaverRequestEvent snapshotDeletionRunner = new SnapshotSaverRequestEvent(uuid, positionInStream, clonedAggregate);
        aggregateSnapshotSaverRequestEventFirer.fireAsync(snapshotDeletionRunner);
    }

    public <T extends Aggregate> void removeAggregateSnapshot(final UUID uuid, final Class<? extends Aggregate> aggregateClass, final long positionInStream, final ZonedDateTime createdAt) {
        final SnapshotDeleterRequestEvent snapshotDeletionRunner = new SnapshotDeleterRequestEvent(uuid, aggregateClass, positionInStream, createdAt);
        aggregateSnapshotDeletionRequestFirer.fireAsync(snapshotDeletionRunner);
    }
}
