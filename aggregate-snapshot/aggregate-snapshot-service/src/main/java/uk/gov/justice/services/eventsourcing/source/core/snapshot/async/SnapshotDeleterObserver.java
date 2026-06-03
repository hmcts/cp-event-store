package uk.gov.justice.services.eventsourcing.source.core.snapshot.async;

import uk.gov.justice.services.eventsourcing.source.core.snapshot.DefaultSnapshotService;
import uk.gov.justice.services.eventsourcing.source.core.snapshot.async.event.SnapshotDeleterRequestEvent;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.ObservesAsync;
import jakarta.inject.Inject;

import org.slf4j.Logger;

@ApplicationScoped
public class SnapshotDeleterObserver {
    @Inject
    private DefaultSnapshotService snapshotService;
    @Inject
    private Logger logger;

    public int onDeleteReceived(@ObservesAsync final SnapshotDeleterRequestEvent aggregateSnapshotDeletionRequestEvent) {
        logger.trace("About to delete snapshot {}", aggregateSnapshotDeletionRequestEvent);
        try {
            final int deleteCount = snapshotService.removeSnapshot(aggregateSnapshotDeletionRequestEvent.getStreamId(), aggregateSnapshotDeletionRequestEvent.getAggregateClass(), aggregateSnapshotDeletionRequestEvent.getPositionInStream(), aggregateSnapshotDeletionRequestEvent.getCreatedAt());
            logger.info("Successfully deleted in background snapshot {}", aggregateSnapshotDeletionRequestEvent);
            return deleteCount;
        } catch (Exception e) {
            logger.error("Failed to delete snapshot %s".formatted(aggregateSnapshotDeletionRequestEvent), e);
        }
        return 0;
    }
}
