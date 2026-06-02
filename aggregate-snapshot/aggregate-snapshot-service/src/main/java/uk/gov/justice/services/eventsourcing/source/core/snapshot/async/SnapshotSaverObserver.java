package uk.gov.justice.services.eventsourcing.source.core.snapshot.async;

import uk.gov.justice.services.eventsourcing.source.core.snapshot.DefaultSnapshotService;
import uk.gov.justice.services.eventsourcing.source.core.snapshot.async.event.SnapshotSaverRequestEvent;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.ObservesAsync;
import jakarta.inject.Inject;

import org.slf4j.Logger;

@ApplicationScoped
public class SnapshotSaverObserver {
    @Inject
    private DefaultSnapshotService snapshotService;
    @Inject
    private Logger logger;

    public boolean onSaveReceived(@ObservesAsync final SnapshotSaverRequestEvent aggregateSnapshotSaverRequestEvent) {
        logger.trace("About to save snapshot {}", aggregateSnapshotSaverRequestEvent);
        try {
            final boolean storedOK = snapshotService.storeAggregateSimply(aggregateSnapshotSaverRequestEvent.getStreamId(), aggregateSnapshotSaverRequestEvent.getPositionInStream(), aggregateSnapshotSaverRequestEvent.getAggregate());
            logger.info("Successfully saved in background snapshot {}", aggregateSnapshotSaverRequestEvent);
            return storedOK;
        } catch (Exception e) {
            logger.error("Failed to save snapshot %s".formatted(aggregateSnapshotSaverRequestEvent), e);
        }

        return false;
    }
}
