package uk.gov.justice.services.core.aggregate;

import uk.gov.justice.domain.aggregate.Aggregate;
import uk.gov.justice.domain.snapshot.VersionedAggregate;
import uk.gov.justice.services.core.aggregate.exception.AggregateChangeDetectedException;
import uk.gov.justice.services.eventsourcing.source.core.EventStream;
import uk.gov.justice.services.eventsourcing.source.core.SnapshotAwareEnvelopeEventStream;
import uk.gov.justice.services.eventsourcing.source.core.snapshot.SnapshotService;
import uk.gov.justice.services.eventsourcing.source.core.snapshot.async.AsyncSnapshotService;

import java.util.Optional;

import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import jakarta.inject.Inject;

import org.slf4j.Logger;

/**
 * The type Snapshot aware aggregate service.
 */
@ApplicationScoped
@Alternative
@Priority(100)
public class SnapshotAwareAggregateService implements AggregateService {

    @Inject
    Logger logger;

    @Inject
    SnapshotService snapshotService;

    @Inject
    DefaultAggregateService defaultAggregateService;

    @Inject
    private AsyncSnapshotService asyncSnapshotService;

    @Override
    @SuppressWarnings("unchecked")
    public <T extends Aggregate> T get(final EventStream stream, final Class<T> clazz) {
        logger.trace("SnapshotAwareAggregateService Recreating aggregate for instance {} of aggregate type {}", stream.getId(), clazz);

        final Optional<VersionedAggregate<T>> versionedAggregate = latestOrChangedAggregateAndEvents(stream, clazz);

        final T aggregate = aggregateOf(stream, clazz, versionedAggregate);

        if (stream instanceof SnapshotAwareEnvelopeEventStream) {
            ((SnapshotAwareEnvelopeEventStream) stream).registerAggregates(clazz, aggregate);
        }

        return aggregate;

    }

    private <T extends Aggregate> T aggregateOf(final EventStream stream, Class<T> clazz, final Optional<VersionedAggregate<T>> versionedAggregate) {
        if (versionedAggregate.isPresent()) {
            final VersionedAggregate<T> versionedAggregateValue = versionedAggregate.get();
            return defaultAggregateService.applyEvents(
                    stream.readFrom(versionAfter(versionedAggregateValue)),
                    versionedAggregateValue.getAggregate());
        }

        return defaultAggregateService.get(stream, clazz);
    }

    private <T extends Aggregate> long versionAfter(final VersionedAggregate<T> versionedAggregate) {
        return versionedAggregate.getVersionId() + 1L;
    }

    private <T extends Aggregate> Optional<VersionedAggregate<T>> latestOrChangedAggregateAndEvents(final EventStream stream, final Class<T> clazz) {
        try {
            return snapshotService.getLatestVersionedAggregate(stream.getId(), clazz);
        } catch (AggregateChangeDetectedException e) {
            asyncSnapshotService.removeAggregateSnapshot(stream.getId(), clazz, e.getPositionInStream(), e.getCreatedAt());
            return Optional.empty();
        }
    }
}
