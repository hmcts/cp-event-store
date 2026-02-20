package uk.gov.justice.eventsourcing.discovery.workers;

import static java.lang.String.format;
import static javax.transaction.Transactional.TxType.REQUIRED;

import uk.gov.justice.eventsourcing.discovery.dataaccess.EventSubscriptionStatus;
import uk.gov.justice.eventsourcing.discovery.dataaccess.EventSubscriptionStatusRepository;
import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.event.buffer.core.repository.subscription.NewStreamStatusRepository;
import uk.gov.justice.services.eventsourcing.discovery.DiscoveryResult;
import uk.gov.justice.services.eventsourcing.discovery.EventSubscriptionDiscoverer;
import uk.gov.justice.services.eventsourcing.repository.jdbc.discovery.StreamPosition;
import uk.gov.justice.subscription.SourceComponentPair;

import java.util.Optional;
import java.util.UUID;

import javax.inject.Inject;
import javax.transaction.Transactional;

import org.slf4j.Logger;

public class EventDiscoveryWorker {

    @Inject
    private EventSubscriptionStatusRepository eventSubscriptionStatusRepository;

    @Inject
    private NewStreamStatusRepository newStreamStatusRepository;

    @Inject
    private EventSubscriptionDiscoverer eventSubscriptionDiscoverer;

    @Inject
    private Logger logger;

    @Inject
    private UtcClock clock;

    @Transactional(REQUIRED)
    public void runEventDiscoveryForSourceComponentPair(final SourceComponentPair sourceComponentPair) {

        final String source = sourceComponentPair.source();
        final String component = sourceComponentPair.component();
        final Optional<EventSubscriptionStatus> eventSubscriptionStatusOptional = eventSubscriptionStatusRepository.findBy(
                source,
                component);

        if (eventSubscriptionStatusOptional.isPresent()) {
            logger.debug(format("Running event discovery for source '%s' component '%s'", source, component));
            final EventSubscriptionStatus eventSubscriptionStatus = eventSubscriptionStatusOptional.get();

            final Optional<UUID> latestKnownEventId = eventSubscriptionStatus.latestEventId();

            final DiscoveryResult discoveryResult = eventSubscriptionDiscoverer.discoverNewEvents(latestKnownEventId);

            discoveryResult.streamPositions()
                    .forEach(streamPosition -> runDiscoveryFor(streamPosition, source, component));

            if (discoveryResult.latestKnownEventId().isPresent()) {
                eventSubscriptionStatusRepository.save(new EventSubscriptionStatus(
                        source,
                        component,
                        discoveryResult.latestKnownEventId(),
                        clock.now()
                ));
            }
        }
    }

    private void runDiscoveryFor(final StreamPosition streamPosition, final String source, final String component) {

        final UUID streamId = streamPosition.streamId();
        final Long latestKnownPosition = streamPosition.positionInStream();

        logger.debug(format("Updating latest known position to '%d' for stream id '%s', source '%s' component '%s'", latestKnownPosition, streamId, source, component));

        newStreamStatusRepository.upsertLatestKnownPosition(
                streamId,
                source,
                component,
                latestKnownPosition,
                clock.now()
        );
    }
}
