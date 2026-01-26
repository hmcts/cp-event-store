package uk.gov.justice.eventsourcing.discovery.workers;

import static java.lang.String.format;

import uk.gov.justice.eventsourcing.discovery.dataaccess.EventSubscriptionStatus;
import uk.gov.justice.eventsourcing.discovery.dataaccess.EventSubscriptionStatusRepository;
import uk.gov.justice.eventsourcing.discovery.subscription.SourceComponentPair;
import uk.gov.justice.services.event.buffer.core.repository.subscription.NewStreamStatusRepository;
import uk.gov.justice.services.eventsourcing.discovery.EventSubscriptionDiscoveryBean;
import uk.gov.justice.services.eventsourcing.repository.jdbc.discovery.StreamPosition;

import java.util.Optional;
import java.util.UUID;

import javax.inject.Inject;

import org.slf4j.Logger;

public class EventDiscoverer {

    @Inject
    private EventSubscriptionStatusRepository eventSubscriptionStatusRepository;

    @Inject
    private NewStreamStatusRepository newStreamStatusRepository;

    @Inject
    private EventSubscriptionDiscoveryBean eventSubscriptionDiscoveryBean;

    @Inject
    private Logger logger;

    public void runEventDiscoveryForSourceComponentPair(final SourceComponentPair sourceComponentPair) {

        final String source = sourceComponentPair.source();
        final String component = sourceComponentPair.component();
        final Optional<EventSubscriptionStatus> eventSubscriptionStatusOptional = eventSubscriptionStatusRepository.findBy(
                source,
                component);

        if (eventSubscriptionStatusOptional.isPresent()) {
            logger.info(format("Running event discovery for source '%s' component '%s'", source, component));
            final EventSubscriptionStatus eventSubscriptionStatus = eventSubscriptionStatusOptional.get();

            final Optional<UUID> latestKnownEventId = eventSubscriptionStatus.latestEventId();

            eventSubscriptionDiscoveryBean.discoverNewEvents(latestKnownEventId)
                    .forEach(streamPosition -> runDiscoveryFor(streamPosition, source, component));
        } 
    }

    private void runDiscoveryFor(final StreamPosition streamPosition, final String source, final String component) {

        final UUID streamId = streamPosition.streamId();
        final Long latestKnownPosition = streamPosition.positionInStream();

        logger.info(format("Updating latest known position to '%d' for stream id '%s', source '%s' component '%s'", latestKnownPosition, streamId, source, component));

        newStreamStatusRepository.updateLatestKnownPosition(
                streamId,
                source,
                component,
                latestKnownPosition
        );
    }
}
