package uk.gov.justice.eventsourcing.discovery.workers;

import uk.gov.justice.eventsourcing.discovery.dataaccess.EventSubscriptionStatus;
import uk.gov.justice.eventsourcing.discovery.dataaccess.EventSubscriptionStatusRepository;
import uk.gov.justice.eventsourcing.discovery.subscription.SourceComponentPair;
import uk.gov.justice.services.event.buffer.core.repository.subscription.NewStreamStatusRepository;
import uk.gov.justice.services.eventsourcing.discovery.EventSubscriptionDiscoveryBean;

import java.util.Optional;
import java.util.UUID;

import javax.inject.Inject;

public class EventDiscoverer {

    @Inject
    private EventSubscriptionStatusRepository eventSubscriptionStatusRepository;

    @Inject
    private NewStreamStatusRepository newStreamStatusRepository;

    @Inject
    private EventSubscriptionDiscoveryBean eventSubscriptionDiscoveryBean;

    public void runEventDiscoveryForSourceComponentPair(final SourceComponentPair sourceComponentPair) {

        final Optional<EventSubscriptionStatus> eventSubscriptionStatusOptional = eventSubscriptionStatusRepository.findBy(
                sourceComponentPair.source(),
                sourceComponentPair.component());

        if (eventSubscriptionStatusOptional.isPresent()) {
            final EventSubscriptionStatus eventSubscriptionStatus = eventSubscriptionStatusOptional.get();

            final UUID latestKnownEventId = eventSubscriptionStatus.latestEventId();

            eventSubscriptionDiscoveryBean.discoverNewEvents(latestKnownEventId)
                    .forEach(streamPosition -> newStreamStatusRepository.updateLatestKnownPosition(
                            streamPosition.streamId(),
                            sourceComponentPair.source(),
                            sourceComponentPair.component(),
                            streamPosition.positionInStream()
                    ));
        }
    }
}
