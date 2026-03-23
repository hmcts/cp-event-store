package uk.gov.justice.eventsourcing.discovery.notifier;

import uk.gov.justice.eventsourcing.discovery.timers.EventDiscoveryTimerConfig;
import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.event.buffer.core.repository.subscription.NewStreamStatusRepository;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.EventLinkedEvent;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.StreamEventsDiscoveredEvent;
import uk.gov.justice.subscription.SourceComponentPair;
import uk.gov.justice.subscription.SubscriptionSourceComponentFinder;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.ejb.Singleton;
import javax.enterprise.event.Event;
import javax.enterprise.event.ObservesAsync;
import javax.inject.Inject;

import org.slf4j.Logger;

@Singleton
public class EventDiscoveryNotifier {

    @Inject
    private NewStreamStatusRepository newStreamStatusRepository;

    @Inject
    private SubscriptionSourceComponentFinder subscriptionSourceComponentFinder;

    @Inject
    private EventDiscoveryTimerConfig eventDiscoveryTimerConfig;

    @Inject
    private Event<StreamEventsDiscoveredEvent> streamEventsDiscoveredFirer;

    @Inject
    private UtcClock clock;

    @Inject
    private Logger logger;

    public void onEventLinkedEvent(@ObservesAsync final EventLinkedEvent eventLinkedEvent) {

        if (!eventDiscoveryTimerConfig.shouldDiscoveryNotified()) {
            return;
        }

        final List<SourceComponentPair> pairs = subscriptionSourceComponentFinder.findListenerOrIndexerPairs();
        final Set<SourceComponentPair> advancedPairs = new HashSet<>();

        for (final Map.Entry<UUID, Long> entry : eventLinkedEvent.streamPositions().entrySet()) {
            for (final SourceComponentPair pair : pairs) {
                if (newStreamStatusRepository.upsertLatestKnownPositionIfIncreased(
                        entry.getKey(), pair.source(), pair.component(), entry.getValue(), clock.now())) {
                    logger.debug("Stream position advanced: stream='{}', source='{}', component='{}', position={}",
                            entry.getKey(), pair.source(), pair.component(), entry.getValue());
                    advancedPairs.add(pair);
                }
            }
        }

        for (final SourceComponentPair pair : advancedPairs) {
            streamEventsDiscoveredFirer.fireAsync(new StreamEventsDiscoveredEvent(pair.source(), pair.component()));
        }
    }
}
