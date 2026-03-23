package uk.gov.justice.eventsourcing.discovery.notifier;

import uk.gov.justice.eventsourcing.discovery.timers.EventDiscoveryTimerConfig;
import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.event.buffer.core.repository.subscription.NewStreamStatusRepository;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.EventsLinkedEvent;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.StreamPosition;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.StreamStatusAdvancedEvent;
import uk.gov.justice.subscription.SourceComponentPair;
import uk.gov.justice.subscription.SubscriptionSourceComponentFinder;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
    private Event<StreamStatusAdvancedEvent> streamStatusAdvancedFirer;

    @Inject
    private UtcClock clock;

    @Inject
    private Logger logger;

    public void onEventLinkedEvent(@ObservesAsync final EventsLinkedEvent eventLinkedEvent) {

        if (!eventDiscoveryTimerConfig.shouldDiscoveryNotified()) {
            return;
        }

        final List<SourceComponentPair> pairs = subscriptionSourceComponentFinder.findListenerOrIndexerPairs();
        final Set<SourceComponentPair> advancedPairs = new HashSet<>();

        for (final StreamPosition streamPosition : eventLinkedEvent.streamPositions()) {
            for (final SourceComponentPair pair : pairs) {
                if (newStreamStatusRepository.upsertLatestKnownPositionIfIncreased(
                        streamPosition.streamId(), pair.source(), pair.component(), streamPosition.positionInStream(), clock.now())) {
                    logger.debug("Stream position advanced: stream='{}', source='{}', component='{}', position={}",
                            streamPosition.streamId(), pair.source(), pair.component(), streamPosition.positionInStream());
                    advancedPairs.add(pair);
                }
            }
        }

        for (final SourceComponentPair pair : advancedPairs) {
            streamStatusAdvancedFirer.fireAsync(new StreamStatusAdvancedEvent(pair.source(), pair.component()));
        }
    }
}
