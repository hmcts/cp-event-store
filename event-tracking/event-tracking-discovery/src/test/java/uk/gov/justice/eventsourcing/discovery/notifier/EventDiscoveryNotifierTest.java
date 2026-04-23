package uk.gov.justice.eventsourcing.discovery.notifier;

import static java.util.Collections.emptyList;
import static java.util.UUID.randomUUID;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import uk.gov.justice.eventsourcing.discovery.timers.EventDiscoveryTimerConfig;
import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.event.buffer.core.repository.subscription.NewStreamStatusRepository;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.EventsLinkedEvent;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.StreamPosition;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.StreamStatusAdvancedEvent;
import uk.gov.justice.subscription.SourceComponentPair;
import uk.gov.justice.subscription.SubscriptionSourceComponentFinder;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.UUID;

import jakarta.enterprise.event.Event;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;

@ExtendWith(MockitoExtension.class)
public class EventDiscoveryNotifierTest {

    @Mock
    private NewStreamStatusRepository newStreamStatusRepository;

    @Mock
    private SubscriptionSourceComponentFinder subscriptionSourceComponentFinder;

    @Mock
    private EventDiscoveryTimerConfig eventDiscoveryTimerConfig;

    @Mock
    private Event<StreamStatusAdvancedEvent> streamStatusAdvancedFirer;

    @Mock
    private UtcClock clock;

    @Mock
    private Logger logger;

    @InjectMocks
    private EventDiscoveryNotifier eventDiscoveryNotifier;

    @Test
    public void shouldDoNothingIfDiscoveryNotifiedIsDisabled() {

        final EventsLinkedEvent event = new EventsLinkedEvent(List.of(new StreamPosition(randomUUID(), 5L)));

        when(eventDiscoveryTimerConfig.shouldDiscoveryNotified()).thenReturn(false);

        eventDiscoveryNotifier.onEventsLinked(event);

        verify(subscriptionSourceComponentFinder, never()).findListenerOrIndexerPairs();
        verify(streamStatusAdvancedFirer, never()).fireAsync(any());
    }

    @Test
    public void shouldUpsertStreamStatusAndFireEventWhenPositionAdvances() {

        final UUID streamId = randomUUID();
        final long position = 7L;
        final ZonedDateTime now = new UtcClock().now();

        final SourceComponentPair pair = mock(SourceComponentPair.class);
        final EventsLinkedEvent event = new EventsLinkedEvent(List.of(new StreamPosition(streamId, position)));

        when(pair.source()).thenReturn("MY_SOURCE");
        when(pair.component()).thenReturn("EVENT_LISTENER");
        when(eventDiscoveryTimerConfig.shouldDiscoveryNotified()).thenReturn(true);
        when(subscriptionSourceComponentFinder.findListenerOrIndexerPairs()).thenReturn(List.of(pair));
        when(clock.now()).thenReturn(now);
        when(newStreamStatusRepository.upsertLatestKnownPositionIfIncreased(
                streamId, "MY_SOURCE", "EVENT_LISTENER", position, now))
                .thenReturn(true);

        eventDiscoveryNotifier.onEventsLinked(event);

        verify(streamStatusAdvancedFirer).fireAsync(any(StreamStatusAdvancedEvent.class));
    }

    @Test
    public void shouldNotFireEventWhenPositionDoesNotAdvance() {

        final UUID streamId = randomUUID();
        final long position = 3L;
        final ZonedDateTime now = new UtcClock().now();

        final SourceComponentPair pair = mock(SourceComponentPair.class);
        final EventsLinkedEvent event = new EventsLinkedEvent(List.of(new StreamPosition(streamId, position)));

        when(pair.source()).thenReturn("MY_SOURCE");
        when(pair.component()).thenReturn("EVENT_LISTENER");
        when(eventDiscoveryTimerConfig.shouldDiscoveryNotified()).thenReturn(true);
        when(subscriptionSourceComponentFinder.findListenerOrIndexerPairs()).thenReturn(List.of(pair));
        when(clock.now()).thenReturn(now);
        when(newStreamStatusRepository.upsertLatestKnownPositionIfIncreased(
                streamId, "MY_SOURCE", "EVENT_LISTENER", position, now))
                .thenReturn(false);

        eventDiscoveryNotifier.onEventsLinked(event);

        verify(streamStatusAdvancedFirer, never()).fireAsync(any());
    }

    @Test
    public void shouldProcessMultipleSourceComponentPairs() {

        final UUID streamId = randomUUID();
        final long position = 10L;
        final ZonedDateTime now = new UtcClock().now();

        final SourceComponentPair pair1 = mock(SourceComponentPair.class);
        final SourceComponentPair pair2 = mock(SourceComponentPair.class);
        final EventsLinkedEvent event = new EventsLinkedEvent(List.of(new StreamPosition(streamId, position)));

        when(pair1.source()).thenReturn("SOURCE_1");
        when(pair1.component()).thenReturn("EVENT_LISTENER");
        when(pair2.source()).thenReturn("SOURCE_2");
        when(pair2.component()).thenReturn("EVENT_INDEXER");
        when(eventDiscoveryTimerConfig.shouldDiscoveryNotified()).thenReturn(true);
        when(subscriptionSourceComponentFinder.findListenerOrIndexerPairs()).thenReturn(List.of(pair1, pair2));
        when(clock.now()).thenReturn(now);
        when(newStreamStatusRepository.upsertLatestKnownPositionIfIncreased(
                streamId, "SOURCE_1", "EVENT_LISTENER", position, now))
                .thenReturn(true);
        when(newStreamStatusRepository.upsertLatestKnownPositionIfIncreased(
                streamId, "SOURCE_2", "EVENT_INDEXER", position, now))
                .thenReturn(false);

        eventDiscoveryNotifier.onEventsLinked(event);

        verify(streamStatusAdvancedFirer, times(1)).fireAsync(any(StreamStatusAdvancedEvent.class));
    }

    @Test
    public void shouldHandleEmptySourceComponentPairList() {

        final EventsLinkedEvent event = new EventsLinkedEvent(List.of(new StreamPosition(randomUUID(), 1L)));

        when(eventDiscoveryTimerConfig.shouldDiscoveryNotified()).thenReturn(true);
        when(subscriptionSourceComponentFinder.findListenerOrIndexerPairs()).thenReturn(emptyList());

        eventDiscoveryNotifier.onEventsLinked(event);

        verify(streamStatusAdvancedFirer, never()).fireAsync(any());
    }
}