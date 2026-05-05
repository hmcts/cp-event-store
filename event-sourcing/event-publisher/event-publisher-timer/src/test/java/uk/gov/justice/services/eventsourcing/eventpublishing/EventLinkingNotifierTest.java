package uk.gov.justice.services.eventsourcing.eventpublishing;

import static java.util.Collections.emptyList;
import static java.util.UUID.randomUUID;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import uk.gov.justice.services.eventsourcing.publishedevent.jdbc.EventDetailsToLink;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.EventAppendedEvent;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.EventsLinkedEvent;

import java.util.List;
import java.util.concurrent.RejectedExecutionException;

import jakarta.enterprise.concurrent.ManagedExecutorService;
import jakarta.enterprise.event.Event;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;

@ExtendWith(MockitoExtension.class)
class EventLinkingNotifierTest {

    @Mock
    private EventNumberLinker eventNumberLinker;

    @Mock
    private EventPublishingNotifier eventPublishingNotifier;

    @Mock
    private Event<EventsLinkedEvent> eventLinkedEventFirer;

    @Mock
    private ManagedExecutorService managedExecutorService;

    @Mock
    private Logger logger;

    @InjectMocks
    private EventLinkingNotifier eventLinkingNotifier;

    @Test
    void shouldWakeUpOnEventAppended() {
        final EventAppendedEvent event = mock(EventAppendedEvent.class);

        eventLinkingNotifier.onEventAppendedEvent(event);

        verify(managedExecutorService, never()).submit(any(Runnable.class));
    }

    @Test
    void shouldNotSubmitIfAlreadyStarted() {
        eventLinkingNotifier.wakeUp(true);
        verify(managedExecutorService).submit(any(Runnable.class));

        eventLinkingNotifier.wakeUp(true);
        verify(managedExecutorService, times(1)).submit(any(Runnable.class));
    }

    @Test
    void shouldProcessBatchAndFirePhase2Events() {
        final EventDetailsToLink event1 = new EventDetailsToLink(randomUUID(), randomUUID(), 1);
        final EventDetailsToLink event2 = new EventDetailsToLink(randomUUID(), randomUUID(), 1);

        when(eventNumberLinker.findAndLinkEventsInBatch())
                .thenReturn(List.of(event1, event2))
                .thenAnswer(invocation -> {
                    Thread.currentThread().interrupt();
                    return emptyList();
                });

        eventLinkingNotifier.wakeUp(true);

        final ArgumentCaptor<Runnable> captor = ArgumentCaptor.forClass(Runnable.class);
        verify(managedExecutorService).submit(captor.capture());
        captor.getValue().run();

        verify(eventNumberLinker, times(2)).findAndLinkEventsInBatch();
        verify(eventPublishingNotifier, times(1)).wakeUp(false);
        verify(eventLinkedEventFirer, times(1)).fireAsync(any(EventsLinkedEvent.class));
    }

    @Test
    void shouldResetStartedFlagWhenThreadExits() {
        when(eventNumberLinker.findAndLinkEventsInBatch())
                .thenAnswer(invocation -> {
                    Thread.currentThread().interrupt();
                    return emptyList();
                });

        eventLinkingNotifier.wakeUp(true);

        final ArgumentCaptor<Runnable> captor = ArgumentCaptor.forClass(Runnable.class);
        verify(managedExecutorService).submit(captor.capture());
        captor.getValue().run();

        eventLinkingNotifier.wakeUp(true);
        verify(managedExecutorService, times(2)).submit(any(Runnable.class));
    }

    @Test
    void shouldResetStartedFlagWhenSubmitFails() {
        doThrow(new RejectedExecutionException("Pool full"))
                .when(managedExecutorService).submit(any(Runnable.class));

        eventLinkingNotifier.wakeUp(true);

        verify(managedExecutorService).submit(any(Runnable.class));
        eventLinkingNotifier.wakeUp(true);
        verify(managedExecutorService, times(2)).submit(any(Runnable.class));
    }
}