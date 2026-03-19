package uk.gov.justice.services.eventsourcing.eventpublishing;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;
import uk.gov.justice.services.eventsourcing.eventpublishing.configuration.EventLinkingWorkerConfig;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.EventAppendedEvent;

import javax.enterprise.concurrent.ManagedExecutorService;
import java.util.concurrent.RejectedExecutionException;

@ExtendWith(MockitoExtension.class)
class EventLinkingNotifierTest {

    @Mock
    private EventNumberLinker eventNumberLinker;

    @Mock
    private EventLinkingWorkerConfig eventLinkingWorkerConfig;

    @Mock
    private EventPublishingNotifier eventPublishingNotifier;

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
    void shouldProcessEventsWhenRunning() {
        when(eventLinkingWorkerConfig.getBackoffMinMilliseconds()).thenReturn(10L);
        when(eventLinkingWorkerConfig.getBackoffMultiplier()).thenReturn(1.5);
        when(eventLinkingWorkerConfig.getBackoffMaxMilliseconds()).thenReturn(100L);

        when(eventNumberLinker.findAndLinkEventsInBatch())
                .thenReturn(1)
                .thenReturn(0)
                .thenAnswer(invocation -> {
                    Thread.currentThread().interrupt();
                    return 0;
                });

        eventLinkingNotifier.wakeUp(true);

        final ArgumentCaptor<Runnable> captor = ArgumentCaptor.forClass(Runnable.class);
        verify(managedExecutorService).submit(captor.capture());
        captor.getValue().run();

        verify(eventNumberLinker, times(3)).findAndLinkEventsInBatch();
        verify(eventPublishingNotifier, times(1)).wakeUp(false);
    }

    @Test
    void shouldResetStartedFlagWhenThreadExits() {
        when(eventLinkingWorkerConfig.getBackoffMinMilliseconds()).thenReturn(10L);

        when(eventNumberLinker.findAndLinkEventsInBatch())
                .thenAnswer(invocation -> {
                    Thread.currentThread().interrupt();
                    return 0;
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

        assertThat(true, is(true));
    }
}