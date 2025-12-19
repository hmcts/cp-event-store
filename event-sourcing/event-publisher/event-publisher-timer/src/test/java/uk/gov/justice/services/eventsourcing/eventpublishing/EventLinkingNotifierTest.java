package uk.gov.justice.services.eventsourcing.eventpublishing;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.services.eventsourcing.eventpublishing.configuration.EventLinkingWorkerConfig;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.EventAppendedEvent;

import javax.enterprise.concurrent.ManagedExecutorService;

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

    @InjectMocks
    private EventLinkingNotifier eventLinkingNotifier;

    @Test
    void shouldWakeUpOnEventAppended() {
        EventAppendedEvent event = mock(EventAppendedEvent.class);
        
        eventLinkingNotifier.onEventAppendedEvent(event);

        // wakeUp(false) shouldn't start the worker if it's not running
        verify(managedExecutorService, never()).submit(any(Runnable.class));
    }

    @Test
    void shouldNotSubmitIfAlreadyStarted() {
        // First wakeUp starts it
        eventLinkingNotifier.wakeUp(true);
        verify(managedExecutorService).submit(any(Runnable.class));

        // Second wakeUp should not submit again
        eventLinkingNotifier.wakeUp(true);
        verify(managedExecutorService, times(1)).submit(any(Runnable.class));
    }

    @Test
    void shouldProcessEventsWhenRunning() {
        // We will execute the runnable directly to test the loop logic
        // But we need to ensure it breaks the loop.
        // The loop breaks if Thread.currentThread().isInterrupted()
        // We can mock eventNumberLinker to interrupt the thread after some calls.
        
        when(eventLinkingWorkerConfig.getBackoffMinMilliseconds()).thenReturn(10L);
        when(eventLinkingWorkerConfig.getBackoffMultiplier()).thenReturn(1.5);
        when(eventLinkingWorkerConfig.getBackoffMaxMilliseconds()).thenReturn(100L);

        when(eventNumberLinker.findAndAndLinkNextUnlinkedEvent())
                .thenReturn(true)
                .thenReturn(false)
                .thenAnswer(invocation -> {
                    Thread.currentThread().interrupt();
                    return false;
                });

        // We need to capture the runnable.
        // However, we can't easily capture it from InjectMocks if we call wakeUp.
        // So we will just call wakeUp and capture the argument passed to managedExecutorService.submit
        
        eventLinkingNotifier.wakeUp(true);
        
        var captor = org.mockito.ArgumentCaptor.forClass(Runnable.class);
        verify(managedExecutorService).submit(captor.capture());
        Runnable runnable = captor.getValue();
        
        runnable.run();
        
        verify(eventNumberLinker, times(3)).findAndAndLinkNextUnlinkedEvent();
        verify(eventPublishingNotifier, times(1)).wakeUp(false);
    }
}
