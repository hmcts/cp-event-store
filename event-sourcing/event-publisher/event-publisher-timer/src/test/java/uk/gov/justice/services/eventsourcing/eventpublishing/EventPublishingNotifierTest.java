package uk.gov.justice.services.eventsourcing.eventpublishing;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.services.eventsourcing.eventpublishing.configuration.EventPublishingWorkerConfig;

import javax.enterprise.concurrent.ManagedExecutorService;

@ExtendWith(MockitoExtension.class)
class EventPublishingNotifierTest {

    @Mock
    private LinkedEventPublisher linkedEventPublisher;

    @Mock
    private EventPublishingWorkerConfig eventPublishingWorkerConfig;

    @Mock
    private ManagedExecutorService managedExecutorService;

    @InjectMocks
    private EventPublishingNotifier eventPublishingNotifier;

    @Test
    void shouldWakeUpWhenRequested() {
        eventPublishingNotifier.wakeUp(true);

        verify(managedExecutorService).submit(any(Runnable.class));
    }

    @Test
    void shouldNotSubmitIfAlreadyStarted() {
        // First wakeUp starts it
        eventPublishingNotifier.wakeUp(true);
        verify(managedExecutorService).submit(any(Runnable.class));

        // Second wakeUp should not submit again
        eventPublishingNotifier.wakeUp(true);
        verify(managedExecutorService, times(1)).submit(any(Runnable.class));
    }

    @Test
    void shouldProcessEventsWhenRunning() {
        when(eventPublishingWorkerConfig.getBackoffMinMilliseconds()).thenReturn(10L);
        when(eventPublishingWorkerConfig.getBackoffMultiplier()).thenReturn(1.5);
        when(eventPublishingWorkerConfig.getBackoffMaxMilliseconds()).thenReturn(100L);

        when(linkedEventPublisher.publishNextNewEvent())
                .thenReturn(true)
                .thenReturn(false)
                .thenAnswer(invocation -> {
                    Thread.currentThread().interrupt();
                    return false;
                });

        eventPublishingNotifier.wakeUp(true);

        ArgumentCaptor<Runnable> captor = ArgumentCaptor.forClass(Runnable.class);
        verify(managedExecutorService).submit(captor.capture());
        Runnable runnable = captor.getValue();

        runnable.run();

        verify(linkedEventPublisher, times(3)).publishNextNewEvent();
    }
}
